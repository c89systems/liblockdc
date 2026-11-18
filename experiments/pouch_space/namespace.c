#include "frame.h"
#include "lc_api_internal.h"
#include "lc_pouch_namespace.h"
#include "namespace_internal.h"
#include "record_io.h"
#include "root_internal.h"
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define NS_HEAD_BYTES 128U
typedef ps_ns_resource segment;
struct ps_namespace_append {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  ps_record_writer writer;
  ps_budget *budget;
  ps_checkpoint_delta *delta;
  uint64_t segment;
  uint64_t expected;
  int complete;
  const char *leaf;
};
struct ps_namespace_window {
  ps_namespace *ns;
  ps_stream_window *reader;
  ps_reader *value;
  uint64_t start, end;
  unsigned int references;
};
typedef struct captured {
  uint64_t id, start, tail;
  char leaf[PS_NS_LEAF_BYTES];
  ps_stream_window *reader;
} captured;
struct ps_namespace_view {
  ps_namespace *ns;
  size_t count;
  captured *items;
  ps_values *metadata;
  ps_checkpoint_info coverage;
  uint64_t active, tail;
};
static int failure(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL,
                      rc == LC_ERR_PROTOCOL ? "pouch_namespace_corrupt" : NULL,
                      "pouch");
}
static int checkpoint_ready(struct ps_namespace_data *, lc_error *);
static void put64(unsigned char *p, uint64_t value) {
  size_t i;
  for (i = 0; i < 8; ++i) {
    p[i] = (unsigned char)value;
    value >>= 8;
  }
}
static uint64_t get64(const unsigned char *p) {
  uint64_t value = 0;
  size_t i;
  for (i = 0; i < 8; ++i)
    value |= (uint64_t)p[i] << (i * 8);
  return value;
}
static ps_key head_key(ps_namespace *ns) {
  ps_key key = {0, 1, PS_ROUTING};
  key.owner = ns->identity.owner;
  return key;
}
static int head_read(struct ps_namespace_data *d, lc_error *e) {
  unsigned char bytes[NS_HEAD_BYTES];
  ps_reader *reader = NULL;
  size_t got, i;
  int rc = ps_read_open(d->ns->root->space, head_key(d->ns), NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc != LC_OK)
    return rc;
  if (ps_read_size(reader) != sizeof(bytes))
    rc = failure(e, LC_ERR_PROTOCOL, "namespace head has invalid length");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, e);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes) || memcmp(bytes, "LCPSNSHD", 8) ||
      get64(bytes + 8) != d->ns->identity.owner || !get64(bytes + 16) ||
      get64(bytes + 24) != get64(bytes + 16) ||
      get64(bytes + 32) != LC_POUCH_RECORD_HEADER_BYTES ||
      get64(bytes + 120) != (uint64_t)crc32(0L, bytes, 120))
    return failure(e, LC_ERR_PROTOCOL,
                   "namespace head identity or checksum is invalid");
  for (i = 40; i < 120; ++i)
    if (bytes[i])
      return failure(e, LC_ERR_PROTOCOL, "namespace head has unknown fields");
  d->active = get64(bytes + 16);
  return LC_OK;
}
int ps_ns_head_write(struct ps_namespace_data *d, ps_budget *budget,
                     uint64_t active, lc_error *e) {
  unsigned char bytes[NS_HEAD_BYTES];
  ps_writer *writer = NULL;
  int rc;
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, "LCPSNSHD", 8);
  put64(bytes + 8, d->ns->identity.owner);
  put64(bytes + 16, active);
  put64(bytes + 24, active);
  put64(bytes + 32, LC_POUCH_RECORD_HEADER_BYTES);
  put64(bytes + 120, (uint64_t)crc32(0L, bytes, 120));
  rc = ps_begin(d->ns->root->space, budget, head_key(d->ns), &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), e);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, e);
  ps_writer_close(writer);
  return rc;
}
static int collect_resource(void *context, const void *name, size_t length,
                            const ps_catalog_info *binding,
                            unsigned int expected_kind, lc_error *e) {
  struct ps_namespace_data *d = context;
  segment *s;
  uint64_t id;
  unsigned int kind;
  char leaf[PS_NS_LEAF_BYTES];
  int rc;
  if (length >= sizeof(leaf) || memchr(name, 0, length))
    return failure(e, LC_ERR_PROTOCOL, "namespace segment label is invalid");
  memcpy(leaf, name, length);
  leaf[length] = 0;
  if (!ps_namespace_parse_source_leaf(leaf, &kind, &id) ||
      kind != expected_kind || binding->stream.kind != PS_STREAM ||
      binding->stream.owner !=
          (kind == PS_CATALOG_SEGMENT ? d->ns->identity.owner : id) ||
      binding->stream.key !=
          (kind == PS_CATALOG_SEGMENT ? id : d->ns->identity.owner))
    return failure(e, LC_ERR_PROTOCOL, "namespace segment binding is invalid");
  rc = ps_ns_index_room(d, e);
  if (rc != LC_OK)
    return rc;
  s = ps_ns_resource_new(kind, id, e);
  if (!s)
    return LC_ERR_NOMEM;
  d->items[d->count++] = s;
  s->label->next = d->labels;
  d->labels = s->label;
  s->pooled = 1;
  s->binding = *binding;
  return LC_OK;
}
static int collect_segment(void *context, const void *name, size_t length,
                           const ps_catalog_info *binding, lc_error *e) {
  return collect_resource(context, name, length, binding, PS_CATALOG_SEGMENT,
                          e);
}
static int collect_body(void *context, const void *name, size_t length,
                        const ps_catalog_info *binding, lc_error *e) {
  return collect_resource(context, name, length, binding, PS_CATALOG_BODY, e);
}
static int compare_segment(const void *a, const void *b) {
  const segment *left = *(segment *const *)a, *right = *(segment *const *)b;
  uint64_t x = left->id, y = right->id;
  if (left->kind != right->kind)
    return left->kind < right->kind ? -1 : 1;
  return x < y ? -1 : x != y;
}
static int recover(ps_stream *stream, uint64_t checkpoint, uint64_t capacity,
                   uint64_t *accepted, void *context, lc_error *e) {
  segment *s = context;
  lc_allocator allocator = {0};
  lc_pouch_state_entry record = {0};
  unsigned char *scratch = NULL;
  uint64_t cursor = s->covered, inner, next;
  ps_frame frame;
  int rc = LC_OK, found, truncated;
  if (s->kind != PS_CATALOG_SEGMENT)
    return failure(e, LC_ERR_PROTOCOL,
                   "namespace immutable source is not sealed");
  if (s->unselected) {
    if (checkpoint)
      return failure(e, LC_ERR_PROTOCOL,
                     "unselected namespace stream has a published tail");
    /* A containing head never selected these bytes. A concurrent checkpoint
     * can retain its zero-tail grants, but not make its private append live. */
    *accepted = 0;
    return LC_OK;
  }
  if (cursor > capacity)
    return failure(e, LC_ERR_PROTOCOL,
                   "checkpoint coverage exceeds source capacity");
  rc = ps_stream_recovery_cover(stream, cursor, e);
  if (rc != LC_OK)
    return rc;
  while (cursor < capacity) {
    found = 0;
    rc = ps_frame_read(ps_record_recovery_input(stream), cursor, capacity,
                       &scratch, &frame, e);
    inner = frame.start;
    if (rc == LC_OK && frame.framed && cursor < checkpoint &&
        frame.end > checkpoint)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "namespace checkpoint splits a batch frame");
    while (rc == LC_OK) {
      rc = lc_pouch_state_record_read(
          ps_record_recovery_input(stream), &allocator, s->leaf, inner,
          frame.end, !frame.framed, cursor >= checkpoint, &scratch, &record,
          &next, &found, &truncated, e);
      if (rc != LC_OK || !found)
        break;
      inner = next;
      if (!frame.framed || inner == frame.end)
        break;
    }
    if (rc == LC_ERR_PROTOCOL && cursor >= checkpoint) {
      lc_error_cleanup(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK || !found)
      break;
    if (cursor < checkpoint && inner > checkpoint) {
      rc = failure(e, LC_ERR_PROTOCOL,
                   "namespace checkpoint splits a canonical record");
      break;
    }
    cursor = inner;
  }
  if (rc == LC_OK && cursor < checkpoint)
    rc =
        failure(e, LC_ERR_PROTOCOL, "namespace committed prefix is incomplete");
  *accepted = cursor;
  lc_pouch_state_entry_cleanup(&allocator, &record);
  lc_free_with_allocator(NULL, scratch);
  return rc;
}
void ps_namespace_data_destroy(struct ps_namespace_data *d) {
  size_t i;
  segment *r, *next;
  ps_ns_label *label, *next_label;
  if (!d)
    return;
  ps_key_checkpoint_close(d->checkpoint);
  for (i = 0; i < d->count; ++i)
    ps_ns_resource_free(d->items[i]);
  for (r = d->retired; r; r = next) {
    next = r->next;
    ps_ns_resource_free(r);
  }
  for (label = d->labels; label; label = next_label) {
    next_label = label->next;
    lc_free_with_allocator(NULL, label);
  }
  lc_free_with_allocator(NULL, d->items);
  pthread_mutex_destroy(&d->mutex);
  lc_free_with_allocator(NULL, d);
}
static int load(ps_namespace *ns, struct ps_namespace_data **out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_stream_info info;
  ps_checkpoint_info coverage = {0};
  size_t i;
  uint64_t seen = 0;
  int rc;
  d = lc_calloc_with_allocator(NULL, 1, sizeof(*d));
  if (!d)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace storage owner");
  d->ns = ns;
  if (pthread_mutex_init(&d->mutex, NULL)) {
    lc_free_with_allocator(NULL, d);
    return failure(e, LC_ERR_TRANSPORT, "cannot initialize namespace mutex");
  }
  rc = head_read(d, e);
  if (rc == LC_OK)
    rc = ps_key_checkpoint_open(ns, &d->checkpoint, e);
  if (rc == LC_OK)
    ps_key_checkpoint_info(d->checkpoint, &coverage);
  if (rc == LC_OK && coverage.segment > d->active)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "checkpoint coverage exceeds namespace head");
  if (rc == LC_OK)
    rc = ps_catalog_visit(ns->root->space, ns->identity.owner,
                          PS_CATALOG_SEGMENT, collect_segment, d, e);
  if (rc == LC_OK)
    rc = ps_catalog_visit(ns->root->space, ns->identity.owner, PS_CATALOG_BODY,
                          collect_body, d, e);
  if (rc == LC_OK && d->count)
    qsort(d->items, d->count, sizeof(*d->items), compare_segment);
  for (i = 0; rc == LC_OK && i < d->count; ++i) {
    segment *s = d->items[i];
    if (i && !compare_segment(d->items + i - 1, d->items + i)) {
      rc = failure(e, LC_ERR_PROTOCOL,
                   "namespace source directory repeats an ID");
      break;
    }
    if (s->kind == PS_CATALOG_SEGMENT) {
      if (s->id > d->active &&
          (d->active == UINT64_MAX || s->id != d->active + 1)) {
        rc = failure(e, LC_ERR_PROTOCOL,
                     "namespace has an unexpected successor");
        break;
      }
      if (s->id > coverage.segment && s->id <= d->active) {
        ++seen;
        if (s->id != coverage.segment + seen) {
          rc = failure(e, LC_ERR_PROTOCOL,
                       "namespace segment directory has a gap");
          break;
        }
      }
    }
    s->unselected = s->kind == PS_CATALOG_SEGMENT && s->id > d->active;
    s->covered = s->kind == PS_CATALOG_SEGMENT && s->id == coverage.segment
                     ? coverage.offset
                     : 0;
    rc = ps_stream_open(ns->root->space, NULL, s->binding.stream, 0,
                        &s->binding.target, recover, s, &s->stream, e);
    if (rc == LC_OK)
      rc = ps_stream_status(s->stream, &info, e);
    if (s->kind != PS_CATALOG_SEGMENT) {
      if (rc == LC_OK && !info.sealed)
        rc = failure(e, LC_ERR_PROTOCOL, "namespace snapshot is not sealed");
      continue;
    }
    if (rc == LC_OK && s->id > d->active && (info.tail || info.sealed))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "unpublished namespace segment is not empty");
    if (rc == LC_OK && s->id == d->active && info.sealed)
      rc = failure(e, LC_ERR_PROTOCOL, "namespace active segment is sealed");
    if (rc == LC_OK && s->id == d->active)
      d->tail = info.tail;
    if (rc == LC_OK && s->id == coverage.segment && coverage.offset > info.tail)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "checkpoint coverage exceeds canonical tail");
    if (rc == LC_OK && s->id < d->active && !info.sealed) {
      if (s->id + 1 != d->active)
        rc = failure(e, LC_ERR_PROTOCOL,
                     "namespace has an unexpected unsealed predecessor");
      else
        d->pending_seal = s->id;
    }
  }
  if (rc == LC_OK &&
      (seen != d->active - coverage.segment ||
       (d->active && !ps_ns_find(d, PS_CATALOG_SEGMENT, d->active))))
    rc = failure(e, LC_ERR_PROTOCOL, "namespace head has missing segments");
  if (rc != LC_OK)
    ps_namespace_data_destroy(d);
  else
    *out = d;
  return rc;
}
int ps_namespace_sources_visit(ps_namespace *ns, ps_namespace_source_fn visit,
                               void *context, lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_source *sources = NULL;
  ps_stream_info info;
  size_t i, count = 0;
  int rc;
  if (!visit)
    return failure(e, LC_ERR_INVALID, "namespace sources need a visitor");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  ps_namespace_retain(ns);
  pthread_mutex_lock(&d->mutex);
  if (d->count > (size_t)-1 / sizeof(*sources))
    rc = failure(e, LC_ERR_NOMEM,
                 "namespace source capture exceeds local limit");
  if (rc == LC_OK && d->count) {
    sources = lc_calloc_with_allocator(NULL, d->count, sizeof(*sources));
    if (!sources)
      rc = failure(e, LC_ERR_NOMEM, "cannot capture namespace sources");
  }
  for (i = 0; rc == LC_OK && i < d->count; ++i) {
    segment *s = d->items[i];
    if (s->unselected)
      continue;
    rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK) {
      sources[count].leaf = s->leaf;
      sources[count].kind = s->kind;
      sources[count].id = s->id;
      sources[count].tail = info.tail;
      sources[count].capacity = info.capacity;
      sources[count].sealed = info.sealed;
      ++count;
    }
  }
  pthread_mutex_unlock(&d->mutex);
  for (i = 0; rc == LC_OK && i < count; ++i)
    rc = visit(context, &sources[i], e);
  lc_free_with_allocator(NULL, sources);
  ps_namespace_release(ns);
  return rc;
}

int ps_ns_data_get(ps_namespace *ns, struct ps_namespace_data **out,
                   lc_error *e) {
  struct ps_namespace_data *fresh = NULL;
  ps_root *root;
  int rc;
  if (out)
    *out = NULL;
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID,
                   "namespace storage requires owner and output");
  root = ns->root;
  rc = ps_check(root->space, e);
  if (rc != LC_OK)
    return rc;
  fresh = __atomic_load_n(&ns->data, __ATOMIC_ACQUIRE);
  if (fresh) {
    *out = fresh;
    return LC_OK;
  }
  pthread_mutex_lock(&root->mutex);
  while (ns->data_loading)
    pthread_cond_wait(&root->changed, &root->mutex);
  if (!ns->data) {
    ns->data_loading = 1;
    pthread_mutex_unlock(&root->mutex);
    rc = load(ns, &fresh, e);
    pthread_mutex_lock(&root->mutex);
    if (rc == LC_OK)
      __atomic_store_n(&ns->data, fresh, __ATOMIC_RELEASE);
    ns->data_loading = 0;
    pthread_cond_broadcast(&root->changed);
  }
  if (rc == LC_OK)
    *out = ns->data;
  pthread_mutex_unlock(&root->mutex);
  return rc;
}
int ps_ns_enter(struct ps_namespace_data *d, lc_error *e) {
  int rc = LC_OK;
  pthread_mutex_lock(&d->mutex);
  if (d->busy)
    rc = lc_error_set(e, LC_ERR_INVALID, 0L,
                      "namespace already has an active producer or rotation",
                      NULL, "pouch_namespace_busy", "pouch");
  else
    d->busy = 1;
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
void ps_ns_leave(struct ps_namespace_data *d) {
  pthread_mutex_lock(&d->mutex);
  d->busy = 0;
  pthread_mutex_unlock(&d->mutex);
}
int ps_ns_finish_seal(struct ps_namespace_data *d, lc_error *e) {
  int rc = LC_OK;
  if (d->pending_seal) {
    /* A durable seal must never outlive the routing decision that made its
     * predecessor inactive. NoSync publication can defer this dependency. */
    rc = ps_flush(d->ns->root->space, e);
    if (rc == LC_OK)
      rc = ps_stream_seal(
          ps_ns_find(d, PS_CATALOG_SEGMENT, d->pending_seal)->stream, e);
    if (rc == LC_OK)
      d->pending_seal = 0;
  }
  return rc;
}
static int prepare(struct ps_namespace_data *d, ps_budget *budget, uint64_t id,
                   lc_error *e) {
  segment *s;
  ps_catalog_view *view = NULL;
  ps_stream_info info;
  int rc;
  if (ps_ns_find(d, PS_CATALOG_SEGMENT, id))
    return LC_OK;
  pthread_mutex_lock(&d->mutex);
  rc = ps_ns_index_room(d, e);
  pthread_mutex_unlock(&d->mutex);
  if (rc != LC_OK)
    return rc;
  s = ps_ns_resource_new(PS_CATALOG_SEGMENT, id, e);
  if (!s)
    return LC_ERR_NOMEM;
  s->unselected = 1;
  rc = ps_catalog_get(d->ns->root->space, d->ns->identity.owner,
                      PS_CATALOG_SEGMENT, s->leaf, strlen(s->leaf), &view, e);
  if (rc == LC_OK && view)
    rc = ps_catalog_view_info(view, &s->binding, e);
  if (rc == LC_OK && view &&
      (s->binding.stream.kind != PS_STREAM ||
       s->binding.stream.owner != d->ns->identity.owner ||
       s->binding.stream.key != id))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "staged namespace segment binding is invalid");
  if (rc == LC_OK && !view) {
    s->binding.stream.owner = d->ns->identity.owner;
    s->binding.stream.key = id;
    s->binding.stream.kind = PS_STREAM;
  }
  if (rc == LC_OK) {
    rc = ps_stream_open(d->ns->root->space, NULL, s->binding.stream, 0,
                        view ? &s->binding.target : NULL, recover, s,
                        &s->stream, e);
    if (rc == LC_ERR_INVALID && !view) {
      lc_error_cleanup(e);
      rc = ps_stream_open(d->ns->root->space, budget, s->binding.stream, 1,
                          NULL, recover, s, &s->stream, e);
    }
  }
  if (rc == LC_OK)
    rc = ps_stream_status(s->stream, &info, e);
  if (rc == LC_OK && (info.tail || info.sealed))
    rc = failure(e, LC_ERR_PROTOCOL, "staged namespace segment is not empty");
  if (rc == LC_OK && !view)
    rc = ps_catalog_put(d->ns->root->space, budget, d->ns->identity.owner,
                        PS_CATALOG_SEGMENT, s->leaf, strlen(s->leaf), NULL,
                        s->binding.stream, &info.ref, &s->binding, e);
  ps_catalog_view_close(view);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    ps_ns_insert(d, s);
    pthread_mutex_unlock(&d->mutex);
  } else {
    ps_ns_resource_free(s);
  }
  return rc;
}
static int begin_reserve(ps_stream *stream, ps_budget *budget, uint64_t minimum,
                         int exact, ps_record_writer *writer, lc_error *e) {
  int rc = ps_record_begin(stream, writer, e);
  if (rc == LC_OK)
    rc = exact ? ps_stream_reserve_exact(writer->writer, budget,
                                         minimum ? minimum : 1, e)
               : ps_record_reserve(writer, budget, minimum ? minimum : 1, e);
  return rc;
}
static int plan_append(struct ps_namespace_data *d, uint64_t selected,
                       uint64_t minimum, enum ps_namespace_append_mode mode,
                       ps_budget **budget, int *exact, lc_error *e) {
  ps_budget_plan plan = {0}, exact_plan;
  ps_stream_info info = {0};
  segment *s = ps_ns_find(d, PS_CATALOG_SEGMENT, selected);
  uint64_t start = selected == d->active ? d->tail : 0;
  uint64_t bytes = minimum ? minimum : 1, need, preferred = PS_PAGE;
  int rc = LC_OK;
  *budget = NULL;
  *exact = mode == PS_APPEND_MAINTENANCE;
  if (s) {
    rc = ps_stream_status(s->stream, &info, e);
    preferred = info.preferred_grant_bytes;
    if (rc == LC_OK && (info.tail != start || info.sealed))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "namespace append stream disagrees with head");
  } else if (selected == d->active)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "namespace active append stream is missing");
  if (rc != LC_OK)
    return rc;
  if (start > (uint64_t)INT64_MAX || bytes > (uint64_t)INT64_MAX - start ||
      info.capacity < start)
    return failure(e, LC_ERR_INVALID, "namespace append exceeds logical range");
  need = bytes > info.capacity - start ? bytes - (info.capacity - start) : 0;
  plan.maintenance = *exact;
  if (selected != d->active)
    rc = ps_budget_plan_routing(&plan, !d->active, e);
  if (rc == LC_OK && !s)
    rc = ps_budget_plan_stream(&plan, e);
  if (rc == LC_OK && !s)
    rc = ps_budget_plan_catalog(&plan, 28, e);
  if (rc != LC_OK)
    return rc;
  exact_plan = plan;
  rc = ps_budget_plan_span(&exact_plan, need, e);
  if (rc != LC_OK)
    return rc;
  if (*exact)
    plan = exact_plan;
  else
    rc = ps_budget_plan_preferred_span(&plan, need, preferred, e);
  if (rc != LC_OK ||
      (!plan.payload_bytes && !plan.metadata_bytes && !plan.entries))
    return rc;
  rc = ps_budget_open(d->ns->root->space, &plan, budget, e);
  if (rc == LC_ERR_INVALID && !*exact &&
      plan.payload_bytes > exact_plan.payload_bytes) {
    lc_error_cleanup(e);
    *exact = 1;
    rc = ps_budget_open(d->ns->root->space, &exact_plan, budget, e);
  }
  return rc;
}
int ps_ns_rotation_plan(struct ps_namespace_data *d, ps_budget_plan *out,
                        lc_error *e) {
  ps_budget_plan plan = *out;
  uint64_t next;
  int rc;
  if (d->active == UINT64_MAX)
    return failure(e, LC_ERR_INVALID, "namespace segment IDs exhausted");
  next = d->active + 1;
  /* The routing record may need a fresh packed page. A new successor needs
   * one stream head and its fixed 28-byte canonical segment label. Existing
   * staged successors need neither. Seal and journal rollover are covered by
   * the checkpoint pool, independently of this payload claim. */
  rc = ps_budget_plan_routing(&plan, !d->active, e);
  if (rc == LC_OK && !ps_ns_find(d, PS_CATALOG_SEGMENT, next)) {
    rc = ps_budget_plan_stream(&plan, e);
    if (rc == LC_OK)
      rc = ps_budget_plan_catalog(&plan, 28, e);
  }
  if (rc == LC_OK)
    *out = plan;
  return rc;
}
int ps_ns_rotate(struct ps_namespace_data *d, ps_budget *budget, lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *owned = NULL;
  uint64_t next;
  int rc, closed;
  plan.maintenance = 1;
  rc = ps_ns_rotation_plan(d, &plan, e);
  if (rc == LC_OK && !budget) {
    rc = ps_budget_open(d->ns->root->space, &plan, &owned, e);
    budget = owned;
  }
  next = d->active + 1;
  if (rc == LC_OK)
    rc = prepare(d, budget, next, e);
  if (rc == LC_OK)
    rc = ps_ns_head_write(d, budget, next, e);
  if (rc == LC_OK)
    rc = ps_flush(d->ns->root->space, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    d->pending_seal = d->active;
    d->active = next;
    d->tail = 0;
    ps_ns_find(d, PS_CATALOG_SEGMENT, next)->unselected = 0;
    pthread_mutex_unlock(&d->mutex);
    rc = ps_ns_finish_seal(d, e);
  }
  closed = ps_budget_close(owned, rc == LC_OK ? e : NULL);
  if (rc == LC_OK)
    rc = closed;
  return rc;
}
int ps_namespace_status_get(ps_namespace *ns, ps_namespace_status *out,
                            lc_error *e) {
  struct ps_namespace_data *d;
  ps_stream_info info;
  ps_checkpoint_info coverage;
  size_t i;
  int rc;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace status requires output");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  memset(out, 0, sizeof(*out));
  ps_key_checkpoint_info(d->checkpoint, &coverage);
  out->active_segment = d->active;
  out->segments = d->active - coverage.segment + (coverage.segment != 0);
  out->checkpoint_epoch = coverage.epoch;
  out->covered_segment = coverage.segment;
  out->covered_offset = coverage.offset;
  out->checkpoint_keys = coverage.slots;
  out->pending_keys = coverage.pending;
  out->pending_metadata_bytes = coverage.pending_bytes;
  out->tail = out->runs = 0;
  for (i = 0; rc == LC_OK && i < d->count; ++i) {
    segment *s = d->items[i];
    if (s->kind == PS_CATALOG_SEGMENT)
      ++out->bound_segments;
    else
      ++out->bound_bodies;
    if (s->unselected)
      continue;
    rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK) {
      out->runs += info.runs;
      if (s->kind == PS_CATALOG_SEGMENT && s->id == d->active)
        out->tail = info.tail;
    }
  }
  {
    segment *s;
    for (s = d->retired; s; s = s->next)
      ++out->pending_retirements;
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
int ps_namespace_append_begin(ps_namespace *ns, uint64_t target,
                              uint64_t minimum,
                              enum ps_namespace_append_mode mode,
                              ps_namespace_append **out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_append *append;
  uint64_t selected;
  uint64_t span;
  int rc, exact = 0;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace append requires output");
  if (mode < PS_APPEND_PREFIX || mode > PS_APPEND_MAINTENANCE ||
      (mode != PS_APPEND_PREFIX && !minimum))
    return failure(
        e, LC_ERR_INVALID,
        "namespace complete append requires a positive span and valid mode");
  if (minimum > (uint64_t)INT64_MAX ||
      (mode != PS_APPEND_PREFIX &&
       minimum > (uint64_t)INT64_MAX - PS_FRAME_BYTES))
    return failure(e, LC_ERR_INVALID,
                   "namespace append minimum exceeds local offset limit");
  span = mode == PS_APPEND_PREFIX ? minimum : minimum + PS_FRAME_BYTES;
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  append = lc_calloc_with_allocator(NULL, 1, sizeof(*append));
  if (!append)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace append");
  rc = ps_ns_enter(d, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, append);
    return rc;
  }
  rc = checkpoint_ready(d, e);
  if (rc == LC_OK)
    rc = ps_ns_finish_seal(d, e);
  if (rc == LC_OK && ps_key_checkpoint_due(d->checkpoint)) {
    pthread_mutex_lock(&d->mutex);
    rc = ps_key_checkpoint_flush(d->checkpoint, e);
    pthread_mutex_unlock(&d->mutex);
  }
  selected = d->active;
  if (rc == LC_OK) {
    if (!selected || (target && d->tail >= target)) {
      if (selected == UINT64_MAX)
        rc = failure(e, LC_ERR_INVALID, "namespace segment IDs exhausted");
      else {
        ++selected;
      }
    }
  }
  if (rc == LC_OK)
    rc = plan_append(d, selected, span, mode, &append->budget, &exact, e);
  if (rc == LC_OK && selected != d->active)
    rc = prepare(d, append->budget, selected, e);
  if (rc == LC_OK) {
    segment *s = ps_ns_find(d, PS_CATALOG_SEGMENT, selected);
    rc = begin_reserve(s->stream, append->budget, span, exact, &append->writer,
                       e);
    append->segment = s->id;
    append->leaf = s->leaf;
    append->complete = mode != PS_APPEND_PREFIX;
    append->expected = span;
    if (rc == LC_OK && append->complete)
      append->writer.limit = append->writer.start + span;
    if (rc == LC_OK && append->complete)
      rc = ps_record_frame_begin(&append->writer, minimum, e);
    if (rc == LC_OK)
      rc = ps_checkpoint_delta_begin(d->checkpoint, &append->writer, s->id,
                                     s->leaf, &append->delta, e);
  }
  if (rc != LC_OK) {
    ps_checkpoint_delta_close(append->delta);
    ps_record_abort(&append->writer);
    ps_budget_close(append->budget, NULL);
    ps_ns_leave(d);
    lc_free_with_allocator(NULL, append);
    return rc;
  }
  append->ns = ns;
  append->data = d;
  ps_namespace_retain(ns);
  *out = append;
  return LC_OK;
}
lc_pouch_record_io ps_namespace_append_output(ps_namespace_append *append) {
  return ps_record_output(append ? &append->writer : NULL);
}
const char *ps_namespace_append_leaf(const ps_namespace_append *append) {
  return append ? append->leaf : NULL;
}
uint64_t ps_namespace_append_segment(const ps_namespace_append *append) {
  return append ? append->segment : 0;
}
uint64_t ps_namespace_append_segments(const ps_namespace_append *append) {
  ps_checkpoint_info coverage;
  if (!append)
    return 0;
  ps_key_checkpoint_info(append->data->checkpoint, &coverage);
  return append->segment - coverage.segment + (coverage.segment != 0);
}
static int publish_append_head(void *context, uint64_t tail, lc_error *e) {
  ps_namespace_append *append = context;
  (void)tail;
  return ps_ns_head_write(append->data, append->budget, append->segment, e);
}
int ps_namespace_append_commit(ps_namespace_append *append,
                               enum ps_commit_mode mode, lc_error *e) {
  uint64_t tail;
  int rc, selecting;
  if (!append)
    return failure(e, LC_ERR_INVALID, "namespace append is not open");
  if (mode != PS_COMMIT_DEFAULT && mode != PS_COMMIT_DEFER_SYNC)
    return failure(e, LC_ERR_INVALID, "invalid canonical commit mode");
  tail = append->writer.tail;
  if (append->complete && tail - append->writer.start != append->expected)
    return failure(e, LC_ERR_INVALID,
                   "canonical append differs from its declared complete span");
  selecting = append->segment != append->data->active;
  rc = ps_checkpoint_delta_prepare(append->delta, e);
  if (rc == LC_OK)
    rc = ps_record_commit(&append->writer, mode,
                          selecting ? publish_append_head : NULL, append, e);
  /* Mutation authority remains held until append_close. Only that authority
   * reads this cache; captured readers use their independently pinned tails. */
  if (rc == LC_OK) {
    pthread_mutex_lock(&append->data->mutex);
    ps_checkpoint_delta_accept(append->delta);
    if (selecting) {
      append->data->pending_seal = append->data->active;
      append->data->active = append->segment;
      ps_ns_find(append->data, PS_CATALOG_SEGMENT, append->segment)
          ->unselected = 0;
    }
    append->data->tail = tail;
    pthread_mutex_unlock(&append->data->mutex);
    /* Seal cleanup stabilizes routing before the next mutation, never as
     * fallible work after this successful logical publication. */
  }
  return rc;
}
void ps_namespace_append_close(ps_namespace_append *append) {
  if (!append)
    return;
  ps_checkpoint_delta_close(append->delta);
  ps_record_abort(&append->writer);
  ps_budget_close(append->budget, NULL);
  ps_ns_leave(append->data);
  ps_namespace_release(append->ns);
  lc_free_with_allocator(NULL, append);
}
int ps_namespace_rotate(ps_namespace *ns, uint64_t expected, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  if (d->active == expected) {
    rc = ps_ns_finish_seal(d, e);
    if (rc == LC_OK)
      rc = ps_ns_rotate(d, NULL, e);
  } else if (expected != UINT64_MAX && d->active == expected + 1) {
    rc = ps_ns_finish_seal(d, e);
  } else {
    rc = failure(e, LC_ERR_INVALID,
                 "namespace rotation expected segment is stale");
  }
  ps_ns_leave(d);
  return rc;
}
int ps_namespace_read_open(ps_namespace *ns, unsigned int kind, uint64_t id,
                           uint64_t offset, uint64_t bytes,
                           ps_namespace_window **out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_window *window;
  segment *s;
  int rc;
  if (out)
    *out = NULL;
  if (!out || !id ||
      (kind != PS_CATALOG_SEGMENT && kind != PS_CATALOG_SNAPSHOT &&
       kind != PS_CATALOG_BODY))
    return failure(e, LC_ERR_INVALID,
                   "namespace read requires segment and output");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  window = lc_calloc_with_allocator(NULL, 1, sizeof(*window));
  if (!window)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace read window");
  pthread_mutex_lock(&d->mutex);
  if (kind == PS_CATALOG_SNAPSHOT) {
    rc = ps_key_checkpoint_read(d->checkpoint, id, &window->value, e);
    if (rc == LC_OK && (offset > ps_read_size(window->value) ||
                        bytes > ps_read_size(window->value) - offset))
      rc = failure(e, LC_ERR_INVALID, "checkpoint read exceeds metadata value");
  } else {
    s = ps_ns_find(d, kind, id);
    if (!s || s->unselected)
      rc = failure(e, LC_ERR_INVALID, "namespace segment does not exist");
    else
      rc = ps_stream_window_open(s->stream, offset, bytes, &window->reader, e);
  }
  pthread_mutex_unlock(&d->mutex);
  if (rc != LC_OK) {
    ps_reader_close(window->value);
    lc_free_with_allocator(NULL, window);
    return rc;
  }
  window->ns = ns;
  window->start = offset;
  window->end = offset + bytes;
  window->references = 1;
  ps_namespace_retain(ns);
  *out = window;
  return LC_OK;
}
static int window_read(void *context, uint64_t offset,
                       const struct iovec *parts, int count, lc_error *e) {
  ps_namespace_window *window = context;
  lc_pouch_record_io io;
  uint64_t end = offset;
  int i;
  if (!window || count < 0 || (count && !parts) || offset < window->start ||
      offset > window->end)
    return failure(e, LC_ERR_INVALID, "namespace read exceeds captured window");
  for (i = 0; i < count; ++i) {
    if (parts[i].iov_len > window->end - end)
      return failure(e, LC_ERR_INVALID,
                     "namespace read exceeds captured window");
    end += parts[i].iov_len;
  }
  io = window->value ? ps_record_value_input(window->value)
                     : ps_record_window_input(window->reader);
  return lc_pouch_record_io_read(io, offset, parts, count, e);
}
lc_pouch_record_io ps_namespace_read_input(ps_namespace_window *window) {
  static const lc_pouch_record_io_ops ops = {window_read, NULL, NULL, NULL,
                                             NULL};
  lc_pouch_record_io io;
  io.ops = &ops;
  io.context = window;
  return io;
}
void ps_namespace_read_retain(ps_namespace_window *window) {
  __atomic_add_fetch(&window->references, 1, __ATOMIC_RELAXED);
}
void ps_namespace_read_close(ps_namespace_window *window) {
  if (!window || __atomic_sub_fetch(&window->references, 1, __ATOMIC_ACQ_REL))
    return;
  ps_stream_window_close(window->reader);
  ps_reader_close(window->value);
  ps_namespace_release(window->ns);
  lc_free_with_allocator(NULL, window);
}
int ps_namespace_view_open(ps_namespace *ns, ps_namespace_view **out,
                           lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_view *view;
  ps_stream_info info;
  size_t i;
  uint64_t first, count;
  int rc;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace view requires output");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  view = lc_calloc_with_allocator(NULL, 1, sizeof(*view));
  if (!view)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace view");
  view->ns = ns;
  ps_namespace_retain(ns);
  pthread_mutex_lock(&d->mutex);
  ++d->views;
  ps_key_checkpoint_info(d->checkpoint, &view->coverage);
  rc = ps_values_capture(ns->root->space, ns->identity.owner, PS_KEY_METADATA,
                         PS_KEY_DECISION, &view->metadata, e);
  view->active = d->active;
  view->tail = d->tail;
  first = view->coverage.segment;
  if (!first || !ps_ns_find(d, PS_CATALOG_SEGMENT, first))
    ++first;
  count = d->active >= first ? d->active - first + 1 : 0;
  if (rc == LC_OK && count > (uint64_t)((size_t)-1 / sizeof(*view->items)))
    rc = failure(e, LC_ERR_NOMEM, "namespace capture exceeds local bounds");
  if (rc == LC_OK)
    view->count = (size_t)count;
  if (rc == LC_OK)
    view->items = lc_calloc_with_allocator(NULL, view->count ? view->count : 1,
                                           sizeof(*view->items));
  if (rc == LC_OK && !view->items)
    rc = failure(e, LC_ERR_NOMEM, "cannot capture namespace segments");
  for (i = 0; rc == LC_OK && i < view->count; ++i) {
    captured *c = &view->items[i];
    segment *s;
    s = ps_ns_find(d, PS_CATALOG_SEGMENT, first + i);
    if (!s) {
      rc = failure(e, LC_ERR_PROTOCOL, "namespace capture has a canonical gap");
      break;
    }
    c->id = s->id;
    c->start = s->id == view->coverage.segment ? view->coverage.offset : 0;
    strcpy(c->leaf, s->leaf);
    rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK) {
      c->tail = info.tail;
      if (c->start > c->tail)
        rc = failure(e, LC_ERR_PROTOCOL,
                     "checkpoint coverage exceeds captured tail");
      else
        rc = ps_stream_window_open(s->stream, c->start, c->tail - c->start,
                                   &c->reader, e);
    }
  }
  pthread_mutex_unlock(&d->mutex);
  if (rc != LC_OK)
    ps_namespace_view_close(view);
  else
    *out = view;
  return rc;
}
typedef struct replay_input {
  ps_stream_window *window;
  unsigned char *buffer;
  uint64_t end, offset;
  size_t used;
} replay_input;
static int replay_read(void *context, uint64_t offset,
                       const struct iovec *parts, int count, lc_error *e) {
  replay_input *input = context;
  int i, rc;
  for (i = 0; i < count; ++i) {
    unsigned char *p = parts[i].iov_base;
    size_t left = parts[i].iov_len;
    while (left) {
      size_t take;
      if (offset >= input->end)
        return failure(e, LC_ERR_PROTOCOL,
                       "namespace replay exceeds its captured prefix");
      if (!input->used || offset < input->offset ||
          offset >= input->offset + input->used) {
        if (!input->buffer)
          input->buffer = lc_alloc_with_allocator(NULL, PS_CHUNK);
        if (!input->buffer)
          return failure(e, LC_ERR_NOMEM,
                         "cannot allocate namespace replay buffer");
        take = input->end - offset < PS_CHUNK ? (size_t)(input->end - offset)
                                              : PS_CHUNK;
        rc = ps_stream_window_read(input->window, offset, input->buffer, take,
                                   e);
        if (rc != LC_OK)
          return rc;
        input->offset = offset;
        input->used = take;
      }
      take = (size_t)(input->offset + input->used - offset);
      if (take > left)
        take = left;
      memcpy(p, input->buffer + (size_t)(offset - input->offset), take);
      p += take;
      offset += take;
      left -= take;
    }
  }
  return LC_OK;
}
static int view_visit(ps_namespace_view *view, int verify, int selected,
                      ps_namespace_record_fn visit, void *context,
                      lc_error *e) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry record = {0};
  unsigned char *scratch = NULL;
  uint64_t cursor, inner, next;
  ps_frame frame;
  size_t i;
  int rc = LC_OK, found, truncated;
  static const lc_pouch_record_io_ops ops = {replay_read, NULL, NULL, NULL,
                                             NULL};
  replay_input input;
  lc_pouch_record_io io;
  ps_reader *metadata_reader = NULL;
  ps_key metadata_key;
  ps_ref metadata_ref;
  uint64_t metadata_bytes;
  char *leaf = NULL;
  if (!view || !visit)
    return failure(e, LC_ERR_INVALID,
                   "namespace replay requires view and callback");
  memset(&input, 0, sizeof(input));
  io.ops = &ops;
  io.context = &input;
  if (selected && view->coverage.epoch) {
    record.key = LC_POUCH_STATE_HIGH_WATER_KEY;
    record.control = record.seen = record.has_query_index_seq = 1;
    record.record_type = LC_POUCH_STATE_RECORD_HIGH_WATER;
    record.index_seq = record.version = view->coverage.state_index;
    record.query_index_seq = view->coverage.query_index;
    rc = visit(context, 0, &record, e);
    memset(&record, 0, sizeof(record));
  }
  for (i = 0; selected && rc == LC_OK && i < ps_values_count(view->metadata);
       ++i) {
    rc = ps_values_info(view->metadata, i, &metadata_key, &metadata_ref,
                        &metadata_bytes, e);
    if (rc == LC_OK) {
      leaf = lc_pouch_namespace_snapshot_leaf(&allocator, metadata_key.key);
      if (!leaf)
        rc = failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint slot label");
    }
    if (rc == LC_OK)
      rc = ps_values_read_open(view->metadata, i, &metadata_reader, e);
    if (rc == LC_OK)
      rc = ps_checkpoint_record_read(metadata_reader, &allocator, leaf, &record,
                                     e);
    if (rc == LC_OK)
      rc = visit(context, 0, &record, e);
    ps_reader_close(metadata_reader);
    metadata_reader = NULL;
    lc_free_with_allocator(&allocator, leaf);
    leaf = NULL;
    lc_pouch_state_entry_cleanup(&allocator, &record);
  }
  for (i = 0; rc == LC_OK && i < view->count; ++i) {
    captured *c = &view->items[i];
    input.window = c->reader;
    input.end = c->tail;
    input.used = 0;
    cursor = c->start;
    while (rc == LC_OK && cursor < c->tail) {
      rc = ps_frame_read(io, cursor, c->tail, &scratch, &frame, e);
      inner = frame.start;
      while (rc == LC_OK) {
        rc = lc_pouch_state_record_read(io, &allocator, c->leaf, inner,
                                        frame.end, 0, verify, &scratch, &record,
                                        &next, &found, &truncated, e);
        if (rc == LC_OK && !found)
          rc = failure(e, LC_ERR_PROTOCOL,
                       "namespace canonical prefix is incomplete");
        if (rc == LC_OK)
          rc = visit(context, c->id, &record, e);
        if (rc != LC_OK)
          break;
        inner = next;
        if (!frame.framed || inner == frame.end)
          break;
      }
      if (rc == LC_OK)
        cursor = inner;
    }
  }
  lc_pouch_state_entry_cleanup(&allocator, &record);
  lc_free_with_allocator(NULL, scratch);
  lc_free_with_allocator(NULL, input.buffer);
  return rc;
}
int ps_namespace_view_visit(ps_namespace_view *view, int verify,
                            ps_namespace_record_fn visit, void *context,
                            lc_error *e) {
  return view_visit(view, verify, 1, visit, context, e);
}
void ps_namespace_view_close(ps_namespace_view *view) {
  size_t i;
  if (!view)
    return;
  if (view->items)
    for (i = 0; i < view->count; ++i)
      ps_stream_window_close(view->items[i].reader);
  lc_free_with_allocator(NULL, view->items);
  ps_values_close(view->metadata);
  pthread_mutex_lock(&view->ns->data->mutex);
  --view->ns->data->views;
  pthread_mutex_unlock(&view->ns->data->mutex);
  ps_namespace_release(view->ns);
  lc_free_with_allocator(NULL, view);
}

static int checkpoint_replay(void *context, uint64_t segment,
                             const lc_pouch_state_entry *entry, lc_error *e) {
  if (!segment)
    return LC_OK; /* Selected slots were loaded by the checkpoint owner. */
  return ps_key_checkpoint_replay(context, entry, e);
}
static int checkpoint_ready(struct ps_namespace_data *d, lc_error *e) {
  ps_key_checkpoint *fresh = NULL, *old;
  ps_namespace_view *view = NULL;
  size_t i;
  int rc;
  if (d->checkpoint_ready)
    return LC_OK;
  rc = ps_key_checkpoint_open(d->ns, &fresh, e);
  if (rc == LC_OK)
    rc = ps_namespace_view_open(d->ns, &view, e);
  if (rc == LC_OK)
    /* The fresh owner already loaded selected slots and their high waters.
     * Replay only the uncovered suffix; decoding those slots again merely to
     * discard them adds needless cold-open I/O and transient allocations. */
    rc = view_visit(view, 0, 0, checkpoint_replay, fresh, e);
  if (rc == LC_OK) {
    for (i = 0; i < view->count; ++i)
      ps_key_checkpoint_replayed(fresh, view->items[i].id, view->items[i].tail);
  }
  ps_namespace_view_close(view);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    if (d->checkpoint_observer.prepare)
      (void)ps_key_checkpoint_observe(fresh, &d->checkpoint_observer, NULL);
    old = d->checkpoint;
    d->checkpoint = fresh;
    d->checkpoint_ready = 1;
    pthread_mutex_unlock(&d->mutex);
    ps_key_checkpoint_close(old);
  } else
    ps_key_checkpoint_close(fresh);
  return rc;
}
int ps_namespace_checkpoint_observe(ps_namespace *ns,
                                    const ps_checkpoint_observer *observer,
                                    lc_error *e) {
  struct ps_namespace_data *d;
  int rc;
  if (!observer || !observer->prepare || !observer->finish)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint observation needs complete callbacks");
  rc = ps_ns_data_get(ns, &d, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    d->checkpoint_observer = *observer;
    rc = ps_key_checkpoint_observe(d->checkpoint, observer, e);
    pthread_mutex_unlock(&d->mutex);
  }
  return rc;
}
int ps_namespace_checkpoint_recover(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = ps_ns_data_get(ns, &d, e);
  if (rc == LC_OK)
    rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  rc = checkpoint_ready(d, e);
  ps_ns_leave(d);
  return rc;
}
int ps_namespace_checkpoint_flush(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = ps_ns_data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_ns_enter(d, e);
  if (rc == LC_OK) {
    rc = checkpoint_ready(d, e);
    if (rc == LC_OK)
      rc = ps_ns_finish_seal(d, e);
    if (rc == LC_OK) {
      pthread_mutex_lock(&d->mutex);
      rc = ps_key_checkpoint_flush(d->checkpoint, e);
      pthread_mutex_unlock(&d->mutex);
    }
    ps_ns_leave(d);
  }
  return rc;
}
uint64_t ps_namespace_source_refs(ps_namespace *ns, unsigned int kind,
                                  uint64_t id) {
  struct ps_namespace_data *d;
  uint64_t refs = UINT64_MAX;
  if (ps_ns_data_get(ns, &d, NULL) != LC_OK)
    return refs;
  pthread_mutex_lock(&d->mutex);
  if (d->checkpoint_ready)
    refs = ps_key_checkpoint_source_refs(d->checkpoint, kind, id);
  pthread_mutex_unlock(&d->mutex);
  return refs;
}
