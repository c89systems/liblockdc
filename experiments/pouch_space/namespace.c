#include "frame.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_storage.h"
#include "namespace_internal.h"
#include "record_io.h"
#include "root_internal.h"
#include "space_internal.h"
#include "wire.h"
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
  uint64_t segment, incarnation;
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
  ps_coordination_guard *witness;
  size_t count;
  captured *items;
  ps_values *metadata;
  ps_checkpoint_info coverage;
  lc_pouch_storage_namespace_info projection;
  const lc_pouch_storage_state_refresh *refresh;
  size_t matched_slots;
  enum ps_checkpoint_capture_mode capture;
  int suffix, registered, incremental;
};
typedef struct namespace_observation {
  lc_pouch_storage_namespace_info projection;
  ps_namespace_view *view;
  ps_namespace_window *window;
  const lc_pouch_storage_namespace_info *expected;
  uint64_t start, source, bytes;
  int ready;
} namespace_observation;
typedef struct namespace_selection {
  ps_catalog_info binding;
  ps_checkpoint_info coverage;
  char leaf[PS_NS_LEAF_BYTES];
} namespace_selection;
static int failure(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL,
                      rc == LC_ERR_PROTOCOL ? "pouch_namespace_corrupt" : NULL,
                      "pouch");
}
static int checkpoint_ready(struct ps_namespace_data *, lc_error *);
static int lock_current(struct ps_namespace_data *, int *, lc_error *);
static void unlock_current(struct ps_namespace_data *, int);
static ps_key head_key(uint64_t owner) {
  ps_key key = {0, 1, PS_ROUTING};
  key.owner = owner;
  return key;
}
static int decode_head(uint64_t owner, const unsigned char *bytes,
                       uint64_t *active, lc_error *e) {
  size_t i;
  if (memcmp(bytes, "LCPSNSHD", 8) || ps_wire_get64(bytes + 8) != owner ||
      !ps_wire_get64(bytes + 16) ||
      ps_wire_get64(bytes + 24) != ps_wire_get64(bytes + 16) ||
      ps_wire_get64(bytes + 32) != LC_POUCH_RECORD_HEADER_BYTES ||
      ps_wire_get64(bytes + 120) != (uint64_t)crc32(0L, bytes, 120))
    return failure(e, LC_ERR_PROTOCOL,
                   "namespace head identity or checksum is invalid");
  for (i = 40; i < 120; ++i)
    if (bytes[i])
      return failure(e, LC_ERR_PROTOCOL, "namespace head has unknown fields");
  *active = ps_wire_get64(bytes + 16);
  return LC_OK;
}
static int head_read(struct ps_namespace_data *d, lc_error *e) {
  unsigned char bytes[NS_HEAD_BYTES];
  ps_reader *reader = NULL;
  ps_ref selected;
  size_t got;
  int rc = ps_read_open(d->ns->root->space, head_key(d->ns->identity.owner),
                        NULL, &reader, e);
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
  ps_read_reference(reader, &selected);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes))
    return failure(e, LC_ERR_PROTOCOL, "namespace head is incomplete");
  rc = decode_head(d->ns->identity.owner, bytes, &d->active, e);
  if (rc == LC_OK)
    d->route_incarnation = selected.incarnation;
  return rc;
}
static int head_write(struct ps_namespace_data *d, ps_budget *budget,
                      uint64_t active, lc_error *e) {
  unsigned char bytes[NS_HEAD_BYTES];
  ps_writer *writer = NULL;
  ps_ref selected;
  int rc;
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, "LCPSNSHD", 8);
  ps_wire_put64(bytes + 8, d->ns->identity.owner);
  ps_wire_put64(bytes + 16, active);
  ps_wire_put64(bytes + 24, active);
  ps_wire_put64(bytes + 32, LC_POUCH_RECORD_HEADER_BYTES);
  ps_wire_put64(bytes + 120, (uint64_t)crc32(0L, bytes, 120));
  rc = ps_begin(d->ns->root->space, budget, head_key(d->ns->identity.owner),
                &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), e);
  if (rc == LC_OK)
    rc = ps_publish(writer, &selected, e);
  if (rc == LC_OK)
    d->route_incarnation = selected.incarnation;
  ps_writer_close(writer);
  return rc;
}
typedef struct source_load {
  struct ps_namespace_data *candidate, *previous;
} source_load;
static int collect_resource(void *context, const void *name, size_t length,
                            const ps_catalog_info *binding,
                            unsigned int expected_kind, lc_error *e) {
  source_load *load = context;
  struct ps_namespace_data *d = load->candidate;
  segment *s, *old;
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
  old = load->previous ? ps_ns_find(load->previous, kind, id) : NULL;
  if (old) {
    lc_free_with_allocator(NULL, s->label);
    s->label = old->label;
    s->leaf = old->leaf;
    /* A bounded sweep resumes by logical offset. A peer checkpoint can
     * refresh the directory without replacing this stream incarnation. */
    if (old->binding.target.incarnation == binding->target.incarnation)
      s->reclaim_cursor = old->reclaim_cursor;
  } else {
    s->label->next = d->labels;
    d->labels = s->label;
  }
  s->pooled = 1;
  s->binding = *binding;
  return LC_OK;
}
static int collect_sources(source_load *load, lc_error *e) {
  ps_namespace *ns = load->candidate->ns;
  ps_space *space = ns->root->space;
  ps_coordination_guard *guard;
  uint64_t prefix;
  int rc;
  /* Namespace authority protects these bindings. The scan copies data from
   * resident descriptors only and can use one prefix for both source kinds. */
  pthread_mutex_lock(&space->mutex);
  rc = ps_internal_observe_begin(space, &guard, e);
  if (rc == LC_OK) {
    prefix = __atomic_load_n(&space->durable_sequence, __ATOMIC_ACQUIRE);
    rc = ps_internal_catalog_visit_locked(space, ns->identity.owner,
                                          (1U << PS_CATALOG_SEGMENT) |
                                              (1U << PS_CATALOG_BODY),
                                          prefix, collect_resource, load, e);
  }
  rc = ps_internal_capture_end(space, guard, rc, e);
  pthread_mutex_unlock(&space->mutex);
  return rc;
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
  rc = ps_stream_recovery_cover(stream, cursor, s->covered_checkpoint, e);
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
      rc = lc_pouch_state_record_read_with_crc(
          ps_record_recovery_input(stream), &allocator, s->leaf, inner,
          frame.end, !frame.framed, cursor >= checkpoint, NULL,
          ps_stream_recovery_crc, &record, &next, &found, &truncated, e);
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
size_t ps_namespace_data_budgets(const struct ps_namespace_data *d) {
  return d ? (d->retention_budget ? 1 : 0) +
                 ps_key_checkpoint_budgets(d->checkpoint)
           : 0;
}
void ps_namespace_data_destroy(struct ps_namespace_data *d) {
  size_t i;
  segment *r, *next;
  ps_ns_label *label, *next_label;
  if (!d)
    return;
  ps_key_checkpoint_close(d->checkpoint);
  (void)ps_budget_close(d->retention_budget, NULL);
  for (i = 0; i < d->count; ++i)
    ps_ns_resource_release(d->items[i]);
  for (r = d->retired; r; r = next) {
    next = r->next;
    ps_ns_resource_release(r);
  }
  for (label = d->labels; label; label = next_label) {
    next_label = label->next;
    lc_free_with_allocator(NULL, label);
  }
  lc_free_with_allocator(NULL, d->items);
  pthread_cond_destroy(&d->scope_changed);
  pthread_mutex_destroy(&d->mutex);
  lc_free_with_allocator(NULL, d);
}
static int allocate_data(ps_namespace *ns, struct ps_namespace_data **out,
                         lc_error *e) {
  struct ps_namespace_data *d;
  d = lc_calloc_with_allocator(NULL, 1, sizeof(*d));
  if (!d)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace storage owner");
  d->ns = ns;
  if (pthread_mutex_init(&d->mutex, NULL)) {
    lc_free_with_allocator(NULL, d);
    return failure(e, LC_ERR_TRANSPORT, "cannot initialize namespace mutex");
  }
  if (pthread_cond_init(&d->scope_changed, NULL)) {
    pthread_mutex_destroy(&d->mutex);
    lc_free_with_allocator(NULL, d);
    return failure(e, LC_ERR_TRANSPORT, "cannot initialize namespace handoff");
  }
  *out = d;
  return LC_OK;
}
static int load(ps_namespace *ns, struct ps_namespace_data *previous,
                struct ps_namespace_data **out, lc_error *e) {
  struct ps_namespace_data *d;
  source_load sources;
  ps_stream_info info;
  ps_checkpoint_info coverage = {0};
  ps_values *values = NULL;
  ps_key_checkpoint *resident = previous ? previous->checkpoint : NULL;
  size_t i;
  uint64_t seen = 0;
  int rc = allocate_data(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  sources.candidate = d;
  sources.previous = previous;
  rc = head_read(d, e);
  if (rc == LC_OK)
    rc = ps_ns_retention_restore(d, e);
  if (rc == LC_OK) {
    /* Shared reload keeps the resident slots stable through adoption. */
    if (resident)
      rc = ps_key_checkpoint_capture(resident, PS_CHECKPOINT_CAPTURE_CHANGED,
                                     &values, &coverage, e);
    else {
      rc = ps_key_checkpoint_open(ns, &d->checkpoint, e);
      if (rc == LC_OK)
        ps_key_checkpoint_info(d->checkpoint, &coverage);
    }
  }
  if (rc == LC_OK && coverage.segment > d->active)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "checkpoint coverage exceeds namespace head");
  if (rc == LC_OK)
    rc = collect_sources(&sources, e);
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
          (d->active == LC_U64_MAX || s->id != d->active + 1)) {
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
    s->covered_checkpoint = s->covered ? coverage.control : 0;
    rc = ps_stream_open(ns->root->space, NULL, s->binding.stream, 0,
                        &s->binding.target, recover, s, &s->stream, e);
    if (rc == LC_OK)
      rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK && s->kind == PS_CATALOG_SEGMENT && !info.sealed)
      s->sparse = ps_stream_has_sparse_runs(s->stream);
    if (rc == LC_OK)
      ps_ns_resource_park(d, s);
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
      if (s->sparse && s->id + 1 != d->active)
        rc = failure(e, LC_ERR_PROTOCOL,
                     "sparse namespace predecessor lacks a final tail");
      else if (s->id + 1 == d->active &&
               (s->sparse || ns->root->space->durable))
        d->seal_pending = s->id;
    }
  }
  if (rc == LC_OK &&
      (seen != d->active - coverage.segment ||
       (d->active && !ps_ns_find(d, PS_CATALOG_SEGMENT, d->active))))
    rc = failure(e, LC_ERR_PROTOCOL, "namespace head has missing segments");
  if (rc == LC_OK && resident) {
    uint64_t active = 0;
    int resumed;
    if (d->active && d->active == previous->active &&
        d->tail >= previous->tail) {
      segment *old = ps_ns_find(previous, PS_CATALOG_SEGMENT, d->active);
      segment *fresh = ps_ns_find(d, PS_CATALOG_SEGMENT, d->active);
      if (old &&
          old->binding.target.incarnation == fresh->binding.target.incarnation)
        active = d->active;
    }
    rc =
        ps_key_checkpoint_adopt(resident, values, &coverage, active,
                                previous->tail, &resumed, &d->replay_offset, e);
    if (rc == LC_OK) {
      d->checkpoint = resident;
      if (resumed)
        d->replay_segment = active;
    }
  }
  ps_values_close(values);
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
      sources[count].stored_bytes = info.stored_bytes;
      sources[count].runs = info.runs;
      sources[count].closed =
          s->kind == PS_CATALOG_SEGMENT ? s->id < d->active : info.sealed;
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

/* A namespace's address, labels, and captured views survive a handoff.
 * Replace only the canonical owner cache after validation succeeds. */
static int reload_shared(struct ps_namespace_data *d, lc_error *e) {
  struct ps_namespace_data *fresh = NULL;
  ps_ns_label **labels;
  ps_ns_resource **items;
  ps_key_checkpoint *checkpoint;
  ps_budget *retention_budget;
  size_t count;
  int rc;
  d->shared_valid = 0;
  rc = load(d->ns, d, &fresh, e);
  if (rc == LC_OK && fresh->checkpoint != d->checkpoint &&
      d->checkpoint_observer.prepare)
    rc = ps_key_checkpoint_observe(fresh->checkpoint, &d->checkpoint_observer,
                                   e);
  if (rc != LC_OK) {
    ps_namespace_data_destroy(fresh);
    return rc;
  }
  items = d->items;
  count = d->count;
  checkpoint = d->checkpoint;
  retention_budget = d->retention_budget;
  d->items = fresh->items;
  d->count = fresh->count;
  d->capacity = fresh->capacity;
  d->active = fresh->active;
  d->tail = fresh->tail;
  d->route_incarnation = fresh->route_incarnation;
  d->seal_pending = fresh->seal_pending;
  d->replay_segment = fresh->replay_segment;
  d->replay_offset = fresh->replay_offset;
  d->checkpoint = fresh->checkpoint;
  d->checkpoint_ready = 0;
  d->retention_loaded = fresh->retention_loaded;
  d->retention_present = fresh->retention_present;
  d->retention_budget = fresh->retention_budget;
  for (labels = &fresh->labels; *labels; labels = &(*labels)->next) {
  }
  *labels = d->labels;
  d->labels = fresh->labels;
  fresh->labels = NULL;
  fresh->items = items;
  fresh->count = count;
  fresh->checkpoint = checkpoint == d->checkpoint ? NULL : checkpoint;
  fresh->retention_budget = retention_budget;
  ps_namespace_data_destroy(fresh);
  d->shared_valid = 1;
  return LC_OK;
}
static int namespace_current(struct ps_namespace_data *d, lc_error *e) {
  int entered, rc;
  if (!ps_coordination_shared(ps_space_coordination(d->ns->root->space)))
    return LC_OK;
  rc = lock_current(d, &entered, e);
  if (rc == LC_OK)
    unlock_current(d, entered);
  return rc;
}
static int data_get(ps_namespace *ns, struct ps_namespace_data **out,
                    int reconcile, lc_error *e) {
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
    if (reconcile)
      rc = namespace_current(fresh, e);
    if (rc == LC_OK)
      *out = fresh;
    return rc;
  }
  pthread_mutex_lock(&root->mutex);
  while (ns->data_loading)
    pthread_cond_wait(&root->changed, &root->mutex);
  if (!ns->data) {
    ns->data_loading = 1;
    pthread_mutex_unlock(&root->mutex);
    rc = ps_coordination_shared(ps_space_coordination(root->space))
             ? allocate_data(ns, &fresh, e)
             : load(ns, NULL, &fresh, e);
    pthread_mutex_lock(&root->mutex);
    if (rc == LC_OK)
      __atomic_store_n(&ns->data, fresh, __ATOMIC_RELEASE);
    ns->data_loading = 0;
    pthread_cond_broadcast(&root->changed);
  }
  fresh = ns->data;
  pthread_mutex_unlock(&root->mutex);
  if (rc == LC_OK && reconcile)
    rc = namespace_current(fresh, e);
  if (rc == LC_OK)
    *out = fresh;
  return rc;
}
int ps_ns_data_get(ps_namespace *ns, struct ps_namespace_data **out,
                   lc_error *e) {
  return data_get(ns, out, 1, e);
}
int ps_ns_data_lookup(ps_namespace *ns, struct ps_namespace_data **out,
                      lc_error *e) {
  return data_get(ns, out, 0, e);
}
static int same_projection(const lc_pouch_storage_namespace_info *a,
                           const lc_pouch_storage_namespace_info *b) {
  return a->active_segment == b->active_segment &&
         a->active_incarnation == b->active_incarnation &&
         a->checkpoint_epoch == b->checkpoint_epoch && a->tail == b->tail;
}
static int retain_changed_state(void *context, ps_key key,
                                uint64_t incarnation) {
  ps_namespace_view *view = context;
  const lc_pouch_storage_state_refresh *refresh = view->refresh;
  size_t at = lc_pouch_storage_state_slot_find(
      refresh->previous, refresh->previous_count, key.key);
  if (at == refresh->previous_count)
    return 1;
  ++view->matched_slots;
  return refresh->previous[at].incarnation != incarnation;
}
/* Namespace exclusion spans the canonical capture, header verification, and
 * acquisition of the view witness. */
static int capture_selected_view_locked(struct ps_namespace_data *d,
                                        namespace_observation *observed,
                                        const namespace_selection *selection,
                                        uint64_t prefix, int provisional,
                                        lc_error *e) {
  ps_namespace_view *view = observed->view;
  ps_space *space = d->ns->root->space;
  const ps_checkpoint_info *coverage = &selection->coverage;
  const ps_catalog_info *binding = &selection->binding;
  uint64_t active = observed->projection.active_segment;
  uint64_t tail = observed->projection.tail;
  uint64_t start = observed->start;
  int rc = LC_OK;
  if (observed->expected) {
    if (!same_projection(observed->expected, &observed->projection))
      return failure(e, LC_ERR_PENDING, "namespace replay boundary changed");
    if (start >= tail ||
        (active == coverage->segment && start < coverage->offset))
      return failure(e, LC_ERR_PROTOCOL,
                     "namespace replay suffix is out of range");
  } else {
    /* The complete view is local to this stream when no predecessor has
     * uncovered records. Multi-stream captures use ordinary recovery. */
    if (coverage->segment != active && (coverage->segment || active != 1))
      return LC_OK;
    start = coverage->offset;
    if (view->refresh && view->refresh->previous_count && coverage->epoch &&
        coverage->segment == active && start == tail) {
      view->incremental = 1;
      rc = ps_internal_values_capture_locked(
          space, d->ns->identity.owner, PS_KEY_METADATA, PS_KEY_METADATA,
          prefix, provisional, retain_changed_state, view, &view->metadata, e);
    } else
      rc = ps_checkpoint_capture_selected_locked(d->ns, prefix, coverage,
                                                 view->capture, provisional,
                                                 &view->metadata, e);
  }
  view->coverage = *coverage;
  if (rc == LC_OK && start < tail) {
    view->items = lc_calloc_with_allocator(NULL, 1, sizeof(*view->items));
    if (!view->items)
      rc = failure(e, LC_ERR_NOMEM, "cannot capture namespace segment");
    else {
      view->count = 1;
      view->items->id = active;
      view->items->start = start;
      view->items->tail = tail;
      strcpy(view->items->leaf, selection->leaf);
      rc = ps_stream_window_pin_locked(space, binding->stream, &binding->target,
                                       start, tail, provisional,
                                       &view->items->reader, e);
    }
  }
  observed->ready = rc == LC_OK;
  return rc;
}
static int active_binding(ps_space *s, ps_key stream, uint64_t prefix,
                          char *leaf, ps_catalog_info *binding) {
  return lc_pouch_namespace_format_segment_leaf(stream.key, leaf,
                                                PS_NS_LEAF_BYTES) &&
         ps_internal_catalog_find(s, stream.owner, PS_CATALOG_SEGMENT, leaf,
                                  strlen(leaf), prefix, binding) &&
         binding->stream.kind == stream.kind &&
         binding->stream.owner == stream.owner &&
         binding->stream.key == stream.key;
}
/* A matching completion stamp gives the selected tail without replaying
 * the producer. An unknown memo falls back to decoding under authority. */
static int select_projection_locked(struct ps_namespace_data *d,
                                    namespace_observation *observed,
                                    namespace_selection *selection,
                                    uint64_t prefix, int memo_only, int *found,
                                    lc_error *e) {
  ps_namespace *ns = d->ns;
  lc_pouch_storage_namespace_info *out = &observed->projection;
  ps_space *s = ns->root->space;
  ps_checkpoint_info *coverage = &selection->coverage;
  ps_key stream = {0, 0, PS_STREAM};
  unsigned char head[NS_HEAD_BYTES];
  uint64_t token, active = 0, tail = 0;
  int rc, current;
  *found = 0;
  rc = ps_internal_fixed_value(s, head_key(ns->identity.owner), prefix,
                               memo_only ? NULL : head, sizeof(head),
                               d->observed_head, &token, &current, NULL, e);
  if (rc != LC_OK || !current)
    return rc;
  if (!token) {
    if (observed->window || (observed->view && !observed->view->refresh))
      return LC_OK;
    *found = 1; /* Metadata can have a checkpoint without a stream head. */
  } else if (token == d->observed_head)
    active = d->observed_active;
  else {
    rc = decode_head(ns->identity.owner, head, &active, e);
    if (rc != LC_OK)
      return rc;
    d->observed_head = token;
    d->observed_active = active;
  }
  if (active) {
    stream.owner = ns->identity.owner;
    stream.key = active;
    *found =
        active_binding(s, stream, prefix, selection->leaf,
                       &selection->binding) &&
        ps_internal_complete_tail(s, stream, &selection->binding.target, &tail);
  }
  if (*found && !observed->window) {
    rc = ps_checkpoint_point(s, ns->identity.owner, prefix, memo_only,
                             &d->observed_coverage, found, e);
    *coverage = d->observed_coverage;
  }
  if (rc != LC_OK || !*found)
    return rc;
  if (!active && (!coverage->epoch || coverage->offset)) {
    *found = 0;
    return LC_OK;
  }
  if (!observed->window &&
      (coverage->segment > active ||
       (coverage->segment == active && coverage->offset > tail)))
    return failure(e, LC_ERR_PROTOCOL, "checkpoint exceeds namespace tail");
  out->active_segment = active;
  out->tail = tail;
  out->active_incarnation = active ? selection->binding.target.incarnation : 0;
  if (!observed->window) {
    out->segments = active - coverage->segment + (coverage->segment != 0);
    out->checkpoint_epoch = coverage->epoch;
  }
  return LC_OK;
}
static void release_view_payloads(ps_namespace_view *view, int locked) {
  size_t i;
  for (i = 0; view->items && i < view->count; ++i) {
    if (locked)
      ps_stream_window_discard_locked(view->items[i].reader);
    else
      ps_stream_window_close(view->items[i].reader);
  }
  lc_free_with_allocator(NULL, view->items);
  if (locked)
    ps_internal_values_discard_locked(view->metadata);
  else
    ps_values_close(view->metadata);
  view->items = NULL;
  view->metadata = NULL;
  view->count = view->matched_slots = 0;
  view->incremental = 0;
}
static int projection_selected_locked(struct ps_namespace_data *d,
                                      namespace_observation *observed,
                                      int provisional, lc_error *e) {
  ps_space *s = d->ns->root->space;
  const lc_pouch_storage_namespace_info *out = &observed->projection;
  namespace_selection selection;
  uint64_t prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  int found, rc = select_projection_locked(d, observed, &selection, prefix,
                                           provisional, &found, e);
  if (rc != LC_OK || !found)
    return rc;
  if (observed->window) {
    if (out->active_segment == observed->source &&
        observed->start <= out->tail &&
        observed->bytes <= out->tail - observed->start) {
      rc = ps_stream_window_pin_locked(
          s, selection.binding.stream, &selection.binding.target,
          observed->start, observed->start + observed->bytes, provisional,
          &observed->window->reader, e);
      observed->ready = rc == LC_OK;
    }
  } else if (observed->view)
    rc = capture_selected_view_locked(d, observed, &selection, prefix,
                                      provisional, e);
  else
    observed->ready = 1;
  return rc;
}
static int projection_selected(struct ps_namespace_data *d,
                               namespace_observation *observed, lc_error *e) {
  ps_namespace *ns = d->ns;
  ps_space *s = ns->root->space;
  ps_coordination_guard *allocator;
  ps_namespace_view *view = observed->view;
  int rc, provisional, point = !view && !observed->window;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_observe_begin(s, &allocator, e);
  provisional = s->readiness && !s->allocation_guard;
  if (rc == LC_OK)
    rc = projection_selected_locked(d, observed, provisional, e);
  if (provisional && (rc != LC_OK || !observed->ready ||
                      (!point && !ps_internal_read_witness_valid(s)))) {
    /* Another thread on this OFD must never adopt while speculative pins
     * could refer to pages the peer has already reused. */
    if (view)
      release_view_payloads(view, 1);
    else if (observed->window) {
      ps_stream_window_discard_locked(observed->window->reader);
      observed->window->reader = NULL;
    }
    observed->ready = 0;
    if (rc == LC_OK) {
      rc = ps_internal_capture_begin(s, &allocator, e);
      if (rc == LC_OK)
        rc = projection_selected_locked(d, observed, 0, e);
    } else if (ps_internal_healthy(s, NULL) != LC_OK) {
      lc_error_cleanup(e);
      rc = ps_internal_healthy(s, e);
    }
  }
  rc = ps_internal_capture_end(s, allocator, rc, e);
  pthread_mutex_unlock(&s->mutex);
  if (rc != LC_OK || !observed->ready || point)
    return rc;
  if (observed->window)
    return ps_stream_window_verify(observed->window->reader, e);
  if (view->count)
    rc = ps_stream_window_verify(view->items[0].reader, e);
  if (rc == LC_OK)
    rc = ps_coordination_lock(ps_space_coordination(s),
                              PS_COORDINATION_NAMESPACE_VIEW, ns->hash, 0,
                              &view->witness, e);
  if (rc == LC_OK) {
    view->projection = observed->projection;
    pthread_mutex_lock(&d->mutex);
    ++d->views;
    view->registered = 1;
    pthread_mutex_unlock(&d->mutex);
  }
  return rc;
}
/* The local scope is reserved before requesting file authority. Waiters can
 * borrow a valid resident projection as soon as an edit has been admitted. */
static int admit_scope(struct ps_namespace_data *d,
                       namespace_observation *observed, lc_error *e) {
  ps_coordination_guard *guard = NULL;
  int continued = 0, rc;
  if (!ps_coordination_shared(ps_space_coordination(d->ns->root->space)))
    return LC_OK;
  rc = ps_namespace_guard(d->ns, &guard, e);
  if (rc == LC_OK && guard && observed)
    rc = projection_selected(d, observed, e);
  if (rc == LC_OK && guard && (!observed || !observed->ready))
    rc = ps_internal_namespace_stamp(d->ns->root->space, d->ns->identity.owner,
                                     &continued, e);
  pthread_mutex_lock(&d->mutex);
  if (rc == LC_OK && guard && (!observed || !observed->ready) &&
      (!d->shared_valid || !continued))
    rc = reload_shared(d, e);
  if (rc == LC_OK && (!observed || !observed->ready)) {
    d->edit = guard;
    if (d->scope == PS_NS_EDIT_PENDING)
      d->scope = PS_NS_EDITING;
  } else {
    rc = ps_namespace_unguard(d->ns, guard, rc, e);
    d->scope = PS_NS_IDLE;
  }
  pthread_cond_broadcast(&d->scope_changed);
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
int ps_ns_enter(struct ps_namespace_data *d, lc_error *e) {
  int rc = LC_OK;
  int shared =
      ps_coordination_shared(ps_space_coordination(d->ns->root->space));
  pthread_mutex_lock(&d->mutex);
  while (rc == LC_OK && d->scope == PS_NS_OBSERVING)
    if (pthread_cond_wait(&d->scope_changed, &d->mutex))
      rc = failure(e, LC_ERR_TRANSPORT, "cannot wait for namespace capture");
  if (rc == LC_OK && d->scope != PS_NS_IDLE)
    rc = lc_error_set(e, LC_ERR_INVALID, 0L,
                      "namespace already has an active producer or rotation",
                      NULL, "pouch_namespace_busy", "pouch");
  if (rc == LC_OK)
    d->scope = shared ? PS_NS_EDIT_PENDING : PS_NS_EDITING;
  pthread_mutex_unlock(&d->mutex);
  return rc == LC_OK ? admit_scope(d, NULL, e) : rc;
}
void ps_ns_leave(struct ps_namespace_data *d) {
  pthread_mutex_lock(&d->mutex);
  (void)ps_namespace_unguard(d->ns, d->edit, LC_OK, NULL);
  d->edit = NULL;
  d->scope = PS_NS_IDLE;
  pthread_cond_broadcast(&d->scope_changed);
  pthread_mutex_unlock(&d->mutex);
}
int ps_ns_require_unpinned(struct ps_namespace_data *d, lc_error *e) {
  int pinned = d->views != 0;
  int rc = LC_OK;
  if (!pinned)
    rc = ps_coordination_namespace_view_conflict(
        ps_space_coordination(d->ns->root->space), d->ns->hash, &pinned, e);
  if (rc == LC_OK && pinned)
    rc = lc_error_set(e, LC_ERR_INVALID, 0,
                      "captured namespace metadata still protects sources",
                      NULL, "pouch_namespace_busy", "pouch");
  return rc;
}
/* Hold the shared name guard through selection and pin acquisition. An
 * existing local writer keeps it while the data mutex protects our capture.
 * A successful point observation returns with ready set and no held guard. */
static int lock_current_scope(struct ps_namespace_data *d, int *entered,
                              namespace_observation *observed, lc_error *e) {
  int rc = LC_OK;
  *entered = 0;
  pthread_mutex_lock(&d->mutex);
  if (!ps_coordination_shared(ps_space_coordination(d->ns->root->space)))
    return LC_OK;
  while (rc == LC_OK && (d->scope == PS_NS_EDIT_PENDING ||
                         (d->scope == PS_NS_OBSERVING && !d->edit)))
    if (pthread_cond_wait(&d->scope_changed, &d->mutex))
      rc = failure(e, LC_ERR_TRANSPORT, "cannot wait for namespace handoff");
  if (rc == LC_OK && d->scope != PS_NS_IDLE) {
    if (d->edit && d->shared_valid)
      return LC_OK;
    rc = failure(e, LC_ERR_PENDING, "namespace is being reconciled");
  }
  if (rc == LC_OK)
    d->scope = PS_NS_OBSERVING;
  pthread_mutex_unlock(&d->mutex);
  if (rc != LC_OK)
    return rc;
  rc = admit_scope(d, observed, e);
  if (rc == LC_OK && (!observed || !observed->ready)) {
    *entered = 1;
    pthread_mutex_lock(&d->mutex);
  }
  return rc;
}
static int lock_current(struct ps_namespace_data *d, int *entered,
                        lc_error *e) {
  return lock_current_scope(d, entered, NULL, e);
}
static void unlock_current(struct ps_namespace_data *d, int entered) {
  pthread_mutex_unlock(&d->mutex);
  if (entered)
    ps_ns_leave(d);
}
int ps_ns_stabilize_cut(struct ps_namespace_data *d, uint64_t coverage,
                        lc_error *e) {
  return ps_internal_namespace_cut_stabilize(d->ns->root->space,
                                             d->ns->identity.owner,
                                             d->route_incarnation, coverage, e);
}
int ps_ns_seal_predecessor(struct ps_namespace_data *d, uint64_t id,
                           lc_error *e) {
  segment *s = ps_ns_find(d, PS_CATALOG_SEGMENT, id);
  ps_stream_info info;
  int rc;
  if (!s)
    return LC_OK;
  if (id >= d->active)
    return failure(e, LC_ERR_INVALID, "cannot seal the namespace append head");
  rc = ps_stream_status(s->stream, &info, e);
  if (rc != LC_OK || info.sealed)
    return rc;
  /* A physical seal must follow the durable route that closed this source. */
  rc = ps_ns_stabilize_cut(d, 0, e);
  if (rc == LC_OK)
    rc = ps_stream_seal(s->stream, e);
  pthread_mutex_lock(&d->mutex);
  ps_ns_resource_park(d, s);
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
int ps_ns_finish_seal(struct ps_namespace_data *d, lc_error *e) {
  int rc = LC_OK;
  if (d->seal_pending) {
    rc = ps_ns_seal_predecessor(d, d->seal_pending, e);
    if (rc == LC_OK)
      d->seal_pending = 0;
  }
  return rc;
}
static int prepare(struct ps_namespace_data *d, ps_budget *budget, uint64_t id,
                   int foreground, lc_error *e) {
  segment *s;
  ps_catalog_view *view = NULL;
  ps_owned_head *staged = NULL;
  ps_ref target;
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
  rc =
      ps_catalog_get_segment_for_edit(d->ns->root->space, d->ns->identity.owner,
                                      s->leaf, strlen(s->leaf), &view, e);
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
      rc = ps_owned_stage_empty(d->ns->root->space, budget, s->binding.stream,
                                &staged, &target, e);
    }
  }
  if (rc == LC_OK && !staged) {
    rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK && (info.tail || info.sealed))
      rc = failure(e, LC_ERR_PROTOCOL, "staged namespace segment is not empty");
    if (rc == LC_OK)
      target = info.ref;
  }
  if (rc == LC_OK && !view) {
    ps_space *space = d->ns->root->space;
    if (foreground)
      rc = ps_catalog_stage_segment(space, budget, d->ns->identity.owner,
                                    s->leaf, strlen(s->leaf), s->binding.stream,
                                    &target, &s->binding, e);
    else
      rc = ps_catalog_put(space, budget, d->ns->identity.owner,
                          PS_CATALOG_SEGMENT, s->leaf, strlen(s->leaf), NULL,
                          s->binding.stream, &target, &s->binding, e);
  }
  if (staged) {
    ps_owned_close(staged);
    if (rc == LC_OK)
      rc = ps_stream_open(d->ns->root->space, NULL, s->binding.stream, 0,
                          &s->binding.target, recover, s, &s->stream, e);
  }
  ps_catalog_view_close(view);
  if (rc == LC_OK) {
    ps_ns_resource_park(d, s);
    pthread_mutex_lock(&d->mutex);
    ps_ns_insert(d, s);
    pthread_mutex_unlock(&d->mutex);
  } else {
    ps_ns_resource_release(s);
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
                       const ps_budget_plan *checkpoint, ps_budget **budget,
                       int *exact, lc_error *e) {
  ps_budget_plan plan = {0}, exact_plan;
  ps_stream_info info = {0};
  segment *s = ps_ns_find(d, PS_CATALOG_SEGMENT, selected);
  uint64_t start = selected == d->active ? d->tail : 0;
  uint64_t bytes = minimum ? minimum : 1, need, preferred = PS_PAGE;
  int rc = LC_OK;
  *budget = NULL;
  *exact =
      mode == PS_APPEND_MAINTENANCE || mode == PS_APPEND_MAINTENANCE_PREFIX;
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
  if (start > (uint64_t)LC_I64_MAX || bytes > (uint64_t)LC_I64_MAX - start ||
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
  if (rc == LC_OK)
    rc = ps_budget_plan_add(&plan, checkpoint, e);
  if (rc == LC_OK)
    rc = ps_budget_plan_add(&exact_plan, checkpoint, e);
  if (rc != LC_OK)
    return rc;
  if (!plan.payload_bytes && !plan.metadata_bytes && !plan.entries)
    return plan.maintenance ? LC_OK
                            : ps_budget_foreground_check(d->ns->root->space, e);
  rc = ps_budget_open(d->ns->root->space, &plan, budget, e);
  if (rc == LC_ERR_INVALID && !*exact &&
      plan.payload_bytes > exact_plan.payload_bytes) {
    lc_error_cleanup(e);
    *exact = 1;
    rc = ps_budget_open(d->ns->root->space, &exact_plan, budget, e);
  }
  return rc;
}
static int pressure_rotation(struct ps_namespace_data *d, uint64_t target,
                             int *rotate, lc_error *e) {
  segment *active;
  ps_stream_info info;
  uint64_t live, threshold = target / 2;
  int rc;
  /* Seal substantial obsolete history early so it can enter the same
   * bounded maintenance and retirement paths as a normal predecessor. */
  if (threshold < PS_NAMESPACE_REPACK_BYTES)
    threshold = PS_NAMESPACE_REPACK_BYTES;
  if (d->tail < threshold)
    return LC_OK;
  active = ps_ns_find(d, PS_CATALOG_SEGMENT, d->active);
  if (!active)
    return failure(e, LC_ERR_PROTOCOL,
                   "namespace active append stream is missing");
  rc = ps_stream_status(active->stream, &info, e);
  if (rc == LC_OK && info.stored_bytes >= threshold) {
    live = ps_key_checkpoint_source_bytes(d->checkpoint, PS_CATALOG_SEGMENT,
                                          d->active);
    *rotate =
        live <= info.stored_bytes && info.stored_bytes - live >= threshold / 2;
  }
  return rc;
}
int ps_ns_rotation_plan(struct ps_namespace_data *d, ps_budget_plan *out,
                        lc_error *e) {
  ps_budget_plan plan = *out;
  uint64_t next;
  int rc;
  if (d->active == LC_U64_MAX)
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
int ps_ns_select_successor(struct ps_namespace_data *d, ps_budget *budget,
                           lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *owned = NULL;
  uint64_t next;
  int rc, closed;
  plan.maintenance = 1;
  rc = ps_ns_finish_seal(d, e);
  if (rc == LC_OK)
    rc = ps_ns_rotation_plan(d, &plan, e);
  if (rc == LC_OK && !budget) {
    rc = ps_budget_open(d->ns->root->space, &plan, &owned, e);
    budget = owned;
  }
  next = d->active + 1;
  if (rc == LC_OK)
    rc = prepare(d, budget, next, 0, e);
  if (rc == LC_OK)
    rc = head_write(d, budget, next, e);
  if (rc == LC_OK)
    rc = ps_ns_stabilize_cut(d, 0, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    d->seal_pending = d->active;
    d->active = next;
    d->tail = 0;
    ps_ns_find(d, PS_CATALOG_SEGMENT, next)->unselected = 0;
    pthread_mutex_unlock(&d->mutex);
  }
  closed = ps_budget_close(owned, rc == LC_OK ? e : NULL);
  if (rc == LC_OK)
    rc = closed;
  return rc;
}
int ps_ns_rotate(struct ps_namespace_data *d, ps_budget *budget, lc_error *e) {
  int rc = ps_ns_select_successor(d, budget, e);
  return rc == LC_OK ? ps_ns_finish_seal(d, e) : rc;
}
static int projection(struct ps_namespace_data *d,
                      const ps_checkpoint_info *coverage,
                      lc_pouch_storage_namespace_info *out, lc_error *e) {
  segment *active = ps_ns_find(d, PS_CATALOG_SEGMENT, d->active);
  if (coverage->segment > d->active)
    return failure(e, LC_ERR_PROTOCOL, "checkpoint exceeds namespace head");
  if (d->active && !active)
    return failure(e, LC_ERR_PROTOCOL, "namespace active stream is missing");
  out->active_segment = d->active;
  out->segments = d->active - coverage->segment + (coverage->segment != 0);
  out->tail = d->tail;
  out->active_incarnation = active ? active->binding.target.incarnation : 0;
  out->checkpoint_epoch = coverage->epoch;
  return LC_OK;
}
int ps_namespace_projection(ps_namespace *ns,
                            lc_pouch_storage_namespace_info *out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_checkpoint_info coverage;
  namespace_observation observed = {0};
  int entered, rc;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace projection requires output");
  rc = data_get(ns, &d, 0, e);
  if (rc == LC_OK)
    rc = lock_current_scope(d, &entered, &observed, e);
  if (rc == LC_OK && observed.ready)
    *out = observed.projection;
  else if (rc == LC_OK) {
    ps_key_checkpoint_info(d->checkpoint, &coverage);
    rc = projection(d, &coverage, out, e);
    unlock_current(d, entered);
  }
  return rc;
}
int ps_namespace_query_sequence(ps_namespace *ns, lc_pouch_generation *out) {
  ps_space *space = ns->root->space;
  struct ps_namespace_data *d = __atomic_load_n(&ns->data, __ATOMIC_ACQUIRE);
  int current = 0;
  if (!d || space->readiness)
    return 0;
  pthread_mutex_lock(&d->mutex);
  /* Startup already replayed this exact tail with exclusive authority. A
   * newer logical mutation requires the ordinary projection path. */
  if (d->checkpoint_ready)
    current = ps_key_checkpoint_query_sequence(d->checkpoint, d->active,
                                               d->tail, out);
  pthread_mutex_unlock(&d->mutex);
  return current;
}
int ps_namespace_status_get(ps_namespace *ns, ps_namespace_status *out,
                            lc_error *e) {
  struct ps_namespace_data *d;
  ps_stream_info info;
  ps_checkpoint_info coverage;
  size_t i;
  int rc, entered;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace status requires output");
  rc = data_get(ns, &d, 0, e);
  if (rc != LC_OK)
    return rc;
  rc = lock_current(d, &entered, e);
  if (rc != LC_OK)
    return rc;
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
  unlock_current(d, entered);
  return rc;
}
int ps_namespace_append_begin(ps_namespace *ns, uint64_t target,
                              uint64_t minimum,
                              enum ps_namespace_append_mode mode,
                              const lc_pouch_storage_batch *batch,
                              ps_namespace_append **out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_append *append;
  segment *s = NULL;
  uint64_t selected;
  uint64_t span;
  ps_budget_plan checkpoint = {0};
  int rc, exact = 0, pressured, early = 0;
  int nosync;
  int metadata = mode == PS_APPEND_METADATA;
  int complete =
      mode == PS_APPEND_COMPLETE || mode == PS_APPEND_MAINTENANCE || metadata;
  int framed = complete || mode == PS_APPEND_FRAMED_PREFIX;
  int maintenance =
      mode == PS_APPEND_MAINTENANCE || mode == PS_APPEND_MAINTENANCE_PREFIX;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace append requires output");
  if (mode < PS_APPEND_PREFIX || mode > PS_APPEND_METADATA ||
      (mode != PS_APPEND_PREFIX && !minimum) ||
      (batch &&
       (mode == PS_APPEND_PREFIX || mode == PS_APPEND_FRAMED_PREFIX)) ||
      ((mode == PS_APPEND_MAINTENANCE_PREFIX || metadata) && !batch))
    return failure(
        e, LC_ERR_INVALID,
        "namespace complete append requires a positive span and valid mode");
  if (minimum > (uint64_t)LC_I64_MAX ||
      (framed && minimum > (uint64_t)LC_I64_MAX - PS_FRAME_BYTES))
    return failure(e, LC_ERR_INVALID,
                   "namespace append minimum exceeds local offset limit");
  span = framed ? minimum + PS_FRAME_BYTES : minimum;
  rc = data_get(ns, &d, 0, e);
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
  pressured =
      !maintenance && ps_maintenance_pressure_requested(ns->root->space);
  if (rc == LC_OK && metadata &&
      (pressured || !d->active || (target && d->tail >= target)))
    rc = failure(
        e, LC_ERR_PENDING,
        "canonical metadata needs ordinary space in the active segment");
  if (rc == LC_OK && pressured)
    rc = ps_ns_reclaim_before_append(d, e);
  nosync = ps_internal_exclusive_nosync(ns->root->space);
  /* NoSync can retain funded changes until the existing byte limits.
   * Repeated edits then replace the pending snapshot for the same key. */
  if (rc == LC_OK && ps_key_checkpoint_due(d->checkpoint, !nosync)) {
    pthread_mutex_lock(&d->mutex);
    rc = ps_key_checkpoint_flush(d->checkpoint, e);
    pthread_mutex_unlock(&d->mutex);
  }
  selected = d->active;
  if (rc == LC_OK) {
    if (!selected || (target && d->tail >= target)) {
      if (selected == LC_U64_MAX)
        rc = failure(e, LC_ERR_INVALID, "namespace segment IDs exhausted");
      else {
        ++selected;
      }
    } else if (pressured && target && selected < LC_U64_MAX) {
      rc = pressure_rotation(d, target, &early, e);
      if (rc == LC_OK && early)
        ++selected;
    }
  }
  if (rc == LC_OK)
    rc =
        ps_checkpoint_delta_open(d->checkpoint, maintenance, &append->delta, e);
  if (rc == LC_OK && batch)
    rc =
        ps_checkpoint_delta_plan(append->delta, batch, minimum, &checkpoint, e);
  if (rc == LC_OK) {
    rc = plan_append(d, selected, span, mode, &checkpoint, &append->budget,
                     &exact, e);
    /* Early rotation is opportunistic. An existing piece can still admit a
     * record when ordinary headroom cannot fund a successor. */
    if (rc == LC_ERR_INVALID && early) {
      lc_error_cleanup(e);
      selected = d->active;
      rc = plan_append(d, selected, span, mode, &checkpoint, &append->budget,
                       &exact, e);
    }
  }
  if (rc == LC_OK && batch)
    rc =
        ps_checkpoint_delta_fund(append->delta, append->budget, &checkpoint, e);
  if (rc == LC_OK && selected != d->active)
    rc = prepare(d, append->budget, selected, !maintenance, e);
  if (rc == LC_OK) {
    s = ps_ns_find(d, PS_CATALOG_SEGMENT, selected);
    rc = begin_reserve(s->stream, append->budget, span, exact, &append->writer,
                       e);
    append->segment = s->id;
    append->incarnation = s->binding.target.incarnation;
    append->leaf = s->leaf;
    append->complete = complete;
    append->expected = span;
    if (rc == LC_OK && (complete || mode == PS_APPEND_MAINTENANCE_PREFIX))
      append->writer.limit = append->writer.start + span;
    if (rc == LC_OK && framed)
      rc = ps_record_frame_begin(&append->writer, complete ? minimum : 0, e);
    if (rc == LC_OK)
      rc = ps_checkpoint_delta_bind(append->delta, &append->writer, s->id,
                                    s->leaf, e);
  }
  if (rc != LC_OK) {
    ps_record_abort(&append->writer);
    if (s) {
      pthread_mutex_lock(&d->mutex);
      ps_ns_resource_park(d, s);
      pthread_mutex_unlock(&d->mutex);
    }
    ps_checkpoint_delta_close(append->delta);
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
uint64_t ps_namespace_append_incarnation(const ps_namespace_append *append) {
  return append ? append->incarnation : 0;
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
  return head_write(append->data, append->budget, append->segment, e);
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
      segment *prior =
          ps_ns_find(append->data, PS_CATALOG_SEGMENT, append->data->active);
      /* Keep the final tail for sparse sources. Eager sealing on durable
       * roots also avoids reopening dense predecessors for tail recovery. */
      if (prior && (prior->sparse || append->ns->root->space->durable))
        append->data->seal_pending = append->data->active;
      append->data->active = append->segment;
      ps_ns_find(append->data, PS_CATALOG_SEGMENT, append->segment)
          ->unselected = 0;
    }
    append->data->tail = tail;
    pthread_mutex_unlock(&append->data->mutex);
  }
  return rc;
}
void ps_namespace_append_close(ps_namespace_append *append) {
  struct ps_namespace_data *d;
  if (!append)
    return;
  d = append->data;
  ps_record_abort(&append->writer);
  pthread_mutex_lock(&d->mutex);
  ps_ns_resource_park(d, ps_ns_find(d, PS_CATALOG_SEGMENT, append->segment));
  pthread_mutex_unlock(&d->mutex);
  ps_checkpoint_delta_close(append->delta);
  ps_budget_close(append->budget, NULL);
  ps_ns_leave(d);
  ps_namespace_release(append->ns);
  lc_free_with_allocator(NULL, append);
}
int ps_namespace_rotate(ps_namespace *ns, uint64_t expected, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = data_get(ns, &d, 0, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  if (d->active == expected) {
    rc = ps_ns_rotate(d, NULL, e);
  } else if (expected != LC_U64_MAX && d->active == expected + 1) {
    rc = ps_ns_seal_predecessor(d, expected, e);
    if (rc == LC_OK && d->seal_pending == expected)
      d->seal_pending = 0;
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
  namespace_observation observed = {0};
  segment *s;
  int rc, entered;
  if (out)
    *out = NULL;
  if (!out || !id ||
      (kind != PS_CATALOG_SEGMENT && kind != PS_CATALOG_SNAPSHOT &&
       kind != PS_CATALOG_BODY))
    return failure(e, LC_ERR_INVALID,
                   "namespace read requires segment and output");
  rc = data_get(ns, &d, 0, e);
  if (rc != LC_OK)
    return rc;
  window = lc_calloc_with_allocator(NULL, 1, sizeof(*window));
  if (!window)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace read window");
  observed.window = window;
  observed.start = offset;
  observed.source = id;
  observed.bytes = bytes;
  rc = lock_current_scope(
      d, &entered, kind == PS_CATALOG_SEGMENT && bytes ? &observed : NULL, e);
  if (rc != LC_OK) {
    ps_stream_window_close(window->reader);
    lc_free_with_allocator(NULL, window);
    return rc;
  }
  if (!observed.ready) {
    if (kind == PS_CATALOG_SNAPSHOT) {
      rc = ps_key_checkpoint_read(d->checkpoint, id, &window->value, e);
      if (rc == LC_OK && (offset > ps_read_size(window->value) ||
                          bytes > ps_read_size(window->value) - offset))
        rc = failure(e, LC_ERR_INVALID,
                     "checkpoint read exceeds metadata value");
    } else {
      s = ps_ns_find(d, kind, id);
      if (!s || s->unselected)
        rc = failure(e, LC_ERR_INVALID, "namespace segment does not exist");
      else
        rc =
            ps_stream_window_open(s->stream, offset, bytes, &window->reader, e);
    }
    unlock_current(d, entered);
  }
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
static int view_open(ps_namespace *ns,
                     const lc_pouch_storage_namespace_info *expected,
                     uint64_t resume_segment, uint64_t start,
                     enum ps_checkpoint_capture_mode capture,
                     const lc_pouch_storage_state_refresh *refresh,
                     ps_namespace_view **out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_view *view;
  namespace_observation observed = {0};
  ps_coordination *coordination;
  ps_stream_info info;
  size_t i;
  uint64_t first, count;
  int rc, entered;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace view requires output");
  rc = data_get(ns, &d, 0, e);
  if (rc != LC_OK)
    return rc;
  view = lc_calloc_with_allocator(NULL, 1, sizeof(*view));
  if (!view)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace view");
  view->ns = ns;
  view->capture = capture;
  view->refresh = refresh;
  view->suffix = expected != NULL;
  ps_namespace_retain(ns);
  observed.view = view;
  observed.expected = expected;
  observed.start = start;
  rc = lock_current_scope(d, &entered, resume_segment ? NULL : &observed, e);
  if (rc != LC_OK) {
    ps_namespace_view_close(view);
    return rc;
  }
  if (observed.ready) {
    *out = view;
    return LC_OK;
  }
  ++d->views;
  view->registered = 1;
  coordination = ps_space_coordination(ns->root->space);
  if (ps_coordination_shared(coordination))
    rc = ps_coordination_lock(coordination, PS_COORDINATION_NAMESPACE_VIEW,
                              ns->hash, 0, &view->witness, e);
  if (rc == LC_OK) {
    if (expected)
      ps_key_checkpoint_info(d->checkpoint, &view->coverage);
    else
      rc = ps_key_checkpoint_capture(d->checkpoint, capture, &view->metadata,
                                     &view->coverage, e);
  }
  if (rc == LC_OK)
    rc = projection(d, &view->coverage, &view->projection, e);
  if (expected) {
    if (rc == LC_OK && !same_projection(expected, &view->projection))
      rc = failure(e, LC_ERR_PENDING, "namespace replay boundary changed");
    if (rc == LC_OK && (!d->active || start >= d->tail ||
                        (d->active == view->coverage.segment &&
                         start < view->coverage.offset)))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "namespace replay suffix is out of range");
    first = d->active;
    count = 1;
  } else if (resume_segment) {
    first = resume_segment;
    if (rc == LC_OK && first > d->active)
      rc = failure(e, LC_ERR_PROTOCOL, "retained replay cursor exceeds head");
    count = first <= d->active ? d->active - first + 1 : 0;
  } else {
    first = view->coverage.segment;
    if (!first || !ps_ns_find(d, PS_CATALOG_SEGMENT, first))
      ++first;
    count = d->active >= first ? d->active - first + 1 : 0;
  }
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
    c->start = expected || s->id == resume_segment ? start
               : s->id == view->coverage.segment   ? view->coverage.offset
                                                   : 0;
    strcpy(c->leaf, s->leaf);
    rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK) {
      c->tail = c->id == view->projection.active_segment ? view->projection.tail
                                                         : info.tail;
      if (c->tail > info.tail || c->start > c->tail)
        rc = failure(e, LC_ERR_PROTOCOL,
                     "namespace capture exceeds selected stream range");
      else
        rc = ps_stream_window_open(s->stream, c->start, c->tail - c->start,
                                   &c->reader, e);
    }
  }
  unlock_current(d, entered);
  if (rc != LC_OK)
    ps_namespace_view_close(view);
  else
    *out = view;
  return rc;
}
int ps_namespace_view_open(ps_namespace *ns, ps_namespace_view **out,
                           lc_error *e) {
  return view_open(ns, NULL, 0, 0, PS_CHECKPOINT_CAPTURE_FULL, NULL, out, e);
}
int ps_namespace_state_view_open(ps_namespace *ns, ps_namespace_view **out,
                                 lc_error *e) {
  return view_open(ns, NULL, 0, 0, PS_CHECKPOINT_CAPTURE_STATE, NULL, out, e);
}
int ps_namespace_view_suffix_open(ps_namespace *ns,
                                  const lc_pouch_storage_namespace_info *info,
                                  uint64_t start, ps_namespace_view **out,
                                  lc_error *e) {
  if (!info) {
    if (out)
      *out = NULL;
    return failure(e, LC_ERR_INVALID, "namespace suffix requires a boundary");
  }
  return view_open(ns, info, 0, start, PS_CHECKPOINT_CAPTURE_FULL, NULL, out,
                   e);
}
void ps_namespace_view_projection(const ps_namespace_view *view,
                                  lc_pouch_storage_namespace_info *out) {
  *out = view->projection;
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
typedef struct checkpoint_visit {
  enum ps_checkpoint_capture_mode capture;
  ps_namespace_record_fn visit;
  const lc_pouch_storage_state_refresh *refresh;
  void *context;
} checkpoint_visit;
static int visit_checkpoint_slot(void *context, ps_key key, ps_reader *reader,
                                 lc_error *e) {
  checkpoint_visit *selected = context;
  lc_allocator allocator = {0};
  lc_pouch_state_entry record = {0};
  char leaf[PS_NS_LEAF_BYTES];
  int rc;
  if (key.kind == PS_NAMESPACE_CHECKPOINT ||
      (selected->capture == PS_CHECKPOINT_CAPTURE_STATE &&
       key.kind == PS_KEY_DECISION))
    return LC_OK;
  if (!lc_pouch_namespace_format_snapshot_leaf(key.key, leaf, sizeof(leaf)))
    return failure(e, LC_ERR_PROTOCOL, "invalid checkpoint slot label");
  rc = ps_checkpoint_record_read(reader, &allocator, leaf, &record, e);
  if (rc == LC_OK) {
    if (selected->refresh)
      rc = selected->refresh->selected(selected->context, key.key, &record, e);
    else
      rc = selected->visit(selected->context, 0, &record, e);
  }
  lc_pouch_state_entry_cleanup(&allocator, &record);
  return rc;
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
  checkpoint_visit checkpoint;
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
  if (selected && rc == LC_OK && ps_values_count(view->metadata)) {
    checkpoint.capture = view->capture;
    checkpoint.visit = visit;
    checkpoint.refresh = view->refresh;
    checkpoint.context = context;
    rc = ps_values_read_walk(view->metadata, visit_checkpoint_slot, &checkpoint,
                             e);
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
  return view_visit(view, verify, view && !view->suffix, visit, context, e);
}
static int state_slot_order(const void *a, const void *b) {
  const lc_pouch_storage_state_slot *x = a, *y = b;
  return x->id < y->id ? -1 : x->id != y->id;
}
int ps_namespace_refresh_state(ps_namespace *ns,
                               lc_pouch_storage_namespace_info *info,
                               const lc_pouch_storage_state_refresh *refresh,
                               lc_error *e) {
  ps_namespace_view *view = NULL;
  lc_pouch_storage_state_slot *slots = NULL;
  size_t i, count, retained = 0;
  int rc =
      view_open(ns, NULL, 0, 0, PS_CHECKPOINT_CAPTURE_STATE, refresh, &view, e);
  if (rc == LC_OK && view->incremental &&
      view->matched_slots != refresh->previous_count)
    rc = failure(e, LC_ERR_PENDING, "checkpoint no longer has prior slots");
  if (rc == LC_OK) {
    count = ps_values_count(view->metadata);
    if (count)
      slots = lc_calloc_with_allocator(NULL, count, sizeof(*slots));
    if (count && !slots)
      rc = failure(e, LC_ERR_NOMEM, "cannot retain state slot identities");
    for (i = 0; rc == LC_OK && i < count; ++i) {
      ps_key key;
      ps_ref ref;
      uint64_t bytes;
      rc = ps_values_info(view->metadata, i, &key, &ref, &bytes, e);
      if (rc == LC_OK && key.kind == PS_KEY_METADATA) {
        slots[retained].id = key.key;
        slots[retained++].incarnation = ref.incarnation;
      }
    }
  }
  if (rc == LC_OK) {
    if (retained > 1)
      qsort(slots, retained, sizeof(*slots), state_slot_order);
    *info = view->projection;
    refresh->begin(refresh->context, view->incremental, slots, retained);
    slots = NULL;
    /* The state cache needs record metadata. Captured payload readers verify
     * their own encoded bytes, including after this metadata view is gone. */
    rc = ps_namespace_view_visit(view, 0, refresh->visit, refresh->context, e);
  }
  lc_free_with_allocator(NULL, slots);
  ps_namespace_view_close(view);
  return rc;
}
void ps_namespace_view_close(ps_namespace_view *view) {
  if (!view)
    return;
  release_view_payloads(view, 0);
  if (view->registered) {
    pthread_mutex_lock(&view->ns->data->mutex);
    --view->ns->data->views;
    ps_coordination_unlock(view->witness);
    pthread_mutex_unlock(&view->ns->data->mutex);
  } else
    ps_coordination_unlock(view->witness);
  ps_namespace_release(view->ns);
  lc_free_with_allocator(NULL, view);
}

static int checkpoint_replay(void *context, uint64_t segment,
                             const lc_pouch_state_entry *entry, lc_error *e) {
  if (!segment)
    return LC_OK; /* Selected slots were loaded by the checkpoint owner. */
  return ps_checkpoint_delta_replay(context, entry, e);
}
static int checkpoint_ready(struct ps_namespace_data *d, lc_error *e) {
  ps_checkpoint_delta *replay = NULL;
  ps_namespace_view *view = NULL;
  ps_checkpoint_info coverage;
  uint64_t segment = 0, end = 0, bytes = 0, span;
  size_t i;
  int rc, attempt, current, unchanged = 0;
  if (d->checkpoint_ready)
    return LC_OK;
  if (d->ns->root->recovering_namespaces && !d->replay_segment) {
    ps_key_checkpoint_info(d->checkpoint, &coverage);
    if (coverage.segment == d->active && coverage.offset == d->tail) {
      const ps_ns_resource *active =
          d->active ? ps_ns_find(d, PS_CATALOG_SEGMENT, d->active) : NULL;
      rc = ps_key_checkpoint_confirm_current(d->checkpoint,
                                             active ? &active->binding : NULL,
                                             d->tail, &current, e);
      if (rc != LC_OK)
        return rc;
      if (current) {
        pthread_mutex_lock(&d->mutex);
        d->checkpoint_ready = 1;
        pthread_mutex_unlock(&d->mutex);
        return LC_OK;
      }
    }
  }
  /* Observation can select the older durable seed while a release or a
   * partial checkpoint has already framed newer key slots. A writer must
   * adopt those identities before allocating slots from the stream suffix. */
  rc = ps_internal_namespace_checkpoint_stabilize(d->ns->root->space,
                                                  d->ns->identity.owner, e);
  if (rc != LC_OK)
    return rc;
  for (attempt = 0;; ++attempt) {
    rc = view_open(d->ns, NULL, d->replay_segment, d->replay_offset,
                   PS_CHECKPOINT_CAPTURE_SEED, NULL, &view, e);
    if (rc != LC_OK)
      break;
    rc = ps_key_checkpoint_confirm_seed(d->checkpoint, view->metadata,
                                        &view->coverage, e);
    if (rc != LC_ERR_PENDING)
      break;
    pthread_mutex_lock(&d->mutex);
    d->shared_valid = 0;
    pthread_mutex_unlock(&d->mutex);
    if (attempt ||
        !ps_coordination_shared(ps_space_coordination(d->ns->root->space)))
      break;
    /* An independent sync can make a previously prepared checkpoint
     * visible while this owner holds namespace authority. Rebase against
     * that selection before replaying any canonical suffix. */
    ps_namespace_view_close(view);
    view = NULL;
    lc_error_cleanup(e);
    pthread_mutex_lock(&d->mutex);
    rc = reload_shared(d, e);
    pthread_mutex_unlock(&d->mutex);
    if (rc != LC_OK)
      break;
  }
  if (rc == LC_OK && d->replay_segment && view->count == 1)
    unchanged = view->items[0].start == view->items[0].tail;
  if (rc == LC_OK && !unchanged)
    rc = ps_checkpoint_delta_open(d->checkpoint, 0, &replay, e);
  if (rc == LC_OK && !unchanged)
    rc = view_visit(view, 0, 0, checkpoint_replay, replay, e);
  if (rc == LC_OK && !unchanged) {
    segment = view->coverage.segment;
    end = view->coverage.offset;
    for (i = 0; i < view->count; ++i) {
      segment = view->items[i].id;
      end = view->items[i].tail;
      span = end - view->items[i].start;
      bytes = span > LC_U64_MAX - bytes ? LC_U64_MAX : bytes + span;
    }
  }
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    if (!unchanged)
      rc = ps_checkpoint_delta_replay_commit(replay, segment, end, bytes, e);
    if (rc == LC_OK) {
      d->checkpoint_ready = 1;
      d->replay_segment = d->replay_offset = 0;
    }
    pthread_mutex_unlock(&d->mutex);
  }
  ps_checkpoint_delta_close(replay);
  ps_namespace_view_close(view);
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
  rc = data_get(ns, &d, 0, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    d->checkpoint_observer = *observer;
    if (d->checkpoint)
      rc = ps_key_checkpoint_observe(d->checkpoint, observer, e);
    pthread_mutex_unlock(&d->mutex);
  }
  return rc;
}
int ps_ns_enter_checkpoint(ps_namespace *ns, struct ps_namespace_data **out,
                           lc_error *e) {
  struct ps_namespace_data *d;
  int rc = data_get(ns, &d, 0, e);
  *out = NULL;
  if (rc == LC_OK)
    rc = ps_ns_enter(d, e);
  if (rc != LC_OK)
    return rc;
  rc = checkpoint_ready(d, e);
  if (rc == LC_OK)
    *out = d;
  else
    ps_ns_leave(d);
  return rc;
}
int ps_namespace_checkpoint_recover(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = ps_ns_enter_checkpoint(ns, &d, e);
  if (rc == LC_OK)
    ps_ns_leave(d);
  return rc;
}
int ps_namespace_admission_proven_locked(ps_space *s, uint64_t owner,
                                         uint64_t prefix, int *proven,
                                         int *private_failure, lc_error *e) {
  ps_checkpoint_info coverage = {0};
  ps_catalog_info binding;
  ps_key stream = {0, 0, PS_STREAM};
  unsigned char head[NS_HEAD_BYTES];
  char leaf[PS_NS_LEAF_BYTES];
  uint64_t token, tail;
  int current = 0, rc = LC_OK;
  *proven = 0;
  stream.owner = owner;
  if (!ps_ns_retention_recovery_needed(s, owner))
    rc = ps_internal_fixed_value(s, head_key(owner), prefix, head, sizeof(head),
                                 0, &token, &current, private_failure, e);
  if (rc == LC_OK && current && token)
    rc = decode_head(owner, head, &stream.key, e);
  if (rc == LC_OK && current)
    rc = ps_checkpoint_point(s, owner, prefix, 0, &coverage, &current, e);
  if (rc == LC_OK && current && coverage.segment > stream.key)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "checkpoint coverage exceeds namespace head");
  if (rc == LC_OK && current && coverage.segment == stream.key)
    *proven = !stream.key ||
              (active_binding(s, stream, prefix, leaf, &binding) &&
               ps_internal_admitted_tail(s, stream, &binding.target, &tail) &&
               coverage.offset == tail);
  return rc;
}
int ps_namespace_admission_proven(ps_space *s, uint64_t owner, int *proven,
                                  lc_error *e) {
  ps_coordination_guard *guard;
  uint64_t prefix;
  int rc;
  *proven = 0;
  if (!ps_readiness_joined(s))
    return LC_OK;
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_capture_begin(s, &guard, e);
  if (rc == LC_OK) {
    prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
    rc =
        ps_namespace_admission_proven_locked(s, owner, prefix, proven, NULL, e);
  }
  rc = ps_internal_capture_end(s, guard, rc, e);
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
/* The opening root has no local callers. Acquire the usual namespace
 * authority and install the recovered owner directly, with no placeholder
 * for a shared handoff to reconcile. */
static int open_checkpoint_data(ps_namespace *ns,
                                struct ps_namespace_data **out, lc_error *e) {
  ps_space *space = ns->root->space;
  struct ps_namespace_data *d = NULL;
  ps_coordination_guard *edit;
  int continued, rc;
  if (__atomic_load_n(&ns->data, __ATOMIC_ACQUIRE) ||
      !ps_coordination_shared(ps_space_coordination(space)))
    return ps_ns_enter_checkpoint(ns, out, e);
  *out = NULL;
  rc = ps_namespace_guard(ns, &edit, e);
  if (rc == LC_OK && edit)
    rc = ps_internal_namespace_stamp(space, ns->identity.owner, &continued, e);
  if (rc == LC_OK)
    rc = load(ns, NULL, &d, e);
  if (rc != LC_OK)
    return ps_namespace_unguard(ns, edit, rc, e);
  d->scope = PS_NS_EDITING;
  d->edit = edit;
  d->shared_valid = 1;
  __atomic_store_n(&ns->data, d, __ATOMIC_RELEASE);
  rc = checkpoint_ready(d, e);
  if (rc == LC_OK)
    *out = d;
  else
    ps_ns_leave(d);
  return rc;
}
int ps_namespace_recover_on_open(ps_namespace *ns, lc_error *e) {
  ps_checkpoint_info coverage;
  struct ps_namespace_data *d;
  int rc = open_checkpoint_data(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  /* Retain any state sources or capacity obligations validated by recovery. */
  ps_key_checkpoint_info(d->checkpoint, &coverage);
  if (d->active && coverage.segment == d->active &&
      coverage.offset == d->tail && !ps_namespace_data_budgets(d)) {
    segment *s = ps_ns_find(d, PS_CATALOG_SEGMENT, d->active);
    rc = ps_internal_certify_tail(ns->root->space, s->binding.stream,
                                  &s->binding.target, d->tail, e);
  }
  if (rc == LC_OK && !ns->root->space->readiness)
    ps_key_checkpoint_note_recovered(d->checkpoint);
  ps_ns_leave(d);
  if (rc == LC_OK && !d->active && !d->count && !coverage.epoch &&
      !ps_namespace_data_budgets(d)) {
    __atomic_store_n(&ns->data, NULL, __ATOMIC_RELEASE);
    ps_namespace_data_destroy(d);
  }
  return rc;
}
int ps_namespace_checkpoint_flush(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = data_get(ns, &d, 0, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_ns_enter(d, e);
  if (rc == LC_OK) {
    rc = checkpoint_ready(d, e);
    if (rc == LC_OK)
      rc = ps_ns_finish_seal(d, e);
    if (rc == LC_OK) {
      pthread_mutex_lock(&d->mutex);
      if (d->active && !d->tail)
        rc = ps_key_checkpoint_cover_empty(d->checkpoint, d->active, e);
      if (rc == LC_OK)
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
  uint64_t refs = LC_U64_MAX;
  if (ps_ns_data_get(ns, &d, NULL) != LC_OK)
    return refs;
  pthread_mutex_lock(&d->mutex);
  if (d->checkpoint_ready)
    refs = ps_key_checkpoint_source_refs(d->checkpoint, kind, id);
  pthread_mutex_unlock(&d->mutex);
  return refs;
}
uint64_t ps_namespace_source_bytes(ps_namespace *ns, unsigned int kind,
                                   uint64_t id) {
  struct ps_namespace_data *d;
  uint64_t bytes = LC_U64_MAX;
  if (ps_ns_data_lookup(ns, &d, NULL) != LC_OK)
    return bytes;
  pthread_mutex_lock(&d->mutex);
  if (d->checkpoint_ready)
    bytes = ps_key_checkpoint_source_bytes(d->checkpoint, kind, id);
  pthread_mutex_unlock(&d->mutex);
  return bytes;
}
