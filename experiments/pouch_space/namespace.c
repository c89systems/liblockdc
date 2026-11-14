#include "namespace.h"
#include "lc_api_internal.h"
#include "lc_pouch_namespace.h"
#include "record_io.h"
#include "root_internal.h"
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define NS_HEAD_BYTES 128U
#define NS_LEAF_BYTES 29U
typedef struct segment {
  uint64_t id;
  char leaf[NS_LEAF_BYTES];
  ps_catalog_info binding;
  ps_stream *stream;
} segment;
struct ps_namespace_data {
  ps_namespace *ns;
  pthread_mutex_t mutex;
  segment *items;
  size_t count, capacity, pending_seal;
  uint64_t active, tail;
  int busy;
  int retention_loaded, retention_present;
};
struct ps_namespace_append {
  ps_namespace *ns;
  struct ps_namespace_data *data;
  ps_record_writer writer;
  uint64_t segment;
  char leaf[NS_LEAF_BYTES];
};
struct ps_namespace_window {
  ps_namespace *ns;
  ps_stream_window *reader;
};
typedef struct captured {
  uint64_t id, tail;
  char leaf[NS_LEAF_BYTES];
  ps_stream_window *reader;
} captured;
struct ps_namespace_view {
  ps_namespace *ns;
  size_t count;
  captured *items;
};
static int failure(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL,
                      rc == LC_ERR_PROTOCOL ? "pouch_namespace_corrupt" : NULL,
                      "pouch");
}
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
  ps_key key = {0, 1, PS_CONTROL};
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
static int head_write(struct ps_namespace_data *d, uint64_t active,
                      lc_error *e) {
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
  rc = ps_begin(d->ns->root->space, head_key(d->ns), &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), e);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, e);
  ps_writer_close(writer);
  return rc;
}
static int room(struct ps_namespace_data *d, lc_error *e) {
  segment *items;
  size_t capacity = d->capacity ? d->capacity * 2 : 8;
  if (d->count < d->capacity)
    return LC_OK;
  if (capacity < d->capacity || capacity > SIZE_MAX / sizeof(*items))
    return failure(e, LC_ERR_NOMEM, "namespace segment directory is too large");
  items = lc_realloc_with_allocator(NULL, d->items, capacity * sizeof(*items));
  if (!items)
    return failure(e, LC_ERR_NOMEM, "cannot grow namespace segment directory");
  d->items = items;
  d->capacity = capacity;
  return LC_OK;
}
static int collect(void *context, const void *name, size_t length,
                   const ps_catalog_info *binding, lc_error *e) {
  struct ps_namespace_data *d = context;
  segment *s;
  uint64_t id;
  char leaf[NS_LEAF_BYTES];
  int rc;
  if (length != sizeof(leaf) - 1 || memchr(name, 0, length))
    return failure(e, LC_ERR_PROTOCOL, "namespace segment label is invalid");
  memcpy(leaf, name, length);
  leaf[length] = 0;
  if (!lc_pouch_namespace_parse_segment_leaf(leaf, &id) ||
      binding->stream.kind != PS_STREAM ||
      binding->stream.owner != d->ns->identity.owner ||
      binding->stream.key != id)
    return failure(e, LC_ERR_PROTOCOL, "namespace segment binding is invalid");
  rc = room(d, e);
  if (rc != LC_OK)
    return rc;
  s = &d->items[d->count++];
  memset(s, 0, sizeof(*s));
  s->id = id;
  memcpy(s->leaf, leaf, sizeof(leaf));
  s->binding = *binding;
  return LC_OK;
}
static int compare_segment(const void *a, const void *b) {
  uint64_t x = ((const segment *)a)->id, y = ((const segment *)b)->id;
  return x < y ? -1 : x != y;
}
static int recover(ps_stream *stream, uint64_t checkpoint, uint64_t capacity,
                   uint64_t *accepted, void *context, lc_error *e) {
  segment *s = context;
  lc_allocator allocator = {0};
  lc_pouch_state_entry record = {0};
  unsigned char *scratch = NULL;
  uint64_t cursor = 0, next;
  int rc = LC_OK, found, truncated;
  while (cursor < capacity) {
    rc = lc_pouch_state_record_read(ps_record_recovery_input(stream),
                                    &allocator, s->leaf, cursor, capacity, 1,
                                    cursor >= checkpoint, &scratch, &record,
                                    &next, &found, &truncated, e);
    if (rc == LC_ERR_PROTOCOL && cursor >= checkpoint) {
      lc_error_cleanup(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK || !found)
      break;
    if (cursor < checkpoint && next > checkpoint) {
      rc = failure(e, LC_ERR_PROTOCOL,
                   "namespace checkpoint splits a canonical record");
      break;
    }
    cursor = next;
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
  if (!d)
    return;
  for (i = 0; i < d->count; ++i)
    ps_stream_close(d->items[i].stream, NULL);
  lc_free_with_allocator(NULL, d->items);
  pthread_mutex_destroy(&d->mutex);
  lc_free_with_allocator(NULL, d);
}
static int load(ps_namespace *ns, struct ps_namespace_data **out, lc_error *e) {
  struct ps_namespace_data *d;
  ps_stream_info info;
  size_t i;
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
    rc = ps_catalog_visit(ns->root->space, ns->identity.owner,
                          PS_CATALOG_SEGMENT, collect, d, e);
  if (rc == LC_OK && d->count)
    qsort(d->items, d->count, sizeof(*d->items), compare_segment);
  if (rc == LC_OK && (d->active > d->count || d->count - (size_t)d->active > 1))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "namespace head has missing or unexpected segments");
  for (i = 0; rc == LC_OK && i < d->count; ++i) {
    segment *s = &d->items[i];
    if (s->id != (uint64_t)i + 1) {
      rc = failure(e, LC_ERR_PROTOCOL, "namespace segment directory has a gap");
      break;
    }
    rc = ps_stream_open(ns->root->space, s->binding.stream, 0,
                        &s->binding.target, recover, s, &s->stream, e);
    if (rc == LC_OK)
      rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK && s->id > d->active && (info.tail || info.sealed))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "unpublished namespace segment is not empty");
    if (rc == LC_OK && s->id == d->active && info.sealed)
      rc = failure(e, LC_ERR_PROTOCOL, "namespace active segment is sealed");
    if (rc == LC_OK && s->id == d->active)
      d->tail = info.tail;
    if (rc == LC_OK && s->id < d->active && !info.sealed) {
      if (s->id + 1 != d->active)
        rc = failure(e, LC_ERR_PROTOCOL,
                     "namespace has an unexpected unsealed predecessor");
      else
        d->pending_seal = i + 1;
    }
  }
  if (rc != LC_OK)
    ps_namespace_data_destroy(d);
  else
    *out = d;
  return rc;
}
static int data_get(ps_namespace *ns, struct ps_namespace_data **out,
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
static int enter(struct ps_namespace_data *d, lc_error *e) {
  int rc = LC_OK;
  pthread_mutex_lock(&d->mutex);
  if (d->busy)
    rc = failure(e, LC_ERR_INVALID,
                 "namespace already has an active producer or rotation");
  else
    d->busy = 1;
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
static void leave(struct ps_namespace_data *d) {
  pthread_mutex_lock(&d->mutex);
  d->busy = 0;
  pthread_mutex_unlock(&d->mutex);
}
static int finish_seal(struct ps_namespace_data *d, lc_error *e) {
  int rc = LC_OK;
  if (d->pending_seal) {
    rc = ps_stream_seal(d->items[d->pending_seal - 1].stream, e);
    if (rc == LC_OK)
      d->pending_seal = 0;
  }
  return rc;
}
static int prepare(struct ps_namespace_data *d, uint64_t id, lc_error *e) {
  segment *s;
  ps_catalog_view *view = NULL;
  ps_stream_info info;
  char *leaf;
  int rc;
  if (d->count >= id)
    return LC_OK;
  pthread_mutex_lock(&d->mutex);
  rc = room(d, e);
  pthread_mutex_unlock(&d->mutex);
  if (rc != LC_OK)
    return rc;
  s = &d->items[d->count];
  memset(s, 0, sizeof(*s));
  s->id = id;
  leaf = lc_pouch_namespace_segment_leaf(NULL, id);
  if (!leaf)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace segment label");
  if (strlen(leaf) != sizeof(s->leaf) - 1) {
    lc_free_with_allocator(NULL, leaf);
    return failure(e, LC_ERR_PROTOCOL, "namespace segment label width changed");
  }
  memcpy(s->leaf, leaf, sizeof(s->leaf));
  lc_free_with_allocator(NULL, leaf);
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
    rc = ps_stream_open(d->ns->root->space, s->binding.stream, 0,
                        view ? &s->binding.target : NULL, recover, s,
                        &s->stream, e);
    if (rc == LC_ERR_INVALID && !view) {
      lc_error_cleanup(e);
      rc = ps_stream_open(d->ns->root->space, s->binding.stream, 1, NULL,
                          recover, s, &s->stream, e);
    }
  }
  if (rc == LC_OK)
    rc = ps_stream_status(s->stream, &info, e);
  if (rc == LC_OK && (info.tail || info.sealed))
    rc = failure(e, LC_ERR_PROTOCOL, "staged namespace segment is not empty");
  if (rc == LC_OK && !view)
    rc = ps_catalog_put(d->ns->root->space, d->ns->identity.owner,
                        PS_CATALOG_SEGMENT, s->leaf, strlen(s->leaf), NULL,
                        s->binding.stream, &info.ref, &s->binding, e);
  ps_catalog_view_close(view);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    ++d->count;
    pthread_mutex_unlock(&d->mutex);
  } else {
    ps_stream_close(s->stream, NULL);
    memset(s, 0, sizeof(*s));
  }
  return rc;
}
static int begin_reserve(ps_stream *stream, uint64_t minimum,
                         ps_record_writer *writer, lc_error *e) {
  int rc = ps_record_begin(stream, writer, e);
  if (rc == LC_OK)
    rc = ps_record_reserve(writer, minimum ? minimum : 1, e);
  return rc;
}
static int rotate(struct ps_namespace_data *d, ps_record_writer *writer,
                  uint64_t minimum, lc_error *e) {
  uint64_t next;
  int rc;
  if (d->active == UINT64_MAX)
    return failure(e, LC_ERR_INVALID, "namespace segment IDs exhausted");
  next = d->active + 1;
  rc = prepare(d, next, e);
  /* Retain the predecessor until the new append owns its known span. Empty
   * structural rotation has no append span to reserve. Prepared zero grants
   * survive a failed admission and are reused by the next attempt. */
  if (rc == LC_OK && writer)
    rc = begin_reserve(d->items[(size_t)next - 1].stream, minimum, writer, e);
  if (rc == LC_OK)
    rc = head_write(d, next, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&d->mutex);
    d->pending_seal = (size_t)d->active;
    d->active = next;
    d->tail = 0;
    pthread_mutex_unlock(&d->mutex);
    rc = finish_seal(d, e);
  }
  return rc;
}
/* Namespace control slot 2 is its conservative maintenance marker; slot 1 is
 * the segment head. A marker binds its owner and validates reserved bytes. */
static int retention_read(ps_namespace *ns, int *present, lc_error *e) {
  ps_key key = {0, 2, PS_CONTROL};
  unsigned char bytes[32];
  ps_reader *reader = NULL;
  size_t got = 0, i;
  int rc;
  *present = 0;
  key.owner = ns->identity.owner;
  rc = ps_read_open(ns->root->space, key, NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc != LC_OK)
    return rc;
  if (ps_read_size(reader) != sizeof(bytes))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "namespace retention hint has invalid size");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, e);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes) || memcmp(bytes, "LCPSRETN", 8) ||
      get64(bytes + 8) != key.owner || bytes[16] != 1 ||
      get64(bytes + 24) != (uint64_t)crc32(0L, bytes, 24))
    return failure(e, LC_ERR_PROTOCOL, "namespace retention hint is invalid");
  for (i = 17; i < 24; ++i)
    if (bytes[i])
      return failure(e, LC_ERR_PROTOCOL,
                     "namespace retention hint has unknown fields");
  *present = 1;
  return LC_OK;
}
int ps_namespace_retention_hint_get(ps_namespace *ns, int *out, lc_error *e) {
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID, "namespace retention hint needs output");
  /* Work enumeration reads only the marker, without opening segments or
   * replaying any canonical body. Physical admission checks poison. */
  return retention_read(ns, out, e);
}
int ps_namespace_retention_hint_ensure(ps_namespace *ns, lc_error *e) {
  struct ps_namespace_data *d;
  ps_key key = {0, 2, PS_CONTROL};
  unsigned char bytes[32];
  ps_writer *writer = NULL;
  int rc = data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  if (!d->retention_loaded) {
    rc = retention_read(ns, &d->retention_present, e);
    if (rc == LC_OK)
      d->retention_loaded = 1;
  }
  if (rc == LC_OK && !d->retention_present) {
    key.owner = ns->identity.owner;
    memset(bytes, 0, sizeof(bytes));
    memcpy(bytes, "LCPSRETN", 8);
    put64(bytes + 8, key.owner);
    bytes[16] = 1;
    put64(bytes + 24, (uint64_t)crc32(0L, bytes, 24));
    rc = ps_begin(ns->root->space, key, &writer, e);
    if (rc == LC_OK)
      rc = ps_write(writer, bytes, sizeof(bytes), e);
    if (rc == LC_OK)
      rc = ps_publish(writer, NULL, e);
    ps_writer_close(writer);
    if (rc == LC_OK)
      d->retention_present = 1;
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}

int ps_namespace_status_get(ps_namespace *ns, ps_namespace_status *out,
                            lc_error *e) {
  struct ps_namespace_data *d;
  ps_stream_info info;
  size_t i;
  int rc;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace status requires output");
  rc = data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&d->mutex);
  out->active_segment = out->segments = d->active;
  out->tail = out->runs = 0;
  for (i = 0; rc == LC_OK && i < (size_t)d->active; ++i) {
    rc = ps_stream_status(d->items[i].stream, &info, e);
    if (rc == LC_OK) {
      out->runs += info.runs;
      if (i + 1 == (size_t)d->active)
        out->tail = info.tail;
    }
  }
  pthread_mutex_unlock(&d->mutex);
  return rc;
}
int ps_namespace_append_begin(ps_namespace *ns, uint64_t target,
                              uint64_t minimum, ps_namespace_append **out,
                              lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_append *append;
  int rc;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace append requires output");
  if (minimum > (uint64_t)INT64_MAX)
    return failure(e, LC_ERR_INVALID,
                   "namespace append minimum exceeds local offset limit");
  rc = data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  append = lc_calloc_with_allocator(NULL, 1, sizeof(*append));
  if (!append)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace append");
  rc = enter(d, e);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, append);
    return rc;
  }
  rc = finish_seal(d, e);
  if (rc == LC_OK) {
    if (!d->active || (target && d->tail >= target))
      rc = rotate(d, &append->writer, minimum, e);
    else
      rc = begin_reserve(d->items[(size_t)d->active - 1].stream, minimum,
                         &append->writer, e);
  }
  if (rc == LC_OK) {
    segment *s = &d->items[(size_t)d->active - 1];
    append->segment = s->id;
    memcpy(append->leaf, s->leaf, sizeof(s->leaf));
  }
  if (rc != LC_OK) {
    ps_record_abort(&append->writer);
    leave(d);
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
int ps_namespace_append_commit(ps_namespace_append *append, lc_error *e) {
  uint64_t tail;
  int rc;
  if (!append)
    return failure(e, LC_ERR_INVALID, "namespace append is not open");
  tail = append->writer.tail;
  rc = ps_record_commit(&append->writer, e);
  /* Mutation authority remains held until append_close. Only that authority
   * reads this cache; captured readers use their independently pinned tails. */
  if (rc == LC_OK)
    append->data->tail = tail;
  return rc;
}
void ps_namespace_append_close(ps_namespace_append *append) {
  if (!append)
    return;
  ps_record_abort(&append->writer);
  leave(append->data);
  ps_namespace_release(append->ns);
  lc_free_with_allocator(NULL, append);
}
int ps_namespace_rotate(ps_namespace *ns, uint64_t expected, lc_error *e) {
  struct ps_namespace_data *d;
  int rc = data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  rc = enter(d, e);
  if (rc != LC_OK)
    return rc;
  if (d->active == expected) {
    rc = finish_seal(d, e);
    if (rc == LC_OK)
      rc = rotate(d, NULL, 0, e);
  } else if (expected != UINT64_MAX && d->active == expected + 1) {
    rc = finish_seal(d, e);
  } else {
    rc = failure(e, LC_ERR_INVALID,
                 "namespace rotation expected segment is stale");
  }
  leave(d);
  return rc;
}
int ps_namespace_read_open(ps_namespace *ns, uint64_t id, uint64_t offset,
                           uint64_t bytes, ps_namespace_window **out,
                           lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_window *window;
  int rc;
  if (out)
    *out = NULL;
  if (!out || !id)
    return failure(e, LC_ERR_INVALID,
                   "namespace read requires segment and output");
  rc = data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  window = lc_calloc_with_allocator(NULL, 1, sizeof(*window));
  if (!window)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace read window");
  pthread_mutex_lock(&d->mutex);
  if (id > d->active)
    rc = failure(e, LC_ERR_INVALID, "namespace segment does not exist");
  else
    rc = ps_stream_window_open(d->items[(size_t)id - 1].stream, offset, bytes,
                               &window->reader, e);
  pthread_mutex_unlock(&d->mutex);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, window);
    return rc;
  }
  window->ns = ns;
  ps_namespace_retain(ns);
  *out = window;
  return LC_OK;
}
lc_pouch_record_io ps_namespace_read_input(ps_namespace_window *window) {
  return ps_record_window_input(window ? window->reader : NULL);
}
void ps_namespace_read_close(ps_namespace_window *window) {
  if (!window)
    return;
  ps_stream_window_close(window->reader);
  ps_namespace_release(window->ns);
  lc_free_with_allocator(NULL, window);
}
int ps_namespace_view_open(ps_namespace *ns, ps_namespace_view **out,
                           lc_error *e) {
  struct ps_namespace_data *d;
  ps_namespace_view *view;
  ps_stream_info info;
  size_t i;
  int rc;
  if (out)
    *out = NULL;
  if (!out)
    return failure(e, LC_ERR_INVALID, "namespace view requires output");
  rc = data_get(ns, &d, e);
  if (rc != LC_OK)
    return rc;
  view = lc_calloc_with_allocator(NULL, 1, sizeof(*view));
  if (!view)
    return failure(e, LC_ERR_NOMEM, "cannot allocate namespace view");
  view->ns = ns;
  ps_namespace_retain(ns);
  pthread_mutex_lock(&d->mutex);
  view->count = (size_t)d->active;
  view->items = lc_calloc_with_allocator(NULL, view->count ? view->count : 1,
                                         sizeof(*view->items));
  if (!view->items)
    rc = failure(e, LC_ERR_NOMEM, "cannot capture namespace segments");
  for (i = 0; rc == LC_OK && i < view->count; ++i) {
    captured *c = &view->items[i];
    segment *s = &d->items[i];
    c->id = s->id;
    memcpy(c->leaf, s->leaf, sizeof(c->leaf));
    rc = ps_stream_status(s->stream, &info, e);
    if (rc == LC_OK) {
      c->tail = info.tail;
      rc = ps_stream_window_open(s->stream, 0, c->tail, &c->reader, e);
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
int ps_namespace_view_visit(ps_namespace_view *view, int verify,
                            ps_namespace_record_fn visit, void *context,
                            lc_error *e) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry record = {0};
  unsigned char *scratch = NULL;
  uint64_t cursor, next;
  size_t i;
  int rc = LC_OK, found, truncated;
  static const lc_pouch_record_io_ops ops = {replay_read, NULL, NULL, NULL,
                                             NULL};
  replay_input input;
  lc_pouch_record_io io;
  if (!view || !visit)
    return failure(e, LC_ERR_INVALID,
                   "namespace replay requires view and callback");
  memset(&input, 0, sizeof(input));
  io.ops = &ops;
  io.context = &input;
  for (i = 0; rc == LC_OK && i < view->count; ++i) {
    captured *c = &view->items[i];
    input.window = c->reader;
    input.end = c->tail;
    input.used = 0;
    cursor = 0;
    while (rc == LC_OK && cursor < c->tail) {
      rc = lc_pouch_state_record_read(io, &allocator, c->leaf, cursor, c->tail,
                                      0, verify, &scratch, &record, &next,
                                      &found, &truncated, e);
      if (rc == LC_OK && !found)
        rc = failure(e, LC_ERR_PROTOCOL,
                     "namespace canonical prefix is incomplete");
      if (rc == LC_OK)
        rc = visit(context, c->id, &record, e);
      if (rc == LC_OK)
        cursor = next;
    }
  }
  lc_pouch_state_entry_cleanup(&allocator, &record);
  lc_free_with_allocator(NULL, scratch);
  lc_free_with_allocator(NULL, input.buffer);
  return rc;
}
void ps_namespace_view_close(ps_namespace_view *view) {
  size_t i;
  if (!view)
    return;
  if (view->items)
    for (i = 0; i < view->count; ++i)
      ps_stream_window_close(view->items[i].reader);
  lc_free_with_allocator(NULL, view->items);
  ps_namespace_release(view->ns);
  lc_free_with_allocator(NULL, view);
}
