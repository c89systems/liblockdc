#include "catalog.h"
#include "lc_api_internal.h"
#include "space_internal.h"
#include <string.h>

struct ps_catalog_view {
  ps_space *space;
  ps_entry *entry, *target;
};
static int cat_error(lc_error *e, int rc, const char *message) {
  return lc_error_set(e, rc, 0, message, NULL, NULL, "pouch-catalog");
}
static uint64_t read64(const unsigned char *p) {
  uint64_t n = 0;
  size_t i;
  for (i = 0; i < 8; ++i)
    n |= (uint64_t)p[i] << (i * 8);
  return n;
}
static uint32_t read32(const unsigned char *p) {
  return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 |
         (uint32_t)p[3] << 24;
}
static void write64(unsigned char *p, uint64_t n) {
  size_t i;
  for (i = 0; i < 8; ++i)
    p[i] = (unsigned char)(n >> (i * 8));
}
static void write32(unsigned char *p, uint32_t n) {
  size_t i;
  for (i = 0; i < 4; ++i)
    p[i] = (unsigned char)(n >> (i * 8));
}
static int arguments(unsigned int kind, const void *name, size_t length) {
  return kind <= PS_CATALOG_CONTROL && (name || !length) &&
         length <= PS_CATALOG_NAME_MAX;
}
static uint64_t name_hash(uint64_t owner, unsigned int kind, const void *name,
                          size_t length) {
  const unsigned char *p = name;
  uint64_t h = UINT64_C(14695981039346656037) ^ owner;
  size_t i;
  h ^= (uint64_t)kind * UINT64_C(0x9e3779b97f4a7c15);
  for (i = 0; i < length; ++i) {
    h ^= p[i];
    h *= UINT64_C(1099511628211);
  }
  h ^= h >> 33;
  return h;
}
static ps_entry *named(ps_space *s, uint64_t owner, unsigned int kind,
                       const void *name, size_t length) {
  ps_entry *p;
  uint64_t hash = name_hash(owner, kind, name, length);
  if (!s->catalogs.capacity)
    return NULL;
  for (p = s->catalogs.buckets[hash & (s->catalogs.capacity - 1)]; p;
       p = p->catalog->next)
    if (p->key.owner == owner && p->catalog->kind == kind &&
        p->catalog->length == length &&
        (!length || !memcmp(p->catalog->name, name, length)))
      return p;
  return NULL;
}
static void link_name(ps_space *s, ps_entry *p) {
  ps_catalog_record *r = p->catalog;
  ps_entry **bucket =
      &s->catalogs.buckets[r->hash & (s->catalogs.capacity - 1)];
  r->next = *bucket;
  r->link = bucket;
  if (r->next)
    r->next->catalog->link = &r->next;
  *bucket = p;
  ++s->catalogs.count;
}
void ps_catalog_unlink(ps_space *s, ps_entry *p) {
  ps_catalog_record *r = p->catalog;
  if (!r || !r->link)
    return;
  *r->link = r->next;
  if (r->next)
    r->next->catalog->link = r->link;
  r->next = NULL;
  r->link = NULL;
  --s->catalogs.count;
}
static int reserve_index(ps_space *s, lc_error *e) {
  ps_entry **buckets, *p, *next;
  size_t capacity, i, old = s->catalogs.capacity;
  if (old && s->catalogs.count < old - old / 4)
    return LC_OK;
  capacity = old ? old * 2 : 16;
  if (capacity < old || capacity > (size_t)-1 / sizeof(*buckets))
    return cat_error(e, LC_ERR_NOMEM, "catalog name index too large");
  buckets = lc_calloc_with_allocator(NULL, capacity, sizeof(*buckets));
  if (!buckets)
    return cat_error(e, LC_ERR_NOMEM, "cannot grow catalog name index");
  for (i = 0; i < old; ++i) {
    for (p = s->catalogs.buckets[i]; p; p = next) {
      next = p->catalog->next;
      p->catalog->next = buckets[p->catalog->hash & (capacity - 1)];
      buckets[p->catalog->hash & (capacity - 1)] = p;
    }
  }
  lc_free_with_allocator(NULL, s->catalogs.buckets);
  s->catalogs.buckets = buckets;
  s->catalogs.capacity = capacity;
  for (i = 0; i < capacity; ++i) {
    ps_entry **link = &buckets[i];
    while (*link) {
      (*link)->catalog->link = link;
      link = &(*link)->catalog->next;
    }
  }
  return LC_OK;
}
static int attach_record(ps_entry *p, unsigned int kind, const void *name,
                         size_t length, lc_error *e) {
  p->catalog = lc_calloc_with_allocator(NULL, 1, sizeof(*p->catalog));
  if (p->catalog)
    p->catalog->name = lc_alloc_with_allocator(NULL, length + 1);
  if (!p->catalog || !p->catalog->name)
    return cat_error(e, LC_ERR_NOMEM, "cannot allocate catalog record");
  if (length)
    memcpy(p->catalog->name, name, length);
  p->catalog->name[length] = 0;
  p->catalog->length = length;
  p->catalog->kind = kind;
  p->catalog->hash = name_hash(p->key.owner, kind, name, length);
  return LC_OK;
}
void ps_catalog_free_record(ps_entry *p) {
  if (p && p->catalog) {
    lc_free_with_allocator(NULL, p->catalog->name);
    lc_free_with_allocator(NULL, p->catalog);
    p->catalog = NULL;
  }
}
void ps_catalog_encode_record(unsigned char *wire, const ps_entry *p) {
  const ps_catalog_record *r = p->catalog;
  write32(wire + 64, r->kind);
  write32(wire + 68, (uint32_t)r->length);
  write32(wire + 72, r->stream.kind);
  write64(wire + 80, r->stream.owner);
  write64(wire + 88, r->stream.key);
  write64(wire + 96, r->token);
  if (r->length)
    memcpy(wire + 104, r->name, r->length);
}
int ps_catalog_decode_record(ps_entry *p, const unsigned char *wire,
                             size_t bytes, lc_error *e) {
  size_t length;
  int rc;
  if (bytes < 104)
    return cat_error(e, LC_ERR_PROTOCOL, "truncated catalog record");
  length = read32(wire + 68);
  if (!arguments(read32(wire + 64), wire + 104, length) ||
      bytes != 104 + length || read32(wire + 72) != PS_STREAM ||
      read32(wire + 76) || !read64(wire + 96))
    return cat_error(e, LC_ERR_PROTOCOL, "invalid catalog record");
  rc = attach_record(p, read32(wire + 64), wire + 104, length, e);
  if (rc == LC_OK) {
    p->catalog->stream.kind = PS_STREAM;
    p->catalog->stream.owner = read64(wire + 80);
    p->catalog->stream.key = read64(wire + 88);
    p->catalog->token = read64(wire + 96);
  }
  return rc;
}
static ps_entry *target_entry(ps_space *s, const ps_catalog_record *r) {
  ps_entry *p = ps_internal_lookup(s, r->stream);
  return p && !p->deleted && p->key.kind == PS_STREAM && p->token == r->token
             ? p
             : NULL;
}
int ps_catalog_prepare_record(ps_space *s, ps_entry *p, int checkpoint,
                              lc_error *e) {
  ps_entry *old = named(s, p->key.owner, p->catalog->kind, p->catalog->name,
                        p->catalog->length);
  ps_entry *by_id = ps_internal_lookup(s, p->key);
  if (!p->key.key || p->token < p->key.key || (by_id && by_id != old) ||
      (checkpoint && old) ||
      (!checkpoint && old &&
       (old->deleted ? old->key.key == p->key.key
                     : old->key.key != p->key.key || old->token >= p->token)))
    return cat_error(e, LC_ERR_PROTOCOL, "catalog name identity mismatch");
  if (!checkpoint) {
    p->catalog->target = target_entry(s, p->catalog);
    if (!p->catalog->target)
      return cat_error(e, LC_ERR_PROTOCOL,
                       "catalog target is stale or retired");
  }
  return reserve_index(s, e);
}
void ps_catalog_apply_record(ps_space *s, ps_entry *p, uint64_t sequence) {
  ps_entry *old = named(s, p->key.owner, p->catalog->kind, p->catalog->name,
                        p->catalog->length);
  if (old) {
    if (!old->deleted && old->catalog->target) {
      --old->catalog->target->catalog_refs;
      if (sequence > old->catalog->target->catalog_release)
        old->catalog->target->catalog_release = sequence;
    }
    ps_catalog_unlink(s, old);
  }
  if (p->catalog->target)
    ++p->catalog->target->catalog_refs;
  link_name(s, p);
}
int ps_catalog_validate_directory(ps_space *s, lc_error *e) {
  size_t i;
  ps_entry *p;
  for (i = 0; i < s->catalogs.capacity; ++i)
    for (p = s->catalogs.buckets[i]; p; p = p->catalog->next) {
      p->catalog->target = target_entry(s, p->catalog);
      if (!p->catalog->target)
        return cat_error(e, LC_ERR_PROTOCOL,
                         "checkpoint catalog target missing");
      ++p->catalog->target->catalog_refs;
    }
  return LC_OK;
}
void ps_catalog_drop_record(ps_space *s, ps_entry *p, uint64_t sequence) {
  --p->catalog->target->catalog_refs;
  if (sequence > p->catalog->target->catalog_release)
    p->catalog->target->catalog_release = sequence;
  ps_internal_remove(s, p, sequence);
}
static int ref_matches(ps_space *s, ps_entry *p, const ps_ref *ref) {
  return ref && !memcmp(ref->container, s->identity, 16) && p && !p->deleted &&
         ref->incarnation == p->token;
}
static int pending(ps_space *s, const ps_entry *p) {
  return p && p->publication_sequence >
                  __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
}
static ps_entry *visible(ps_entry *p, uint64_t prefix) {
  if (p && p->deleted)
    return p->publication_sequence > prefix ? p : NULL;
  while (p && p->publication_sequence > prefix)
    p = p->previous;
  return p;
}
static void information(ps_space *s, const ps_entry *p, ps_catalog_info *out) {
  out->id = p->key.key;
  memcpy(out->revision.container, s->identity, 16);
  out->revision.incarnation = p->token;
  memcpy(out->target.container, s->identity, 16);
  out->target.incarnation = p->catalog->token;
  out->stream = p->catalog->stream;
}
int ps_catalog_allocate_id(ps_space *s, uint64_t *out, lc_error *e) {
  int rc;
  if (out)
    *out = 0;
  if (!s || !out)
    return cat_error(e, LC_ERR_INVALID,
                     "catalog identity requires space and output");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  if (rc == LC_OK && s->token_next > s->token_limit)
    rc = cat_error(e, LC_ERR_INVALID, "identity grant consumed; reopen");
  if (rc == LC_OK)
    *out = s->token_next++;
  pthread_mutex_unlock(&s->mutex);
  return rc;
}
int ps_catalog_put(ps_space *s, uint64_t owner, unsigned int kind,
                   const void *name, size_t length, const ps_ref *expected,
                   ps_key stream, const ps_ref *target, ps_catalog_info *out,
                   lc_error *e) {
  ps_entry *p, *old, *head;
  ps_catalog_info info;
  uint64_t sequence = 0;
  int rc, installed = 0;
  if (out)
    memset(out, 0, sizeof(*out));
  if (!s || !out || !target || stream.kind != PS_STREAM ||
      !arguments(kind, name, length))
    return cat_error(e, LC_ERR_INVALID,
                     "catalog publication arguments invalid");
  p = lc_calloc_with_allocator(NULL, 1, sizeof(*p));
  if (!p)
    return cat_error(e, LC_ERR_NOMEM, "cannot allocate catalog publication");
  p->key.kind = PS_CATALOG;
  p->key.owner = owner;
  rc = attach_record(p, kind, name, length, e);
  if (rc == LC_OK) {
    p->catalog->stream = stream;
    p->catalog->token = target->incarnation;
    pthread_mutex_lock(&s->mutex);
    rc = ps_internal_healthy(s, e);
    old = named(s, owner, kind, name, length);
    head = ps_internal_lookup(s, stream);
    if (rc == LC_OK && pending(s, old))
      rc = cat_error(e, LC_ERR_INVALID, "catalog change in flight; retry");
    if (rc == LC_OK &&
        (expected ? !ref_matches(s, old, expected) : old && !old->deleted))
      rc = cat_error(e, LC_ERR_INVALID,
                     "catalog revision changed; reload binding");
    if (rc == LC_OK &&
        (!ref_matches(s, head, target) || s->token_next > s->token_limit))
      rc = cat_error(e, LC_ERR_INVALID,
                     "catalog stream reference or identity invalid");
    if (rc == LC_OK) {
      p->token = s->token_next++;
      p->key.key = old && !old->deleted ? old->key.key : p->token;
      rc = ps_catalog_prepare_record(s, p, 0, e);
    }
    if (rc == LC_OK)
      rc = ps_internal_allowance(s, s->metadata + 104 + length,
                                 s->live_entries + 1, e);
    if (rc == LC_OK)
      rc = ps_internal_event(s, p, PS_PUT, e);
    if (rc == LC_OK) {
      sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
      /* Automatic checkpointing may collect a prior deleted name. Apply
       * resolves the current index again, rather than retaining that pointer.
       */
      ps_catalog_apply_record(s, p, sequence);
      p->publication_sequence = sequence;
      ps_internal_install(s, p);
      installed = 1;
      information(s, p, &info);
    }
    pthread_mutex_unlock(&s->mutex);
  }
  if (!installed) {
    ps_catalog_free_record(p);
    lc_free_with_allocator(NULL, p);
  }
  if (rc == LC_OK)
    rc = ps_internal_durable(s, sequence, e);
  if (rc == LC_OK)
    *out = info;
  return rc;
}
int ps_catalog_remove(ps_space *s, uint64_t owner, unsigned int kind,
                      const void *name, size_t length, const ps_ref *expected,
                      lc_error *e) {
  ps_entry *p;
  uint64_t sequence = 0;
  int rc;
  if (!s || !expected || !arguments(kind, name, length))
    return cat_error(e, LC_ERR_INVALID, "catalog removal arguments invalid");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  p = named(s, owner, kind, name, length);
  if (rc == LC_OK && (pending(s, p) || !ref_matches(s, p, expected)))
    rc = cat_error(e, LC_ERR_INVALID,
                   "catalog revision changed or busy; reload binding");
  if (rc == LC_OK)
    rc = ps_internal_event(s, p, PS_REMOVE, e);
  if (rc == LC_OK) {
    sequence = __atomic_load_n(&s->pending_sequence, __ATOMIC_RELAXED);
    ps_catalog_drop_record(s, p, sequence);
  }
  pthread_mutex_unlock(&s->mutex);
  return rc == LC_OK ? ps_internal_durable(s, sequence, e) : rc;
}
int ps_catalog_get(ps_space *s, uint64_t owner, unsigned int kind,
                   const void *name, size_t length, ps_catalog_view **out,
                   lc_error *e) {
  ps_catalog_view *view;
  ps_entry *p;
  uint64_t prefix;
  int rc;
  if (out)
    *out = NULL;
  if (!s || !out || !arguments(kind, name, length))
    return cat_error(e, LC_ERR_INVALID, "catalog lookup arguments invalid");
  view = lc_calloc_with_allocator(NULL, 1, sizeof(*view));
  if (!view)
    return cat_error(e, LC_ERR_NOMEM, "cannot allocate catalog view");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  p = named(s, owner, kind, name, length);
  p = visible(p, prefix);
  if (rc == LC_OK && p) {
    ++p->pins;
    ++p->catalog->target->pins;
    ++p->catalog->target->catalog_pins;
    ++s->readers;
    view->space = s;
    view->entry = p;
    view->target = p->catalog->target;
    *out = view;
  }
  pthread_mutex_unlock(&s->mutex);
  if (!*out)
    lc_free_with_allocator(NULL, view);
  return rc;
}
int ps_catalog_view_info(const ps_catalog_view *view, ps_catalog_info *out,
                         lc_error *e) {
  if (!view || !out)
    return cat_error(e, LC_ERR_INVALID,
                     "catalog information requires view and output");
  information(view->space, view->entry, out);
  return LC_OK;
}
void ps_catalog_view_close(ps_catalog_view *view) {
  ps_space *s;
  if (!view)
    return;
  s = view->space;
  pthread_mutex_lock(&s->mutex);
  --s->readers;
  --view->target->pins;
  --view->target->catalog_pins;
  --view->entry->pins;
  if (view->entry->state == 2 && view->entry->safe && !view->entry->pins)
    ps_internal_collect(s);
  pthread_mutex_unlock(&s->mutex);
  lc_free_with_allocator(NULL, view);
}
int ps_catalog_visit(ps_space *s, uint64_t owner, unsigned int kind,
                     ps_catalog_visit_fn callback, void *context, lc_error *e) {
  ps_entry **entries = NULL, *p, *v;
  ps_catalog_info info;
  size_t i, count = 0, at = 0;
  uint64_t prefix;
  int rc, collect = 0;
  if (!s || !callback || kind > PS_CATALOG_CONTROL)
    return cat_error(e, LC_ERR_INVALID, "catalog visit arguments invalid");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  /* Sync completion does not need the allocator mutex. Both passes must use
   * one watermark, or a newly visible name can exceed the counted vector. */
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  for (i = 0; rc == LC_OK && i < s->catalogs.capacity; ++i)
    for (p = s->catalogs.buckets[i]; p; p = p->catalog->next)
      if (p->key.owner == owner && p->catalog->kind == kind &&
          visible(p, prefix))
        ++count;
  if (rc == LC_OK && count) {
    if (count > (size_t)-1 / sizeof(*entries))
      rc = cat_error(e, LC_ERR_NOMEM, "catalog snapshot too large");
    else {
      entries = lc_calloc_with_allocator(NULL, count, sizeof(*entries));
      if (!entries)
        rc = cat_error(e, LC_ERR_NOMEM, "cannot allocate catalog snapshot");
    }
  }
  for (i = 0; rc == LC_OK && i < s->catalogs.capacity; ++i)
    for (p = s->catalogs.buckets[i]; p; p = p->catalog->next)
      if (p->key.owner == owner && p->catalog->kind == kind &&
          (v = visible(p, prefix)) != NULL) {
        entries[at++] = v;
        ++v->pins;
        ++v->catalog->target->pins;
        ++v->catalog->target->catalog_pins;
        ++s->readers;
      }
  pthread_mutex_unlock(&s->mutex);
  for (i = 0; i < at && rc == LC_OK; ++i) {
    information(s, entries[i], &info);
    rc = callback(context, entries[i]->catalog->name,
                  entries[i]->catalog->length, &info, e);
  }
  if (at) {
    pthread_mutex_lock(&s->mutex);
    for (i = 0; i < at; ++i) {
      p = entries[i];
      --p->pins;
      --p->catalog->target->pins;
      --p->catalog->target->catalog_pins;
      --s->readers;
      if (p->state == 2 && p->safe && !p->pins)
        collect = 1;
    }
    if (collect)
      ps_internal_collect(s);
    pthread_mutex_unlock(&s->mutex);
  }
  lc_free_with_allocator(NULL, entries);
  return rc;
}
static int orphan(ps_entry *p, uint64_t prefix) {
  return !p->catalog_refs && !p->catalog_pins && !p->stream_open &&
         p->catalog_release <= prefix;
}
int ps_catalog_visit_orphans(ps_space *s, ps_catalog_orphan_fn callback,
                             void *context, lc_error *e) {
  ps_entry **entries = NULL, *p;
  ps_ref ref;
  size_t i, count = 0, at = 0;
  uint64_t prefix;
  int rc, collect = 0;
  if (!s || !callback)
    return cat_error(e, LC_ERR_INVALID, "orphan visit arguments invalid");
  pthread_mutex_lock(&s->mutex);
  rc = ps_internal_healthy(s, e);
  prefix = __atomic_load_n(&s->durable_sequence, __ATOMIC_ACQUIRE);
  for (i = 0; rc == LC_OK && i < BUCKETS; ++i)
    for (p = s->stream_buckets[i]; p; p = p->stream_next)
      if (orphan(p, prefix))
        ++count;
  if (rc == LC_OK && count) {
    if (count > (size_t)-1 / sizeof(*entries))
      rc = cat_error(e, LC_ERR_NOMEM, "orphan snapshot too large");
    else {
      entries = lc_calloc_with_allocator(NULL, count, sizeof(*entries));
      if (!entries)
        rc = cat_error(e, LC_ERR_NOMEM, "cannot allocate orphan snapshot");
    }
  }
  for (i = 0; rc == LC_OK && i < BUCKETS; ++i)
    for (p = s->stream_buckets[i]; p; p = p->stream_next)
      if (orphan(p, prefix)) {
        entries[at++] = p;
        ++p->pins;
        ++s->readers;
      }
  pthread_mutex_unlock(&s->mutex);
  memcpy(ref.container, s->identity, 16);
  for (i = 0; i < at && rc == LC_OK; ++i) {
    p = entries[i];
    ref.incarnation = p->token;
    rc = callback(context, p->key, &ref, e);
  }
  if (at) {
    pthread_mutex_lock(&s->mutex);
    for (i = 0; i < at; ++i) {
      p = entries[i];
      --p->pins;
      --s->readers;
      if (p->state == 2 && p->safe && !p->pins)
        collect = 1;
    }
    if (collect)
      ps_internal_collect(s);
    pthread_mutex_unlock(&s->mutex);
  }
  lc_free_with_allocator(NULL, entries);
  return rc;
}
