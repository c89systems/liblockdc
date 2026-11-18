#include "lc_api_internal.h"
#include "lc_pouch_namespace.h"
#include "namespace_internal.h"
#include <string.h>

static int failure(lc_error *e, const char *message) {
  return lc_error_set(e, LC_ERR_NOMEM, 0, message, NULL, NULL, "pouch");
}
int ps_namespace_parse_source_leaf(const char *leaf, unsigned int *kind,
                                   uint64_t *id) {
  uint64_t value = 0;
  size_t i, length;
  if (!leaf || !kind || !id)
    return 0;
  length = strlen(leaf);
  if (length == 28 && lc_pouch_namespace_parse_segment_leaf(leaf, id)) {
    *kind = PS_CATALOG_SEGMENT;
    return 1;
  }
  if (length == 33 && !strncmp(leaf, "snapshot-", 9) &&
      !strcmp(leaf + 29, ".log")) {
    *kind = PS_CATALOG_SNAPSHOT;
    i = 9;
  } else if (length == 29 && !strncmp(leaf, "body-", 5) &&
             !strcmp(leaf + 25, ".log")) {
    *kind = PS_CATALOG_BODY;
    i = 5;
  } else
    return 0;
  for (; i < length - 4; ++i) {
    unsigned int digit = (unsigned int)(unsigned char)leaf[i] - '0';
    if (digit > 9 || value > (UINT64_MAX - digit) / 10)
      return 0;
    value = value * 10 + digit;
  }
  if (!value)
    return 0;
  *id = value;
  return 1;
}
ps_ns_resource *ps_ns_resource_new(unsigned int kind, uint64_t id,
                                   lc_error *e) {
  ps_ns_resource *r = NULL;
  ps_ns_label *label = NULL;
  char *leaf;
  char number[32];
  int digits;
  leaf = NULL;
  if (kind == PS_CATALOG_BODY && id) {
    digits = lc_u64_format_base10(id, number, sizeof(number));
    if (digits > 0 && digits <= 20) {
      leaf = lc_alloc_with_allocator(NULL, 30);
      if (leaf) {
        memcpy(leaf, "body-", 5);
        memset(leaf + 5, '0', 20 - (size_t)digits);
        memcpy(leaf + 25 - digits, number, (size_t)digits);
        memcpy(leaf + 25, ".log", 5);
      }
    }
  } else if (kind == PS_CATALOG_SEGMENT)
    leaf = lc_pouch_namespace_segment_leaf(NULL, id);
  if (leaf) {
    r = lc_calloc_with_allocator(NULL, 1, sizeof(*r));
    label = lc_calloc_with_allocator(NULL, 1, sizeof(*label));
  }
  if (!leaf || !r || !label) {
    lc_free_with_allocator(NULL, leaf);
    lc_free_with_allocator(NULL, r);
    lc_free_with_allocator(NULL, label);
    failure(e, "cannot allocate namespace source metadata");
    return NULL;
  }
  memcpy(label->value, leaf, strlen(leaf) + 1);
  lc_free_with_allocator(NULL, leaf);
  r->id = id;
  r->kind = kind;
  r->label = label;
  r->leaf = label->value;
  return r;
}
void ps_ns_resource_free(ps_ns_resource *r) {
  if (!r)
    return;
  ps_stream_close(r->stream, NULL);
  if (!r->pooled)
    lc_free_with_allocator(NULL, r->label);
  lc_free_with_allocator(NULL, r);
}
int ps_ns_index_room(struct ps_namespace_data *d, lc_error *e) {
  ps_ns_resource **items;
  size_t capacity = d->capacity ? d->capacity * 2 : 8;
  if (d->count < d->capacity)
    return LC_OK;
  if (capacity < d->capacity || capacity > SIZE_MAX / sizeof(*items))
    return failure(e, "namespace source directory is too large");
  items = lc_realloc_with_allocator(NULL, d->items, capacity * sizeof(*items));
  if (!items)
    return failure(e, "cannot grow namespace source directory");
  d->items = items;
  d->capacity = capacity;
  return LC_OK;
}
static size_t position(struct ps_namespace_data *d, unsigned int kind,
                       uint64_t id) {
  size_t lo = 0, hi = d->count, mid;
  while (lo < hi) {
    ps_ns_resource *r;
    mid = lo + (hi - lo) / 2;
    r = d->items[mid];
    if (r->kind < kind || (r->kind == kind && r->id < id))
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}
ps_ns_resource *ps_ns_find(struct ps_namespace_data *d, unsigned int kind,
                           uint64_t id) {
  size_t at = position(d, kind, id);
  return at < d->count && d->items[at]->kind == kind && d->items[at]->id == id
             ? d->items[at]
             : NULL;
}
void ps_ns_insert(struct ps_namespace_data *d, ps_ns_resource *r) {
  size_t at = position(d, r->kind, r->id);
  memmove(d->items + at + 1, d->items + at,
          (d->count - at) * sizeof(*d->items));
  d->items[at] = r;
  ++d->count;
  if (!r->pooled) {
    r->label->next = d->labels;
    d->labels = r->label;
    r->pooled = 1;
  }
}
void ps_ns_remove(struct ps_namespace_data *d, ps_ns_resource *r) {
  size_t at = position(d, r->kind, r->id);
  if (at < d->count && d->items[at] == r) {
    memmove(d->items + at, d->items + at + 1,
            (d->count - at - 1) * sizeof(*d->items));
    --d->count;
    r->next = d->retired;
    d->retired = r;
  }
}
