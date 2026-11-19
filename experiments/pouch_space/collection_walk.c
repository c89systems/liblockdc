#include "collection_walk.h"
#include "collection_conversion_internal.h"
#include "collection_reclaim.h"
#include "lc_api_internal.h"
#include "lc_collection_chunks.h"
#include <stdlib.h>
#include <string.h>

#define WALK_ROWS 64U
#define WALK_PAGE (32U + WALK_ROWS * 48U)
#define WALK_HEADER 96U
typedef struct walk_vector {
  ps_collection_walk_item *items;
  size_t count, room;
} walk_vector;
typedef struct walk_build {
  ps_collection_conversion *work;
  lc_collection_read_storage input;
  lc_pouch_crypto *crypto;
  const char *id;
  walk_vector frames, refs;
  lc_u64 publication_segment, publication_offset;
  unsigned char scratch[40U + LC_COLLECTION_CHUNK_PAGE];
} walk_build;
struct ps_collection_walk {
  ps_collection_conversion *work;
  uint64_t collection;
  ps_ref source;
  lc_u64 count, cached_page, position, next_segment, next_offset;
  int has_cursor;
  unsigned char scratch[WALK_PAGE];
};
static int fail(lc_error *e, int code, const char *text) {
  return lc_error_set(e, code, 0L, text, NULL,
                      code == LC_ERR_PROTOCOL ? "collection_corrupt" : NULL,
                      "pouch");
}
static lc_u64 number(const unsigned char *p) {
  lc_u64 n = 0U;
  size_t i;
  for (i = 0U; i < 8U; ++i)
    n |= (lc_u64)p[i] << (8U * i);
  return n;
}
static void encode(unsigned char *p, lc_u64 n) {
  size_t i;
  for (i = 0U; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static int coordinate(lc_u64 as, lc_u64 ao, lc_u64 bs, lc_u64 bo) {
  if (as != bs) {
    if (!as || !bs)
      return as ? 1 : -1;
    as ^= LC_COLLECTION_SNAPSHOT_BIT;
    bs ^= LC_COLLECTION_SNAPSHOT_BIT;
    return as < bs ? -1 : 1;
  }
  return ao == bo ? 0 : ao < bo ? -1 : 1;
}
static int order(const void *a, const void *b) {
  const ps_collection_walk_item *x = a, *y = b;
  return coordinate(x->segment, x->offset, y->segment, y->offset);
}
static int add(walk_vector *v, const ps_collection_walk_item *item,
               lc_error *e) {
  ps_collection_walk_item *grown;
  size_t room;
  if (v->count == v->room) {
    room = v->room ? v->room * 2U : 32U;
    if (room < v->room || room > (size_t)-1 / sizeof(*grown))
      return fail(e, LC_ERR_NOMEM,
                  "collection walk metadata exceeds address space");
    grown = lc_realloc_with_allocator(NULL, v->items, room * sizeof(*grown));
    if (!grown)
      return fail(e, LC_ERR_NOMEM, "cannot allocate collection walk metadata");
    v->items = grown;
    v->room = room;
  }
  v->items[v->count++] = *item;
  return LC_OK;
}
static int mark(void *context, const lc_collection_chunk_ref *ref,
                unsigned int height, lc_error *e) {
  walk_build *b = context;
  ps_collection_walk_item item;
  lc_pouch_record_io input;
  void *pin = NULL;
  struct iovec part;
  size_t at;
  int rc;
  if (coordinate(ref->segment, ref->offset, b->publication_segment,
                 b->publication_offset) >= 0 ||
      !ref->bytes || height >= LC_COLLECTION_CHUNK_LEVELS ||
      ref->offset >
          LC_U64_MAX - ref->bytes - (height ? 40U : LC_COLLECTION_UNIT_HEADER))
    return fail(e, LC_ERR_PROTOCOL,
                "collection dependency does not precede publication");
  memset(&item, 0, sizeof(item));
  item.segment = ref->segment;
  item.offset = ref->offset;
  item.bytes = ref->bytes;
  item.height = height;
  item.kind = height ? LC_POUCH_COLLECTION_NODE : LC_POUCH_COLLECTION_CHUNK;
  item.next_offset =
      item.offset + item.bytes + (height ? 40U : LC_COLLECTION_UNIT_HEADER);
  if (height) {
    if (ref->bytes > LC_COLLECTION_CHUNK_PAGE ||
        ref->bytes % LC_COLLECTION_CHUNK_TUPLE)
      return fail(e, LC_ERR_PROTOCOL,
                  "collection dependency page is too large");
    rc = b->input.open(b->input.context, ref->segment, &input, &pin, e);
    part.iov_base = b->scratch;
    part.iov_len = 40U + (size_t)ref->bytes;
    if (rc == LC_OK)
      rc = lc_pouch_record_io_read(input, ref->offset, &part, 1, e);
    if (pin)
      b->input.close(b->input.context, pin);
    if (rc != LC_OK)
      return rc;
    for (at = 40U; at < part.iov_len; at += LC_COLLECTION_CHUNK_TUPLE)
      if (coordinate(number(b->scratch + at), number(b->scratch + at + 8U),
                     ref->segment, ref->offset) >= 0)
        return fail(e, LC_ERR_PROTOCOL,
                    "collection index is not in dependency order");
  }
  return add(&b->refs, &item, e);
}
static int alias(walk_build *b, const lc_pouch_collection_record *r,
                 lc_u64 segment, lc_error *e) {
  lc_pouch_collection_record body;
  ps_catalog_info binding;
  ps_owned_status status;
  lc_pouch_record_io input;
  ps_space *space;
  uint64_t collection;
  lc_u64 id = 0U;
  void *pin = NULL;
  char previous_leaf[64];
  int rc = lc_collection_record_segment(r->payload_container, &id, e);
  if (rc == LC_OK && (coordinate(id, r->payload_record_offset, segment,
                                 r->record_offset) >= 0 ||
                      (r->previous_segment &&
                       coordinate(r->previous_segment, r->previous_offset,
                                  segment, r->record_offset) >= 0)))
    rc = fail(e, LC_ERR_PROTOCOL,
              "collection metadata history does not precede reference");
  ps_collection_conversion_context(b->work, &space, &collection);
  if (rc == LC_OK)
    rc = ps_collection_conversion_source_get(b->work, id, &binding, e);
  if (rc == LC_OK)
    rc = ps_owned_inspect(space, binding.stream, &binding.target, &status, e);
  if (rc == LC_OK)
    rc = b->input.open(b->input.context, id, &input, &pin, e);
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_read(input, r->payload_record_offset,
                                         status.tail, r->payload_container,
                                         &body, e);
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_check_body(r, &body, e);
  if (pin)
    b->input.close(b->input.context, pin);
  pin = NULL;
  if (rc == LC_OK && r->previous_segment) {
    rc = lc_collection_record_leaf(r->previous_segment, previous_leaf, e);
    if (rc == LC_OK)
      rc = ps_collection_conversion_source_get(b->work, r->previous_segment,
                                               &binding, e);
    if (rc == LC_OK)
      rc = ps_owned_inspect(space, binding.stream, &binding.target, &status, e);
    if (rc == LC_OK)
      rc =
          b->input.open(b->input.context, r->previous_segment, &input, &pin, e);
    if (rc == LC_OK)
      rc = lc_pouch_collection_record_read(
          input, r->previous_offset, status.tail, previous_leaf, &body, e);
    if (rc == LC_OK && (body.position != r->position || body.deleted ||
                        body.generation >= r->generation ||
                        strcmp(body.append_id, r->append_id)))
      rc = fail(e, LC_ERR_PROTOCOL,
                "collection metadata history has a different identity");
    if (pin)
      b->input.close(b->input.context, pin);
  }
  return rc;
}
static int scan(walk_build *b, lc_u64 id, lc_u64 start, lc_u64 tail,
                lc_error *e) {
  lc_collection_chunk_reader *reader = NULL;
  lc_pouch_collection_record record;
  ps_collection_walk_item item;
  lc_pouch_record_io input;
  char leaf[64];
  void *pin = NULL;
  lc_u64 at = start;
  int rc = lc_collection_record_leaf(id, leaf, e);
  if (rc == LC_OK)
    rc = b->input.open(b->input.context, id, &input, &pin, e);
  while (rc == LC_OK && at < tail) {
    rc = lc_pouch_collection_record_read(input, at, tail, leaf, &record, e);
    if (rc != LC_OK)
      break;
    if (record.next_offset <= at || record.next_offset > tail) {
      rc = fail(e, LC_ERR_PROTOCOL,
                "collection framing does not advance within source");
      break;
    }
    memset(&item, 0, sizeof(item));
    item.segment = id;
    item.offset = at;
    item.next_offset = record.next_offset;
    item.bytes = record.next_offset - at;
    item.kind = record.kind;
    rc = add(&b->frames, &item, e);
    if (rc == LC_OK && record.kind == LC_POUCH_COLLECTION_STREAM) {
      reader = lc_calloc_with_allocator(NULL, 1U, sizeof(*reader));
      if (!reader)
        rc = fail(e, LC_ERR_NOMEM,
                  "cannot allocate collection dependency iterator");
      if (rc == LC_OK)
        rc = lc_collection_chunks_open(reader, NULL, b->crypto, b->id, b->input,
                                       input, &record, e);
      b->publication_segment = id;
      b->publication_offset = at;
      if (rc == LC_OK)
        rc = lc_collection_chunks_visit(reader, mark, b, e);
      if (reader)
        lc_collection_chunks_reader_close(reader);
      lc_free_with_allocator(NULL, reader);
      reader = NULL;
    } else if (rc == LC_OK && record.metadata_only)
      rc = alias(b, &record, id, e);
    at = record.next_offset;
  }
  if (pin)
    b->input.close(b->input.context, pin);
  if (rc == LC_OK) {
    memset(&item, 0, sizeof(item));
    item.segment = id;
    item.offset = item.next_offset = tail;
    rc = add(&b->frames, &item, e);
  }
  return rc;
}
static int merge(walk_build *b, walk_vector *out, lc_error *e) {
  ps_collection_walk_item *f, *r, *prior;
  size_t i, at = 0U, used = 0U;
  int rc = LC_OK, found;
  if (b->refs.count > 1U)
    qsort(b->refs.items, b->refs.count, sizeof(*r), order);
  for (i = 0U; i < b->refs.count; ++i) {
    r = &b->refs.items[i];
    if (used && !order(&b->refs.items[used - 1U], r)) {
      prior = &b->refs.items[used - 1U];
      if (prior->bytes != r->bytes || prior->height != r->height)
        return fail(e, LC_ERR_PROTOCOL,
                    "collection gives conflicting dependency references");
    } else
      b->refs.items[used++] = *r;
  }
  b->refs.count = used;
  for (i = 0U; rc == LC_OK && i < b->frames.count; ++i) {
    f = &b->frames.items[i];
    found = 0;
    while (rc == LC_OK && at < used &&
           coordinate(b->refs.items[at].segment, b->refs.items[at].offset,
                      f->segment, f->next_offset) < 0) {
      r = &b->refs.items[at++];
      if (r->segment != f->segment || r->offset < f->offset ||
          r->next_offset > f->next_offset ||
          (f->kind == LC_POUCH_COLLECTION_REGION
               ? r->height || r->offset < f->offset + 32U
               : r->kind != f->kind || r->offset != f->offset ||
                     r->next_offset != f->next_offset))
        return fail(e, LC_ERR_PROTOCOL,
                    "collection dependency escapes canonical allocation");
      if (out->count && out->items[out->count - 1U].segment == r->segment &&
          out->items[out->count - 1U].next_offset > r->offset)
        return fail(e, LC_ERR_PROTOCOL, "collection dependencies overlap");
      rc = add(out, r, e);
      found = 1;
    }
    if (rc != LC_OK)
      break;
    if (f->kind != LC_POUCH_COLLECTION_REGION &&
        f->kind != LC_POUCH_COLLECTION_NODE &&
        f->kind != LC_POUCH_COLLECTION_CHUNK) {
      if (found)
        return fail(e, LC_ERR_PROTOCOL,
                    "collection dependency overlaps publication");
      if (f->kind == LC_POUCH_COLLECTION_EDIT_BODY) {
        if (i + 1U == b->frames.count ||
            b->frames.items[i + 1U].segment != f->segment ||
            b->frames.items[i + 1U].offset != f->next_offset ||
            b->frames.items[i + 1U].kind != LC_POUCH_COLLECTION_EDIT)
          return fail(e, LC_ERR_PROTOCOL,
                      "collection edit body has no adjacent receipt");
        f->next_offset = b->frames.items[++i].next_offset;
      }
      rc = add(out, f, e);
    }
  }
  if (rc == LC_OK && at != used)
    rc = fail(e, LC_ERR_PROTOCOL,
              "collection dependency escapes selected history");
  return rc;
}
static void item_encode(unsigned char *p, const ps_collection_walk_item *r) {
  encode(p, r->segment);
  encode(p + 8U, r->offset);
  encode(p + 16U, r->next_offset);
  encode(p + 24U, r->bytes);
  encode(p + 32U, r->height);
  encode(p + 40U, r->kind);
}
static void item_decode(const unsigned char *p, ps_collection_walk_item *r) {
  r->segment = number(p);
  r->offset = number(p + 8U);
  r->next_offset = number(p + 16U);
  r->bytes = number(p + 24U);
  r->height = number(p + 32U);
  r->kind = number(p + 40U);
}
int ps_collection_walk_prepare(ps_collection_conversion *work, const char *id,
                               lc_pouch_crypto *crypto,
                               lc_collection_read_storage input, lc_error *e) {
  walk_build *b = NULL;
  walk_vector tokens = {0};
  ps_collection_resources sources;
  ps_collection_resource *source;
  ps_collection_conversion_progress progress;
  lc_collection_disk_head head;
  ps_owned_status status;
  ps_ref ref;
  ps_space *space;
  uint64_t collection;
  unsigned char *page = NULL;
  size_t i, at, n, source_count = 0U;
  lc_u64 selected, count, index = 0U, start;
  int found, rc;
  if (!work || !id || !lc_xid_is_valid(id) || !input.open || !input.close)
    return fail(e, LC_ERR_INVALID,
                "collection walk requires retained canonical inputs");
  ps_collection_conversion_context(work, &space, &collection);
  ps_collection_conversion_status(work, &progress);
  if (progress.source_segment || progress.records || progress.candidates)
    return fail(e, LC_ERR_INVALID,
                "collection walk preparation follows conversion progress");
  page = lc_calloc_with_allocator(NULL, 1U, WALK_PAGE);
  b = lc_calloc_with_allocator(NULL, 1U, sizeof(*b));
  if (!page || !b) {
    rc = fail(e, LC_ERR_NOMEM, "cannot allocate collection walk planner");
    goto done;
  }
  rc = ps_collection_conversion_meta_read(work, 0U, page, WALK_HEADER, &found,
                                          e);
  if (rc != LC_OK)
    goto done;
  if (found) {
    rc = fail(e, LC_ERR_INVALID, "collection walk plan already completed");
    goto done;
  }
  ps_collection_conversion_source_head(work, &head, &ref);
  selected = head.active - head.tail_segment + 1U;
  if (head.snapshot) {
    if (selected > LC_U64_MAX - head.snapshot_extra_parts - 1U) {
      rc = fail(e, LC_ERR_PROTOCOL,
                "collection selected source count overflows");
      goto done;
    }
    selected += head.snapshot_extra_parts + 1U;
  }
  rc = ps_collection_resources_capture(space, collection, &sources, e);
  if (rc != LC_OK) {
    ps_collection_resources_clear(&sources);
    goto done;
  }
  b->work = work;
  b->id = id;
  b->input = input;
  b->crypto = crypto;
  /* Bind and inspect every source before a dependency iterator can address it.
   * Catalog capture order is arbitrary, so scan by canonical coordinates. */
  for (i = 0U; rc == LC_OK && i < sources.count; ++i) {
    source = &sources.items[i];
    if (source->kind == PS_CATALOG_CONTROL ||
        !lc_collection_segment_selected(&head, source->id))
      continue;
    rc = ps_owned_inspect(space, source->binding.stream,
                          &source->binding.target, &status, e);
    if (rc == LC_OK && !status.sealed)
      rc = fail(e, LC_ERR_PENDING, "collection walk source must be sealed");
    if (rc == LC_OK)
      rc = ps_collection_conversion_source_bind(work, source->id,
                                                &source->binding, e);
    ++source_count;
  }
  if (rc == LC_OK && source_count != selected)
    rc = fail(e, LC_ERR_PROTOCOL, "collection selected topology is incomplete");
  for (index = 0U; rc == LC_OK && index < selected; ++index) {
    lc_u64 sid;
    ps_catalog_info binding;
    if (head.snapshot && index <= head.snapshot_extra_parts)
      sid = (head.snapshot + index) | LC_COLLECTION_SNAPSHOT_BIT;
    else
      sid = head.tail_segment + index -
            (head.snapshot ? head.snapshot_extra_parts + 1U : 0U);
    rc = ps_collection_conversion_source_get(work, sid, &binding, e);
    if (rc == LC_OK)
      rc = ps_owned_inspect(space, binding.stream, &binding.target, &status, e);
    start = sid == head.tail_segment ? head.tail_offset : 0U;
    if (rc == LC_OK && start > status.tail)
      rc = fail(e, LC_ERR_PROTOCOL, "collection selected tail exceeds source");
    if (rc == LC_OK)
      rc = scan(b, sid, start, status.tail, e);
  }
  ps_collection_resources_clear(&sources);
  if (rc == LC_OK)
    rc = merge(b, &tokens, e);
  for (at = 0U, index = 1U; rc == LC_OK && at < tokens.count; ++index) {
    n = tokens.count - at;
    if (n > WALK_ROWS)
      n = WALK_ROWS;
    memset(page, 0, WALK_PAGE);
    memcpy(page, "LCPSCWPG", 8U);
    encode(page + 8U, collection);
    encode(page + 16U, index);
    encode(page + 24U, n);
    for (i = 0U; i < n; ++i)
      item_encode(page + 32U + 48U * i, &tokens.items[at + i]);
    rc = ps_collection_conversion_meta_write(work, index, page, WALK_PAGE, e);
    at += n;
  }
  if (rc == LC_OK) {
    count = tokens.count;
    memset(page, 0, WALK_HEADER);
    memcpy(page, "LCPSCWPL", 8U);
    encode(page + 8U, collection);
    memcpy(page + 16U, ref.container, 16U);
    encode(page + 32U, ref.incarnation);
    encode(page + 40U, count);
    encode(page + 48U, source_count);
    memcpy(page + 64U, id, LC_XID_STRING_SIZE);
    rc = ps_collection_conversion_meta_write(work, 0U, page, WALK_HEADER, e);
  }
done:
  if (b) {
    lc_free_with_allocator(NULL, b->frames.items);
    lc_free_with_allocator(NULL, b->refs.items);
  }
  lc_free_with_allocator(NULL, tokens.items);
  lc_free_with_allocator(NULL, b);
  lc_free_with_allocator(NULL, page);
  return rc;
}
int ps_collection_walk_open(ps_collection_conversion *work, const char *id,
                            ps_collection_walk **out, lc_error *e) {
  ps_collection_walk *w;
  ps_space *space;
  lc_collection_disk_head head;
  unsigned char *p;
  int found, rc;
  if (out)
    *out = NULL;
  if (!work || !id || !lc_xid_is_valid(id) || !out)
    return fail(e, LC_ERR_INVALID,
                "collection walk requires identity and output");
  w = lc_calloc_with_allocator(NULL, 1U, sizeof(*w));
  if (!w)
    return fail(e, LC_ERR_NOMEM, "cannot allocate collection plan reader");
  w->work = work;
  ps_collection_conversion_context(work, &space, &w->collection);
  ps_collection_conversion_source_head(work, &head, &w->source);
  p = w->scratch;
  rc = ps_collection_conversion_meta_read(work, 0U, p, WALK_HEADER, &found, e);
  if (rc == LC_OK && !found)
    rc = fail(e, LC_ERR_INVALID, "collection walk has no completed plan");
  if (rc == LC_OK &&
      (memcmp(p, "LCPSCWPL", 8U) || number(p + 8U) != w->collection ||
       memcmp(p + 16U, w->source.container, 16U) ||
       number(p + 32U) != w->source.incarnation || !number(p + 40U) ||
       number(p + 40U) >= LC_COLLECTION_SNAPSHOT_BIT || !number(p + 48U) ||
       memcmp(p + 64U, id, LC_XID_STRING_SIZE)))
    rc = fail(e, LC_ERR_PROTOCOL, "collection walk plan header is invalid");
  if (rc == LC_OK) {
    w->count = number(p + 40U);
    *out = w;
  } else
    ps_collection_walk_close(w);
  return rc;
}
void ps_collection_walk_close(ps_collection_walk *w) {
  lc_free_with_allocator(NULL, w);
}
static int item_read(ps_collection_walk *w, lc_u64 index,
                     ps_collection_walk_item *out, lc_error *e) {
  unsigned char *p = w->scratch;
  lc_u64 page = index / WALK_ROWS + 1U, count;
  int found, rc;
  count = w->count - (page - 1U) * WALK_ROWS;
  if (count > WALK_ROWS)
    count = WALK_ROWS;
  rc = LC_OK;
  if (w->cached_page != page) {
    w->cached_page = 0U;
    rc = ps_collection_conversion_meta_read(w->work, page, p, WALK_PAGE, &found,
                                            e);
    if (rc == LC_OK && (!found || memcmp(p, "LCPSCWPG", 8U) ||
                        number(p + 8U) != w->collection ||
                        number(p + 16U) != page || number(p + 24U) != count))
      rc = fail(e, LC_ERR_PROTOCOL, "collection walk page is invalid");
    if (rc == LC_OK)
      w->cached_page = page;
  }
  if (rc == LC_OK)
    item_decode(p + 32U + (size_t)(index % WALK_ROWS) * 48U, out);
  return rc;
}
int ps_collection_walk_next(ps_collection_walk *w, lc_u64 segment,
                            lc_u64 offset, ps_collection_walk_item *out,
                            lc_u64 *next_segment, lc_u64 *next_offset,
                            int *found, lc_error *e) {
  ps_collection_walk_item item, next;
  lc_u64 lo = 0U, hi, mid;
  int rc;
  if (!w || !out || !next_segment || !next_offset || !found)
    return fail(e, LC_ERR_INVALID,
                "collection walk requires result and cursor");
  hi = w->count;
  if (w->has_cursor && segment == w->next_segment && offset == w->next_offset)
    lo = hi = w->position;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2U;
    rc = item_read(w, mid, &item, e);
    if (rc != LC_OK)
      return rc;
    if (coordinate(item.segment, item.offset, segment, offset) < 0)
      lo = mid + 1U;
    else
      hi = mid;
  }
  if (lo == w->count) {
    *found = 0;
    return LC_OK;
  }
  rc = item_read(w, lo, &item, e);
  if (rc != LC_OK)
    return rc;
  if (lo + 1U < w->count)
    rc = item_read(w, lo + 1U, &next, e);
  else {
    memset(&next, 0, sizeof(next));
    next.segment = item.segment;
    next.offset = item.offset + 1U;
  }
  if (rc == LC_OK &&
      (!item.segment || !next.segment ||
       coordinate(item.segment, item.offset, next.segment, next.offset) >= 0 ||
       (item.kind && (item.next_offset <= item.offset || !item.bytes)) ||
       (!item.kind && (item.next_offset != item.offset || item.bytes))))
    rc = fail(e, LC_ERR_PROTOCOL, "collection walk item is invalid");
  if (rc == LC_OK) {
    *out = item;
    *next_segment = next.segment;
    *next_offset = next.offset;
    *found = 1;
    w->position = lo + 1U;
    w->next_segment = next.segment;
    w->next_offset = next.offset;
    w->has_cursor = 1;
  }
  return rc;
}
