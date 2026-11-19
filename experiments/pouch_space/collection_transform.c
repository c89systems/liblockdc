#include "collection_transform.h"
#include "collection_conversion_internal.h"
#include "collection_reclaim.h"
#include "collection_walk.h"
#include "collections.h"
#include "control.h"
#include "lc_api_internal.h"
#include "lc_collection_record_recode.h"
#include "lc_collection_unit_recode.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_record.h"
#include "query_index.h"
#include "root_internal.h"
#include <string.h>

#define TRANSFORM_HANDLES 16U
#define TRANSFORM_HEADER (LC_COLLECTION_SNAPSHOT_BIT - 1U)
#define TRANSFORM_READY (LC_COLLECTION_SNAPSHOT_BIT - 2U)
#define TRANSFORM_HEADER_BYTES 64U
#define TRANSFORM_READY_BYTES (16U + LC_COLLECTION_HEAD_BYTES)
#define TRANSFORM_COPY_BATCH (256U * 1024U)

typedef struct transform_handle {
  ps_collection_segment *segment;
  lc_u64 id, used;
  unsigned int pins;
  int target;
} transform_handle;
struct ps_collection_transform {
  ps_root *root;
  ps_space *space;
  lc_pouch *pouch;
  lc_pouch_crypto *target;
  ps_collection_conversion *work;
  ps_collection_walk *walk;
  lc_collection_unit_recode *codec;
  lc_collection_disk_head source, selected;
  ps_ref source_ref;
  uint64_t collection;
  char id[LC_XID_STRING_SIZE];
  lc_u64 target_bytes, normal_base, snapshot_base;
  lc_u64 next_normal, next_snapshot, cursor_segment, cursor_offset, clock;
  lc_u64 staged_bytes;
  transform_handle handles[TRANSFORM_HANDLES], *output;
  ps_collection_relocation rows[PS_COLLECTION_CONVERSION_MAP_ROWS];
  size_t count;
  lc_pouch_collection_record current, body, converted;
  lc_collection_recode_stream stream;
  ps_collection_transform_result result;
  int failed, done, ready, dirty, root_pin;
};
static int failure(lc_error *e, int code, const char *text) {
  return lc_error_set(e, code, 0L, text, NULL,
                      code == LC_ERR_PROTOCOL ? "collection_corrupt" : NULL,
                      "pouch");
}
static lc_u64 get(const unsigned char *p) {
  lc_u64 n = 0U;
  size_t i;
  for (i = 0U; i < 8U; ++i)
    n |= (lc_u64)p[i] << (8U * i);
  return n;
}
static void put(unsigned char *p, lc_u64 n) {
  size_t i;
  for (i = 0U; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static int same_ref(const ps_ref *a, const ps_ref *b) {
  return a->incarnation == b->incarnation &&
         !memcmp(a->container, b->container, 16U);
}
static unsigned int catalog_kind(lc_u64 id) {
  return id & LC_COLLECTION_SNAPSHOT_BIT ? PS_CATALOG_SNAPSHOT
                                         : PS_CATALOG_SEGMENT;
}
static int canonical_binding(ps_collection_transform *c, lc_u64 id,
                             ps_catalog_info *info, int *found, lc_error *e) {
  unsigned char name[8];
  ps_catalog_view *view = NULL;
  int rc;
  put(name, id);
  rc = ps_catalog_get(c->space, c->collection, catalog_kind(id), name, 8U,
                      &view, e);
  if (rc == LC_OK && view)
    rc = ps_catalog_view_info(view, info, e);
  if (rc == LC_OK)
    *found = view != NULL;
  ps_catalog_view_close(view);
  return rc;
}
static void close_handles(ps_collection_transform *c) {
  size_t i;
  c->output = NULL;
  for (i = 0U; i < TRANSFORM_HANDLES; ++i) {
    ps_collection_segment_close(c->handles[i].segment);
    memset(&c->handles[i], 0, sizeof(c->handles[i]));
  }
}
static int handle_get(ps_collection_transform *c, lc_u64 id, int target,
                      int create, transform_handle **out, lc_error *e) {
  transform_handle *slot = NULL, *h;
  ps_catalog_info binding;
  char leaf[64];
  size_t i;
  int rc;
  for (i = 0U; i < TRANSFORM_HANDLES; ++i) {
    h = &c->handles[i];
    if (h->segment && h->id == id && h->target == target) {
      if (create)
        return failure(e, LC_ERR_INVALID,
                       "conversion output identity already exists");
      h->used = ++c->clock;
      *out = h;
      return LC_OK;
    }
    if (!h->pins && h != c->output &&
        (!slot || !h->segment || (slot->segment && h->used < slot->used)))
      slot = h;
  }
  if (!slot)
    return failure(e, LC_ERR_PENDING, "conversion byte handles are pinned");
  ps_collection_segment_close(slot->segment);
  memset(slot, 0, sizeof(*slot));
  if (create)
    rc = ps_collection_conversion_candidate_create(c->work, id, &slot->segment,
                                                   e);
  else if (target)
    rc =
        ps_collection_conversion_candidate_open(c->work, id, &slot->segment, e);
  else {
    rc = ps_collection_conversion_source_get(c->work, id, &binding, e);
    if (rc == LC_OK)
      rc = lc_collection_record_leaf(id, leaf, e);
    if (rc == LC_OK)
      rc = ps_collection_segment_open(
          c->space, binding.stream, 0, &binding.target,
          ps_collection_canonical_recover, leaf, &slot->segment, e);
  }
  if (rc == LC_OK) {
    slot->id = id;
    slot->target = target;
    slot->used = ++c->clock;
    *out = slot;
  }
  return rc;
}
static int source_open(void *context, lc_u64 id, lc_pouch_record_io *io,
                       void **pin, lc_error *e) {
  ps_collection_transform *c = context;
  transform_handle *h = NULL;
  int rc = handle_get(c, id, 0, 0, &h, e);
  if (rc == LC_OK) {
    ++h->pins;
    *io = ps_collection_segment_io(h->segment);
    *pin = h;
  }
  return rc;
}
static void source_close(void *context, void *pin) {
  transform_handle *h = pin;
  (void)context;
  --h->pins;
}
static lc_collection_read_storage source_access(ps_collection_transform *c) {
  lc_collection_read_storage access;
  memset(&access, 0, sizeof(access));
  access.open = source_open;
  access.close = source_close;
  access.context = c;
  return access;
}
static int find_mapping(ps_collection_transform *c, lc_u64 id, lc_u64 offset,
                        ps_collection_relocation *out, lc_error *e) {
  size_t i;
  int found, rc;
  for (i = 0U; i < c->count; ++i)
    if (c->rows[i].source_segment == id && c->rows[i].source_offset == offset) {
      *out = c->rows[i];
      return LC_OK;
    }
  rc = ps_collection_conversion_lookup(c->work, id, offset, out, &found, e);
  if (rc == LC_OK && !found)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "collection dependency has no converted mapping");
  return rc;
}
static int resolve(void *context, const lc_collection_chunk_ref *source,
                   unsigned int height, lc_collection_recode_tree *out,
                   lc_error *e) {
  ps_collection_transform *c = context;
  ps_collection_relocation map;
  int rc = find_mapping(c, source->segment, source->offset, &map, e);
  if (rc == LC_OK) {
    memset(out, 0, sizeof(*out));
    out->root.segment = map.target_segment;
    out->root.offset = map.target_offset;
    out->root.bytes = height ? source->bytes : map.stored;
    out->stored = map.stored;
    out->units = map.units;
    out->first.segment = map.first_segment;
    out->first.offset = map.first_offset;
  }
  return rc;
}
static int seal_output(ps_collection_transform *c, lc_error *e) {
  int rc = LC_OK;
  if (c->output) {
    rc = ps_collection_segment_seal(c->output->segment, e);
    if (rc == LC_OK)
      c->output = NULL;
  }
  return rc;
}
static int place(ps_collection_transform *c, lc_u64 source, lc_error *e) {
  lc_u64 tail, id;
  int snapshot = !!(source & LC_COLLECTION_SNAPSHOT_BIT), rc = LC_OK;
  if (c->output) {
    rc = lc_pouch_record_io_tail(ps_collection_segment_io(c->output->segment),
                                 &tail, e);
    if (rc == LC_OK &&
        (tail >= c->target_bytes ||
         !!(c->output->id & LC_COLLECTION_SNAPSHOT_BIT) != snapshot))
      rc = seal_output(c, e);
  }
  if (rc == LC_OK && !c->output) {
    id = snapshot ? c->next_snapshot : c->next_normal;
    if (!id || id >= LC_COLLECTION_SNAPSHOT_BIT - 1U)
      return failure(e, LC_ERR_INVALID,
                     "collection conversion identities are exhausted");
    if (snapshot)
      id |= LC_COLLECTION_SNAPSHOT_BIT;
    rc = handle_get(c, id, 1, 1, &c->output, e);
    if (rc == LC_OK) {
      if (snapshot)
        ++c->next_snapshot;
      else
        ++c->next_normal;
    }
  }
  return rc;
}
static int prune_one(ps_collection_transform *c, lc_u64 id, lc_u64 *removed,
                     lc_error *e) {
  transform_handle *h = NULL;
  int rc = handle_get(c, id, 0, 0, &h, e);
  if (rc == LC_OK && h->pins)
    rc =
        failure(e, LC_ERR_PENDING, "conversion source byte handles are pinned");
  return rc == LC_OK ? ps_collection_conversion_source_prune(
                           c->work, id, h->segment, removed, e)
                     : rc;
}
static int prune_prefix(ps_collection_transform *c, lc_error *e) {
  ps_collection_conversion_progress p;
  lc_u64 previous = 0U, id, bytes = 0U, removed;
  int rc = LC_OK;
  ps_collection_conversion_status(c->work, &p);
  if (!p.source_segment)
    return LC_OK;
  id = p.source_segment;
  if (id & LC_COLLECTION_SNAPSHOT_BIT) {
    if ((id & ~LC_COLLECTION_SNAPSHOT_BIT) > c->source.snapshot)
      previous = id - 1U;
  } else if (id > c->source.tail_segment)
    previous = id - 1U;
  else if (c->source.snapshot)
    previous = (c->source.snapshot + c->source.snapshot_extra_parts) |
               LC_COLLECTION_SNAPSHOT_BIT;
  if (previous) {
    rc = prune_one(c, previous, &removed, e);
    if (rc == LC_OK)
      bytes += removed;
  }
  if (rc == LC_OK) {
    rc = prune_one(c, id, &removed, e);
    if (rc == LC_OK)
      bytes += removed;
  }
  if (rc == LC_OK && bytes)
    rc = ps_checkpoint(c->space, e);
  if (rc == LC_OK)
    c->result.reclaimed += bytes;
  return rc;
}
static int flush(ps_collection_transform *c, lc_error *e) {
  int rc;
  if (!c->dirty && !c->count && !c->output)
    return LC_OK;
  rc = seal_output(c, e);
  if (rc == LC_OK)
    rc = ps_collection_conversion_checkpoint(
        c->work, c->cursor_segment, c->cursor_offset, c->rows, c->count, e);
  if (rc == LC_OK) {
    c->count = 0U;
    c->staged_bytes = 0U;
    c->dirty = 0;
    rc = prune_prefix(c, e);
  }
  return rc;
}
static int recode_record(ps_collection_transform *c, transform_handle *source,
                         lc_u64 offset, lc_u64 limit,
                         ps_collection_relocation *map, lc_error *e) {
  lc_pouch_record_io input = ps_collection_segment_io(source->segment);
  lc_collection_recode_links links;
  lc_collection_recode_tree tree;
  lc_collection_chunk_ref ref;
  ps_collection_relocation location;
  lc_pouch_record_header header;
  unsigned char prefix[32];
  struct iovec part;
  transform_handle *h = NULL;
  lc_u64 id, tail;
  char leaf[64], target_leaf[64];
  int rc = lc_collection_record_leaf(source->id, leaf, e);
  if (rc == LC_OK)
    rc = lc_collection_record_leaf(c->output->id, target_leaf, e);
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_read(input, offset, limit, leaf,
                                         &c->current, e);
  memset(&links, 0, sizeof(links));
  if (rc == LC_OK && c->current.metadata_only) {
    rc = lc_collection_record_segment(c->current.payload_container, &id, e);
    if (rc == LC_OK)
      rc = find_mapping(c, id, c->current.payload_record_offset, &location, e);
    if (rc == LC_OK)
      rc = handle_get(c, location.target_segment, 1, 0, &h, e);
    if (rc == LC_OK)
      rc = lc_collection_record_leaf(h->id, target_leaf, e);
    if (rc == LC_OK)
      rc = lc_pouch_record_io_tail(ps_collection_segment_io(h->segment), &tail,
                                   e);
    if (rc == LC_OK)
      rc = lc_pouch_collection_record_read(ps_collection_segment_io(h->segment),
                                           location.target_offset, tail,
                                           target_leaf, &c->body, e);
    links.body = &c->body;
    if (rc == LC_OK && c->current.previous_segment) {
      rc = find_mapping(c, c->current.previous_segment,
                        c->current.previous_offset, &location, e);
      if (rc == LC_OK) {
        links.previous_segment = location.target_segment;
        links.previous_offset = location.target_offset;
      }
    }
  } else if (rc == LC_OK && c->current.stream_extents) {
    ref.segment = c->current.stream_segment;
    ref.offset = c->current.stream_offset;
    ref.bytes = c->current.stream_root_bytes;
    rc = resolve(c, &ref, c->current.stream_height, &tree, e);
    if (rc == LC_OK)
      rc = handle_get(c, tree.first.segment, 1, 0, &h, e);
    part.iov_base = prefix;
    part.iov_len = sizeof(prefix);
    if (rc == LC_OK)
      rc = lc_pouch_record_io_read(ps_collection_segment_io(h->segment),
                                   tree.first.offset, &part, 1, e);
    if (rc == LC_OK)
      rc = lc_pouch_record_header_decode(prefix, &header, e);
    if (rc == LC_OK)
      rc = lc_collection_unit_read(ps_collection_segment_io(h->segment),
                                   tree.first.offset, header.payload_len,
                                   &c->stream.first, e);
    if (rc == LC_OK) {
      c->stream.root = tree.root;
      c->stream.stored = tree.stored;
      c->stream.units = tree.units;
      c->stream.height = c->current.stream_height;
      links.stream = &c->stream;
    }
  }
  if (rc == LC_OK)
    rc = lc_collection_record_leaf(c->output->id, target_leaf, e);
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_recode(
        c->pouch, c->target, c->id, input, leaf, offset, limit, &links,
        ps_collection_segment_io(c->output->segment), target_leaf,
        &c->converted, e);
  if (rc == LC_OK) {
    memset(map, 0, sizeof(*map));
    map->source_segment = source->id;
    map->source_offset = offset;
    map->target_segment = c->output->id;
    map->target_offset = c->converted.record_offset;
    ++c->result.publications;
    c->staged_bytes += c->converted.next_offset - c->converted.record_offset;
  }
  return rc;
}
static int transform_token(ps_collection_transform *c,
                           const ps_collection_walk_item *item, lc_error *e) {
  transform_handle *source = NULL;
  ps_collection_relocation *map = &c->rows[c->count];
  lc_collection_recode_tree tree;
  lc_collection_unit unit;
  int rc = handle_get(c, item->segment, 0, 0, &source, e);
  if (rc == LC_OK)
    ++source->pins;
  if (rc == LC_OK)
    rc = place(c, item->segment, e);
  memset(map, 0, sizeof(*map));
  if (rc == LC_OK && item->kind == LC_POUCH_COLLECTION_CHUNK) {
    if (!c->codec)
      rc = lc_collection_unit_recode_open(&c->pouch->allocator, c->target,
                                          &c->codec, e);
    if (rc == LC_OK)
      rc = lc_collection_unit_recode_run(
          c->codec, ps_collection_segment_io(source->segment), item->offset,
          item->bytes, ps_collection_segment_io(c->output->segment),
          &map->target_offset, &unit, e);
    if (rc == LC_OK) {
      map->stored = unit.stored;
      map->units = 1U;
      map->first_segment = c->output->id;
      map->first_offset = map->target_offset;
      ++c->result.units;
      c->staged_bytes += LC_COLLECTION_UNIT_HEADER + unit.stored;
    }
  } else if (rc == LC_OK && item->kind == LC_POUCH_COLLECTION_NODE) {
    rc = lc_collection_index_recode(
        ps_collection_segment_io(source->segment), item->offset, item->bytes,
        (unsigned int)item->height, resolve, c,
        ps_collection_segment_io(c->output->segment), c->output->id, &tree, e);
    if (rc == LC_OK) {
      map->target_offset = tree.root.offset;
      map->stored = tree.stored;
      map->units = tree.units;
      map->first_segment = tree.first.segment;
      map->first_offset = tree.first.offset;
      c->staged_bytes += 40U + tree.root.bytes;
    }
  } else if (rc == LC_OK) {
    rc = recode_record(c, source, item->offset,
                       item->kind == LC_POUCH_COLLECTION_EDIT_BODY
                           ? item->offset + item->bytes
                           : item->next_offset,
                       map, e);
    if (rc == LC_OK && item->kind == LC_POUCH_COLLECTION_EDIT_BODY) {
      ++c->count;
      map = &c->rows[c->count];
      rc = recode_record(c, source, item->offset + item->bytes,
                         item->next_offset, map, e);
    }
  }
  if (rc == LC_OK) {
    map->source_segment = item->segment;
    if (item->kind != LC_POUCH_COLLECTION_EDIT_BODY)
      map->source_offset = item->offset;
    map->target_segment = c->output->id;
    ++c->count;
  }
  if (source && source->pins)
    --source->pins;
  return rc;
}
static int bootstrap(ps_collection_transform *c, lc_u64 preferred,
                     lc_error *e) {
  ps_collection_resources resources;
  ps_collection_conversion_progress p;
  ps_collection_resource *r;
  ps_owned_status status;
  transform_handle *h = NULL;
  unsigned char header[96];
  lc_u64 normal, snapshot;
  size_t i;
  int found, plan, rc;
  ps_collection_conversion_status(c->work, &p);
  rc = ps_collection_conversion_meta_read(c->work, TRANSFORM_HEADER, header,
                                          TRANSFORM_HEADER_BYTES, &found, e);
  if (rc != LC_OK)
    return rc;
  if (found) {
    if (memcmp(header, "LCPSCOTR", 8U) || get(header + 8U) != c->collection ||
        get(header + 40U) != c->source_ref.incarnation ||
        memcmp(header + 48U, c->source_ref.container, 16U) ||
        !get(header + 16U) || !get(header + 24U) || !get(header + 32U))
      return failure(e, LC_ERR_PROTOCOL,
                     "collection transform header is invalid");
    c->normal_base = get(header + 16U);
    c->snapshot_base = get(header + 24U);
    c->target_bytes = get(header + 32U);
  } else {
    if (p.source_segment || p.candidates || p.records)
      return failure(e, LC_ERR_INVALID,
                     "collection transform cannot adopt foreign progress");
    normal = c->source.active;
    snapshot = c->source.next_snapshot;
    rc =
        ps_collection_resources_capture(c->space, c->collection, &resources, e);
    for (i = 0U; rc == LC_OK && i < resources.count; ++i) {
      lc_u64 id = resources.items[i].id & ~LC_COLLECTION_SNAPSHOT_BIT;
      if (resources.items[i].id & LC_COLLECTION_SNAPSHOT_BIT) {
        if (snapshot < id)
          snapshot = id;
      } else if (normal < id)
        normal = id;
    }
    ps_collection_resources_clear(&resources);
    if (rc != LC_OK)
      return rc;
    if (normal >= LC_COLLECTION_SNAPSHOT_BIT - 2U ||
        snapshot >= LC_COLLECTION_SNAPSHOT_BIT - 2U)
      return failure(e, LC_ERR_INVALID,
                     "collection transform identities exhausted");
    c->normal_base = normal + 1U;
    c->snapshot_base = snapshot + 1U;
    c->target_bytes = preferred ? preferred : 65536U;
    memset(header, 0, TRANSFORM_HEADER_BYTES);
    memcpy(header, "LCPSCOTR", 8U);
    put(header + 8U, c->collection);
    put(header + 16U, c->normal_base);
    put(header + 24U, c->snapshot_base);
    put(header + 32U, c->target_bytes);
    put(header + 40U, c->source_ref.incarnation);
    memcpy(header + 48U, c->source_ref.container, 16U);
    rc = ps_collection_conversion_meta_write(c->work, TRANSFORM_HEADER, header,
                                             TRANSFORM_HEADER_BYTES, e);
    if (rc != LC_OK)
      return rc;
  }
  c->next_normal = p.normal ? p.normal + 1U : c->normal_base;
  c->next_snapshot = p.snapshot ? p.snapshot + 1U : c->snapshot_base;
  c->cursor_segment = p.source_segment;
  c->cursor_offset = p.source_offset;
  rc = ps_collection_conversion_meta_read(c->work, 0U, header, 96U, &plan, e);
  if (rc != LC_OK)
    return rc;
  if (!plan) {
    rc =
        ps_collection_resources_capture(c->space, c->collection, &resources, e);
    for (i = 0U; rc == LC_OK && i < resources.count; ++i) {
      r = &resources.items[i];
      if (r->kind != PS_CATALOG_CONTROL &&
          lc_collection_segment_selected(&c->source, r->id))
        rc = ps_collection_conversion_source_bind(c->work, r->id, &r->binding,
                                                  e);
    }
    for (i = 0U; rc == LC_OK && i < resources.count; ++i) {
      r = &resources.items[i];
      if (r->kind == PS_CATALOG_CONTROL ||
          !lc_collection_segment_selected(&c->source, r->id))
        continue;
      rc = handle_get(c, r->id, 0, 0, &h, e);
      if (rc == LC_OK)
        rc = ps_collection_segment_status(h->segment, &status, NULL, e);
      if (rc == LC_OK && !status.sealed)
        rc = ps_collection_conversion_require_completion(c->work, e);
      if (rc == LC_OK && !status.sealed)
        rc = ps_collection_segment_seal(h->segment, e);
    }
    ps_collection_resources_clear(&resources);
    if (rc == LC_OK)
      rc = ps_collection_walk_prepare(c->work, c->id, c->pouch->crypto,
                                      source_access(c), e);
    if (rc != LC_OK)
      return rc;
  }
  return ps_collection_walk_open(c->work, c->id, &c->walk, e);
}
static int bind_candidate(void *context, const void *name, size_t length,
                          const ps_catalog_info *info, lc_error *e) {
  ps_collection_transform *c = context;
  ps_catalog_info existing, created;
  lc_u64 id = get(name);
  int found, rc = canonical_binding(c, id, &existing, &found, e);
  if (rc == LC_OK && found &&
      (existing.stream.owner != info->stream.owner ||
       existing.stream.key != info->stream.key ||
       existing.stream.kind != info->stream.kind ||
       !same_ref(&existing.target, &info->target)))
    rc =
        failure(e, LC_ERR_PROTOCOL, "converted collection binding has changed");
  if (rc == LC_OK && !found)
    rc = ps_catalog_put(c->space, NULL, c->collection, catalog_kind(id), name,
                        length, NULL, info->stream, &info->target, &created, e);
  return rc;
}
static int create_active(ps_collection_transform *c, lc_error *e) {
  ps_collection_segment *active = NULL;
  ps_catalog_info binding, created;
  ps_owned_status status;
  ps_key key;
  unsigned char name[8];
  int found, rc = canonical_binding(c, c->selected.active, &binding, &found, e);
  key.owner = c->collection;
  key.key = c->selected.active;
  key.kind = PS_STREAM;
  if (rc == LC_OK && found &&
      (binding.stream.owner != key.owner || binding.stream.key != key.key ||
       binding.stream.kind != key.kind))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "converted append head has a different identity");
  if (rc == LC_OK && found)
    rc = ps_owned_inspect(c->space, key, &binding.target, &status, e);
  if (rc == LC_OK && !found) {
    rc = ps_collection_segment_open(c->space, key, 1, NULL, NULL, NULL, &active,
                                    e);
    if (rc == LC_ERR_INVALID) {
      lc_error_cleanup(e);
      rc = ps_collection_segment_open(c->space, key, 0, NULL, NULL, NULL,
                                      &active, e);
    }
    if (rc == LC_OK)
      rc = ps_collection_segment_status(active, &status, NULL, e);
    if (rc == LC_OK && (!status.tail && !status.capacity && !status.sealed)) {
      put(name, key.key);
      rc = ps_catalog_put(c->space, NULL, c->collection, PS_CATALOG_SEGMENT,
                          name, 8U, NULL, key, &status.ref, &created, e);
    } else if (rc == LC_OK)
      rc = failure(e, LC_ERR_PROTOCOL, "converted append head is not empty");
  }
  if (rc == LC_OK && (status.tail || status.capacity || status.sealed))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "converted append head is not private and empty");
  ps_collection_segment_close(active);
  return rc;
}
static int same_head(const lc_collection_disk_head *a,
                     const lc_collection_disk_head *b, int *same, lc_error *e) {
  unsigned char x[LC_COLLECTION_HEAD_BYTES], y[LC_COLLECTION_HEAD_BYTES];
  int rc = lc_collection_head_encode(a, x, e);
  if (rc == LC_OK)
    rc = lc_collection_head_encode(b, y, e);
  if (rc == LC_OK)
    *same = !memcmp(x, y, sizeof(x));
  return rc;
}
static int finish(ps_collection_transform *c, lc_error *e) {
  ps_collection_conversion_progress p;
  ps_collection_resources resources;
  lc_collection_disk_head current;
  unsigned char ready[TRANSFORM_READY_BYTES];
  size_t i;
  int selected, original, rc = LC_OK;
  if (!c->ready) {
    ps_collection_conversion_status(c->work, &p);
    c->selected = c->source;
    c->selected.tail_segment = c->normal_base;
    c->selected.tail_offset = 0U;
    c->selected.active = p.normal ? p.normal + 1U : c->normal_base;
    c->selected.next_snapshot = c->snapshot_base;
    if (p.snapshot) {
      c->selected.snapshot = c->snapshot_base;
      c->selected.snapshot_extra_parts = p.snapshot - c->snapshot_base;
      c->selected.next_snapshot = p.snapshot + 1U;
    }
    memset(ready, 0, sizeof(ready));
    memcpy(ready, "LCPSCSEL", 8U);
    put(ready + 8U, c->collection);
    rc = lc_collection_head_encode(&c->selected, ready + 16U, e);
    if (rc == LC_OK)
      rc = ps_collection_conversion_meta_write(c->work, TRANSFORM_READY, ready,
                                               sizeof(ready), e);
    if (rc != LC_OK)
      return rc;
    c->ready = 1;
  }
  rc = ps_collection_head_load(c->space, c->collection, NULL, &current, e);
  if (rc == LC_OK)
    rc = same_head(&current, &c->selected, &selected, e);
  if (rc == LC_OK && !selected) {
    rc = same_head(&current, &c->source, &original, e);
    if (rc == LC_OK && !original)
      rc = failure(e, LC_ERR_INVALID,
                   "collection changed during offline conversion");
    if (rc == LC_OK)
      rc = ps_collection_conversion_candidates(c->work, bind_candidate, c, e);
    if (rc == LC_OK)
      rc = create_active(c, e);
    if (rc == LC_OK)
      rc = ps_query_artifacts_reclaim(c->space, c->collection, e);
    if (rc == LC_OK)
      rc = ps_collection_head_store(c->space, c->collection, &c->selected, NULL,
                                    e);
  }
  if (rc == LC_OK) {
    rc =
        ps_collection_resources_capture(c->space, c->collection, &resources, e);
    for (i = 0U; rc == LC_OK && i < resources.count; ++i)
      if (!lc_collection_segment_selected(&c->selected, resources.items[i].id))
        rc = ps_collection_resources_remove(c->space, &resources,
                                            resources.items[i].id, 0, e);
    ps_collection_resources_clear(&resources);
  }
  if (rc == LC_OK) {
    close_handles(c);
    rc = ps_collection_conversion_finish(c->work, &c->selected, e);
  }
  if (rc == LC_OK)
    c->done = 1;
  return rc;
}
int ps_collection_transform_open(ps_root *root, lc_pouch *pouch,
                                 lc_pouch_crypto *target,
                                 ps_collection_conversion *work, const char *id,
                                 lc_u64 target_bytes,
                                 ps_collection_transform **out, lc_error *e) {
  ps_collection_transform *c;
  ps_space *space;
  uint64_t collection, reclaimed, skipped;
  unsigned char ready[TRANSFORM_READY_BYTES];
  int found, rc;
  if (out)
    *out = NULL;
  if (!root || !pouch || !work || !id || !lc_xid_is_valid(id) || !out ||
      target_bytes > (lc_u64)LC_I64_MAX)
    return failure(e, LC_ERR_INVALID,
                   "collection transform requires offline owners");
  ps_collection_conversion_context(work, &space, &collection);
  if (space != ps_root_space(root))
    return failure(e, LC_ERR_INVALID,
                   "collection transform belongs to another root");
  c = lc_calloc_with_allocator(&pouch->allocator, 1U, sizeof(*c));
  if (!c)
    return failure(e, LC_ERR_NOMEM,
                   "cannot allocate native collection transform");
  c->root = root;
  c->space = space;
  c->collection = collection;
  c->pouch = pouch;
  c->target = target;
  c->work = work;
  pthread_mutex_lock(&root->mutex);
  ++root->calls;
  c->root_pin = 1;
  pthread_mutex_unlock(&root->mutex);
  memcpy(c->id, id, LC_XID_STRING_SIZE);
  ps_collection_conversion_source_head(work, &c->source, &c->source_ref);
  rc = ps_collection_conversion_target_matches(work, target, e);
  if (rc == LC_OK)
    rc = ps_root_crypto_matches(root, target, e);
  if (rc == LC_OK)
    rc = ps_collection_conversion_release_cache(root, collection, e);
  if (rc == LC_OK)
    rc = ps_root_reclaim_orphans(root, &reclaimed, &skipped, e);
  if (rc == LC_OK && reclaimed)
    rc = ps_checkpoint(space, e);
  if (rc == LC_OK)
    rc = ps_collection_conversion_meta_read(work, TRANSFORM_READY, ready,
                                            sizeof(ready), &found, e);
  if (rc == LC_OK && found) {
    if (memcmp(ready, "LCPSCSEL", 8U) || get(ready + 8U) != collection)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "collection final-selection control is invalid");
    if (rc == LC_OK)
      rc = lc_collection_head_decode(ready + 16U, &c->selected, e);
    c->ready = rc == LC_OK;
  } else if (rc == LC_OK) {
    rc = bootstrap(c, target_bytes, e);
    if (rc == LC_OK)
      rc = prune_prefix(c, e);
  }
  if (rc == LC_OK)
    *out = c;
  else
    ps_collection_transform_close(c);
  return rc;
}
int ps_collection_transform_step(ps_collection_transform *c, size_t limit,
                                 int *finished,
                                 ps_collection_transform_result *out,
                                 lc_error *e) {
  ps_collection_walk_item item;
  ps_collection_conversion_progress p;
  lc_u64 next_segment, next_offset;
  size_t visited = 0U, needed;
  int found, rc = LC_OK;
  if (!c || !limit || !finished || !out || c->failed)
    return failure(e, LC_ERR_INVALID,
                   "collection transform step requires live work and limit");
  if (c->ready && !c->done)
    rc = finish(c, e);
  while (rc == LC_OK && !c->done && visited < limit) {
    rc = ps_collection_walk_next(c->walk, c->cursor_segment, c->cursor_offset,
                                 &item, &next_segment, &next_offset, &found, e);
    if (rc != LC_OK)
      break;
    if (!found) {
      rc = flush(c, e);
      if (rc == LC_OK)
        rc = finish(c, e);
      break;
    }
    needed = item.kind == LC_POUCH_COLLECTION_EDIT_BODY ? 2U
             : item.kind                                ? 1U
                                                        : 0U;
    if (c->count + needed > PS_COLLECTION_CONVERSION_MAP_ROWS)
      rc = flush(c, e);
    if (rc != LC_OK)
      break;
    if (item.kind)
      rc = transform_token(c, &item, e);
    else {
      ps_collection_conversion_status(c->work, &p);
      if ((item.segment & LC_COLLECTION_SNAPSHOT_BIT) && !p.snapshot &&
          !c->output)
        rc = place(c, item.segment, e);
    }
    if (rc != LC_OK)
      break;
    c->cursor_segment = next_segment;
    c->cursor_offset = next_offset;
    c->dirty = 1;
    ++visited;
    ++c->result.tokens;
    if (!item.kind || c->staged_bytes >= TRANSFORM_COPY_BATCH)
      rc = flush(c, e);
  }
  if (rc == LC_OK && !c->done)
    rc = flush(c, e);
  if (rc == LC_OK) {
    *finished = c->done;
    *out = c->result;
  } else
    c->failed = 1;
  return rc;
}
void ps_collection_transform_close(ps_collection_transform *c) {
  lc_allocator allocator;
  if (!c)
    return;
  allocator = c->pouch->allocator;
  lc_collection_unit_recode_close(c->codec);
  ps_collection_walk_close(c->walk);
  close_handles(c);
  if (c->root_pin) {
    pthread_mutex_lock(&c->root->mutex);
    --c->root->calls;
    pthread_cond_broadcast(&c->root->changed);
    pthread_mutex_unlock(&c->root->mutex);
  }
  lc_free_with_allocator(&allocator, c);
}
