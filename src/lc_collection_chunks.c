#include "lc_collection_chunks.h"
#include "lc_pouch_path.h"
#include "lc_pouch_record.h"
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>
#include <zlib.h>

#ifdef LOCKDC_TEST_BUILD
lc_collection_chunk_pwrite_fn lc_collection_chunks_test_pwrite;
int (*lc_collection_chunks_test_sync_fd)(int);
#endif

static void lc_ck_put(unsigned char *p, lc_u64 n) {
  size_t i;
  for (i = 0; i < 8; ++i)
    p[i] = (unsigned char)(n >> (i * 8));
}
static lc_u64 lc_ck_get(const unsigned char *p) {
  lc_u64 n = 0;
  size_t i;
  for (i = 0; i < 8; ++i)
    n |= (lc_u64)p[i] << (i * 8);
  return n;
}
static void lc_ck_tuple(unsigned char *p, const lc_collection_chunk_ref *ref) {
  lc_ck_put(p, ref->segment);
  lc_ck_put(p + 8, ref->offset);
  lc_ck_put(p + 16, ref->bytes);
}
static lc_collection_chunk_ref lc_ck_ref(const unsigned char *p) {
  lc_collection_chunk_ref ref;
  ref.segment = lc_ck_get(p);
  ref.offset = lc_ck_get(p + 8);
  ref.bytes = lc_ck_get(p + 16);
  return ref;
}
static int lc_ck_corrupt(lc_error *e) {
  return lc_collection_failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                               "collection payload index is inconsistent");
}
static int lc_ck_write_failed(lc_collection_segment *segment, lc_u64 offset,
                              int failed, lc_error *e) {
  int rc = lc_collection_segment_truncate(segment, offset, e);
  return rc == LC_OK ? failed : rc;
}
int lc_collection_chunks_storage_bytes(lc_u64 bytes, lc_u64 extents,
                                       lc_u64 *total) {
  lc_u64 count = extents, nodes, amount;
  if (!count || count > bytes || bytes > (lc_u64)LC_I64_MAX ||
      count > ((lc_u64)LC_I64_MAX - bytes) / LC_COLLECTION_UNIT_HEADER)
    return 0;
  *total = bytes + count * LC_COLLECTION_UNIT_HEADER;
  while (count > 1) {
    nodes = count / LC_COLLECTION_CHUNK_FANOUT +
            (count % LC_COLLECTION_CHUNK_FANOUT != 0);
    if (count > (lc_u64)LC_I64_MAX / LC_COLLECTION_CHUNK_TUPLE)
      return 0;
    amount = count * LC_COLLECTION_CHUNK_TUPLE;
    if (nodes > ((lc_u64)LC_I64_MAX - amount) / 40U)
      return 0;
    amount += nodes * 40U;
    if (amount > (lc_u64)LC_I64_MAX - *total)
      return 0;
    *total += amount;
    count = nodes;
  }
  return 1;
}
#ifdef LOCKDC_TEST_BUILD
static int lc_ck_phase(lc_collection_handle *c, int phase, lc_error *e) {
  return lc_collection_append_test_hook
             ? lc_collection_append_test_hook(lc_collection_append_test_context,
                                              &c->pub, phase, e)
             : LC_OK;
}
#endif
int lc_collection_chunks_write_index(lc_pouch_record_io output, lc_u64 segment,
                                     unsigned int level, const void *bytes,
                                     size_t count, lc_collection_chunk_ref *ref,
                                     lc_error *e) {
  unsigned char prefix[40];
  lc_pouch_record_header header;
  struct iovec parts[2];
  uint64_t offset;
  int rc;
  if (!output.ops || !output.ops->discard || !ref || !bytes || !count ||
      count > LC_COLLECTION_CHUNK_PAGE || count % LC_COLLECTION_CHUNK_TUPLE ||
      level >= LC_COLLECTION_CHUNK_LEVELS - 1U)
    return lc_ck_corrupt(e);
  rc = lc_pouch_record_io_tail(output, &offset, e);
  if (rc != LC_OK)
    return rc;
  if (offset > (uint64_t)LC_I64_MAX - sizeof(prefix) - count)
    return lc_ck_corrupt(e);
  memset(&header, 0, sizeof(header));
  header.type = LC_POUCH_COLLECTION_NODE;
  header.flags = LC_POUCH_RECORD_FLAG_PENDING;
  header.meta_len = sizeof(prefix) - 32U;
  header.payload_len = count;
  header.payload_crc = (unsigned long)crc32(0L, bytes, (uInt)count);
  lc_pouch_record_header_encode(&header, prefix);
  lc_ck_put(prefix + 32U, level);
  parts[0].iov_base = prefix;
  parts[0].iov_len = sizeof(prefix);
  parts[1].iov_base = (void *)bytes;
  parts[1].iov_len = count;
  rc = lc_pouch_record_io_append(output, parts, 2, e);
  if (rc == LC_OK) {
    header.flags = 0;
    lc_pouch_record_header_encode(&header, prefix);
    parts[0].iov_len = LC_COLLECTION_CHUNK_HEADER;
    rc = lc_pouch_record_io_patch(output, offset, parts, 1, e);
  }
  if (rc != LC_OK) {
    /* This metadata append owns its complete unpublished suffix. A failed
     * container repair poisons its owner before any later write admission. */
    output.ops->discard(output.context, offset);
    return rc;
  }
  ref->segment = segment;
  ref->offset = offset;
  ref->bytes = count;
  return LC_OK;
}
static int lc_ck_append_index(lc_collection_handle *c, lc_collection_chunks *s,
                              unsigned int level, const void *bytes,
                              size_t count, lc_collection_chunk_ref *ref,
                              lc_error *e) {
  lc_u64 amount = 40U + count;
  int rc;
  if (s->append_index)
    return s->append_index(s->index_context, level, bytes, count, ref, e);
  if (!c)
    return lc_ck_corrupt(e);
  /* Allocation owns rotation. This bounded carry stack and its publication
   * follow the reserved bytes without another segment handoff. */
  rc = lc_collection_chunks_write_index(c->active.unit_io, c->head.active,
                                        level, bytes, count, ref, e);
  if (rc == LC_OK)
    c->order.reclaimable_bytes =
        amount > LC_U64_MAX - c->order.reclaimable_bytes
            ? LC_U64_MAX
            : c->order.reclaimable_bytes + amount;
  return rc;
}
static int lc_ck_push(lc_collection_handle *c, lc_collection_chunks *s,
                      lc_collection_chunk_ref ref, unsigned int level,
                      lc_error *e) {
  lc_collection_chunk_level *page;
  int rc;
  for (;;) {
    if (level >= LC_COLLECTION_CHUNK_LEVELS)
      return lc_ck_corrupt(e);
    page = &s->levels[level];
    lc_ck_tuple(page->tuples + page->count++ * LC_COLLECTION_CHUNK_TUPLE, &ref);
    if (page->count < LC_COLLECTION_CHUNK_FANOUT)
      return LC_OK;
    rc = lc_ck_append_index(c, s, level, page->tuples,
                            page->count * LC_COLLECTION_CHUNK_TUPLE, &ref, e);
    if (rc != LC_OK)
      return rc;
    page->count = 0;
    ++level;
  }
}
static void lc_ck_close_region(lc_collection_chunks *s) {
  lc_collection_segment_close(&s->region);
  memset(&s->pending, 0, sizeof(s->pending));
  s->region_end = 0;
}

void lc_collection_chunks_close(lc_collection_chunks *s) {
  lc_ck_close_region(s);
  lc_collection_segment_close(&s->first_storage);
  s->first_sealed = 0;
}

int lc_collection_chunks_seal_first(lc_collection_chunks *s, lc_error *error) {
  int rc = LC_OK;
  if (!s->first_storage.ops)
    return lc_ck_corrupt(error);
  if (!s->first_sealed) {
    rc = lc_collection_segment_sync_private(&s->first_storage, error);
    if (rc == LC_OK)
      s->first_sealed = 1;
  }
  return rc;
}

static int lc_ck_region_release(lc_collection_handle *c,
                                lc_collection_chunks *s, int reuse,
                                lc_error *error) {
  size_t i;
  int rc = LC_OK;
  if (!s->pending.segment)
    return LC_OK;
  /* A peer may have sealed this inode while this upload still owned a range.
   * Its later private writes must be durable before another inode publishes
   * their reference. Usual same-inode publication shares the commit fsync. */
  /* Like ordinary segment rotation, this seal is durable even in NoSync:
   * a clean checkpoint subsequently syncs only the current active inode. */
  /* Binding seals the first segment after its final manifest update. */
  if (s->pending.segment != c->head.active &&
      s->pending.segment != s->first.segment)
    rc = lc_collection_segment_sync_private(&s->region, error);
  if (rc == LC_OK && reuse && s->pending.segment == c->head.active &&
      s->pending.offset < s->region_end &&
      s->region_end - s->pending.offset >= 65536U + LC_COLLECTION_UNIT_HEADER) {
    for (i = 0; i < LC_COLLECTION_IDLE_REGIONS; ++i) {
      lc_collection_region *idle = &c->stream_regions[i];
      if (idle->segment != c->head.active) {
        idle->segment = s->pending.segment;
        idle->next = s->pending.offset;
        idle->end = s->region_end;
        break;
      }
    }
  }
  lc_ck_close_region(s);
  return rc;
}

lc_u64 lc_collection_chunks_capacity(const lc_collection_handle *collection) {
  lc_u64 size = collection->pouch->segment_target_bytes;
  lc_u64 limit = collection->segments.private_limit;
  if (size > LC_COLLECTION_CHUNK_MAX)
    size = LC_COLLECTION_CHUNK_MAX;
  if (size < 131072U)
    size = 131072U;
  if (limit && size + 65536U + LC_COLLECTION_CHUNK_HEADER > limit) {
    if (limit <=
        65536U + LC_COLLECTION_UNIT_HEADER + LC_COLLECTION_CHUNK_HEADER)
      return 0U;
    size = limit - 65536U - LC_COLLECTION_CHUNK_HEADER;
  }
  return size + 65536U - LC_COLLECTION_UNIT_HEADER;
}

static int lc_ck_region_reserve(lc_collection_handle *c,
                                lc_collection_chunks *s, lc_u64 minimum,
                                lc_error *error) {
  lc_pouch_record_header header;
  unsigned char prefix[32];
  lc_u64 size, end;
  uint64_t offset;
  struct iovec part;
  size_t i;
  int rc;
  rc = lc_collection_rotate_stream(c, error);
  if (rc != LC_OK)
    return rc;
  for (i = 0; i < LC_COLLECTION_IDLE_REGIONS; ++i) {
    lc_collection_region *idle = &c->stream_regions[i];
    if (idle->segment == c->head.active &&
        idle->end - idle->next >= minimum + LC_COLLECTION_UNIT_HEADER) {
      rc = lc_collection_segment_clone(&c->active, &s->region, error);
      if (rc != LC_OK)
        return rc;
      s->pending.segment = idle->segment;
      s->pending.offset = idle->next;
      s->region_end = idle->end;
      idle->segment = 0;
      return LC_OK;
    }
  }
  rc = lc_pouch_record_io_tail(c->active.io, &offset, error);
  if (rc != LC_OK)
    return rc;
  size = lc_collection_chunks_capacity(c) + LC_COLLECTION_UNIT_HEADER;
  if ((lc_u64)offset > (lc_u64)LC_I64_MAX - size - 32U)
    return lc_ck_corrupt(error);
  end = (lc_u64)offset + 32U + size;
  memset(&header, 0, sizeof(header));
  header.type = LC_POUCH_COLLECTION_REGION;
  header.payload_len = size;
  lc_pouch_record_header_encode(&header, prefix);
  part.iov_base = prefix;
  part.iov_len = sizeof(prefix);
  rc = lc_pouch_record_io_append(c->active.io, &part, 1, error);
  if (rc == LC_OK)
    rc = lc_collection_segment_reserve(&c->active, end, error);
  if (rc == LC_OK)
    rc = lc_collection_segment_clone(&c->active, &s->region, error);
  if (rc != LC_OK)
    return lc_ck_write_failed(&c->active, offset, rc, error);
  s->pending.segment = c->head.active;
  s->pending.offset = (lc_u64)offset + 32U;
  s->region_end = end;
  c->order.reclaimable_bytes =
      size + 32U > LC_U64_MAX - c->order.reclaimable_bytes
          ? LC_U64_MAX
          : c->order.reclaimable_bytes + size + 32U;
  return LC_OK;
}

int lc_collection_chunks_prepare(lc_collection_handle *c,
                                 lc_collection_chunks *s, lc_u64 minimum,
                                 lc_u64 *available, lc_error *error) {
  int rc;
  if (s->pending.bytes)
    return lc_ck_corrupt(error);
  if (s->pending.segment &&
      s->region_end - s->pending.offset < LC_COLLECTION_UNIT_HEADER + minimum) {
    rc = lc_ck_region_release(c, s, 0, error);
    if (rc != LC_OK)
      return rc;
  }
  if (!s->pending.segment) {
    rc = lc_ck_region_reserve(c, s, minimum, error);
    if (rc != LC_OK)
      return rc;
  }
  if (s->region_end - s->pending.offset < LC_COLLECTION_UNIT_HEADER + minimum)
    return lc_ck_corrupt(error);
  *available = s->region_end - s->pending.offset - LC_COLLECTION_UNIT_HEADER;
  return LC_OK;
}

int lc_collection_chunks_end_unit(lc_collection_handle *c,
                                  lc_collection_chunks *s,
                                  const lc_collection_unit *unit,
                                  lc_error *error) {
  lc_collection_chunk_ref ref = s->pending;
  int rc;
  if (!ref.bytes || unit->stored != ref.bytes || unit->ordinal != s->extents)
    return lc_ck_corrupt(error);
  rc =
      lc_collection_unit_write_meta(s->region.unit_io, ref.offset, unit, error);
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK)
    rc = lc_ck_phase(c, LC_COLLECTION_APPEND_CHUNK_WRITTEN, error);
#endif
  if (rc == LC_OK) {
    if (!s->extents) {
      s->first = ref;
      s->first_unit = *unit;
      rc = lc_collection_segment_clone(&s->region, &s->first_storage, error);
    }
    if (rc == LC_OK) {
      ++s->extents;
      rc = lc_ck_push(c, s, ref, 0, error);
    }
  }
  if (rc == LC_OK) {
    s->pending.offset += LC_COLLECTION_UNIT_HEADER + ref.bytes;
    s->pending.bytes = 0;
    /* Under append authority, finishing on the active inode proves a later
     * rotation will seal these final bytes before naming its successor.
     * Any later producer write to this inode clears the proof again. */
    if (ref.segment == s->first.segment)
      s->first_sealed = ref.segment == c->head.active;
  }
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK)
    rc = lc_ck_phase(c, LC_COLLECTION_APPEND_CHUNK_LINKED, error);
#endif
  return rc;
}

int lc_collection_chunks_link(lc_collection_handle *c, lc_collection_chunks *s,
                              const lc_collection_chunk_ref *ref,
                              lc_error *error) {
  if (!ref->bytes || ref->bytes > (lc_u64)LC_I64_MAX - s->bytes ||
      s->extents == (lc_u64)LC_I64_MAX)
    return lc_ck_corrupt(error);
  s->bytes += ref->bytes;
  ++s->extents;
  return lc_ck_push(c, s, *ref, 0, error);
}

int lc_collection_chunks_append(lc_collection_handle *c,
                                lc_collection_chunks *s, const void *bytes,
                                size_t count, lc_error *error) {
  lc_pouch_record_header header;
  unsigned char prefix[32];
  lc_u64 available;
  int rc;
  if (!count)
    return LC_OK;
  if (!s->pending.segment) {
    rc = lc_collection_chunks_prepare(c, s, count, &available, error);
    if (rc != LC_OK)
      return rc;
  }
  if ((lc_u64)count > (lc_u64)LC_I64_MAX - s->bytes ||
      s->region_end - s->pending.offset < LC_COLLECTION_UNIT_HEADER ||
      (lc_u64)count > s->region_end - s->pending.offset -
                          LC_COLLECTION_UNIT_HEADER - s->pending.bytes)
    return lc_ck_corrupt(error);
  if (!s->pending.bytes) {
    memset(&header, 0, sizeof(header));
    header.type = LC_POUCH_COLLECTION_CHUNK;
    header.meta_len = LC_COLLECTION_UNIT_META;
    header.flags = LC_POUCH_RECORD_FLAG_PENDING;
    lc_pouch_record_header_encode(&header, prefix);
    rc = lc_collection_segment_patch(&s->region, s->pending.offset, prefix,
                                     sizeof(prefix), error);
    if (rc != LC_OK)
      return rc;
  }
  if (s->pending.segment == s->first.segment)
    s->first_sealed = 0;
  rc = lc_collection_segment_patch(
      &s->region,
      s->pending.offset + LC_COLLECTION_UNIT_HEADER + s->pending.bytes, bytes,
      count, error);
  if (rc != LC_OK)
    return rc;
  s->pending.bytes += count;
  s->bytes += count;
#ifdef LOCKDC_TEST_BUILD
  return lc_ck_phase(c, LC_COLLECTION_APPEND_CHUNK_PENDING, error);
#else
  return LC_OK;
#endif
}

int lc_collection_chunks_bind(lc_collection_handle *c, lc_collection_chunks *s,
                              lc_pouch_collection_record *record,
                              lc_error *error) {
  char binding[256], *descriptor = NULL;
  int rc = lc_collection_unit_manifest(record, c->id, NULL, binding,
                                       record->manifest, error);
  if (rc != LC_OK || !s->first.segment || !s->first_storage.ops)
    return rc != LC_OK ? rc : lc_ck_corrupt(error);
  if (s->first_unit.descriptor[0])
    rc = lc_pouch_crypto_rebind_descriptor(
        c->pouch->crypto, s->first_unit.descriptor, s->first_unit.context,
        binding, &descriptor, error);
  if (rc == LC_OK) {
    if (descriptor)
      strcpy(record->descriptor, descriptor);
    else
      record->descriptor[0] = 0;
  }
  lc_free_with_allocator(&c->allocator, descriptor);
  if (rc != LC_OK)
    return rc;
  if (s->first.segment != c->head.active)
    rc = lc_collection_chunks_seal_first(s, error);
  if (rc == LC_OK) {
    record->stored_crc = 0;
  }
  return rc;
}
int lc_collection_chunks_finish(lc_collection_handle *c,
                                lc_collection_chunks *s, lc_error *e) {
  lc_collection_chunk_ref ref;
  lc_collection_chunk_level *page;
  unsigned int level, higher;
  int rc, above;
  if (s->pending.bytes)
    return lc_ck_corrupt(e);
  rc = lc_ck_region_release(c, s, 1, e);
  if (rc != LC_OK)
    return rc;
  for (level = 0; level < LC_COLLECTION_CHUNK_LEVELS; ++level) {
    page = &s->levels[level];
    if (!page->count)
      continue;
    above = 0;
    for (higher = level + 1; higher < LC_COLLECTION_CHUNK_LEVELS; ++higher)
      if (s->levels[higher].count)
        above = 1;
    if (page->count == 1 && !above) {
      s->root = lc_ck_ref(page->tuples);
      s->height = level;
      return LC_OK;
    }
    rc = lc_ck_append_index(c, s, level, page->tuples,
                            page->count * LC_COLLECTION_CHUNK_TUPLE, &ref, e);
    if (rc != LC_OK)
      return rc;
    page->count = 0;
    rc = lc_ck_push(c, s, ref, level + 1, e);
    if (rc != LC_OK)
      return rc;
  }
  return lc_ck_corrupt(e);
}
lc_collection_read_storage
lc_collection_chunks_file_access(lc_collection_chunk_reader *r,
                                 const char *directory) {
  if (r->initialized && r->storage.context == &r->files &&
      strcmp(r->files.directory, directory))
    lc_collection_chunks_reader_close(r);
  if (!r->initialized)
    lc_collection_file_reads_init(&r->files, directory);
  return lc_collection_file_reads_access(&r->files);
}
static void lc_ck_close_input(lc_collection_chunk_reader *r,
                              lc_collection_read_handle *handle) {
  if (handle->input.ops)
    r->storage.close(r->storage.context, handle->pin);
  memset(handle, 0, sizeof(*handle));
}
static int lc_ck_input(lc_collection_chunk_reader *r, lc_u64 segment,
                       lc_u64 offset, int index, lc_pouch_record_io *input,
                       lc_error *e) {
  lc_u64 window = offset / LC_COLLECTION_REGION_MAX;
  lc_collection_read_handle *slot = &r->index;
  lc_pouch_record_io opened;
  void *pin = NULL;
  size_t i;
  int rc;
  if (index && segment == r->publication_segment) {
    *input = r->publication;
    return LC_OK;
  }
  if (index && r->index.input.ops && r->index.segment == segment) {
    *input = r->index.input;
    return LC_OK;
  }
  if (!index) {
    slot = NULL;
    ++r->handle_clock;
    for (i = 0; i < LC_COLLECTION_READ_HANDLES; ++i) {
      lc_collection_read_handle *candidate = &r->handles[i];
      if (candidate->input.ops && candidate->segment == segment &&
          candidate->window == window) {
        candidate->used = r->handle_clock;
        *input = candidate->input;
        return LC_OK;
      }
      if (!slot || !candidate->input.ops ||
          (slot->input.ops && candidate->used < slot->used))
        slot = candidate;
    }
  }
  memset(&opened, 0, sizeof(opened));
  rc = r->storage.open(r->storage.context, segment, &opened, &pin, e);
  if (rc != LC_OK)
    return rc;
  lc_ck_close_input(r, slot);
  slot->input = opened;
  slot->pin = pin;
  slot->segment = segment;
  slot->window = window;
  slot->used = r->handle_clock;
  *input = opened;
  return LC_OK;
}
static int lc_ck_read(lc_pouch_record_io input, void *bytes, size_t count,
                      lc_u64 offset, lc_error *e) {
  struct iovec part;
  part.iov_base = bytes;
  part.iov_len = count;
  return lc_pouch_record_io_read(input, offset, &part, 1, e);
}
static int lc_ck_before(const lc_collection_chunk_ref *ref, unsigned int height,
                        lc_u64 segment, lc_u64 offset) {
  lc_u64 overhead = height ? 40 : LC_COLLECTION_UNIT_HEADER;
  if (!ref->segment || (ref->segment & (lc_u64)LC_I64_MAX) == 0 ||
      ((ref->segment ^ segment) & LC_COLLECTION_SNAPSHOT_BIT) || !ref->bytes ||
      ref->bytes > LC_COLLECTION_CHUNK_MAX || ref->segment > segment ||
      ref->offset > (lc_u64)LC_I64_MAX - overhead - ref->bytes)
    return 0;
  if (height ? (height >= LC_COLLECTION_CHUNK_LEVELS ||
                ref->bytes > LC_COLLECTION_CHUNK_PAGE ||
                ref->bytes % LC_COLLECTION_CHUNK_TUPLE)
             : ref->bytes > LC_COLLECTION_CHUNK_MAX)
    return 0;
  return ref->segment != segment ||
         (ref->offset <= offset &&
          overhead + ref->bytes <= offset - ref->offset);
}
static int lc_ck_node(lc_collection_chunk_reader *r,
                      const lc_collection_chunk_ref *ref, unsigned int height,
                      lc_error *e) {
  lc_collection_chunk_frame *frame = &r->frames[r->depth];
  lc_pouch_record_header header;
  lc_pouch_record_io input;
  int rc = lc_ck_input(r, ref->segment, ref->offset, 1, &input, e);
  if (rc == LC_OK)
    rc = lc_ck_read(input, frame->encoded, (size_t)ref->bytes + 40, ref->offset,
                    e);
  if (rc == LC_OK)
    rc = lc_pouch_record_header_decode(frame->encoded, &header, e);
  if (rc != LC_OK)
    return rc;
  if (header.type != LC_POUCH_COLLECTION_NODE || header.flags ||
      header.key_len || header.meta_len != 8 ||
      header.payload_len != ref->bytes ||
      lc_ck_get(frame->encoded + 32) != height - 1 ||
      header.payload_crc !=
          (unsigned long)crc32(0L, frame->encoded + 40, (uInt)ref->bytes))
    return lc_ck_corrupt(e);
  frame->ref = *ref;
  frame->count = (size_t)(ref->bytes / LC_COLLECTION_CHUNK_TUPLE);
  frame->next = 0;
  frame->level = height - 1;
  ++r->depth;
  return LC_OK;
}
static int lc_ck_next(lc_collection_chunk_reader *r, int *found, lc_error *e) {
  lc_collection_chunk_frame *frame;
  lc_collection_chunk_ref ref;
  int rc;
  *found = 0;
  if (!r->started) {
    r->started = 1;
    if (!r->height) {
      r->current = r->root;
      *found = 1;
      return LC_OK;
    }
    if (!r->frames) {
      r->frames = lc_calloc_with_allocator(
          r->allocator, LC_COLLECTION_CHUNK_LEVELS, sizeof(*r->frames));
      if (!r->frames)
        return lc_collection_failure(
            e, LC_ERR_NOMEM, NULL,
            "failed to allocate bounded collection index reader");
    }
    rc = lc_ck_node(r, &r->root, r->height, e);
    if (rc != LC_OK)
      return rc;
  }
  while (r->depth) {
    frame = &r->frames[r->depth - 1];
    if (frame->next == frame->count) {
      --r->depth;
      continue;
    }
    ref = lc_ck_ref(frame->encoded + 40 +
                    frame->next++ * LC_COLLECTION_CHUNK_TUPLE);
    if (!lc_ck_before(&ref, frame->level, frame->ref.segment,
                      frame->ref.offset))
      return lc_ck_corrupt(e);
    if (!frame->level) {
      r->current = ref;
      *found = 1;
      return LC_OK;
    }
    rc = lc_ck_node(r, &ref, frame->level, e);
    if (rc != LC_OK)
      return rc;
  }
  return LC_OK;
}
static int lc_ck_payload_span(lc_collection_chunk_reader *r,
                              const lc_collection_chunk_ref *ref,
                              lc_pouch_record_io *input, lc_u64 *offset,
                              lc_error *error) {
  char context[256];
  lc_u64 ordinal = r->extent_count - r->extents_remaining;
  int rc = lc_ck_input(r, ref->segment, ref->offset, 0, input, error);
  if (rc == LC_OK)
    rc = lc_collection_unit_read(*input, ref->offset, ref->bytes, &r->unit,
                                 error);
  if (rc != LC_OK)
    return rc;
  if (r->unit.ordinal != ordinal || r->unit.plain > r->plaintext ||
      r->unit.stored > r->remaining)
    return lc_ck_corrupt(error);
  rc = lc_collection_unit_context(r->record->context, ordinal, context, error);
  if (rc != LC_OK)
    return rc;
  if (strcmp(r->unit.context, context) &&
      (r->unit.context[0] || r->unit.descriptor[0] ||
       lc_pouch_crypto_enabled(r->crypto)))
    return lc_ck_corrupt(error);
  if (!ordinal) {
    rc = lc_pouch_crypto_descriptor_same_payload(r->unit.descriptor,
                                                 r->record->descriptor, error);
    if (rc != LC_OK)
      return rc;
  }
  *offset = ref->offset + LC_COLLECTION_UNIT_HEADER;
  return LC_OK;
}

int lc_collection_chunks_next_unit(lc_collection_chunk_reader *r, int *found,
                                   lc_pouch_record_io *input,
                                   lc_collection_chunk_ref *ref,
                                   lc_collection_unit *unit, lc_error *error) {
  lc_u64 offset;
  int rc;
  if (!r->initialized || r->unit_active)
    return lc_ck_corrupt(error);
  rc = lc_ck_next(r, found, error);
  if (rc != LC_OK)
    return rc;
  if (!*found)
    return r->remaining || r->plaintext || r->extents_remaining
               ? lc_ck_corrupt(error)
               : LC_OK;
  if (!r->extents_remaining || r->current.bytes > r->remaining)
    return lc_ck_corrupt(error);
  rc = lc_ck_payload_span(r, &r->current, input, &offset, error);
  if (rc != LC_OK)
    return rc;
  if (r->unit.descriptor[0] || lc_pouch_crypto_enabled(r->crypto)) {
    if (!r->decoder) {
      rc = lc_pouch_crypto_reader_open(r->crypto, &r->decoder, error);
      if (rc != LC_OK)
        return rc;
    }
    rc = lc_pouch_crypto_reader_verify(r->decoder, r->unit.descriptor,
                                       r->unit.context, error);
    if (rc != LC_OK)
      return rc;
  }
  *unit = r->unit;
  *ref = r->current;
  r->remaining -= r->unit.stored;
  r->plaintext -= r->unit.plain;
  --r->extents_remaining;
  return LC_OK;
}

int lc_collection_chunks_visit(lc_collection_chunk_reader *r,
                               lc_collection_chunk_visit_fn visit,
                               void *context, lc_error *error) {
  lc_collection_chunk_ref seen[LC_COLLECTION_CHUNK_LEVELS], ref;
  lc_collection_unit unit;
  unsigned int i;
  lc_pouch_record_io input;
  int found, rc;
  memset(seen, 0, sizeof(seen));
  for (;;) {
    rc = lc_collection_chunks_next_unit(r, &found, &input, &ref, &unit, error);
    if (rc != LC_OK || !found)
      return rc;
    /* The iterator already validated this complete path. Observe a new page
     * once without adding any callback branch to ordinary payload reads. */
    for (i = 0; i < r->depth; ++i) {
      lc_collection_chunk_frame *frame = &r->frames[i];
      if (seen[i].segment == frame->ref.segment &&
          seen[i].offset == frame->ref.offset)
        continue;
      seen[i] = frame->ref;
      rc = visit(context, &frame->ref, frame->level + 1U, error);
      if (rc != LC_OK)
        return rc;
    }
    rc = visit(context, &ref, 0, error);
    if (rc != LC_OK)
      return rc;
  }
}

int lc_collection_chunks_single(lc_collection_chunk_reader *r,
                                lc_pouch_record_io *input, lc_u64 *offset,
                                lc_error *error) {
  int rc;
  if (!r->initialized || r->height || r->extent_count != 1)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection body is not one contiguous span");
  rc = lc_ck_payload_span(r, &r->root, input, offset, error);
  if (rc == LC_OK && r->unit.plain != r->record->bytes)
    return lc_ck_corrupt(error);
  return rc;
}

static size_t lc_ck_reader_read(lc_source *source, void *bytes, size_t capacity,
                                lc_error *e) {
  lc_collection_chunk_reader *r = source->impl;
  unsigned char *p = bytes;
  size_t take, used = 0;
  lc_pouch_record_io input;
  int found, rc;
  lc_u64 offset;
  if (!capacity)
    return 0;
  for (;;) {
    if (!r->unit_active) {
      rc = lc_ck_next(r, &found, e);
      if (rc != LC_OK)
        return 0;
      if (!found) {
        if (r->remaining || r->plaintext || r->extents_remaining)
          lc_ck_corrupt(e);
        return used;
      }
      if (!r->extents_remaining || r->current.bytes > r->remaining) {
        lc_ck_corrupt(e);
        return 0;
      }
      rc = lc_ck_payload_span(r, &r->current, &input, &offset, e);
      if (rc != LC_OK)
        return 0;
      rc = lc_pouch_record_span_bind(&r->encoded_input, input, offset,
                                     r->unit.stored, NULL, 0U, e);
      if (rc != LC_OK)
        return 0;
      r->extent_remaining = r->unit.plain;
      r->decoded = &r->encoded_input.source;
      if (r->unit.descriptor[0] || lc_pouch_crypto_enabled(r->crypto)) {
        if (!r->decoder) {
          rc = lc_pouch_crypto_reader_open(r->crypto, &r->decoder, e);
          if (rc != LC_OK)
            return 0;
        }
        rc = lc_pouch_crypto_reader_bind_source(
            r->decoder, r->unit.context, r->unit.context,
            &r->encoded_input.source, r->unit.stored, r->unit.descriptor,
            &r->decoded, e);
        if (rc != LC_OK)
          return 0;
      }
      r->unit_active = 1;
    }
    take = r->decoded->read(r->decoded, p, capacity, e);
    if (e && e->code != LC_OK)
      return 0;
    if (take > capacity || (lc_u64)take > r->extent_remaining ||
        (lc_u64)take > r->plaintext) {
      lc_ck_corrupt(e);
      return 0;
    }
    if (take) {
      r->plaintext -= take;
      r->extent_remaining -= take;
      p += take;
      used += take;
      capacity -= take;
      if (!capacity)
        return used;
    } else {
      if (r->extent_remaining) {
        lc_ck_corrupt(e);
        return 0;
      }
      r->remaining -= r->unit.stored;
      --r->extents_remaining;
      r->unit_active = 0;
      lc_pouch_crypto_reader_clear(r->decoder);
    }
  }
}
static int lc_ck_reset(lc_source *s, lc_error *e) {
  lc_collection_chunk_reader *r = s->impl;
  (void)e;
  lc_pouch_crypto_reader_clear(r->decoder);
  r->decoded = NULL;
  r->unit_active = 0;
  r->plaintext = r->record->bytes;
  r->remaining = r->length;
  r->extents_remaining = r->extent_count;
  r->extent_remaining = 0;
  r->started = 0;
  r->depth = 0;
  return LC_OK;
}
int lc_collection_chunks_open(
    lc_collection_chunk_reader *r, const lc_allocator *allocator,
    lc_pouch_crypto *crypto, const char *collection_id,
    lc_collection_read_storage storage, lc_pouch_record_io publication_input,
    const lc_pouch_collection_record *record, lc_error *e) {
  lc_collection_chunk_ref root;
  lc_u64 publication;
  root.segment = record->stream_segment;
  root.offset = record->stream_offset;
  root.bytes = record->stream_root_bytes;
  if (!publication_input.ops || !storage.open || !storage.close ||
      (strncmp(record->payload_container, "seg-", 4) &&
       strncmp(record->payload_container, "snapshot-", 9)))
    return lc_ck_corrupt(e);
  {
    size_t prefix = !strncmp(record->payload_container, "seg-", 4) ? 4U : 9U;
    if (strlen(record->payload_container) != prefix + 24U ||
        !lc_parse_u64_base10_range_checked(record->payload_container + prefix,
                                           20, &publication))
      return lc_ck_corrupt(e);
    if (prefix == 9U)
      publication |= LC_COLLECTION_SNAPSHOT_BIT;
  }
  if (!lc_ck_before(&root, record->stream_height, publication,
                    record->payload_record_offset) ||
      !record->stream_extents ||
      record->stream_extents > record->stored_bytes ||
      (!record->stream_height &&
       (record->stream_extents != 1 || root.bytes != record->stored_bytes)))
    return lc_ck_corrupt(e);
  if (r->initialized && (r->storage.open != storage.open ||
                         r->storage.context != storage.context))
    lc_collection_chunks_reader_close(r);
  if (!r->initialized) {
    memset(r->handles, 0, sizeof(r->handles));
    memset(&r->index, 0, sizeof(r->index));
    r->files.fd = -1;
    r->initialized = 1;
  }
  r->storage = storage;
  r->source.read = lc_ck_reader_read;
  r->source.reset = lc_ck_reset;
  r->source.impl = r;
  r->crypto = crypto;
  r->record = record;
  {
    unsigned char proof[32];
    int rc = lc_collection_unit_manifest(record, collection_id, &r->manifest,
                                         r->binding, proof, e);
    if (rc != LC_OK)
      return rc;
    if (memcmp(record->manifest, proof, sizeof(proof)))
      return lc_ck_corrupt(e);
    if (record->descriptor[0] || lc_pouch_crypto_enabled(crypto)) {
      if (!r->decoder)
        rc = lc_pouch_crypto_reader_open(crypto, &r->decoder, e);
      if (rc == LC_OK)
        rc = lc_pouch_crypto_reader_verify(r->decoder, record->descriptor,
                                           r->binding, e);
      if (rc != LC_OK)
        return rc;
    }
  }
  r->allocator = allocator;
  r->root = root;
  r->height = record->stream_height;
  r->length = record->stored_bytes;
  r->extent_count = record->stream_extents;
  r->publication = publication_input;
  r->publication_segment = publication;
  r->publication_offset = record->payload_record_offset;
  return lc_ck_reset(&r->source, e);
}
void lc_collection_chunks_reader_close(lc_collection_chunk_reader *r) {
  size_t i;
  if (r->initialized) {
    for (i = 0; i < LC_COLLECTION_READ_HANDLES; ++i)
      lc_ck_close_input(r, &r->handles[i]);
    lc_ck_close_input(r, &r->index);
    lc_collection_file_reads_cleanup(&r->files);
  }
  lc_pouch_crypto_reader_close(r->decoder);
  r->decoder = NULL;
  EVP_MD_CTX_free(r->manifest);
  r->manifest = NULL;
  lc_free_with_allocator(r->allocator, r->frames);
  r->frames = NULL;
  r->initialized = 0;
}
