#include "collection_conversion.h"
#include "collection_conversion_internal.h"
#include "control.h"
#include "lc_api_internal.h"
#include <string.h>

#define INTENT_BYTES (144U + LC_COLLECTION_HEAD_BYTES)
#define PAGE_BYTES (40U + PS_COLLECTION_CONVERSION_MAP_ROWS * 64U)
#define PROGRESS_BYTES 112U

typedef struct conversion_candidate {
  lc_u64 id;
  ps_catalog_info binding;
} conversion_candidate;
struct ps_collection_conversion {
  ps_space *space;
  uint64_t collection, owner;
  ps_collection_segment *anchor;
  ps_catalog_info binding;
  ps_ref source_ref;
  lc_collection_disk_head source;
  ps_collection_conversion_progress progress;
  lc_u64 watermark, last_segment, last_offset;
  conversion_candidate pending[PS_COLLECTION_CONVERSION_BATCH];
  size_t count;
  int discarded, aborting, irreversible;
  unsigned int target_flags;
  char target_key[80];
  unsigned char scratch[PAGE_BYTES];
};
static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message, NULL,
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
         !memcmp(a->container, b->container, sizeof(a->container));
}
/* Zero is the initial cursor, then snapshot group, then normal history. */
static int compare(lc_u64 as, lc_u64 ao, lc_u64 bs, lc_u64 bo) {
  if (as != bs) {
    if (!as || !bs)
      return as ? 1 : -1;
    as ^= LC_COLLECTION_SNAPSHOT_BIT;
    bs ^= LC_COLLECTION_SNAPSHOT_BIT;
    return as < bs ? -1 : 1;
  }
  return ao == bo ? 0 : ao < bo ? -1 : 1;
}
static ps_key key(uint64_t owner, lc_u64 id, unsigned int kind) {
  ps_key k;
  k.owner = owner;
  k.key = id;
  k.kind = kind;
  return k;
}
static int load_value(ps_collection_conversion *w, lc_u64 id, void *bytes,
                      size_t length, int optional, int *present, lc_error *e) {
  ps_reader *reader = NULL;
  ps_ref ref;
  uint64_t size;
  int found, rc;
  rc = ps_value_info(w->space, key(w->owner, id, PS_CONTROL), &ref, &size,
                     &found, e);
  if (rc == LC_OK && !found && !optional)
    rc = failure(e, LC_ERR_PROTOCOL, "collection conversion page is missing");
  if (rc == LC_OK && found && size != length)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "collection conversion value has wrong size");
  if (rc == LC_OK && found)
    rc =
        ps_read_open(w->space, key(w->owner, id, PS_CONTROL), &ref, &reader, e);
  if (rc == LC_OK && found)
    rc = ps_read_at(reader, 0U, bytes, length, e);
  ps_reader_close(reader);
  if (rc == LC_OK && present)
    *present = found;
  return rc;
}
static int store_value(ps_collection_conversion *w, lc_u64 id,
                       const void *bytes, size_t length, lc_error *e) {
  ps_writer *writer = NULL;
  int rc = ps_begin(w->space, NULL, key(w->owner, id, PS_CONTROL), &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, length, e);
  if (rc == LC_OK)
    rc = ps_publish(writer, NULL, e);
  ps_writer_close(writer);
  return rc;
}
static int binding_get(ps_space *s, uint64_t owner, const void *name,
                       size_t length, ps_catalog_info *out, int *found,
                       lc_error *e) {
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  int rc =
      ps_catalog_get(s, owner, PS_CATALOG_CONVERSION, name, length, &view, e);
  if (rc == LC_OK && view)
    rc = ps_catalog_view_info(view, &info, e);
  if (rc == LC_OK) {
    *found = view != NULL;
    if (view)
      *out = info;
  }
  ps_catalog_view_close(view);
  return rc;
}
static int candidate_get(ps_collection_conversion *w, lc_u64 id,
                         ps_catalog_info *info, lc_error *e) {
  unsigned char name[8];
  int found, rc;
  put(name, id);
  rc = binding_get(w->space, w->owner, name, sizeof(name), info, &found, e);
  if (rc == LC_OK && (!found || info->stream.owner != w->owner ||
                      !info->stream.key || info->stream.kind != PS_STREAM))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "collection conversion candidate is missing");
  return rc;
}
static int progress_load(ps_collection_conversion *w, lc_error *e) {
  unsigned char *p = w->scratch;
  int found, rc = load_value(w, 1U, p, PROGRESS_BYTES, 1, &found, e);
  if (rc != LC_OK || !found)
    return rc;
  if (memcmp(p, "LCPSCCPR", 8U) || get(p + 8U) != w->owner)
    return failure(e, LC_ERR_PROTOCOL,
                   "collection conversion progress is invalid");
  w->progress.pages = get(p + 16U);
  w->progress.records = get(p + 24U);
  w->progress.candidates = get(p + 32U);
  w->progress.source_segment = get(p + 40U);
  w->progress.source_offset = get(p + 48U);
  w->watermark = get(p + 56U);
  w->last_segment = get(p + 64U);
  w->last_offset = get(p + 72U);
  w->aborting = get(p + 80U) != 0U;
  w->irreversible = get(p + 88U) != 0U;
  w->progress.normal = get(p + 96U);
  w->progress.snapshot = get(p + 104U);
  if (get(p + 80U) > 1U || get(p + 88U) > 1U ||
      w->progress.pages >= LC_COLLECTION_SNAPSHOT_BIT - 1U ||
      (w->aborting && w->irreversible) ||
      w->progress.pages > w->progress.records ||
      (w->progress.records &&
       (w->progress.records - 1U) / PS_COLLECTION_CONVERSION_MAP_ROWS >=
           w->progress.pages) ||
      (!w->progress.pages != !w->progress.records) ||
      (!w->watermark != !w->progress.candidates) ||
      (!w->last_segment != !w->progress.records) ||
      (!w->progress.source_segment && w->progress.source_offset) ||
      (w->last_segment &&
       compare(w->last_segment, w->last_offset, w->progress.source_segment,
               w->progress.source_offset) >= 0))
    return failure(e, LC_ERR_PROTOCOL,
                   "collection conversion progress is inconsistent");
  return LC_OK;
}
typedef struct cleanup {
  ps_space *space;
  uint64_t owner, watermark, pages;
  int speculative, allow_anchor, keep_progress;
} cleanup;
static int remove_value(void *context, ps_key k, const ps_ref *ref,
                        uint64_t bytes, lc_error *e) {
  cleanup *c = context;
  (void)bytes;
  if (c->keep_progress && k.key == 1U)
    return LC_OK;
  if (c->speculative && (k.key & LC_COLLECTION_SNAPSHOT_BIT))
    return LC_OK;
  if (c->speculative && k.key <= c->pages + 1U)
    return LC_OK;
  return ps_value_remove(c->space, k, ref, e);
}
static int remove_binding(void *context, const void *name, size_t length,
                          const ps_catalog_info *info, lc_error *e) {
  cleanup *c = context;
  ps_owned_head *head = NULL;
  ps_owned_status status;
  cleanup children;
  int rc = LC_OK;
  if (!length) {
    if (c->speculative || !c->allow_anchor || info->stream.kind != PS_STREAM ||
        info->stream.key || !info->stream.owner ||
        info->stream.owner == c->owner)
      return failure(e, LC_ERR_PROTOCOL,
                     "collection conversion anchor is invalid");
    /* An active converter owns this head. Acquire it before touching children,
     * so collection/orphan cleanup cannot dismantle a running workspace. */
    rc = ps_owned_open(c->space, NULL, info->stream, 0, &info->target, &head,
                       &status, e);
    /* Once cancellation is selected by the caller's collection removal,
     * detach the anchor first. A failed cascade is then an ordinary orphan
     * tree, never a resumable conversion with partly removed dependencies. */
    if (rc == LC_OK)
      rc = ps_catalog_remove(c->space, c->owner, PS_CATALOG_CONVERSION, name,
                             length, &info->revision, e);
    if (rc == LC_OK) {
      children = *c;
      children.owner = info->stream.owner;
      children.allow_anchor = 0;
      rc = ps_catalog_visit(c->space, children.owner, PS_CATALOG_CONVERSION,
                            remove_binding, &children, e);
      if (rc == LC_OK)
        rc = ps_values_visit(c->space, children.owner, PS_CONTROL, PS_CONTROL,
                             remove_value, &children, e);
    }
    ps_owned_close(head);
    return rc;
  } else if ((length != 8U && length != 9U) ||
             (length == 9U && ((const unsigned char *)name)[8] != 1U) ||
             !get(name) || info->stream.kind != PS_STREAM ||
             (length == 8U && info->stream.owner != c->owner))
    rc =
        failure(e, LC_ERR_PROTOCOL, "collection conversion binding is invalid");
  if (rc == LC_OK && c->speculative &&
      (length != 8U || info->id <= c->watermark)) {
    ps_owned_close(head);
    return LC_OK;
  }
  if (rc == LC_OK)
    rc = ps_catalog_remove(c->space, c->owner, PS_CATALOG_CONVERSION, name,
                           length, &info->revision, e);
  /* Visitor pins defer actual head retirement to the next orphan pass. */
  ps_owned_close(head);
  return rc;
}
int ps_collection_conversion_reclaim(ps_space *s, uint64_t owner, lc_error *e) {
  cleanup c;
  memset(&c, 0, sizeof(c));
  c.space = s;
  c.owner = owner;
  c.allow_anchor = 1;
  return ps_catalog_visit(s, owner, PS_CATALOG_CONVERSION, remove_binding, &c,
                          e);
}
static int recover_prefix(ps_collection_conversion *w, lc_error *e) {
  cleanup c;
  int rc;
  memset(&c, 0, sizeof(c));
  c.space = w->space;
  c.owner = w->owner;
  c.watermark = w->watermark;
  c.pages = w->progress.pages;
  c.speculative = 1;
  rc = ps_catalog_visit(w->space, w->owner, PS_CATALOG_CONVERSION,
                        remove_binding, &c, e);
  if (rc == LC_OK)
    rc = ps_values_visit(w->space, w->owner, PS_CONTROL, PS_CONTROL,
                         remove_value, &c, e);
  return rc;
}
void ps_collection_conversion_context(ps_collection_conversion *w, ps_space **s,
                                      uint64_t *collection) {
  *s = w->space;
  *collection = w->collection;
}
int ps_collection_conversion_meta_read(ps_collection_conversion *w, lc_u64 slot,
                                       void *bytes, size_t size, int *found,
                                       lc_error *e) {
  if (!w || w->discarded || w->aborting || slot >= LC_COLLECTION_SNAPSHOT_BIT ||
      !bytes || !size || size > PS_PAGE || !found)
    return failure(e, LC_ERR_INVALID,
                   "conversion metadata requires a bounded slot");
  return load_value(w, slot | LC_COLLECTION_SNAPSHOT_BIT, bytes, size, 1, found,
                    e);
}
int ps_collection_conversion_meta_write(ps_collection_conversion *w,
                                        lc_u64 slot, const void *bytes,
                                        size_t size, lc_error *e) {
  if (!w || w->discarded || w->aborting || slot >= LC_COLLECTION_SNAPSHOT_BIT ||
      !bytes || !size || size > PS_PAGE)
    return failure(e, LC_ERR_INVALID,
                   "conversion metadata requires a bounded slot");
  return store_value(w, slot | LC_COLLECTION_SNAPSHOT_BIT, bytes, size, e);
}
typedef struct candidate_visit {
  ps_catalog_visit_fn visit;
  void *context;
} candidate_visit;
static int candidate_visit_one(void *context, const void *name, size_t length,
                               const ps_catalog_info *info, lc_error *e) {
  candidate_visit *v = context;
  return length == 8U ? v->visit(v->context, name, length, info, e) : LC_OK;
}
int ps_collection_conversion_candidates(ps_collection_conversion *w,
                                        ps_catalog_visit_fn visit,
                                        void *context, lc_error *e) {
  candidate_visit v;
  if (!w || w->discarded || w->aborting || !visit)
    return failure(e, LC_ERR_INVALID,
                   "conversion candidate visit requires owner");
  v.visit = visit;
  v.context = context;
  return ps_catalog_visit(w->space, w->owner, PS_CATALOG_CONVERSION,
                          candidate_visit_one, &v, e);
}
int ps_collection_conversion_open(ps_space *s, uint64_t collection,
                                  lc_pouch_crypto *target, int create,
                                  ps_collection_conversion **out, lc_error *e) {
  ps_collection_conversion *w;
  ps_owned_status status;
  ps_reader *reader = NULL;
  unsigned char *p;
  char *key_id = NULL;
  struct iovec part;
  unsigned int flags;
  int found, rc;
  if (out)
    *out = NULL;
  if (!s || !collection || !out)
    return failure(e, LC_ERR_INVALID,
                   "collection conversion requires an owner");
  w = lc_calloc_with_allocator(NULL, 1U, sizeof(*w));
  if (!w)
    return failure(e, LC_ERR_NOMEM,
                   "cannot allocate collection conversion workspace");
  w->space = s;
  w->collection = collection;
  p = w->scratch;
  flags = (lc_pouch_crypto_enabled(target) ? 1U : 0U) |
          (lc_pouch_crypto_compression_enabled(target) ? 2U : 0U);
  rc = flags & 1U ? lc_pouch_crypto_key_id(target, &key_id, e) : LC_OK;
  if (rc == LC_OK && key_id && strlen(key_id) >= 80U)
    rc = failure(e, LC_ERR_INVALID,
                 "collection conversion key identity is too long");
  if (rc == LC_OK) {
    w->target_flags = flags;
    if (key_id)
      strcpy(w->target_key, key_id);
  }
  if (rc == LC_OK)
    rc = binding_get(s, collection, NULL, 0U, &w->binding, &found, e);
  if (rc == LC_OK && !found && !create)
    rc = failure(e, LC_ERR_INVALID, "collection has no conversion workspace");
  if (rc == LC_OK && found) {
    if (w->binding.stream.kind != PS_STREAM || w->binding.stream.key ||
        !w->binding.stream.owner || w->binding.stream.owner == collection)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "collection conversion anchor is invalid");
    if (rc == LC_OK) {
      w->owner = w->binding.stream.owner;
      rc = ps_collection_segment_open(s, w->binding.stream, 0,
                                      &w->binding.target, NULL, NULL,
                                      &w->anchor, e);
    }
    if (rc == LC_OK)
      rc = ps_collection_segment_status(w->anchor, &status, NULL, e);
    if (rc == LC_OK && (!status.sealed || status.tail != INTENT_BYTES))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "collection conversion intent is incomplete");
    part.iov_base = p;
    part.iov_len = INTENT_BYTES;
    if (rc == LC_OK)
      rc = lc_pouch_record_io_read(ps_collection_segment_io(w->anchor), 0U,
                                   &part, 1, e);
    if (rc == LC_OK && (memcmp(p, "LCPSCCIN", 8U) ||
                        get(p + 8U) != collection || get(p + 16U) != w->owner ||
                        get(p + 24U) > 3U || !memchr(p + 56U, 0, 80U)))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "collection conversion intent is invalid");
    if (rc == LC_OK && (get(p + 24U) != flags ||
                        strcmp((const char *)p + 56U, key_id ? key_id : "")))
      rc = failure(e, LC_ERR_INVALID,
                   "collection conversion target differs from durable intent");
    if (rc == LC_OK) {
      memcpy(w->source_ref.container, p + 32U, 16U);
      w->source_ref.incarnation = get(p + 48U);
      rc = lc_collection_head_decode(p + 144U, &w->source, e);
    }
  } else if (rc == LC_OK) {
    rc = ps_read_open(s, key(collection, 1U, PS_CONTROL), NULL, &reader, e);
    if (rc == LC_OK && ps_read_size(reader) != LC_COLLECTION_HEAD_BYTES)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "collection conversion source head is invalid");
    if (rc == LC_OK) {
      ps_read_reference(reader, &w->source_ref);
      rc = ps_read_at(reader, 0U, p + 144U, LC_COLLECTION_HEAD_BYTES, e);
    }
    if (rc == LC_OK)
      rc = lc_collection_head_decode(p + 144U, &w->source, e);
    if (rc == LC_OK)
      rc = ps_catalog_allocate_id(s, &w->owner, e);
    if (rc == LC_OK && w->owner == collection)
      rc = failure(e, LC_ERR_INVALID,
                   "collection identity is outside reserved allocator");
    if (rc == LC_OK)
      rc = ps_collection_segment_open(s, key(w->owner, 0U, PS_STREAM), 1, NULL,
                                      NULL, NULL, &w->anchor, e);
    if (rc == LC_OK) {
      memcpy(p, "LCPSCCIN", 8U);
      put(p + 8U, collection);
      put(p + 16U, w->owner);
      put(p + 24U, flags);
      memcpy(p + 32U, w->source_ref.container, 16U);
      put(p + 48U, w->source_ref.incarnation);
      if (key_id)
        memcpy(p + 56U, key_id, strlen(key_id));
      part.iov_base = p;
      part.iov_len = INTENT_BYTES;
      rc = lc_pouch_record_io_append(ps_collection_segment_io(w->anchor), &part,
                                     1, e);
    }
    if (rc == LC_OK)
      rc = ps_collection_segment_seal(w->anchor, e);
    if (rc == LC_OK)
      rc = ps_collection_segment_status(w->anchor, &status, NULL, e);
    if (rc == LC_OK)
      rc = ps_catalog_put(s, NULL, collection, PS_CATALOG_CONVERSION, NULL, 0U,
                          NULL, key(w->owner, 0U, PS_STREAM), &status.ref,
                          &w->binding, e);
  }
  ps_reader_close(reader);
  lc_free_with_allocator(NULL, key_id);
  if (rc == LC_OK)
    rc = progress_load(w, e);
  if (rc == LC_OK && w->aborting) {
    rc = ps_collection_conversion_discard(w, e);
    ps_collection_conversion_close(w);
    if (rc == LC_OK && create)
      return ps_collection_conversion_open(s, collection, target, 1, out, e);
    return rc == LC_OK ? failure(e, LC_ERR_INVALID,
                                 "collection conversion was cancelled")
                       : rc;
  }
  if (rc == LC_OK)
    rc = recover_prefix(w, e);
  if (rc == LC_OK)
    *out = w;
  else
    ps_collection_conversion_close(w);
  return rc;
}
void ps_collection_conversion_close(ps_collection_conversion *w) {
  if (w) {
    ps_collection_segment_close(w->anchor);
    lc_free_with_allocator(NULL, w);
  }
}
int ps_collection_conversion_target_matches(ps_collection_conversion *w,
                                            lc_pouch_crypto *target,
                                            lc_error *e) {
  unsigned int flags = (lc_pouch_crypto_enabled(target) ? 1U : 0U) |
                       (lc_pouch_crypto_compression_enabled(target) ? 2U : 0U);
  char *key = NULL;
  int rc;
  if (!w || w->discarded || w->aborting || flags != w->target_flags)
    return failure(e, LC_ERR_INVALID,
                   "conversion target differs from durable intent");
  rc = flags & 1U ? lc_pouch_crypto_key_id(target, &key, e) : LC_OK;
  if (rc == LC_OK && strcmp(w->target_key, key ? key : ""))
    rc = failure(e, LC_ERR_INVALID,
                 "conversion target key differs from durable intent");
  lc_free_with_allocator(NULL, key);
  return rc;
}
void ps_collection_conversion_status(const ps_collection_conversion *w,
                                     ps_collection_conversion_progress *out) {
  *out = w->progress;
}
void ps_collection_conversion_source_head(const ps_collection_conversion *w,
                                          lc_collection_disk_head *head,
                                          ps_ref *ref) {
  *head = w->source;
  *ref = w->source_ref;
}
int ps_collection_conversion_source_bind(ps_collection_conversion *w, lc_u64 id,
                                         const ps_catalog_info *source,
                                         lc_error *e) {
  unsigned char name[9];
  ps_catalog_info info;
  int found, rc;
  if (!w || w->discarded || w->aborting || !id || !source ||
      source->stream.kind != PS_STREAM || !source->stream.owner ||
      !source->stream.key)
    return failure(e, LC_ERR_INVALID,
                   "collection conversion source is invalid");
  put(name, id);
  name[8] = 1U;
  rc = binding_get(w->space, w->owner, name, sizeof(name), &info, &found, e);
  if (rc == LC_OK && found &&
      (info.stream.owner != source->stream.owner ||
       info.stream.key != source->stream.key ||
       !same_ref(&info.target, &source->target)))
    rc = failure(e, LC_ERR_INVALID,
                 "collection conversion source identity changed");
  if (rc == LC_OK && !found)
    rc = ps_catalog_put(w->space, NULL, w->owner, PS_CATALOG_CONVERSION, name,
                        sizeof(name), NULL, source->stream, &source->target,
                        &info, e);
  return rc;
}
int ps_collection_conversion_source_get(ps_collection_conversion *w, lc_u64 id,
                                        ps_catalog_info *out, lc_error *e) {
  unsigned char name[9];
  ps_catalog_info info;
  int found, rc;
  if (!w || w->discarded || w->aborting || !id || !out)
    return failure(e, LC_ERR_INVALID,
                   "collection conversion source requires an ID");
  put(name, id);
  name[8] = 1U;
  rc = binding_get(w->space, w->owner, name, sizeof(name), &info, &found, e);
  if (rc == LC_OK && !found)
    rc = failure(e, LC_ERR_INVALID, "collection conversion source is missing");
  if (rc == LC_OK)
    *out = info;
  return rc;
}
static int prune_select(void *context, ps_piece_publish_fn publish,
                        void *publication, lc_error *e) {
  ps_collection_conversion *w = context;
  int rc = ps_check(w->space, e);
  if (rc == LC_OK && (w->discarded || w->aborting))
    rc = failure(e, LC_ERR_INVALID,
                 "conversion prefix no longer selects source disposal");
  return rc == LC_OK ? publish(publication, e) : rc;
}
static int mark_irreversible(ps_collection_conversion *w, lc_error *e) {
  unsigned char *p = w->scratch;
  int rc;
  if (w->irreversible)
    return LC_OK;
  memset(p, 0, PROGRESS_BYTES);
  memcpy(p, "LCPSCCPR", 8U);
  put(p + 8U, w->owner);
  put(p + 16U, w->progress.pages);
  put(p + 24U, w->progress.records);
  put(p + 32U, w->progress.candidates);
  put(p + 40U, w->progress.source_segment);
  put(p + 48U, w->progress.source_offset);
  put(p + 56U, w->watermark);
  put(p + 64U, w->last_segment);
  put(p + 72U, w->last_offset);
  put(p + 88U, 1U);
  put(p + 96U, w->progress.normal);
  put(p + 104U, w->progress.snapshot);
  rc = store_value(w, 1U, p, PROGRESS_BYTES, e);
  if (rc == LC_OK)
    w->irreversible = 1;
  return rc;
}
int ps_collection_conversion_require_completion(ps_collection_conversion *w,
                                                lc_error *e) {
  if (!w || w->discarded || w->aborting)
    return failure(e, LC_ERR_INVALID,
                   "conversion is not available for preparation");
  return mark_irreversible(w, e);
}
int ps_collection_conversion_source_prune(ps_collection_conversion *w,
                                          lc_u64 id,
                                          ps_collection_segment *source,
                                          uint64_t *removed, lc_error *e) {
  ps_catalog_info info;
  ps_owned_status status;
  uint64_t through;
  int order, rc;
  if (!w || !source || !removed)
    return failure(e, LC_ERR_INVALID, "source disposal requires an owner");
  rc = ps_collection_conversion_source_get(w, id, &info, e);
  if (rc == LC_OK)
    rc = ps_collection_segment_status(source, &status, NULL, e);
  if (rc != LC_OK)
    return rc;
  order = compare(id, 0U, w->progress.source_segment, 0U);
  if (!same_ref(&info.target, &status.ref) || !status.sealed || order > 0)
    return failure(e, LC_ERR_INVALID,
                   "source disposal is outside durable conversion progress");
  through = order < 0 || w->progress.source_offset >= status.tail
                ? status.tail
                : w->progress.source_offset;
  rc = mark_irreversible(w, e);
  if (rc != LC_OK)
    return rc;
  return ps_collection_segment_discard_prefix(source, through, prune_select, w,
                                              removed, e);
}
int ps_collection_conversion_candidate_create(ps_collection_conversion *w,
                                              lc_u64 id,
                                              ps_collection_segment **out,
                                              lc_error *e) {
  unsigned char name[8];
  ps_catalog_info info;
  ps_owned_status status;
  ps_collection_segment *segment = NULL;
  uint64_t physical;
  int found, rc;
  if (out)
    *out = NULL;
  if (!w || w->discarded || w->aborting || !id || !out)
    return failure(e, LC_ERR_INVALID,
                   "collection conversion candidate requires an ID");
  if (w->count == PS_COLLECTION_CONVERSION_BATCH)
    return failure(e, LC_ERR_PENDING,
                   "collection conversion candidates need a checkpoint");
  put(name, id);
  rc = binding_get(w->space, w->owner, name, sizeof(name), &info, &found, e);
  if (rc == LC_OK && found)
    rc = failure(e, LC_ERR_INVALID,
                 "collection conversion candidate ID already exists");
  if (rc == LC_OK)
    rc = ps_catalog_allocate_id(w->space, &physical, e);
  if (rc == LC_OK)
    rc =
        ps_collection_segment_open(w->space, key(w->owner, physical, PS_STREAM),
                                   1, NULL, NULL, NULL, &segment, e);
  if (rc == LC_OK)
    rc = ps_collection_segment_status(segment, &status, NULL, e);
  if (rc == LC_OK)
    rc = ps_catalog_put(w->space, NULL, w->owner, PS_CATALOG_CONVERSION, name,
                        sizeof(name), NULL, key(w->owner, physical, PS_STREAM),
                        &status.ref, &info, e);
  if (rc == LC_OK) {
    w->pending[w->count].id = id;
    w->pending[w->count++].binding = info;
    *out = segment;
  } else
    ps_collection_segment_close(segment);
  return rc;
}
int ps_collection_conversion_candidate_open(ps_collection_conversion *w,
                                            lc_u64 id,
                                            ps_collection_segment **out,
                                            lc_error *e) {
  ps_catalog_info info;
  int rc;
  if (out)
    *out = NULL;
  if (!w || w->discarded || w->aborting || !id || !out)
    return failure(e, LC_ERR_INVALID,
                   "collection conversion candidate requires an ID");
  rc = candidate_get(w, id, &info, e);
  if (rc == LC_OK)
    rc = ps_collection_segment_open(w->space, info.stream, 0, &info.target,
                                    NULL, NULL, out, e);
  return rc;
}
static void entry_get(const unsigned char *p, ps_collection_relocation *r) {
  r->source_segment = get(p);
  r->source_offset = get(p + 8U);
  r->target_segment = get(p + 16U);
  r->target_offset = get(p + 24U);
  r->stored = get(p + 32U);
  r->units = get(p + 40U);
  r->first_segment = get(p + 48U);
  r->first_offset = get(p + 56U);
}
static int page_load(ps_collection_conversion *w, lc_u64 page, size_t *count,
                     lc_error *e) {
  unsigned char *p = w->scratch;
  ps_collection_relocation current, prior;
  size_t i, n;
  int rc = load_value(w, page + 1U, p, PAGE_BYTES, 0, NULL, e);
  if (rc != LC_OK)
    return rc;
  if (memcmp(p, "LCPSCCMP", 8U) || get(p + 8U) != w->owner ||
      get(p + 16U) != page || !get(p + 24U) ||
      get(p + 24U) > PS_COLLECTION_CONVERSION_MAP_ROWS || get(p + 32U))
    return failure(e, LC_ERR_PROTOCOL,
                   "collection conversion relocation page is invalid");
  n = (size_t)get(p + 24U);
  memset(&prior, 0, sizeof(prior));
  for (i = 0U; i < n; ++i) {
    entry_get(p + 40U + i * 64U, &current);
    if (!current.source_segment || !current.target_segment ||
        compare(prior.source_segment, prior.source_offset,
                current.source_segment, current.source_offset) >= 0)
      return failure(e, LC_ERR_PROTOCOL,
                     "collection conversion relocation order is invalid");
    prior = current;
  }
  *count = n;
  return LC_OK;
}
int ps_collection_conversion_checkpoint(ps_collection_conversion *w,
                                        lc_u64 segment, lc_u64 offset,
                                        const ps_collection_relocation *rows,
                                        size_t count, lc_error *e) {
  ps_collection_conversion_progress next;
  ps_catalog_info info;
  ps_owned_status status;
  lc_u64 watermark, last_segment, last_offset;
  unsigned char *p;
  size_t i;
  int rc;
  if (!w || w->discarded || w->aborting || !segment ||
      count > PS_COLLECTION_CONVERSION_MAP_ROWS || (count && !rows))
    return failure(e, LC_ERR_INVALID,
                   "collection conversion checkpoint is invalid");
  next = w->progress;
  if (compare(segment, offset, next.source_segment, next.source_offset) < 0 ||
      next.records > LC_U64_MAX - count ||
      next.candidates > LC_U64_MAX - w->count ||
      (count && next.pages >= LC_COLLECTION_SNAPSHOT_BIT - 2U))
    return failure(e, LC_ERR_INVALID,
                   "collection conversion checkpoint does not advance");
  watermark = w->watermark;
  last_segment = w->last_segment;
  last_offset = w->last_offset;
  /* Validate the entire proposed selection before writing a page. */
  for (i = 0U; i < w->count; ++i) {
    rc = ps_owned_inspect(w->space, w->pending[i].binding.stream,
                          &w->pending[i].binding.target, &status, e);
    if (rc != LC_OK)
      return rc;
    if (!status.sealed)
      return failure(e, LC_ERR_PENDING,
                     "collection conversion candidate is not sealed");
    if (w->pending[i].binding.id > watermark)
      watermark = w->pending[i].binding.id;
    if (w->pending[i].id & LC_COLLECTION_SNAPSHOT_BIT) {
      lc_u64 id = w->pending[i].id & ~LC_COLLECTION_SNAPSHOT_BIT;
      if (next.snapshot < id)
        next.snapshot = id;
    } else if (next.normal < w->pending[i].id)
      next.normal = w->pending[i].id;
  }
  for (i = 0U; i < count; ++i) {
    if (!rows[i].source_segment || !rows[i].target_segment ||
        compare(rows[i].source_segment, rows[i].source_offset,
                w->progress.source_segment, w->progress.source_offset) < 0 ||
        compare(last_segment, last_offset, rows[i].source_segment,
                rows[i].source_offset) >= 0 ||
        compare(rows[i].source_segment, rows[i].source_offset, segment,
                offset) >= 0)
      return failure(e, LC_ERR_INVALID,
                     "collection conversion mapping does not precede cursor");
    rc = candidate_get(w, rows[i].target_segment, &info, e);
    if (rc == LC_OK)
      rc = ps_owned_inspect(w->space, info.stream, &info.target, &status, e);
    if (rc != LC_OK)
      return rc;
    if (!status.sealed || rows[i].target_offset >= status.tail)
      return failure(e, LC_ERR_INVALID,
                     "collection conversion mapping escapes candidate");
    last_segment = rows[i].source_segment;
    last_offset = rows[i].source_offset;
  }
  p = w->scratch;
  if (count) {
    memset(p, 0, PAGE_BYTES);
    memcpy(p, "LCPSCCMP", 8U);
    put(p + 8U, w->owner);
    put(p + 16U, next.pages + 1U);
    put(p + 24U, count);
    for (i = 0U; i < count; ++i) {
      put(p + 40U + 64U * i, rows[i].source_segment);
      put(p + 48U + 64U * i, rows[i].source_offset);
      put(p + 56U + 64U * i, rows[i].target_segment);
      put(p + 64U + 64U * i, rows[i].target_offset);
      put(p + 72U + 64U * i, rows[i].stored);
      put(p + 80U + 64U * i, rows[i].units);
      put(p + 88U + 64U * i, rows[i].first_segment);
      put(p + 96U + 64U * i, rows[i].first_offset);
    }
    rc = store_value(w, next.pages + 2U, p, PAGE_BYTES, e);
    if (rc != LC_OK)
      return rc;
    ++next.pages;
  }
  next.records += count;
  next.candidates += w->count;
  next.source_segment = segment;
  next.source_offset = offset;
  memset(p, 0, PROGRESS_BYTES);
  memcpy(p, "LCPSCCPR", 8U);
  put(p + 8U, w->owner);
  put(p + 16U, next.pages);
  put(p + 24U, next.records);
  put(p + 32U, next.candidates);
  put(p + 40U, next.source_segment);
  put(p + 48U, next.source_offset);
  put(p + 56U, watermark);
  put(p + 64U, last_segment);
  put(p + 72U, last_offset);
  put(p + 88U, w->irreversible ? 1U : 0U);
  put(p + 96U, next.normal);
  put(p + 104U, next.snapshot);
  rc = store_value(w, 1U, p, PROGRESS_BYTES, e);
  if (rc == LC_OK) {
    w->progress = next;
    w->watermark = watermark;
    w->last_segment = last_segment;
    w->last_offset = last_offset;
    w->count = 0U;
  }
  return rc;
}
int ps_collection_conversion_lookup(ps_collection_conversion *w, lc_u64 segment,
                                    lc_u64 offset,
                                    ps_collection_relocation *out, int *present,
                                    lc_error *e) {
  ps_collection_relocation row, first, last;
  lc_u64 lo, hi, mid;
  size_t count = 0U, a, b, m;
  int rc;
  if (!w || w->discarded || w->aborting || !segment || !out || !present)
    return failure(e, LC_ERR_INVALID,
                   "collection relocation lookup requires an ID");
  lo = 1U;
  hi = w->progress.pages + 1U;
  while (lo < hi) {
    mid = lo + (hi - lo) / 2U;
    rc = page_load(w, mid, &count, e);
    if (rc != LC_OK)
      return rc;
    entry_get(w->scratch + 40U, &first);
    entry_get(w->scratch + 40U + 64U * (count - 1U), &last);
    if (compare(segment, offset, first.source_segment, first.source_offset) < 0)
      hi = mid;
    else if (compare(segment, offset, last.source_segment, last.source_offset) >
             0)
      lo = mid + 1U;
    else {
      a = 0U;
      b = count;
      while (a < b) {
        m = a + (b - a) / 2U;
        entry_get(w->scratch + 40U + 64U * m, &row);
        rc = compare(segment, offset, row.source_segment, row.source_offset);
        if (rc < 0)
          b = m;
        else if (rc > 0)
          a = m + 1U;
        else {
          *out = row;
          *present = 1;
          return LC_OK;
        }
      }
      break;
    }
  }
  *present = 0;
  return LC_OK;
}
int ps_collection_conversion_discard(ps_collection_conversion *w, lc_error *e) {
  cleanup c;
  int rc;
  if (!w || w->discarded || w->irreversible)
    return failure(e, LC_ERR_INVALID,
                   "collection conversion is closed or requires completion "
                   "after source disposal");
  memset(&c, 0, sizeof(c));
  c.space = w->space;
  c.owner = w->owner;
  c.keep_progress = 1;
  if (!w->aborting) {
    unsigned char *p = w->scratch;
    memset(p, 0, PROGRESS_BYTES);
    memcpy(p, "LCPSCCPR", 8U);
    put(p + 8U, w->owner);
    put(p + 16U, w->progress.pages);
    put(p + 24U, w->progress.records);
    put(p + 32U, w->progress.candidates);
    put(p + 40U, w->progress.source_segment);
    put(p + 48U, w->progress.source_offset);
    put(p + 56U, w->watermark);
    put(p + 64U, w->last_segment);
    put(p + 72U, w->last_offset);
    put(p + 80U, 1U);
    put(p + 96U, w->progress.normal);
    put(p + 104U, w->progress.snapshot);
    rc = store_value(w, 1U, p, PROGRESS_BYTES, e);
    if (rc != LC_OK)
      return rc;
    w->aborting = 1;
  }
  rc = ps_catalog_visit(w->space, w->owner, PS_CATALOG_CONVERSION,
                        remove_binding, &c, e);
  if (rc == LC_OK)
    rc = ps_values_visit(w->space, w->owner, PS_CONTROL, PS_CONTROL,
                         remove_value, &c, e);
  if (rc == LC_OK)
    rc = ps_catalog_remove(w->space, w->collection, PS_CATALOG_CONVERSION, NULL,
                           0U, &w->binding.revision, e);
  if (rc == LC_OK)
    w->discarded = 1;
  return rc;
}
typedef struct finish_check {
  ps_collection_conversion *work;
  const lc_collection_disk_head *head;
} finish_check;
static int verify_selected(void *context, const void *name, size_t length,
                           const ps_catalog_info *info, lc_error *e) {
  finish_check *check = context;
  ps_collection_conversion *w = check->work;
  ps_catalog_view *view = NULL;
  ps_catalog_info selected;
  lc_u64 id;
  unsigned int kind;
  int rc;
  if (length == 9U)
    return LC_OK;
  if (length != 8U)
    return failure(e, LC_ERR_PROTOCOL, "conversion candidate name is invalid");
  id = get(name);
  if (!lc_collection_segment_selected(check->head, id))
    return failure(e, LC_ERR_INVALID, "conversion head omits a candidate");
  kind = id & LC_COLLECTION_SNAPSHOT_BIT ? PS_CATALOG_SNAPSHOT
                                         : PS_CATALOG_SEGMENT;
  rc = ps_catalog_get(w->space, w->collection, kind, name, length, &view, e);
  if (rc == LC_OK && !view)
    rc = failure(e, LC_ERR_INVALID,
                 "conversion candidate is not canonically selected");
  if (rc == LC_OK)
    rc = ps_catalog_view_info(view, &selected, e);
  if (rc == LC_OK && (selected.stream.owner != info->stream.owner ||
                      selected.stream.key != info->stream.key ||
                      selected.stream.kind != info->stream.kind ||
                      !same_ref(&selected.target, &info->target)))
    rc = failure(e, LC_ERR_INVALID,
                 "conversion candidate selection has changed");
  ps_catalog_view_close(view);
  return rc;
}
int ps_collection_conversion_finish(ps_collection_conversion *w,
                                    const lc_collection_disk_head *selected,
                                    lc_error *e) {
  lc_collection_disk_head current;
  unsigned char a[LC_COLLECTION_HEAD_BYTES], b[LC_COLLECTION_HEAD_BYTES];
  cleanup c;
  finish_check check;
  int rc;
  if (!w || w->discarded || w->aborting || !selected || w->count)
    return failure(e, LC_ERR_INVALID,
                   "conversion completion requires selected work");
  rc = ps_collection_head_load(w->space, w->collection, NULL, &current, e);
  if (rc == LC_OK)
    rc = lc_collection_head_encode(selected, a, e);
  if (rc == LC_OK)
    rc = lc_collection_head_encode(&current, b, e);
  if (rc == LC_OK && memcmp(a, b, sizeof(a)))
    rc = failure(e, LC_ERR_INVALID, "conversion completion head has changed");
  if (rc == LC_OK)
    rc = lc_collection_head_encode(&w->source, b, e);
  if (rc == LC_OK && !memcmp(a, b, sizeof(a)))
    rc = failure(e, LC_ERR_INVALID, "conversion has not selected a new head");
  check.work = w;
  check.head = selected;
  if (rc == LC_OK)
    rc = ps_catalog_visit(w->space, w->owner, PS_CATALOG_CONVERSION,
                          verify_selected, &check, e);
  memset(&c, 0, sizeof(c));
  c.space = w->space;
  c.owner = w->owner;
  if (rc == LC_OK)
    rc = ps_catalog_visit(w->space, w->owner, PS_CATALOG_CONVERSION,
                          remove_binding, &c, e);
  if (rc == LC_OK)
    rc = ps_catalog_remove(w->space, w->collection, PS_CATALOG_CONVERSION, NULL,
                           0U, &w->binding.revision, e);
  if (rc == LC_OK)
    w->discarded = 1;
  return rc;
}
