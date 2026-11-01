#include "lc_collection_payload.h"
#include "lc_pouch_path.h"
#include "lc_utf8.h"
#include <lonejson.h>
#include <openssl/crypto.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Control values are bounded metadata, not buffered replacement documents.
 * Keep the complete work object on the owning collection's allocator. */
#define CE_FIXED 416U
#define CE_RAW 4096U
#define CE_JSON (CE_RAW * 2U + 32U)

typedef struct ce_control {
  unsigned char fingerprint[32];
  lc_collection_edit_range ranges[LC_COLLECTION_EDIT_MAX_RANGES];
  size_t count;
  char replacement_id[129];
  lc_collection_edit_res result;
  lc_u64 revision;
  lc_u64 anchor;
  char recovery[LC_COLLECTION_RECOVERY_MAX_BYTES + 1U];
} ce_control;

typedef struct ce_work {
  lc_collection_edit_req request;
  lc_collection_append_req replacement;
  lc_collection_edit_range requested_ranges[LC_COLLECTION_EDIT_MAX_RANGES];
  char operation_id[129];
  char append_id[129];
  char expected_head[65];
  char recovery[LC_COLLECTION_RECOVERY_MAX_BYTES + 1U];
  ce_control control;
  ce_control previous;
  lc_pouch_collection_record body;
  lc_pouch_collection_record receipt;
  lc_collection_payload payload;
  unsigned char raw[CE_RAW];
  char json[CE_JSON];
} ce_work;

#ifdef LOCKDC_TEST_BUILD
lc_collection_edit_hook_fn lc_collection_edit_test_hook;
void *lc_collection_edit_test_context;
#endif
static int ce_phase(lc_collection_handle *c, int phase, lc_error *e) {
#ifdef LOCKDC_TEST_BUILD
  if (lc_collection_edit_test_hook)
    return lc_collection_edit_test_hook(lc_collection_edit_test_context,
                                        c ? &c->pub : NULL, phase, e);
#else
  (void)c;
  (void)phase;
  (void)e;
#endif
  return LC_OK;
}
#ifdef LOCKDC_TEST_BUILD
int lc_collection_edit_page_test(lc_error *e) {
  return ce_phase(NULL, LC_COLLECTION_EDIT_PAGE, e);
}
#endif
static int ce_error(lc_error *e, int code, const char *reason,
                    const char *text) {
  return lc_collection_failure(e, code, reason, text);
}
static void ce_put(unsigned char *p, lc_u64 n) {
  unsigned int i;
  for (i = 0; i < 8; ++i) {
    p[i] = (unsigned char)n;
    n >>= 8;
  }
}
static lc_u64 ce_get(const unsigned char *p) {
  lc_u64 n = 0;
  unsigned int i;
  for (i = 8; i; --i)
    n = (n << 8) | p[i - 1];
  return n;
}
static void ce_hex(const unsigned char *p, char out[65]) {
  static const char hex[] = "0123456789abcdef";
  size_t i;
  for (i = 0; i < 32; ++i) {
    out[i * 2] = hex[p[i] >> 4];
    out[i * 2 + 1] = hex[p[i] & 15];
  }
  out[64] = 0;
}
static int ce_id(const char *id) {
  size_t n;
  if (!id || !(n = strlen(id)) || n > 128)
    return 0;
  return strspn(id, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ012345"
                    "6789-_.~") == n;
}
static int ce_token_valid(const char *s) {
  return s && strlen(s) == 64 && strspn(s, "0123456789abcdef") == 64;
}
lc_u64 lc_collection_binding_count(const lc_collection_order *o) {
  return o->count + o->edit_count + (o->edit_count ? 1U : 0U);
}
int lc_collection_head_token(lc_collection_handle *c, char out[65],
                             lc_error *e) {
  unsigned char bytes[112], hash[32];
  int rc;
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, c->id, LC_XID_STRING_SIZE - 1U);
  memcpy(bytes + 24U, c->order.query_id, LC_XID_STRING_SIZE - 1U);
  ce_put(bytes + 48U, c->order.generation);
  ce_put(bytes + 56U, (lc_u64)lc_collection_retention_deadline(c));
  ce_put(bytes + 64U, c->head.retention_generation);
  ce_put(bytes + 72U, c->head.max_entries);
  rc = lc_collection_hash(bytes, sizeof(bytes), hash, e);
  if (rc == LC_OK)
    ce_hex(hash, out);
  return rc;
}
int lc_collection_head_match(lc_collection_handle *c, const char *expected,
                             lc_error *e) {
  char actual[65];
  int rc;
  if (!expected)
    return LC_OK;
  if (!ce_token_valid(expected))
    return ce_error(e, LC_ERR_INVALID, NULL, "invalid collection head token");
  rc = lc_collection_head_token(c, actual, e);
  if (rc == LC_OK && strcmp(actual, expected))
    rc =
        ce_error(e, LC_ERR_INVALID, "collection_changed",
                 "collection head changed; observe a new head before rebasing");
  return rc;
}
static int ce_edit_match(lc_collection_handle *c, const char *expected,
                         lc_collection_edit_res *out, lc_error *e) {
  int rc = lc_collection_head_match(c, expected, e);
  if (rc != LC_OK && e->server_code &&
      strcmp(e->server_code, "collection_changed") == 0) {
    int observed = lc_collection_head_token(c, out->head_etag, e);
    if (observed != LC_OK)
      return observed;
    out->upper_position = c->order.upper_position;
    out->live_count = c->order.live_count;
  }
  return rc;
}
static int ce_key(const char *id, unsigned char out[32], lc_error *e) {
  char key[136];
  size_t n = id ? strlen(id) : 0;
  memcpy(key, id ? "edit:" : "head:", 5);
  if (n)
    memcpy(key + 5, id, n);
  return lc_collection_hash(key, n + 5, out, e);
}
static size_t ce_pack(ce_work *w) {
  ce_control *c = &w->control;
  lc_collection_edit_res *r = &c->result;
  size_t i, n = strlen(c->recovery);
  unsigned char *p = w->raw;
  memset(p, 0, CE_RAW);
  memcpy(p, "LCCE001", 7);
  ce_put(p + 8, c->count);
  memcpy(p + 16, c->fingerprint, 32);
  memcpy(p + 48, r->head_etag, 64);
  ce_put(p + 112, r->upper_position);
  ce_put(p + 120, r->live_count);
  ce_put(p + 128, r->deleted_count);
  ce_put(p + 136, r->has_replacement);
  ce_put(p + 144, r->replacement.position);
  ce_put(p + 152, r->replacement.bytes);
  memcpy(p + 160, r->replacement.etag, 64);
  ce_put(p + 224, r->replacement.has_retention_expires_at_unix);
  ce_put(p + 232, (lc_u64)r->replacement.retention_expires_at_unix);
  ce_put(p + 240, r->replacement.has_collection_retention_expires_at_unix);
  ce_put(p + 248, (lc_u64)r->replacement.collection_retention_expires_at_unix);
  ce_put(p + 256, c->revision);
  ce_put(p + 264, c->anchor);
  ce_put(p + 272, n);
  ce_put(p + 280, strlen(c->replacement_id));
  memcpy(p + 288, c->replacement_id, strlen(c->replacement_id));
  for (i = 0; i < c->count; ++i) {
    ce_put(p + CE_FIXED + i * 16, c->ranges[i].after);
    ce_put(p + CE_FIXED + i * 16 + 8, c->ranges[i].through);
  }
  memcpy(p + CE_FIXED + c->count * 16, c->recovery, n);
  return CE_FIXED + c->count * 16 + n;
}
static int ce_unpack(ce_work *w, size_t length, lc_error *e) {
  ce_control *c = &w->control;
  lc_collection_edit_res *r;
  const unsigned char *p = w->raw;
  lc_u64 count, n, idlen;
  size_t i;
  if (length < CE_FIXED || memcmp(p, "LCCE001\0", 8))
    goto corrupt;
  count = ce_get(p + 8);
  n = ce_get(p + 272);
  idlen = ce_get(p + 280);
  if (count > LC_COLLECTION_EDIT_MAX_RANGES ||
      n > LC_COLLECTION_RECOVERY_MAX_BYTES || idlen > 128 ||
      length != CE_FIXED + count * 16 + n || ce_get(p + 136) > 1 ||
      ce_get(p + 224) > 1 || ce_get(p + 240) > 1 ||
      ce_get(p + 232) > (lc_u64)LC_I64_MAX ||
      ce_get(p + 248) > (lc_u64)LC_I64_MAX)
    goto corrupt;
  memset(c, 0, sizeof(*c));
  r = &c->result;
  c->count = (size_t)count;
  memcpy(c->fingerprint, p + 16, 32);
  memcpy(r->head_etag, p + 48, 64);
  r->published = 1;
  r->upper_position = ce_get(p + 112);
  r->live_count = ce_get(p + 120);
  r->deleted_count = ce_get(p + 128);
  r->has_replacement = (int)ce_get(p + 136);
  r->replacement.position = ce_get(p + 144);
  r->replacement.bytes = ce_get(p + 152);
  memcpy(r->replacement.etag, p + 160, 64);
  r->replacement.has_retention_expires_at_unix = (int)ce_get(p + 224);
  r->replacement.retention_expires_at_unix = (lc_unix_seconds)ce_get(p + 232);
  r->replacement.has_collection_retention_expires_at_unix =
      (int)ce_get(p + 240);
  r->replacement.collection_retention_expires_at_unix =
      (lc_unix_seconds)ce_get(p + 248);
  r->replacement.retention_support = LC_STATE_RETENTION_SUPPORTED;
  c->revision = ce_get(p + 256);
  c->anchor = ce_get(p + 264);
  memcpy(c->replacement_id, p + 288, (size_t)idlen);
  memcpy(c->recovery, p + CE_FIXED + count * 16, (size_t)n);
  if (strlen(c->recovery) != n || !ce_token_valid(r->head_etag) ||
      r->live_count > r->upper_position ||
      (r->has_replacement &&
       (!ce_id(c->replacement_id) || !ce_token_valid(r->replacement.etag) ||
        !r->replacement.position)))
    goto corrupt;
  for (i = 0; i < c->count; ++i) {
    c->ranges[i].after = ce_get(p + CE_FIXED + i * 16);
    c->ranges[i].through = ce_get(p + CE_FIXED + i * 16 + 8);
    if (c->ranges[i].after >= c->ranges[i].through ||
        (i && c->ranges[i].after < c->ranges[i - 1].through) ||
        c->ranges[i].through >
            r->upper_position - (r->has_replacement ? 1U : 0U))
      goto corrupt;
  }
  return LC_OK;
corrupt:
  return ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                  "invalid collection edit receipt");
}
static int ce_load(lc_collection_handle *c, int fd,
                   const lc_pouch_collection_record *record, ce_work *w,
                   lc_error *e) {
  lonejson_error je;
  size_t used = 0, got = 0, raw = 0;
  int rc;
  if (record->kind != LC_POUCH_COLLECTION_EDIT || record->bytes > CE_JSON - 1 ||
      record->bytes < 11)
    return ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                    "invalid edit record kind or size");
  rc = lc_collection_payload_open(&w->payload, c, fd, record, e);
  while (rc == LC_OK) {
    rc = lc_collection_payload_read(&w->payload, w->json + used,
                                    CE_JSON - 1 - used, &got, e);
    if (rc != LC_OK || !got)
      break;
    used += got;
    if (used >= CE_JSON - 1) {
      rc = LC_ERR_PROTOCOL;
      break;
    }
  }
  if (rc == LC_OK)
    rc = lc_collection_payload_finish(&w->payload, e);
  lc_collection_payload_cleanup(&w->payload);
  if (rc != LC_OK)
    return rc;
  if (used < 11 || memcmp(w->json, "{\"edit\":\"", 9) ||
      memcmp(w->json + used - 2, "\"}", 2))
    return ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                    "invalid edit envelope");
  lonejson_error_init(&je);
  if (lonejson_base64_decode(w->json + 9, used - 11, LONEJSON_BASE64_URL_RAW,
                             w->raw, CE_RAW, &raw, &je) != LONEJSON_STATUS_OK)
    return ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                    "invalid edit encoding");
  return ce_unpack(w, raw, e);
}
typedef struct ce_lookup {
  lc_collection_handle *c;
  const char *id;
  ce_work *work;
} ce_lookup;
static int ce_match(void *ctx, const lc_collection_id_ref *ref, int *matched,
                    lc_error *e) {
  ce_lookup *look = ctx;
  lc_pouch_collection_record record;
  lc_u64 segment = 0, end = 0;
  int fd = -1, rc;
  *matched = 0;
  rc = lc_collection_record_ref(look->c, ref->segment, ref->offset, &fd,
                                &segment, &end, &record, e);
  if (rc == LC_OK && record.kind == LC_POUCH_COLLECTION_EDIT &&
      (!look->id || strcmp(look->id, record.append_id) == 0)) {
    if (record.generation != ref->generation ||
        (look->id && record.position != ref->position))
      rc = ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                    "invalid edit index binding");
    else
      rc = ce_load(look->c, fd, &record, look->work, e);
    if (rc == LC_OK)
      *matched = 1;
  }
  if (fd >= 0)
    close(fd);
  return rc;
}
static int ce_find(lc_collection_handle *c, const char *id, ce_work *w,
                   int *found, lc_error *e) {
  ce_lookup look;
  lc_collection_id_ref ref;
  unsigned char hash[32];
  int rc = ce_key(id, hash, e);
  look.c = c;
  look.id = id;
  look.work = w;
  *found = 0;
  if (rc == LC_OK)
    rc = lc_collection_ids_find(&c->ids, hash, ce_match, &look, &ref, found, e);
  return rc;
}
static int ce_index(lc_collection_ids *ids,
                    const lc_pouch_collection_record *record, lc_u64 segment,
                    lc_error *e) {
  lc_collection_id_ref ref;
  int rc;
  memset(&ref, 0, sizeof(ref));
  ref.position = record->position;
  ref.generation = record->generation;
  ref.segment = segment;
  ref.offset = record->record_offset;
  rc = ce_key(record->append_id, ref.digest, e);
  if (rc == LC_OK)
    rc = lc_collection_ids_put(ids, &ref, e);
  ref.position = 1;
  if (rc == LC_OK)
    rc = ce_key(NULL, ref.digest, e);
  if (rc == LC_OK)
    rc = lc_collection_ids_put(ids, &ref, e);
  return rc;
}

int lc_collection_head_method(lc_collection *self, lc_collection_head_res *out,
                              lc_error *error) {
  lc_collection_call call;
  lc_collection_handle *c;
  lc_collection_order_row row;
  ce_work *w;
  int rc, pin, found;
  if (!out)
    return ce_error(error, LC_ERR_INVALID, NULL,
                    "collection head requires output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  c = call.collection;
  error = call.error;
  w = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*w));
  rc = w ? lc_collection_capture_enter(c, &pin, error)
         : ce_error(error, LC_ERR_NOMEM, NULL,
                    "failed to allocate collection edit observation");
  if (rc == LC_OK) {
    rc = lc_collection_refresh_view(c, error);
    if (rc == LC_OK)
      rc = lc_collection_ids_refresh(&c->ids, error);
    if (rc == LC_OK)
      rc = lc_collection_lease_check(c, 0, error);
    if (rc == LC_OK)
      rc = lc_collection_head_token(c, out->etag, error);
    if (rc == LC_OK && c->order.edit_count) {
      rc = ce_find(c, NULL, w, &found, error);
      if (rc == LC_OK && !found)
        rc = ce_error(error, LC_ERR_PROTOCOL, "collection_corrupt",
                      "latest edit binding is missing");
    }
    if (rc == LC_OK) {
      strcpy(out->collection_id, c->id);
      out->upper_position = c->order.upper_position;
      out->live_count = c->order.live_count;
      out->recovery_revision = w->control.revision;
      out->recovery_anchor = w->control.anchor;
      strcpy(out->recovery, w->control.recovery);
      out->recovery_anchor_available = out->recovery_anchor == 0;
      if (out->recovery_anchor) {
        rc = lc_collection_order_find(&c->order, out->recovery_anchor, &row,
                                      &found, error);
        if (rc == LC_OK)
          out->recovery_anchor_available = found && !row.deleted_at;
      }
    }
    lc_collection_capture_leave(c, &pin);
  }
  lc_free_with_allocator(&c->allocator, w);
  lc_collection_call_end(&call);
  return rc;
}
int lc_collection_edit_status_method(lc_collection *self, const char *id,
                                     lc_collection_edit_status_res *out,
                                     lc_error *error) {
  lc_collection_call call;
  lc_collection_handle *c;
  ce_work *w;
  int rc, pin;
  if (!out || !ce_id(id))
    return ce_error(error, LC_ERR_INVALID, NULL,
                    "edit status requires operation ID and output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 0, &call, error);
  if (rc != LC_OK)
    return rc;
  c = call.collection;
  error = call.error;
  w = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*w));
  rc = w ? lc_collection_capture_enter(c, &pin, error)
         : ce_error(error, LC_ERR_NOMEM, NULL,
                    "failed to allocate collection edit observation");
  if (rc == LC_OK) {
    rc = lc_collection_refresh_view(c, error);
    if (rc == LC_OK)
      rc = lc_collection_ids_refresh(&c->ids, error);
    if (rc == LC_OK)
      rc = lc_collection_lease_check(c, 0, error);
    if (rc == LC_OK)
      rc = ce_find(c, id, w, &out->found, error);
    if (rc == LC_OK && out->found)
      out->result = w->control.result;
    lc_collection_capture_leave(c, &pin);
  }
  lc_free_with_allocator(&c->allocator, w);
  lc_collection_call_end(&call);
  return rc;
}

static int ce_project(lc_collection_handle *c, lc_collection_order *order,
                      lc_collection_ids *ids, const ce_control *control,
                      const lc_pouch_collection_record *record,
                      const lc_pouch_collection_record *body, lc_u64 segment,
                      int snapshot, lc_error *e) {
  lc_u64 removed = 0;
  int rc = LC_OK;
  if (!snapshot && record->generation > order->generation) {
    if (control->result.has_replacement &&
        (!body || body->kind != LC_POUCH_COLLECTION_EDIT_BODY ||
         body->next_offset != record->record_offset ||
         body->generation != record->generation ||
         body->position != control->result.replacement.position ||
         body->bytes != control->result.replacement.bytes ||
         strcmp(body->etag, control->result.replacement.etag) ||
         strcmp(body->append_id, control->replacement_id)))
      rc = ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                    "edit replacement binding is missing");
    if (rc == LC_OK)
      rc = lc_collection_order_edit_begin(order, record->generation, e);
    if (rc == LC_OK)
      rc = ce_phase(c, LC_COLLECTION_EDIT_DIRTY, e);
    if (rc == LC_OK)
      rc = lc_collection_order_cut(order, &c->allocator, control->ranges,
                                   control->count, record->generation, &removed,
                                   e);
    if (rc == LC_OK && control->result.has_replacement)
      rc = lc_collection_project_record(order, ids, body, segment, 0, e);
    if (rc == LC_OK &&
        (removed != control->result.deleted_count ||
         order->live_count != control->result.live_count ||
         order->upper_position != control->result.upper_position))
      rc = ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                    "edit outcome differs from canonical history");
  }
  if (rc == LC_OK)
    rc = ce_index(ids, record, segment, e);
  if (rc == LC_OK) {
    if (record->position > order->edit_count)
      order->edit_count = record->position;
    if (record->generation > order->generation)
      order->generation = record->generation;
    order->edit_in_progress = 0;
    rc = lc_collection_order_checkpoint(order, snapshot ? 1U : segment,
                                        record->next_offset, e);
  }
  return rc;
}

int lc_collection_edit_project(lc_collection_handle *c,
                               lc_collection_order *order,
                               lc_collection_ids *ids, int fd,
                               const lc_pouch_collection_record *record,
                               const lc_pouch_collection_record *body,
                               lc_u64 segment, int snapshot, lc_error *e) {
  ce_work *w = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*w));
  int rc =
      w ? ce_load(c, fd, record, w, e)
        : ce_error(e, LC_ERR_NOMEM, NULL, "failed to allocate edit replay");
  if (rc == LC_OK)
    rc = ce_project(c, order, ids, &w->control, record, body, segment, snapshot,
                    e);
  lc_free_with_allocator(&c->allocator, w);
  return rc;
}

/* Compact input metadata is canonicalized into a fixed-width request digest;
 * irrelevant fields behind absent options are deliberately ignored. */
static int ce_fingerprint(ce_work *w, const lc_collection_edit_req *req,
                          lc_error *e) {
  unsigned char *p = w->raw;
  size_t at = 0, i, n;
  memset(p, 0, CE_RAW);
  memcpy(p, req->expected_head, 64);
  at = 64;
  ce_put(p + at, req->range_count);
  at += 8;
  for (i = 0; i < req->range_count; ++i) {
    ce_put(p + at, req->ranges[i].after);
    ce_put(p + at + 8, req->ranges[i].through);
    at += 16;
  }
  ce_put(p + at, req->replacement != NULL);
  at += 8;
  if (req->replacement) {
    const lc_collection_append_req *r = req->replacement;
    memcpy(p + at, r->append_id, strlen(r->append_id));
    at += 128;
    ce_put(p + at, r->has_retention_seconds != 0);
    at += 8;
    ce_put(p + at, r->has_retention_seconds ? (lc_u64)r->retention_seconds : 0);
    at += 8;
    ce_put(p + at, r->has_collection_retention_seconds != 0);
    at += 8;
    ce_put(p + at, r->has_collection_retention_seconds
                       ? (lc_u64)r->collection_retention_seconds
                       : 0);
    at += 8;
    memcpy(p + at, w->body.etag, 64);
    at += 64;
    ce_put(p + at, w->body.bytes);
    at += 8;
  }
  ce_put(p + at, req->has_recovery != 0);
  at += 8;
  if (req->has_recovery) {
    ce_put(p + at, req->recovery_anchor);
    at += 8;
    n = strlen(req->recovery);
    memcpy(p + at, req->recovery, n);
    at += n;
  }
  return lc_collection_hash(p, at, w->control.fingerprint, e);
}
typedef struct ce_source {
  lc_source pub;
  lc_source *inner;
  lc_collection_call *call;
} ce_source;
static size_t ce_read(lc_source *s, void *p, size_t n, lc_error *e) {
  ce_source *source = (ce_source *)s;
  size_t got;
  lc_collection_call_callback(source->call, 1, 1);
  got = source->inner->read(source->inner, p, n, e);
  lc_collection_call_callback(source->call, 0, 0);
  return got;
}
static int ce_validate(lc_collection_handle *c, const lc_collection_edit_req *r,
                       lc_source *s, lc_error *e) {
  size_t i, n;
  lc_utf8_state utf8;
  if (!r || !ce_id(r->operation_id) || !ce_token_valid(r->expected_head) ||
      r->range_count > LC_COLLECTION_EDIT_MAX_RANGES ||
      (r->range_count && !r->ranges) ||
      ((r->replacement != NULL) != (s != NULL)) || (s && !s->read))
    return ce_error(e, LC_ERR_INVALID, NULL,
                    "edit requires operation ID, expected head, valid ranges "
                    "and paired replacement/source");
  for (i = 0; i < r->range_count; ++i)
    if (r->ranges[i].after >= r->ranges[i].through ||
        (i && r->ranges[i].after < r->ranges[i - 1].through))
      return ce_error(e, LC_ERR_INVALID, NULL,
                      "edit ranges must be finite, sorted and disjoint");
  if (r->has_recovery) {
    memset(&utf8, 0, sizeof(utf8));
    if (!r->recovery ||
        (n = strlen(r->recovery)) > LC_COLLECTION_RECOVERY_MAX_BYTES ||
        !lc_utf8_push(&utf8, (const unsigned char *)r->recovery, n) ||
        utf8.remaining || (!n && r->recovery_anchor) ||
        (r->recovery_anchor == LC_U64_MAX && !r->replacement))
      return ce_error(e, LC_ERR_INVALID, NULL,
                      "invalid recovery descriptor or anchor");
  }
  return r->replacement ? lc_collection_append_validate(c, r->replacement, e)
                        : LC_OK;
}
static void ce_append_result(const lc_pouch_collection_record *record,
                             lc_collection_append_res *out) {
  memset(out, 0, sizeof(*out));
  out->position = record->position;
  out->bytes = record->bytes;
  strcpy(out->etag, record->etag);
  out->retention_support = LC_STATE_RETENTION_SUPPORTED;
  out->has_retention_expires_at_unix = 1;
  out->retention_expires_at_unix = record->retention;
  out->has_collection_retention_expires_at_unix =
      record->has_collection_retention;
  out->collection_retention_expires_at_unix = record->collection_retention;
}

/* Freeze bounded options before entering any producer callback. The caller
 * may prepare its next request while producing this one's immutable body. */
static void ce_snapshot(ce_work *w, const lc_collection_edit_req *request) {
  w->request = *request;
  strcpy(w->operation_id, request->operation_id);
  strcpy(w->expected_head, request->expected_head);
  w->request.operation_id = w->operation_id;
  w->request.expected_head = w->expected_head;
  w->request.ranges = w->requested_ranges;
  if (request->range_count)
    memcpy(w->requested_ranges, request->ranges,
           request->range_count * sizeof(*request->ranges));
  if (request->replacement) {
    w->replacement = *request->replacement;
    strcpy(w->append_id, request->replacement->append_id);
    w->replacement.append_id = w->append_id;
    w->request.replacement = &w->replacement;
  }
  if (request->has_recovery) {
    strcpy(w->recovery, request->recovery);
    w->request.recovery = w->recovery;
  } else
    w->request.recovery = NULL;
}

int lc_collection_edit_method(lc_collection *self,
                              const lc_collection_edit_req *req,
                              lc_source *source, lc_collection_edit_res *out,
                              lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_collection_handle *c;
  lc_pouch_collection_writer *writer = NULL;
  lc_source *control_source = NULL;
  ce_source guarded;
  ce_work *w;
  lc_u64 generation, segment, fencing, saved_generation,
      saved_parent_generation;
  lc_unix_seconds saved_parent;
  lc_collection_order_row anchor;
  lonejson_error je;
  char leaf[64];
  size_t raw, encoded = 0, i;
  int rc, entered = 0, metadata = 0, found = 0, published = 0, dirty = 0,
          body_published = 0;
  if (!out)
    return ce_error(error, LC_ERR_INVALID, NULL, "edit requires output");
  memset(out, 0, sizeof(*out));
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  c = call.collection;
  error = call.error;
  w = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*w));
  rc = w ? ce_validate(c, req, source, error)
         : ce_error(error, LC_ERR_NOMEM, NULL,
                    "failed to allocate edit working buffers");
  if (rc == LC_OK) {
    ce_snapshot(w, req);
    req = &w->request;
    rc = lc_collection_append_enter(&call, &guard, error);
    entered = metadata = rc == LC_OK;
  }
  generation = segment = fencing = 0;
  if (rc == LC_OK) {
    generation = c->order.generation + 1U;
    segment = c->head.active;
    fencing = c->head.lease.fencing_token;
    rc = ce_find(c, req->operation_id, w, &found, error);
    if (rc == LC_OK && found)
      w->previous = w->control;
    if (rc == LC_OK && !found)
      rc = ce_edit_match(c, req->expected_head, out, error);
    if (rc == LC_OK && !found &&
        (!generation ||
         c->order.edit_count >= LC_U64_MAX - c->order.count - 2U ||
         (req->replacement && c->order.upper_position == LC_U64_MAX)))
      rc = ce_error(error, LC_ERR_INVALID, "sequence_exhausted",
                    "collection sequence exhausted");
    for (i = 0; rc == LC_OK && !found && i < req->range_count; ++i)
      if (req->ranges[i].through > c->order.upper_position)
        rc = ce_error(error, LC_ERR_INVALID, NULL,
                      "edit range exceeds observed collection end");
    if (rc == LC_OK && !found && req->replacement) {
      int exists;
      rc = lc_collection_lookup_id(c, req->replacement->append_id, &w->body,
                                   NULL, &exists, error);
      if (rc == LC_OK && exists)
        rc = ce_error(error, LC_ERR_INVALID, "append_id_conflict",
                      "edit replacement append ID is already bound");
    }
    if (rc == LC_OK && !found)
      rc = lc_collection_rotate(c, 0, error);
    segment = c->head.active;
    if (rc == LC_OK)
      rc = lc_collection_record_leaf(segment, leaf, error);
    lc_collection_metadata_leave(c);
    metadata = 0;
  }
  guarded.pub.read = ce_read;
  guarded.inner = source;
  guarded.call = &call;
  if (rc == LC_OK && req->replacement) {
    const lc_collection_append_req *r = req->replacement;
    rc = lc_pouch_collection_writer_open(
        c->pouch, found ? -1 : c->active_fd, found ? NULL : leaf, c->id,
        r->append_id, 0, 0,
        r->has_retention_seconds ? -(lc_unix_seconds)r->retention_seconds : 0,
        r->has_collection_retention_seconds,
        r->has_collection_retention_seconds
            ? -(lc_unix_seconds)r->collection_retention_seconds
            : 0,
        &writer, error);
    if (rc == LC_OK)
      lc_pouch_collection_writer_kind(writer, LC_POUCH_COLLECTION_EDIT_BODY);
    if (rc == LC_OK)
      rc = lc_pouch_collection_writer_consume(writer, c->pouch, &guarded.pub,
                                              &w->body, error);
  }
  if (rc == LC_OK)
    rc = ce_fingerprint(w, req, error);
  if (rc == LC_OK && found &&
      CRYPTO_memcmp(w->control.fingerprint, w->previous.fingerprint, 32))
    rc = ce_error(error, LC_ERR_INVALID, "operation_id_conflict",
                  "operation ID is bound to a different edit request");
  if (rc == LC_OK && !found)
    rc = ce_phase(c, LC_COLLECTION_EDIT_PREPARED, error);
  if (rc == LC_OK && !found)
    rc = lc_collection_edit_barrier(&guard, error);
  /* Retries also consume caller input without metadata authority. Observing
   * an old receipt does not authorize a receiver that lost ownership meanwhile.
   */
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(c, 1, error);
    metadata = rc == LC_OK;
    if (rc == LC_OK)
      rc = lc_collection_head_read(c, error);
    if (rc == LC_OK)
      rc = lc_collection_lease_check(c, 1, error);
    if (rc == LC_OK && fencing != c->head.lease.fencing_token)
      rc = ce_error(error, LC_ERR_INVALID, "lease_changed",
                    "collection ownership changed during edit input");
  }
  if (rc == LC_OK && found) {
    *out = w->previous.result;
    out->duplicate = 1;
  }
  if (rc == LC_OK && !found) {
    rc = ce_edit_match(c, req->expected_head, out, error);
    if (rc == LC_OK && c->order.edit_count) {
      rc = ce_find(c, NULL, w, &found, error);
      if (rc == LC_OK && !found)
        rc = ce_error(error, LC_ERR_PROTOCOL, "collection_corrupt",
                      "latest edit binding is missing");
      found = 0;
    }
    if (rc == LC_OK) {
      w->control.count = req->range_count;
      if (req->range_count)
        memcpy(w->control.ranges, req->ranges,
               req->range_count * sizeof(*req->ranges));
      rc = ce_fingerprint(w, req, error);
    }
    if (rc == LC_OK && req->has_recovery) {
      strcpy(w->control.recovery, req->recovery);
      w->control.anchor = req->recovery_anchor == LC_U64_MAX
                              ? c->order.upper_position + 1U
                              : req->recovery_anchor;
      w->control.revision = generation;
      if (w->control.anchor && req->recovery_anchor != LC_U64_MAX) {
        int live;
        rc = lc_collection_order_find(&c->order, w->control.anchor, &anchor,
                                      &live, error);
        if (rc == LC_OK && (!live || anchor.deleted_at))
          rc = ce_error(error, LC_ERR_INVALID, "entry_not_found",
                        "recovery anchor is unavailable");
        for (i = 0; rc == LC_OK && i < req->range_count; ++i)
          if (w->control.anchor > req->ranges[i].after &&
              w->control.anchor <= req->ranges[i].through)
            rc = ce_error(error, LC_ERR_INVALID, NULL,
                          "edit removes its new recovery anchor");
      }
    }
    if (rc == LC_OK && req->replacement) {
      rc = lc_pouch_collection_writer_publish(writer, leaf,
                                              c->order.upper_position + 1U,
                                              generation, &w->body, error);
      body_published = w->body.published;
    }
    if (rc == LC_OK && req->replacement)
      rc = ce_phase(c, LC_COLLECTION_EDIT_BODY, error);
    lc_pouch_collection_writer_close(writer);
    writer = NULL;
    if (rc == LC_OK) {
      memset(&w->control.result, 0, sizeof(w->control.result));
      w->control.result.published = 1;
      w->control.result.has_replacement = req->replacement != NULL;
      if (req->replacement) {
        strcpy(w->control.replacement_id, req->replacement->append_id);
        ce_append_result(&w->body, &w->control.result.replacement);
      } else
        w->control.replacement_id[0] = 0;
      rc = lc_collection_order_count_ranges(
          &c->order, &c->allocator, req->ranges, req->range_count,
          &w->control.result.deleted_count, error);
      w->control.result.upper_position =
          c->order.upper_position + (req->replacement ? 1U : 0U);
      w->control.result.live_count = c->order.live_count -
                                     w->control.result.deleted_count +
                                     (req->replacement ? 1U : 0U);
      saved_generation = c->order.generation;
      saved_parent_generation = c->order.collection_retention_generation;
      saved_parent = c->order.collection_retention;
      c->order.generation = generation;
      if (req->replacement && w->body.has_collection_retention) {
        c->order.collection_retention_generation = generation;
        c->order.collection_retention = w->body.collection_retention;
      }
      if (rc == LC_OK)
        rc = lc_collection_head_token(c, w->control.result.head_etag, error);
      c->order.generation = saved_generation;
      c->order.collection_retention_generation = saved_parent_generation;
      c->order.collection_retention = saved_parent;
    }
    if (rc == LC_OK) {
      /* Even a reader that probed an active upload before acquiring its
       * commit pin must discover a failed publication and repair first. */
      c->order.edit_in_progress = 1;
      dirty = 1;
      rc = lc_collection_order_checkpoint(&c->order, c->order.replay_segment,
                                          c->order.replay_offset, error);
      raw = ce_pack(w);
      lonejson_error_init(&je);
      memcpy(w->json, "{\"edit\":\"", 9);
      if (lonejson_base64_encode(w->raw, raw, LONEJSON_BASE64_URL_RAW,
                                 w->json + 9, CE_JSON - 12, &encoded,
                                 &je) != LONEJSON_STATUS_OK)
        rc = ce_error(error, LC_ERR_PROTOCOL, NULL,
                      "failed to encode edit receipt");
      memcpy(w->json + 9 + encoded, "\"}", 2);
      if (rc == LC_OK)
        rc = lc_source_from_memory(w->json, encoded + 11, &control_source,
                                   error);
      if (rc == LC_OK)
        rc = lc_pouch_collection_writer_open(
            c->pouch, c->active_fd, leaf, c->id, req->operation_id,
            c->order.edit_count + 1U, generation, 0, 0, 0, &writer, error);
      if (rc == LC_OK)
        lc_pouch_collection_writer_kind(writer, LC_POUCH_COLLECTION_EDIT);
      if (rc == LC_OK)
        rc = lc_pouch_collection_writer_consume(
            writer, c->pouch, control_source, &w->receipt, error);
      if (rc == LC_OK)
        rc = lc_pouch_collection_writer_publish(writer, leaf,
                                                c->order.edit_count + 1U,
                                                generation, &w->receipt, error);
      published = w->receipt.published;
      if (published)
        *out = w->control.result;
      if (rc == LC_OK)
        rc = ce_phase(c, LC_COLLECTION_EDIT_PUBLISHED, error);
      if (rc == LC_OK)
        /* The writer owns this exact bounded control. Recovery still decodes
         * canonical receipts; publication need not read/decrypt its own write.
         */
        rc = ce_project(c, &c->order, &c->ids, &w->control, &w->receipt,
                        req->replacement ? &w->body : NULL, segment, 0, error);
      if (rc == LC_OK)
        rc = lc_collection_ids_checkpoint(
            &c->ids, lc_collection_binding_count(&c->order),
            c->order.generation, segment, w->receipt.next_offset, error);
      if (rc == LC_OK)
        rc = ce_phase(c, LC_COLLECTION_EDIT_PROJECTED, error);
    }
  }
  lc_pouch_collection_writer_close(writer);
  lc_source_close(control_source);
  /* The body alone has no committed identity. Reclaim the whole failed group
   * while append authority still excludes another producer. */
  if (body_published && !published &&
      ftruncate(c->active_fd, (off_t)w->body.record_offset) != 0)
    c->trusted = 0;
  if (metadata)
    lc_collection_metadata_leave(c);
  if (dirty && rc != LC_OK)
    c->trusted = 0;
  if ((published || found) && rc == LC_OK)
    rc = lc_pouch_fsync_commit(c->pouch, c->active_fd, error);
  if (entered)
    lc_collection_write_leave(&guard);
  if (published)
    lc_pouch_compaction_note_mutation(c->pouch, c->ns, 0);
  if (w) {
    OPENSSL_cleanse(w, sizeof(*w));
    lc_free_with_allocator(&c->allocator, w);
  }
  lc_collection_call_end(&call);
  return rc;
}

int lc_collection_edit_compact(lc_collection_handle *c,
                               lc_collection_ids *captured, lc_u64 generation,
                               int destination, const char *leaf,
                               lc_u64 snapshot, lc_collection_order *order,
                               lc_collection_ids *ids, unsigned char *buffer,
                               size_t capacity, lc_error *e) {
  lc_collection_id_ref ref;
  lc_pouch_collection_record record, copied;
  lc_u64 slot, segment = 0, end = 0, copied_count = 0;
  unsigned char digest[32];
  char source_leaf[64];
  int rc = LC_OK, occupied, fd = -1;
  for (slot = 0; rc == LC_OK && slot < captured->capacity; ++slot) {
    rc = lc_collection_metadata_enter(c, 0, e);
    if (rc == LC_OK) {
      rc = lc_collection_ids_slot(captured, slot, &ref, &occupied, e);
      lc_collection_metadata_leave(c);
    }
    if (rc != LC_OK || !occupied || ref.generation > generation)
      continue;
    rc = lc_collection_record_ref(c, ref.segment, ref.offset, &fd, &segment,
                                  &end, &record, e);
    if (rc != LC_OK || record.kind != LC_POUCH_COLLECTION_EDIT)
      continue;
    rc = ce_key(record.append_id, digest, e);
    if (rc != LC_OK || memcmp(digest, ref.digest, 32))
      continue;
    if (record.position != ref.position || record.generation != ref.generation)
      rc = ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                    "edit receipt binding differs from canonical history");
    if (rc == LC_OK)
      ++copied_count;
    if (rc == LC_OK)
      rc = lc_collection_record_leaf(ref.segment, source_leaf, e);
    if (rc == LC_OK)
      rc = lc_pouch_collection_record_copy(c->pouch, fd, fd, source_leaf,
                                           &record, destination, leaf, buffer,
                                           capacity, &copied, e);
    if (rc == LC_OK)
      rc = lc_collection_edit_project(c, order, ids, destination, &copied, NULL,
                                      snapshot, 1, e);
  }
  if (fd >= 0)
    close(fd);
  if (rc == LC_OK && order->edit_count != copied_count)
    rc = ce_error(e, LC_ERR_PROTOCOL, "collection_corrupt",
                  "compaction did not retain all edit receipts");
  return rc;
}
