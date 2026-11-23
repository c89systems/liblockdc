#include "checkpoint.h"
#include "budget.h"
#include "catalog.h"
#include "frame.h"
#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_storage.h"
#include "revision.h"
#include "root_internal.h"
#include "space_internal.h"
#include "wire.h"
#include <pthread.h>
#include <string.h>
#include <zlib.h>

#define CHECKPOINT_CONTROL_BYTES 128U
#define CHECKPOINT_BATCH 64U
#define CHECKPOINT_TARGET_BYTES (16U * PS_CHUNK)
#define CHECKPOINT_HISTORY_BYTES PS_MIB

typedef struct key_slot key_slot;
typedef struct source_count source_count;
typedef struct slot_change slot_change;
typedef struct key_summary {
  uint64_t index, query, version, lifetime;
  uint64_t body_start, body_end;
  uint64_t deletion_bytes;
  unsigned char type;
  int found, body, has_hidden, hidden;
} key_summary;
struct source_count {
  unsigned int kind;
  uint64_t id, references, prepared, epoch;
  uint64_t bytes, byte_carries;
  key_slot *ranges;
  int indexed;
  source_count *next;
};
struct key_slot {
  char *key;
  uint64_t id, hash, bound, selected_incarnation;
  unsigned int kind;
  ps_reserve *reserve;
  uint64_t required_deletion, selected_deletion;
  key_summary summary;
  ps_checkpoint_record pending;
  source_count *body;
  key_slot *next, **link, *hash_next, *id_next;
  key_slot *dirty_next, **dirty_link;
  key_slot *retire_next, **retire_link;
  uint64_t retire_sequence;
  union {
    struct {
      key_slot *next, **link;
    } list;
    struct {
      key_slot *left, *right;
      uint64_t end;
      unsigned int height;
    } tree;
  } range;
  int retained;
  char leaf[LC_POUCH_SNAPSHOT_LEAF_BYTES];
};
struct slot_change {
  key_slot *slot;
  uint64_t bound, selected_incarnation;
  uint64_t prior_deletion;
  size_t ordinal;
  int created, prepared, remove;
  key_summary summary;
  source_count *body;
  ps_checkpoint_record record;
  slot_change *next, *hash_next;
};
struct ps_key_checkpoint {
  ps_namespace *ns;
  lc_allocator allocator;
  pthread_mutex_t mutex;
  key_slot **keys, **ids, *slots, *dirty, *retiring;
  source_count **sources;
  size_t capacity, count, selected_count, source_capacity, source_count;
  uint64_t dirty_count, dirty_bytes, uncovered_bytes;
  uint64_t latest_segment, latest_offset, state_index, query_index;
  uint64_t body_epoch;
  uint64_t logical_epoch;
  ps_checkpoint_info selected;
  ps_budget *budget;
  ps_budget_plan claim;
  ps_checkpoint_observer observer;
  int changed;
};
struct ps_checkpoint_delta {
  ps_key_checkpoint *checkpoint;
  ps_record_writer *writer;
  uint64_t segment, start, end, state_index, query_index;
  const char *leaf;
  slot_change *changes;
  slot_change **buckets;
  size_t capacity, count, created;
  ps_budget_plan claim;
  int prepared, accepted, maintenance, planning, planned;
  int metadata_only;
  int body_replacement;
  size_t metadata_count;
  uint64_t metadata_bytes;
};

static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0, message, NULL, NULL, "pouch");
}
static void dirty_remove(ps_key_checkpoint *, key_slot *);
/* Exact interval queries start at repack. Until then, aggregate accounting
 * and an intrusive list are sufficient even during replay and active writes.
 * Each slot has space for either representation, so neither canonical accept
 * nor construction of an interval index needs an allocation. */
static unsigned int range_height(key_slot *s) {
  return s ? s->range.tree.height : 0;
}
static void range_update(key_slot *s) {
  key_slot *left = s->range.tree.left, *right = s->range.tree.right;
  unsigned int a = range_height(left), b = range_height(right);
  s->range.tree.height = 1 + (a > b ? a : b);
  s->range.tree.end = s->summary.body_end;
  if (left && left->range.tree.end > s->range.tree.end)
    s->range.tree.end = left->range.tree.end;
  if (right && right->range.tree.end > s->range.tree.end)
    s->range.tree.end = right->range.tree.end;
}
static key_slot *range_left(key_slot *s) {
  key_slot *r = s->range.tree.right;
  s->range.tree.right = r->range.tree.left;
  r->range.tree.left = s;
  range_update(s);
  range_update(r);
  return r;
}
static key_slot *range_right(key_slot *s) {
  key_slot *l = s->range.tree.left;
  s->range.tree.left = l->range.tree.right;
  l->range.tree.right = s;
  range_update(s);
  range_update(l);
  return l;
}
static key_slot *range_balance(key_slot *s) {
  key_slot *left = s->range.tree.left, *right = s->range.tree.right;
  unsigned int a = range_height(left), b = range_height(right);
  range_update(s);
  if (a > b + 1) {
    if (range_height(left->range.tree.right) >
        range_height(left->range.tree.left))
      s->range.tree.left = range_left(left);
    return range_right(s);
  }
  if (b > a + 1) {
    if (range_height(right->range.tree.left) >
        range_height(right->range.tree.right))
      s->range.tree.right = range_right(right);
    return range_left(s);
  }
  return s;
}
static int range_before(key_slot *a, key_slot *b) {
  return a->summary.body_start < b->summary.body_start ||
         (a->summary.body_start == b->summary.body_start && a->id < b->id);
}
static key_slot *range_insert(key_slot *root, key_slot *s) {
  if (!root) {
    s->range.tree.left = s->range.tree.right = NULL;
    range_update(s);
    return s;
  }
  if (range_before(s, root))
    root->range.tree.left = range_insert(root->range.tree.left, s);
  else
    root->range.tree.right = range_insert(root->range.tree.right, s);
  return range_balance(root);
}
static key_slot *range_detach_first(key_slot *root, key_slot **first) {
  if (!root->range.tree.left) {
    *first = root;
    return root->range.tree.right;
  }
  root->range.tree.left = range_detach_first(root->range.tree.left, first);
  return range_balance(root);
}
static key_slot *range_remove(key_slot *root, key_slot *s) {
  key_slot *next;
  if (root == s) {
    if (!root->range.tree.right)
      return root->range.tree.left;
    next = NULL;
    root->range.tree.right = range_detach_first(root->range.tree.right, &next);
    next->range.tree.left = root->range.tree.left;
    next->range.tree.right = root->range.tree.right;
    return range_balance(next);
  }
  if (range_before(s, root))
    root->range.tree.left = range_remove(root->range.tree.left, s);
  else
    root->range.tree.right = range_remove(root->range.tree.right, s);
  return range_balance(root);
}
static void source_link_range(source_count *source, key_slot *s) {
  uint64_t bytes = s->summary.body_end - s->summary.body_start;
  if (!bytes)
    return;
  /* Keep the exact aggregate so subtraction can recover after saturation. */
  source->bytes += bytes;
  source->byte_carries += source->bytes < bytes;
  if (source->indexed)
    source->ranges = range_insert(source->ranges, s);
  else {
    s->range.list.next = source->ranges;
    s->range.list.link = &source->ranges;
    if (source->ranges)
      source->ranges->range.list.link = &s->range.list.next;
    source->ranges = s;
  }
}
static void source_unlink_range(source_count *source, key_slot *s) {
  uint64_t bytes = s->summary.body_end - s->summary.body_start;
  if (!bytes)
    return;
  source->byte_carries -= source->bytes < bytes;
  source->bytes -= bytes;
  if (source->indexed)
    source->ranges = range_remove(source->ranges, s);
  else {
    *s->range.list.link = s->range.list.next;
    if (s->range.list.next)
      s->range.list.next->range.list.link = s->range.list.link;
  }
}
static void source_index_ranges(source_count *source) {
  key_slot *slot, *next;
  if (source->indexed)
    return;
  slot = source->ranges;
  source->ranges = NULL;
  for (; slot; slot = next) {
    next = slot->range.list.next;
    source->ranges = range_insert(source->ranges, slot);
  }
  source->indexed = 1;
}
static uint64_t hash_key(const void *key, size_t n, unsigned int kind) {
  const unsigned char *p = key;
  uint64_t h = LC_U64_WORDS(0x14650fb0UL, 0x739d0383UL) ^ kind;
  while (n--) {
    h ^= *p++;
    h *= LC_U64_WORDS(0x00000100UL, 0x000001b3UL);
  }
  return h;
}
static key_slot *find_hashed_key(ps_key_checkpoint *c, const char *key,
                                 unsigned int kind, uint64_t hash) {
  key_slot *s;
  size_t bucket = (size_t)(hash & (c->capacity - 1));
  for (s = c->keys[bucket]; s; s = s->hash_next)
    if (s->hash == hash && s->kind == kind && !strcmp(s->key, key))
      return s;
  return NULL;
}
static key_slot *find_key(ps_key_checkpoint *c, const char *key,
                          unsigned int kind) {
  return find_hashed_key(c, key, kind, hash_key(key, strlen(key), kind));
}
static key_slot *find_id(ps_key_checkpoint *c, uint64_t id) {
  key_slot *s;
  for (s = c->ids[id & (c->capacity - 1)]; s; s = s->id_next)
    if (s->id == id)
      return s;
  return NULL;
}
static int room(ps_key_checkpoint *c, size_t wanted, lc_error *e) {
  key_slot **keys, **ids, *s;
  size_t capacity = c->capacity ? c->capacity : 16, bucket;
  while (wanted > capacity / 2) {
    if (capacity > (size_t)-1 / 2 / sizeof(*keys))
      return failure(e, LC_ERR_NOMEM,
                     "checkpoint slot index exceeds local bounds");
    capacity *= 2;
  }
  if (capacity == c->capacity)
    return LC_OK;
  keys = lc_calloc_with_allocator(&c->allocator, capacity, sizeof(*keys));
  ids = lc_calloc_with_allocator(&c->allocator, capacity, sizeof(*ids));
  if (!keys || !ids) {
    lc_free_with_allocator(&c->allocator, keys);
    lc_free_with_allocator(&c->allocator, ids);
    return failure(e, LC_ERR_NOMEM, "cannot grow checkpoint slot index");
  }
  for (s = c->slots; s; s = s->next) {
    bucket = (size_t)(s->hash & (capacity - 1));
    s->hash_next = keys[bucket];
    keys[bucket] = s;
    bucket = (size_t)(s->id & (capacity - 1));
    s->id_next = ids[bucket];
    ids[bucket] = s;
  }
  lc_free_with_allocator(&c->allocator, c->keys);
  lc_free_with_allocator(&c->allocator, c->ids);
  c->keys = keys;
  c->ids = ids;
  c->capacity = capacity;
  return LC_OK;
}
static void insert_slot(ps_key_checkpoint *c, key_slot *s) {
  size_t bucket = (size_t)(s->hash & (c->capacity - 1));
  s->hash_next = c->keys[bucket];
  c->keys[bucket] = s;
  bucket = (size_t)(s->id & (c->capacity - 1));
  s->id_next = c->ids[bucket];
  c->ids[bucket] = s;
  s->next = c->slots;
  s->link = &c->slots;
  if (s->next)
    s->next->link = &s->next;
  c->slots = s;
  ++c->count;
}
static void close_slot(ps_key_checkpoint *c, key_slot *s) {
  ps_reserve_close(s->reserve);
  ps_checkpoint_record_close(&s->pending);
  if (s->key != (char *)(s + 1))
    lc_free_with_allocator(&c->allocator, s->key);
  lc_free_with_allocator(&c->allocator, s);
}
static key_slot *new_slot(ps_key_checkpoint *c, const void *key, size_t bytes,
                          unsigned int kind, uint64_t id, lc_error *e) {
  key_slot *s = NULL;
  if (bytes <= SIZE_MAX - sizeof(*s) - 1)
    s = lc_calloc_with_allocator(&c->allocator, 1,
                                 sizeof(*s) + (key ? bytes + 1 : 0));
  if (!s ||
      !lc_pouch_namespace_format_snapshot_leaf(id, s->leaf, sizeof(s->leaf))) {
    if (s)
      close_slot(c, s);
    failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint key slot");
    return NULL;
  }
  if (key) {
    s->key = (char *)(s + 1);
    memcpy(s->key, key, bytes);
    s->key[bytes] = 0;
    s->hash = hash_key(s->key, bytes, kind);
  }
  s->kind = kind;
  s->id = id;
  return s;
}
static void summarize(key_summary *s, const lc_pouch_state_entry *r) {
  s->index = r->index_seq;
  s->query = r->query_index_seq;
  s->version = r->version;
  s->lifetime = r->lifetime;
  s->type = r->record_type;
  s->found = r->found;
  s->body = r->payload_span.present;
  s->body_start = r->payload_span.payload_offset;
  s->body_end = s->body_start + r->payload_span.payload_length;
  s->has_hidden = r->has_query_hidden;
  s->hidden = r->query_hidden;
  s->deletion_bytes = lc_pouch_state_entry_delete_bytes(r);
}
static int reserve_slot(ps_key_checkpoint *c, key_slot *s, uint64_t bytes,
                        int recover, lc_error *e) {
  ps_budget_plan plan = {0};
  int rc;
  if (bytes == s->required_deletion)
    return LC_OK;
  if (bytes <= s->selected_deletion) {
    ps_reserve_close(s->reserve);
    s->reserve = NULL;
    s->required_deletion = bytes;
    return LC_OK;
  }
  rc = ps_budget_plan_maintenance(&plan, bytes, e);
  if (rc == LC_OK)
    rc = s->reserve ? ps_reserve_resize(s->reserve, &plan, recover, e)
                    : ps_reserve_open(c->ns->root->space, &plan, recover,
                                      &s->reserve, e);
  if (rc == LC_OK)
    s->required_deletion = bytes;
  return rc;
}
static void retire_remove(key_slot *s) {
  if (!s->retire_link)
    return;
  *s->retire_link = s->retire_next;
  if (s->retire_next)
    s->retire_next->retire_link = s->retire_link;
  s->retire_link = NULL;
  s->retire_next = NULL;
  s->retire_sequence = 0;
}
static void retire_note(ps_key_checkpoint *c, key_slot *s, uint64_t sequence) {
  if (s->required_deletion == s->summary.deletion_bytes) {
    retire_remove(s);
    return;
  }
  if (!s->retire_link) {
    s->retire_next = c->retiring;
    if (c->retiring)
      c->retiring->retire_link = &s->retire_next;
    c->retiring = s;
    s->retire_link = &c->retiring;
  }
  s->retire_sequence = sequence;
}
static int settle_retired(ps_key_checkpoint *c, lc_error *e) {
  key_slot *s, *next;
  uint64_t selected, durable;
  int rc = LC_OK;
  if (!c->retiring)
    return LC_OK;
  rc = ps_publication_prefix(c->ns->root->space, &selected, &durable, e);
  for (s = c->retiring; rc == LC_OK && s; s = next) {
    next = s->retire_next;
    if (s->retire_sequence <= durable) {
      rc = reserve_slot(c, s, s->summary.deletion_bytes, 0, e);
      if (rc == LC_OK)
        retire_remove(s);
    }
  }
  return rc;
}
static void summary_view(key_slot *slot, const key_summary *s,
                         lc_pouch_state_entry *r) {
  memset(r, 0, sizeof(*r));
  r->key = slot->key;
  r->seen = 1;
  r->index_seq = s->index;
  r->query_index_seq = s->query;
  r->has_query_index_seq = 1;
  r->version = s->version;
  r->lifetime = s->lifetime;
  r->record_type = s->type;
  r->found = s->found;
  r->payload_span.present = s->body;
  r->has_query_hidden = s->has_hidden;
  r->query_hidden = s->hidden;
}
static source_count *source_get(ps_key_checkpoint *c,
                                const lc_pouch_state_entry *r, int *rc,
                                lc_error *e) {
  source_count *source, *next, **table;
  unsigned int kind;
  uint64_t id;
  size_t bucket, i, capacity;
  if (!r->found || !r->payload_span.present)
    return NULL;
  if (r->payload_span.payload_offset > (uint64_t)LC_I64_MAX ||
      r->payload_span.payload_length >
          (uint64_t)LC_I64_MAX - r->payload_span.payload_offset) {
    *rc = failure(e, LC_ERR_PROTOCOL, "checkpoint body range exceeds bounds");
    return NULL;
  }
  if (!ps_namespace_parse_source_leaf(r->payload_span.container_leaf, &kind,
                                      &id)) {
    *rc = failure(e, LC_ERR_PROTOCOL,
                  "checkpoint body has an invalid logical source");
    return NULL;
  }
  bucket = (size_t)((id ^ kind) & (c->source_capacity - 1));
  for (source = c->sources[bucket]; source; source = source->next)
    if (source->kind == kind && source->id == id)
      return source;
  if (c->source_count >= c->source_capacity / 2) {
    if (c->source_capacity > (size_t)-1 / 2 / sizeof(*table)) {
      *rc = failure(e, LC_ERR_NOMEM,
                    "checkpoint source index exceeds local bounds");
      return NULL;
    }
    capacity = c->source_capacity * 2;
    table = lc_calloc_with_allocator(&c->allocator, capacity, sizeof(*table));
    if (!table) {
      *rc = failure(e, LC_ERR_NOMEM, "cannot grow checkpoint source index");
      return NULL;
    }
    for (i = 0; i < c->source_capacity; ++i)
      for (source = c->sources[i]; source; source = next) {
        next = source->next;
        bucket = (size_t)((source->id ^ source->kind) & (capacity - 1));
        source->next = table[bucket];
        table[bucket] = source;
      }
    lc_free_with_allocator(&c->allocator, c->sources);
    c->sources = table;
    c->source_capacity = capacity;
    bucket = (size_t)((id ^ kind) & (capacity - 1));
  }
  source = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*source));
  if (!source) {
    *rc = failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint body ownership");
    return NULL;
  }
  source->kind = kind;
  source->id = id;
  source->next = c->sources[bucket];
  c->sources[bucket] = source;
  ++c->source_count;
  return source;
}
static void release_source(ps_key_checkpoint *c, source_count *source) {
  source_count **link;
  size_t bucket;
  if (!source || source->references || source->prepared)
    return;
  bucket = (size_t)((source->id ^ source->kind) & (c->source_capacity - 1));
  for (link = &c->sources[bucket]; *link != source; link = &(*link)->next) {
  }
  *link = source->next;
  --c->source_count;
  lc_free_with_allocator(&c->allocator, source);
}
static void bind_source(ps_key_checkpoint *c, key_slot *s, source_count *source,
                        const key_summary *summary) {
  source_count *prior = s->body;
  if (prior == source && s->summary.body_start == summary->body_start &&
      s->summary.body_end == summary->body_end) {
    s->summary = *summary;
    return;
  }
  ++c->body_epoch; /* preflighted before the infallible canonical accept */
  if (prior) {
    source_unlink_range(prior, s);
    --prior->references;
    prior->epoch = c->body_epoch;
  }
  s->summary = *summary;
  s->body = source;
  if (source) {
    ++source->references;
    source->epoch = c->body_epoch;
    source_link_range(source, s);
  }
  release_source(c, prior);
}
static void release_prepared_source(ps_key_checkpoint *c,
                                    source_count *source) {
  if (source)
    --source->prepared;
  release_source(c, source);
}
static ps_key control_key(ps_namespace *ns) {
  ps_key key = {0, 1, PS_NAMESPACE_CHECKPOINT};
  key.owner = ns->identity.owner;
  return key;
}
static int decode_control(uint64_t owner, const unsigned char *bytes,
                          uint64_t token, ps_checkpoint_info *out,
                          lc_error *e) {
  size_t i;
  if (memcmp(bytes, "LCPSCKPT", 8) || ps_wire_get64(bytes + 8) != owner ||
      !ps_wire_get64(bytes + 16) ||
      (!ps_wire_get64(bytes + 24) && ps_wire_get64(bytes + 32)) ||
      ps_wire_get64(bytes + 120) != (uint64_t)crc32(0, bytes, 120))
    return failure(e, LC_ERR_PROTOCOL,
                   "checkpoint coverage identity is invalid");
  for (i = 56; i < 120; ++i)
    if (bytes[i])
      return failure(e, LC_ERR_PROTOCOL,
                     "checkpoint coverage has unknown fields");
  memset(out, 0, sizeof(*out));
  out->control = token;
  out->epoch = ps_wire_get64(bytes + 16);
  out->segment = ps_wire_get64(bytes + 24);
  out->offset = ps_wire_get64(bytes + 32);
  out->state_index = ps_wire_get64(bytes + 40);
  out->query_index = ps_wire_get64(bytes + 48);
  if (out->query_index > out->state_index)
    return failure(e, LC_ERR_PROTOCOL,
                   "checkpoint query boundary exceeds state");
  return LC_OK;
}
static int read_control(ps_namespace *ns, ps_reader *reader,
                        ps_checkpoint_info *out, lc_error *e) {
  unsigned char bytes[CHECKPOINT_CONTROL_BYTES];
  ps_ref ref;
  size_t got;
  int rc;
  if (ps_read_size(reader) != sizeof(bytes))
    return failure(e, LC_ERR_PROTOCOL,
                   "checkpoint coverage has an invalid size");
  rc = ps_read(reader, bytes, sizeof(bytes), &got, e);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes))
    return failure(e, LC_ERR_PROTOCOL, "checkpoint coverage is incomplete");
  ps_read_reference(reader, &ref);
  return decode_control(ns->identity.owner, bytes, ref.incarnation, out, e);
}
int ps_checkpoint_point(ps_space *s, uint64_t owner, uint64_t prefix,
                        int memo_only, ps_checkpoint_info *out, int *current,
                        int *private_failure, lc_error *e) {
  unsigned char bytes[CHECKPOINT_CONTROL_BYTES];
  ps_key key = {0, 1, PS_NAMESPACE_CHECKPOINT};
  uint64_t token;
  ps_checkpoint_info decoded;
  int rc;
  key.owner = owner;
  rc = ps_internal_fixed_value(s, key, prefix, memo_only ? NULL : bytes,
                               sizeof(bytes), out->control, &token, current,
                               private_failure, e);
  if (rc == LC_OK && *current && token != out->control) {
    memset(&decoded, 0, sizeof(decoded));
    if (token)
      rc = decode_control(owner, bytes, token, &decoded, e);
    if (rc == LC_OK)
      *out = decoded;
  }
  return rc;
}
typedef struct checkpoint_capture {
  ps_key_checkpoint *resident;
  const ps_checkpoint_info *known;
  enum ps_checkpoint_capture_mode mode;
} checkpoint_capture;
static int keep_selected_slot(const key_slot *slot, ps_key key,
                              uint64_t incarnation) {
  return slot && slot->kind == key.kind && !slot->dirty_link &&
         slot->selected_incarnation == incarnation;
}
static int checkpoint_retain_value(void *context, ps_key key,
                                   uint64_t incarnation) {
  checkpoint_capture *capture = context;
  if (key.kind == PS_NAMESPACE_CHECKPOINT)
    return incarnation != capture->known->control;
  if (capture->mode == PS_CHECKPOINT_CAPTURE_SEED)
    return 0;
  if (capture->mode == PS_CHECKPOINT_CAPTURE_STATE)
    return key.kind == PS_KEY_METADATA;
  return !keep_selected_slot(find_id(capture->resident, key.key), key,
                             incarnation);
}
static int checkpoint_capture_values(ps_namespace *ns,
                                     checkpoint_capture *capture,
                                     ps_values **out,
                                     ps_checkpoint_info *coverage,
                                     lc_error *e) {
  ps_values *values = NULL;
  ps_reader *reader = NULL;
  ps_key key;
  ps_ref ref;
  uint64_t bytes;
  size_t i;
  int rc;
  *out = NULL;
  memset(coverage, 0, sizeof(*coverage));
  rc = ps_internal_values_capture(ns->root->space, ns->identity.owner,
                                  PS_KEY_METADATA, PS_NAMESPACE_CHECKPOINT,
                                  capture->mode == PS_CHECKPOINT_CAPTURE_FULL
                                      ? NULL
                                      : checkpoint_retain_value,
                                  capture, &values, e);
  for (i = 0; rc == LC_OK && i < ps_values_count(values); ++i) {
    rc = ps_values_info(values, i, &key, &ref, &bytes, e);
    if (rc != LC_OK || key.kind != PS_NAMESPACE_CHECKPOINT)
      continue;
    if (key.key != 1)
      rc = failure(e, LC_ERR_PROTOCOL, "unknown namespace coverage slot");
    else if (ref.incarnation == capture->known->control)
      *coverage = *capture->known;
    else {
      rc = ps_values_read_open(values, i, &reader, e);
      if (rc == LC_OK)
        rc = read_control(ns, reader, coverage, e);
      ps_reader_close(reader);
      reader = NULL;
    }
  }
  if (rc == LC_OK)
    *out = values;
  else
    ps_values_close(values);
  return rc;
}
typedef struct selected_checkpoint {
  uint64_t control;
  int seed_only, invalid;
} selected_checkpoint;
static int retain_selected_record(void *context, ps_key key,
                                  uint64_t incarnation) {
  selected_checkpoint *selected = context;
  if (key.kind != PS_NAMESPACE_CHECKPOINT)
    return !selected->seed_only;
  if (key.key != 1 || incarnation != selected->control)
    selected->invalid = 1;
  return 0;
}
int ps_checkpoint_capture_selected_locked(ps_namespace *ns, uint64_t prefix,
                                          const ps_checkpoint_info *coverage,
                                          enum ps_checkpoint_capture_mode mode,
                                          int provisional, ps_values **out,
                                          lc_error *e) {
  selected_checkpoint selected;
  int rc;
  if (mode == PS_CHECKPOINT_CAPTURE_STATE)
    return ps_internal_values_capture_locked(
        ns->root->space, ns->identity.owner, PS_KEY_METADATA, PS_KEY_METADATA,
        prefix, provisional, NULL, NULL, out, e);
  selected.control = coverage->control;
  selected.seed_only = mode == PS_CHECKPOINT_CAPTURE_SEED;
  selected.invalid = 0;
  rc = ps_internal_values_capture_locked(
      ns->root->space, ns->identity.owner, PS_KEY_METADATA,
      PS_NAMESPACE_CHECKPOINT, prefix, provisional, retain_selected_record,
      &selected, out, e);
  if (rc == LC_OK && selected.invalid)
    rc = failure(e, LC_ERR_PROTOCOL, "unexpected namespace coverage slot");
  return rc;
}
int ps_key_checkpoint_capture(ps_key_checkpoint *c,
                              enum ps_checkpoint_capture_mode mode,
                              ps_values **out, ps_checkpoint_info *coverage,
                              lc_error *e) {
  checkpoint_capture capture;
  int rc;
  if (!c || !out || !coverage)
    return failure(e, LC_ERR_INVALID, "metadata capture requires output");
  pthread_mutex_lock(&c->mutex);
  capture.resident = c;
  capture.known = &c->selected;
  capture.mode = mode;
  rc = checkpoint_capture_values(c->ns, &capture, out, coverage, e);
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
typedef union checkpoint_decode_alignment {
  void *pointer;
  long double number;
  uint64_t integer;
} checkpoint_decode_alignment;
typedef struct checkpoint_decode_scratch {
  union {
    checkpoint_decode_alignment alignment;
    unsigned char bytes[2 * PS_PAGE];
  } storage;
  size_t used;
  int full;
} checkpoint_decode_scratch;
static void *checkpoint_decode_allocate(void *context, size_t bytes) {
  checkpoint_decode_scratch *scratch = context;
  const size_t align = sizeof(checkpoint_decode_alignment);
  size_t at = (scratch->used + align - 1) / align * align;
  if (at > sizeof(scratch->storage.bytes) ||
      bytes > sizeof(scratch->storage.bytes) - at) {
    scratch->full = 1;
    return NULL;
  }
  scratch->used = at + bytes;
  return scratch->storage.bytes + at;
}
static void checkpoint_decode_release(void *context, void *bytes) {
  /* A packed record finishes with the entire workspace at once. */
  (void)context;
  (void)bytes;
}
static int decode_selected_slot(ps_key_checkpoint *c, ps_reader *reader,
                                ps_key key, ps_ref ref, key_slot **out,
                                lc_error *e) {
  lc_pouch_state_entry entry = {0}, projected;
  lc_pouch_state_terminal terminal = {0};
  checkpoint_decode_scratch scratch;
  lc_allocator transient = {checkpoint_decode_allocate, NULL,
                            checkpoint_decode_release, NULL};
  const lc_allocator *allocator = &c->allocator;
  const unsigned char *span;
  key_slot *slot;
  int rc = ps_internal_checkpoint_span(reader, &span, e);
  *out = NULL;
  if (rc == LC_OK && span)
    rc = lc_pouch_state_record_terminal_span(span, (size_t)ps_read_size(reader),
                                             &terminal, e);
  if (rc != LC_OK)
    return rc;
  slot = new_slot(c, terminal.key, terminal.key_bytes, key.kind, key.key, e);
  if (!slot)
    return LC_ERR_NOMEM;
  if (terminal.type) {
    /* The ledger needs identity and the ordering fields. The canonical
     * value retains its etag and decision for callers that need the record. */
    entry.key = slot->key;
    entry.seen = 1;
    entry.control = terminal.decision != NULL;
    entry.decision = (char *)terminal.decision;
    entry.record_type = terminal.type;
    entry.version = terminal.version;
    entry.index_seq = terminal.index_seq;
    entry.lifetime = terminal.lifetime;
    entry.query_index_seq = terminal.query_index_seq;
    entry.has_query_index_seq = terminal.has_query_index_seq;
  } else if (span) {
    /* The checked packed record fits one page. The workspace also covers
     * string terminators, allocator headers and the synthesized record name.
     * Only the key copied into the slot survives this visit. */
    scratch.used = 0;
    scratch.full = 0;
    transient.context = &scratch;
    allocator = &transient;
    rc = lc_pouch_state_record_decode_span(span, (size_t)ps_read_size(reader),
                                           allocator, slot->leaf, &entry, e);
    if (rc == LC_ERR_NOMEM && scratch.full) {
      lc_error_cleanup(e);
      allocator = &c->allocator;
      rc = lc_pouch_state_record_decode_span(span, (size_t)ps_read_size(reader),
                                             allocator, slot->leaf, &entry, e);
    }
  } else
    rc =
        ps_checkpoint_record_read(reader, &c->allocator, slot->leaf, &entry, e);
  if (rc == LC_OK &&
      ((!entry.key || !entry.seen) ||
       (key.kind == PS_KEY_DECISION ? !entry.control || !entry.decision
                                    : entry.control || entry.decision)))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "checkpoint slot has the wrong canonical domain");
  if (rc == LC_OK) {
    projected = entry;
    /* Normalize the class of snapshot body links for subsequent META rows. */
    if (!entry.control &&
        !lc_pouch_state_entry_project(NULL, &entry, &projected))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "checkpoint has no canonical key projection");
    if (rc == LC_OK) {
      summarize(&slot->summary, &projected);
      if (ps_reader_key_deletion(reader) != slot->summary.deletion_bytes)
        rc = failure(e, LC_ERR_PROTOCOL,
                     "checkpoint deletion requirement disagrees with its slot");
    }
  }
  if (rc == LC_OK) {
    if (!span) {
      slot->key = entry.key;
      slot->hash = hash_key(entry.key, strlen(entry.key), key.kind);
      entry.key = NULL;
    }
    slot->selected_incarnation = ref.incarnation;
    slot->body = source_get(c, &entry, &rc, e);
    if (slot->body)
      ++slot->body->prepared;
    slot->required_deletion = slot->selected_deletion =
        slot->summary.deletion_bytes;
  }
  if (rc == LC_OK)
    *out = slot;
  else {
    release_prepared_source(c, slot->body);
    close_slot(c, slot);
  }
  if (!terminal.type)
    lc_pouch_state_entry_cleanup(allocator, &entry);
  return rc;
}
static void select_watermarks(ps_key_checkpoint *c, const key_slot *slot) {
  if (slot->summary.index > c->state_index)
    c->state_index = slot->summary.index;
  if (slot->summary.query > c->query_index)
    c->query_index = slot->summary.query;
}
static void select_incarnation(ps_key_checkpoint *c, key_slot *slot,
                               uint64_t incarnation, uint64_t deletion) {
  if (!slot->selected_incarnation)
    ++c->selected_count;
  slot->selected_incarnation = incarnation;
  slot->selected_deletion = deletion;
  /* The allocator retains the preceding descriptor through durability. A
   * subsequent append must fund growth against the new selected requirement,
   * since that older descriptor can stop covering it while input is active.
   * Keep any larger obligation that still belongs to an uncheckpointed tail. */
  if (!slot->reserve || slot->required_deletion <= deletion) {
    ps_reserve_close(slot->reserve);
    slot->reserve = NULL;
    slot->required_deletion = deletion;
  }
}
static void install_selected_slot(ps_key_checkpoint *c, key_slot *slot) {
  source_count *source = slot->body;
  slot->body = NULL;
  bind_source(c, slot, source, &slot->summary);
  release_prepared_source(c, source);
  insert_slot(c, slot);
  ++c->selected_count;
  select_watermarks(c, slot);
}
static void select_checkpoint(ps_key_checkpoint *c,
                              const ps_checkpoint_info *selected) {
  c->selected = *selected;
  c->state_index = selected->state_index;
  c->query_index = selected->query_index;
  c->latest_segment = selected->segment;
  c->latest_offset = selected->offset;
}
static int read_unique_slot(ps_key_checkpoint *c, ps_key_checkpoint *slots,
                            ps_key key, ps_reader *reader, key_slot **out,
                            lc_error *e) {
  ps_ref ref;
  key_slot *slot = NULL;
  int rc = room(slots, slots->count + 1, e);
  *out = NULL;
  if (rc == LC_OK) {
    ps_read_reference(reader, &ref);
    rc = decode_selected_slot(c, reader, key, ref, &slot, e);
  }
  if (rc == LC_OK && (find_hashed_key(slots, slot->key, key.kind, slot->hash) ||
                      find_id(slots, key.key)))
    rc = failure(e, LC_ERR_PROTOCOL, "checkpoint repeats a canonical key slot");
  if (rc == LC_OK)
    *out = slot;
  else if (slot) {
    release_prepared_source(c, slot->body);
    close_slot(c, slot);
  }
  return rc;
}
typedef struct checkpoint_seed {
  ps_key_checkpoint *checkpoint;
  ps_checkpoint_info selected;
} checkpoint_seed;
static int load_seed(void *context, ps_key key, ps_reader *reader,
                     lc_error *e) {
  checkpoint_seed *seed = context;
  ps_key_checkpoint *c = seed->checkpoint;
  key_slot *slot;
  int rc;
  if (key.kind == PS_NAMESPACE_CHECKPOINT) {
    if (key.key != 1)
      return failure(e, LC_ERR_PROTOCOL, "unknown namespace coverage slot");
    return read_control(c->ns, reader, &seed->selected, e);
  }
  rc = read_unique_slot(c, c, key, reader, &slot, e);
  if (rc == LC_OK)
    install_selected_slot(c, slot);
  return rc;
}
static int new_checkpoint(ps_namespace *ns, ps_key_checkpoint **out,
                          lc_error *e) {
  ps_key_checkpoint *c;
  int rc;
  *out = NULL;
  c = lc_calloc_with_allocator(&ns->root->crypto_allocator, 1, sizeof(*c));
  if (!c)
    return failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint owner");
  c->ns = ns;
  c->allocator = ns->root->crypto_allocator;
  if (pthread_mutex_init(&c->mutex, NULL)) {
    lc_free_with_allocator(&c->allocator, c);
    return failure(e, LC_ERR_TRANSPORT, "cannot initialize checkpoint owner");
  }
  c->source_capacity = 64;
  c->sources = lc_calloc_with_allocator(&c->allocator, c->source_capacity,
                                        sizeof(*c->sources));
  rc = c->sources
           ? room(c, 1, e)
           : failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint sources");
  if (rc == LC_OK)
    *out = c;
  else
    ps_key_checkpoint_close(c);
  return rc;
}
int ps_key_checkpoint_open(ps_namespace *ns, ps_key_checkpoint **out,
                           lc_error *e) {
  checkpoint_seed seed = {0};
  ps_key_checkpoint *c;
  ps_values *values = NULL;
  ps_space *s;
  uint64_t state_index, query_index;
  int rc, startup, accepted = 0;
  if (out)
    *out = NULL;
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint owner requires namespace and output");
  s = ns->root->space;
  startup = ns->root->recovering_namespaces &&
            ps_coordination_shared(ps_space_coordination(s));
  rc = new_checkpoint(ns, &c, e);
  if (rc != LC_OK)
    return rc;
  seed.checkpoint = c;
  if (startup) {
    rc = ps_internal_values_startup_walk(s, ns->identity.owner, load_seed,
                                         &seed, &accepted, e);
    if (rc == LC_OK && !accepted) {
      ps_key_checkpoint_close(c);
      memset(&seed, 0, sizeof(seed));
      rc = new_checkpoint(ns, &c, e);
      seed.checkpoint = c;
    }
  }
  if (rc == LC_OK && !accepted) {
    rc = ps_values_capture(s, ns->identity.owner, PS_KEY_METADATA,
                           PS_NAMESPACE_CHECKPOINT, &values, e);
    if (rc == LC_OK)
      rc = ps_values_read_walk(values, load_seed, &seed, e);
    ps_values_close(values);
  }
  if (rc == LC_OK) {
    state_index = c->state_index;
    query_index = c->query_index;
    select_checkpoint(c, &seed.selected);
    if (state_index > c->state_index)
      c->state_index = state_index;
    if (query_index > c->query_index)
      c->query_index = query_index;
  }
  if (rc == LC_OK)
    *out = c;
  else if (c)
    ps_key_checkpoint_close(c);
  return rc;
}
static int match_current_seed(void *context, ps_key key, uint64_t incarnation) {
  ps_key_checkpoint *c = context;
  key_slot *slot;
  if (key.kind == PS_NAMESPACE_CHECKPOINT)
    return key.key == 1 && incarnation == c->selected.control;
  slot = find_id(c, key.key);
  return slot && slot->kind == key.kind &&
         slot->selected_incarnation == incarnation;
}
static int seed_matches(ps_key_checkpoint *c, const ps_values *values,
                        const ps_checkpoint_info *coverage, int *matches,
                        lc_error *e) {
  ps_key key;
  ps_ref ref;
  uint64_t bytes;
  size_t i, count = 0;
  int rc = LC_OK, same;
  same = c->selected.control == coverage->control;
  for (i = 0; same && rc == LC_OK && i < ps_values_count(values); ++i) {
    rc = ps_values_info(values, i, &key, &ref, &bytes, e);
    if (rc != LC_OK || key.kind == PS_NAMESPACE_CHECKPOINT)
      continue;
    same = match_current_seed(c, key, ref.incarnation);
    ++count;
  }
  *matches = same && count == c->selected_count;
  return rc;
}
int ps_key_checkpoint_confirm_current(ps_key_checkpoint *c,
                                      const ps_catalog_info *active,
                                      uint64_t tail, int *current,
                                      lc_error *e) {
  int rc;
  pthread_mutex_lock(&c->mutex);
  rc = ps_internal_namespace_seed_current(
      c->ns->root->space, c->ns->identity.owner, active, tail,
      c->selected_count + (c->selected.control != 0), match_current_seed, c,
      current, e);
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
int ps_key_checkpoint_confirm_seed(ps_key_checkpoint *c,
                                   const ps_values *values,
                                   const ps_checkpoint_info *coverage,
                                   lc_error *e) {
  int rc, same;
  pthread_mutex_lock(&c->mutex);
  rc = seed_matches(c, values, coverage, &same, e);
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK && !same)
    rc = failure(e, LC_ERR_PENDING,
                 "selected checkpoint changed during recovery");
  return rc;
}
static void drop_selected_slot(ps_key_checkpoint *c, key_slot *slot) {
  key_slot **at;
  key_summary empty = {0};
  size_t bucket = (size_t)(slot->hash & (c->capacity - 1));
  *slot->link = slot->next;
  if (slot->next)
    slot->next->link = slot->link;
  for (at = &c->keys[bucket]; *at != slot; at = &(*at)->hash_next) {
  }
  *at = slot->hash_next;
  for (at = &c->ids[slot->id & (c->capacity - 1)]; *at != slot;
       at = &(*at)->id_next) {
  }
  *at = slot->id_next;
  if (slot->dirty_link)
    dirty_remove(c, slot);
  retire_remove(slot);
  bind_source(c, slot, NULL, &empty);
  --c->count;
  close_slot(c, slot);
}
typedef struct checkpoint_rebase {
  ps_key_checkpoint *resident, *staged;
} checkpoint_rebase;
static int stage_selected_slot(void *context, ps_key key, ps_reader *reader,
                               lc_error *e) {
  checkpoint_rebase *rebase = context;
  key_slot *old, *slot;
  int rc;
  if (key.kind == PS_NAMESPACE_CHECKPOINT)
    return LC_OK;
  old = find_id(rebase->resident, key.key);
  if (old && old->retained && old->kind == key.kind)
    return LC_OK;
  rc =
      read_unique_slot(rebase->resident, rebase->staged, key, reader, &slot, e);
  if (rc == LC_OK)
    insert_slot(rebase->staged, slot);
  return rc;
}
/* Prepare all changed rows and their deletion obligations before consuming the
 * old ledger. The temporary slot indexes contain only rows needing decoding. */
static int rebase_selected(ps_key_checkpoint *c, const ps_values *values,
                           const ps_checkpoint_info *coverage, lc_error *e) {
  ps_key_checkpoint staged = {0};
  checkpoint_rebase rebase;
  key_slot **link, *s, *next, *old;
  ps_key key;
  ps_ref ref;
  uint64_t bytes;
  size_t i, kept = 0, changed = 0;
  int rc = LC_OK;
  staged.allocator = c->allocator;
  for (i = 0; rc == LC_OK && i < ps_values_count(values); ++i) {
    rc = ps_values_info(values, i, &key, &ref, &bytes, e);
    if (rc != LC_OK || key.kind == PS_NAMESPACE_CHECKPOINT)
      continue;
    old = find_id(c, key.key);
    if (keep_selected_slot(old, key, ref.incarnation)) {
      if (old->retained) {
        rc = failure(e, LC_ERR_PROTOCOL, "checkpoint repeats a retained slot");
        break;
      }
      old->retained = 1;
      ++kept;
    } else
      ++changed;
  }
  if (rc == LC_OK && changed) {
    rebase.resident = c;
    rebase.staged = &staged;
    rc = ps_values_read_walk(values, stage_selected_slot, &rebase, e);
    if (rc == LC_OK && staged.count != changed)
      rc = failure(e, LC_ERR_PROTOCOL,
                   "checkpoint changed slot has no captured payload");
  }
  for (s = staged.slots; rc == LC_OK && s; s = s->next) {
    old = find_id(c, s->id);
    if (!old || !old->retained)
      old = find_key(c, s->key, s->kind);
    if (old && old->retained)
      rc = failure(e, LC_ERR_PROTOCOL, "checkpoint repeats a retained slot");
  }
  if (rc == LC_OK &&
      (c->count - kept > LC_U64_MAX - c->body_epoch ||
       staged.count > LC_U64_MAX - c->body_epoch - (c->count - kept)))
    rc = failure(e, LC_ERR_INVALID, "checkpoint body epoch exhausted");
  if (rc == LC_OK)
    rc = room(c, kept + staged.count, e);
  if (rc == LC_OK) {
    select_checkpoint(c, coverage);
    c->selected_count = kept;
    c->changed = 0;
    c->dirty_bytes = c->uncovered_bytes = 0;
    memset(&c->claim, 0, sizeof(c->claim));
    (void)ps_budget_close(c->budget, NULL);
    c->budget = NULL;
    if (c->logical_epoch != LC_U64_MAX)
      ++c->logical_epoch;
  }
  for (link = &c->slots; (s = *link) != NULL;) {
    if (rc == LC_OK && !s->retained)
      drop_selected_slot(c, s);
    else {
      s->retained = 0;
      if (rc == LC_OK)
        select_watermarks(c, s);
      link = &s->next;
    }
  }
  for (s = staged.slots; s; s = next) {
    next = s->next;
    if (rc == LC_OK)
      install_selected_slot(c, s);
    else {
      release_prepared_source(c, s->body);
      close_slot(c, s);
    }
  }
  lc_free_with_allocator(&c->allocator, staged.keys);
  lc_free_with_allocator(&c->allocator, staged.ids);
  return rc;
}
int ps_key_checkpoint_adopt(ps_key_checkpoint *c, const ps_values *values,
                            const ps_checkpoint_info *coverage, uint64_t active,
                            uint64_t prior_tail, int *resumed,
                            uint64_t *resume_offset, lc_error *e) {
  int rc, same;
  *resumed = 0;
  *resume_offset = 0;
  pthread_mutex_lock(&c->mutex);
  rc = seed_matches(c, values, coverage, &same, e);
  if (rc == LC_OK && same && active && c->latest_segment == active &&
      c->latest_offset <= prior_tail) {
    *resumed = 1;
    *resume_offset = c->latest_offset;
  } else if (rc == LC_OK)
    rc = rebase_selected(c, values, coverage, e);
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
size_t ps_key_checkpoint_budgets(const ps_key_checkpoint *c) {
  return c && c->budget ? 1 : 0;
}
void ps_key_checkpoint_close(ps_key_checkpoint *c) {
  key_slot *slot, *next;
  size_t i;
  if (!c)
    return;
  for (slot = c->slots; slot; slot = next) {
    next = slot->next;
    close_slot(c, slot);
  }
  if (c->sources)
    for (i = 0; i < c->source_capacity; ++i) {
      source_count *source = c->sources[i], *following;
      while (source) {
        following = source->next;
        lc_free_with_allocator(&c->allocator, source);
        source = following;
      }
    }
  (void)ps_budget_close(c->budget, NULL);
  lc_free_with_allocator(&c->allocator, c->sources);
  lc_free_with_allocator(&c->allocator, c->keys);
  lc_free_with_allocator(&c->allocator, c->ids);
  pthread_mutex_destroy(&c->mutex);
  lc_free_with_allocator(&c->allocator, c);
}
int ps_key_checkpoint_observe(ps_key_checkpoint *c,
                              const ps_checkpoint_observer *observer,
                              lc_error *e) {
  if (!c || !observer || !observer->prepare || !observer->finish)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint observer requires complete callbacks");
  pthread_mutex_lock(&c->mutex);
  c->observer = *observer;
  pthread_mutex_unlock(&c->mutex);
  return LC_OK;
}
void ps_key_checkpoint_info(ps_key_checkpoint *c, ps_checkpoint_info *out) {
  pthread_mutex_lock(&c->mutex);
  *out = c->selected;
  out->slots = c->count;
  out->pending = c->dirty_count;
  out->pending_bytes = c->dirty_bytes;
  pthread_mutex_unlock(&c->mutex);
}
uint64_t ps_key_checkpoint_source_refs(ps_key_checkpoint *c, unsigned int kind,
                                       uint64_t id) {
  source_count *source;
  uint64_t count = 0;
  size_t bucket;
  pthread_mutex_lock(&c->mutex);
  bucket = (size_t)((id ^ kind) & (c->source_capacity - 1));
  for (source = c->sources[bucket]; source; source = source->next)
    if (source->kind == kind && source->id == id) {
      count = source->references;
      break;
    }
  pthread_mutex_unlock(&c->mutex);
  return count;
}
static source_count *source_find(ps_key_checkpoint *c, unsigned int kind,
                                 uint64_t id) {
  source_count *s;
  for (s = c->sources[(id ^ kind) & (c->source_capacity - 1)]; s; s = s->next)
    if (s->kind == kind && s->id == id)
      return s;
  return NULL;
}
uint64_t ps_key_checkpoint_source_bytes(ps_key_checkpoint *c, unsigned int kind,
                                        uint64_t id) {
  source_count *source;
  uint64_t bytes = 0;
  pthread_mutex_lock(&c->mutex);
  source = source_find(c, kind, id);
  if (source)
    bytes = source->byte_carries ? LC_U64_MAX : source->bytes;
  pthread_mutex_unlock(&c->mutex);
  return bytes;
}
static int range_busy(lc_error *e) {
  return lc_error_set(e, LC_ERR_INVALID, 0,
                      "captured body ownership is changing or stale", NULL,
                      "pouch_namespace_busy", "pouch");
}
static int range_collect(key_slot *s, uint64_t base, uint64_t end,
                         ps_checkpoint_ranges *out, size_t *capacity,
                         lc_error *e) {
  ps_owned_range *last, *grown;
  uint64_t a, b;
  size_t n;
  int rc;
  if (!s || s->range.tree.end <= base)
    return LC_OK;
  rc = range_collect(s->range.tree.left, base, end, out, capacity, e);
  if (rc != LC_OK || s->summary.body_start >= end)
    return rc;
  a = s->summary.body_start > base ? s->summary.body_start : base;
  b = s->summary.body_end < end ? s->summary.body_end : end;
  if (b > a) {
    a -= base;
    b -= base;
    last = out->count ? &out->ranges[out->count - 1] : NULL;
    if (last && a <= last->offset + last->bytes) {
      if (b > last->offset + last->bytes) {
        out->bytes += b - last->offset - last->bytes;
        last->bytes = b - last->offset;
      }
    } else {
      if (out->count == *capacity) {
        n = *capacity ? *capacity * 2 : 8;
        grown =
            lc_realloc_with_allocator(NULL, out->ranges, n * sizeof(*grown));
        if (!grown)
          return failure(e, LC_ERR_NOMEM,
                         "cannot capture bounded body intervals");
        out->ranges = grown;
        *capacity = n;
      }
      out->ranges[out->count].offset = a;
      out->ranges[out->count++].bytes = b - a;
      out->bytes += b - a;
    }
  }
  return range_collect(s->range.tree.right, base, end, out, capacity, e);
}
int ps_key_checkpoint_ranges(ps_key_checkpoint *c, unsigned int kind,
                             uint64_t id, uint64_t base, uint64_t bytes,
                             ps_checkpoint_ranges *out, lc_error *e) {
  source_count *s;
  size_t capacity = 0;
  int rc = LC_OK;
  if (!c || !out || !bytes || bytes > PS_STREAM_PIECE_BYTES ||
      base > (uint64_t)LC_I64_MAX - bytes)
    return failure(e, LC_ERR_INVALID, "body capture requires a bounded piece");
  memset(out, 0, sizeof(*out));
  pthread_mutex_lock(&c->mutex);
  s = source_find(c, kind, id);
  if (s && s->prepared)
    rc = range_busy(e);
  if (rc == LC_OK && s) {
    source_index_ranges(s);
    out->epoch = s->epoch;
    rc = range_collect(s->ranges, base, base + bytes, out, &capacity, e);
  }
  pthread_mutex_unlock(&c->mutex);
  if (rc != LC_OK) {
    lc_free_with_allocator(NULL, out->ranges);
    memset(out, 0, sizeof(*out));
  }
  return rc;
}
int ps_key_checkpoint_ranges_select(ps_key_checkpoint *c, unsigned int kind,
                                    uint64_t id, uint64_t epoch,
                                    ps_piece_publish_fn publish, void *ctx,
                                    lc_error *e) {
  source_count *s;
  int rc;
  pthread_mutex_lock(&c->mutex);
  s = source_find(c, kind, id);
  rc = (s && s->prepared) || (s ? s->epoch : 0) != epoch ? range_busy(e)
                                                         : publish(ctx, e);
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
int ps_key_checkpoint_read(ps_key_checkpoint *c, uint64_t id, ps_reader **out,
                           lc_error *e) {
  key_slot *slot;
  ps_key key = {0, 0, 0};
  int rc;
  if (out)
    *out = NULL;
  if (!c || !out)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint read requires owner and output");
  pthread_mutex_lock(&c->mutex);
  for (slot = c->ids[id & (c->capacity - 1)]; slot; slot = slot->id_next)
    if (slot->id == id) {
      key.owner = c->ns->identity.owner;
      key.key = slot->id;
      key.kind = slot->kind;
      break;
    }
  rc = slot ? ps_read_open(c->ns->root->space, key, NULL, out, e)
            : failure(e, LC_ERR_INVALID, "checkpoint slot does not exist");
  pthread_mutex_unlock(&c->mutex);
  return rc;
}

static int add_plan(ps_budget_plan *p, const ps_budget_plan *extra,
                    lc_error *e) {
  if (extra->payload_bytes > LC_U64_MAX - p->payload_bytes ||
      extra->metadata_bytes > LC_U64_MAX - p->metadata_bytes ||
      extra->entries > LC_U64_MAX - p->entries)
    return failure(e, LC_ERR_INVALID, "checkpoint capacity totals overflow");
  p->payload_bytes += extra->payload_bytes;
  p->metadata_bytes += extra->metadata_bytes;
  p->entries += extra->entries;
  return LC_OK;
}
static void subtract_plan(ps_budget_plan *p, const ps_budget_plan *cost) {
  p->payload_bytes -= cost->payload_bytes;
  p->metadata_bytes -= cost->metadata_bytes;
  p->entries -= cost->entries;
}
static int fund(ps_key_checkpoint *c, const ps_budget_plan *plan, lc_error *e) {
  ps_budget_plan completion = *plan;
  completion.maintenance = 1;
  if (c->budget)
    return ps_budget_resize(c->budget, &completion, e);
  return ps_budget_open(c->ns->root->space, &completion, &c->budget, e);
}
static void settle(ps_key_checkpoint *c) {
  if (!c->changed && !c->dirty) {
    (void)ps_budget_close(c->budget, NULL);
    c->budget = NULL;
  } else if (c->budget) {
    /* Admission/flush funds the exact outstanding plan again before work.
     * A failed shrink retains excess credit; failed I/O may have spent credit
     * and poisoned the root, whose health check rejects further admission. */
    (void)fund(c, &c->claim, NULL);
  }
}
static int delta_room(ps_checkpoint_delta *d, lc_error *e) {
  ps_key_checkpoint *c = d->checkpoint;
  slot_change **buckets, *change;
  size_t capacity, bucket;
  if (d->capacity && d->count < d->capacity / 2)
    return LC_OK;
  if (d->capacity > (size_t)-1 / 2 / sizeof(*buckets))
    return failure(e, LC_ERR_NOMEM,
                   "checkpoint append index exceeds local bounds");
  capacity = d->capacity ? d->capacity * 2 : 16;
  buckets = lc_calloc_with_allocator(&c->allocator, capacity, sizeof(*buckets));
  if (!buckets)
    return failure(e, LC_ERR_NOMEM, "cannot grow checkpoint append index");
  for (change = d->changes; change; change = change->next) {
    bucket = (size_t)(hash_key(change->slot->key, strlen(change->slot->key),
                               change->slot->kind) &
                      (capacity - 1));
    change->hash_next = buckets[bucket];
    buckets[bucket] = change;
  }
  lc_free_with_allocator(&c->allocator, d->buckets);
  d->buckets = buckets;
  d->capacity = capacity;
  return LC_OK;
}
static slot_change *delta_find(ps_checkpoint_delta *d, const void *key,
                               size_t bytes, unsigned int kind) {
  slot_change *change;
  size_t bucket;
  if (!d->capacity)
    return NULL;
  bucket = (size_t)(hash_key(key, bytes, kind) & (d->capacity - 1));
  for (change = d->buckets[bucket]; change; change = change->hash_next)
    if (change->slot->kind == kind && strlen(change->slot->key) == bytes &&
        !memcmp(change->slot->key, key, bytes))
      return change;
  return NULL;
}
/* The namespace appender owns exclusion. All allocations and capacity growth
 * here precede any application body bytes for the admitted record. */
static int admit_key(ps_checkpoint_delta *d, const void *key, size_t bytes,
                     unsigned int kind, uint64_t bound, lc_error *e) {
  ps_key_checkpoint *c = d->checkpoint;
  ps_budget_plan plan = d->claim, before = {0}, after = {0};
  slot_change *change;
  key_slot *slot;
  uint64_t id;
  size_t bucket;
  int rc;
  if (!plan.entries) {
    rc = ps_budget_plan_value(&plan, CHECKPOINT_CONTROL_BYTES, e);
    if (rc != LC_OK)
      return rc;
  }
  if (!key) {
    rc = d->planning ? LC_OK : fund(c, &plan, e);
    if (rc == LC_OK)
      d->claim = plan;
    return rc;
  }
  change = delta_find(d, key, bytes, kind);
  if (!change) {
    rc = delta_room(d, e);
    if (rc != LC_OK)
      return rc;
    change = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*change));
    if (!change)
      return failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint append key");
    bucket = (size_t)(hash_key(key, bytes, kind) & (c->capacity - 1));
    for (slot = c->keys[bucket]; slot; slot = slot->hash_next)
      if (slot->kind == kind && strlen(slot->key) == bytes &&
          !memcmp(slot->key, key, bytes))
        break;
    if (!slot) {
      rc = room(c, c->count + d->created + 1, e);
      if (rc == LC_OK)
        rc = ps_catalog_allocate_id(c->ns->root->space, &id, e);
      if (rc == LC_OK) {
        slot = new_slot(c, key, bytes, kind, id, e);
        if (!slot)
          rc = LC_ERR_NOMEM;
      }
      if (rc != LC_OK) {
        lc_free_with_allocator(&c->allocator, change);
        return rc;
      }
      change->created = 1;
      ++d->created;
    }
    change->slot = slot;
    change->prior_deletion = slot->required_deletion;
    change->bound = slot->pending.bytes ? slot->bound : 0;
    change->next = d->changes;
    d->changes = change;
    bucket = (size_t)(hash_key(key, bytes, kind) & (d->capacity - 1));
    change->hash_next = d->buckets[bucket];
    d->buckets[bucket] = change;
    ++d->count;
  }
  if (bound > change->bound) {
    if (change->bound) {
      rc = ps_budget_plan_key_slot(&before, kind, change->bound, 1, e);
      if (rc != LC_OK)
        return rc;
    }
    rc = ps_budget_plan_key_slot(&after, kind, bound, 1, e);
    if (rc != LC_OK)
      return rc;
    subtract_plan(&plan, &before);
    rc = add_plan(&plan, &after, e);
    if (rc != LC_OK)
      return rc;
  }
  rc = d->planning ? LC_OK : fund(c, &plan, e);
  if (rc == LC_OK) {
    d->claim = plan;
    if (bound > change->bound)
      change->bound = bound;
  }
  return rc;
}
static int admission(void *context, const lc_pouch_record_header *h,
                     const void *key, const void *metadata, lc_error *e) {
  ps_checkpoint_delta *d = context;
  ps_key_checkpoint *c = d->checkpoint;
  unsigned int kind;
  uint64_t deletion;
  int rc;
  if (h->type == LC_POUCH_STATE_RECORD_HIGH_WATER) {
    if (h->key_len || h->payload_len)
      return failure(e, LC_ERR_PROTOCOL, "invalid canonical high-water prefix");
    key = NULL;
  } else if (h->type < LC_POUCH_STATE_RECORD_STATE_PUT ||
             h->type > LC_POUCH_STATE_RECORD_OBJECT_DELETE || !h->key_len ||
             h->key_len > LC_POUCH_STATE_RECORD_KEY_MAX_BYTES || !key ||
             memchr(key, 0, h->key_len)) {
    return failure(e, LC_ERR_PROTOCOL,
                   "invalid canonical checkpoint key prefix");
  }
  if (h->meta_len > LC_POUCH_STATE_RECORD_META_MAX_BYTES)
    return failure(e, LC_ERR_PROTOCOL,
                   "canonical checkpoint metadata exceeds bounds");
  kind = h->type == LC_POUCH_STATE_RECORD_DECISION ? PS_KEY_DECISION
                                                   : PS_KEY_METADATA;
  rc = lc_pouch_state_record_delete_bound(h->type, h->key_len, metadata,
                                          (size_t)h->meta_len, &deletion, e);
  if (rc != LC_OK)
    return rc;
  pthread_mutex_lock(&c->mutex);
  if (d->planned) {
    slot_change *change =
        key ? delta_find(d, key, (size_t)h->key_len, kind) : NULL;
    rc = key && (!change ||
                 LC_POUCH_RECORD_HEADER_BYTES + h->key_len + h->meta_len + 512 >
                     change->bound ||
                 deletion > change->slot->required_deletion)
             ? failure(e, LC_ERR_PROTOCOL,
                       "canonical prefix exceeds its admitted batch plan")
             : LC_OK;
    pthread_mutex_unlock(&c->mutex);
    return rc;
  }
  rc = admit_key(d, key, (size_t)h->key_len, kind,
                 LC_POUCH_RECORD_HEADER_BYTES + h->key_len + h->meta_len + 512,
                 e);
  if (rc == LC_OK && deletion) {
    slot_change *change = delta_find(d, key, (size_t)h->key_len, kind);
    if (deletion > change->slot->required_deletion)
      /* Completion records can introduce bounded control slots after spending
       * the standing floor. They retain their future obligation, while their
       * complete frame/checkpoint plan owns immediate admission. Ordinary
       * writes must still preserve the unspent maintenance floor. */
      rc = reserve_slot(c, change->slot, deletion, d->maintenance, e);
  }
  if (rc == LC_OK && !d->maintenance && !d->planning)
    rc = ps_budget_foreground_check(c->ns->root->space, e);
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
int ps_checkpoint_delta_open(ps_key_checkpoint *c, int maintenance,
                             ps_checkpoint_delta **out, lc_error *e) {
  ps_checkpoint_delta *d;
  if (out)
    *out = NULL;
  if (!c || !out)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint delta requires an owner and output");
  d = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*d));
  if (!d)
    return failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint append");
  pthread_mutex_lock(&c->mutex);
  {
    int rc = settle_retired(c, e);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&c->mutex);
      lc_free_with_allocator(&c->allocator, d);
      return rc;
    }
  }
  d->checkpoint = c;
  d->maintenance = maintenance;
  d->claim = c->claim;
  pthread_mutex_unlock(&c->mutex);
  *out = d;
  return LC_OK;
}
static int delta_plan_cost(ps_checkpoint_delta *d, ps_budget_plan *extra,
                           lc_error *e) {
  ps_key_checkpoint *c = d->checkpoint;
  ps_budget_plan current = {0};
  int rc = LC_OK;
  memset(extra, 0, sizeof(*extra));
  pthread_mutex_lock(&c->mutex);
  if (c->budget)
    rc = ps_budget_status(c->budget, &current, e);
  if (rc == LC_OK) {
    extra->payload_bytes = d->claim.payload_bytes > current.payload_bytes
                               ? d->claim.payload_bytes - current.payload_bytes
                               : 0;
    extra->metadata_bytes =
        d->claim.metadata_bytes > current.metadata_bytes
            ? d->claim.metadata_bytes - current.metadata_bytes
            : 0;
    extra->entries = d->claim.entries > current.entries
                         ? d->claim.entries - current.entries
                         : 0;
    d->planned = 1;
  }
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
int ps_checkpoint_delta_plan(ps_checkpoint_delta *d,
                             const lc_pouch_storage_batch *batch,
                             uint64_t minimum, ps_budget_plan *extra,
                             lc_error *e) {
  lc_pouch_record_header h;
  const void *key, *metadata;
  uint64_t total = 0, fields[4];
  size_t i, j;
  int rc = LC_OK;
  if (!batch || !batch->prefix || !batch->count || !extra || d->writer ||
      batch->count > minimum / LC_POUCH_RECORD_HEADER_BYTES)
    return failure(e, LC_ERR_INVALID,
                   "batch admission requires bounded prefixes");
  memset(extra, 0, sizeof(*extra));
  d->planning = 1;
  for (i = 0; rc == LC_OK && i < batch->count; ++i) {
    memset(&h, 0, sizeof(h));
    key = metadata = NULL;
    rc = batch->prefix(batch->context, i, &h, &key, &metadata, e);
    if (rc == LC_OK && (h.flags || (h.meta_len && !metadata)))
      rc = failure(e, LC_ERR_INVALID,
                   "batch prefix must be complete and bounded");
    fields[0] = LC_POUCH_RECORD_HEADER_BYTES;
    fields[1] = h.key_len;
    fields[2] = h.meta_len;
    fields[3] = h.payload_len;
    for (j = 0; rc == LC_OK && j < 4; ++j) {
      if (fields[j] > minimum - total)
        rc = failure(e, LC_ERR_INVALID,
                     "batch prefixes exceed declared canonical span");
      else
        total += fields[j];
    }
    if (rc == LC_OK)
      rc = admission(d, &h, key, metadata, e);
  }
  d->planning = 0;
  if (rc == LC_OK && total != minimum)
    rc = failure(e, LC_ERR_INVALID,
                 "batch prefixes differ from declared canonical span");
  return rc == LC_OK ? delta_plan_cost(d, extra, e) : rc;
}
int ps_checkpoint_delta_fund(ps_checkpoint_delta *d, ps_budget *source,
                             const ps_budget_plan *extra, lc_error *e) {
  ps_key_checkpoint *c = d->checkpoint;
  int rc = LC_OK;
  if (!extra->payload_bytes && !extra->metadata_bytes && !extra->entries)
    return LC_OK;
  pthread_mutex_lock(&c->mutex);
  if (!c->budget)
    rc = ps_budget_open_completion(c->ns->root->space, NULL, &c->budget, e);
  if (rc == LC_OK)
    rc = ps_budget_transfer(source, c->budget, extra, e);
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
int ps_checkpoint_delta_bind(ps_checkpoint_delta *d, ps_record_writer *writer,
                             uint64_t segment, const char *leaf, lc_error *e) {
  if (!d || !writer || !writer->writer || !segment || !leaf || d->writer)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint binding requires a private append");
  d->writer = writer;
  d->segment = segment;
  d->leaf = leaf;
  d->start = writer->tail;
  return ps_record_admission_set(writer, admission, d, e);
}
static int prepare_entry(ps_checkpoint_delta *d,
                         const lc_pouch_state_entry *entry, lc_error *e) {
  ps_key_checkpoint *c = d->checkpoint;
  lc_pouch_state_entry prior, projected;
  ps_checkpoint_record record = {0};
  source_count *source;
  slot_change *change;
  unsigned int kind;
  int accepted, rc = LC_OK;
  if (entry->index_seq > d->state_index)
    d->state_index = entry->index_seq;
  if (entry->control &&
      entry->record_type == LC_POUCH_STATE_RECORD_HIGH_WATER) {
    if (entry->query_index_seq > d->query_index)
      d->query_index = entry->query_index_seq;
    return LC_OK;
  }
  if (!entry->key)
    return failure(e, LC_ERR_PROTOCOL,
                   "canonical checkpoint record lacks a key");
  kind = entry->control ? PS_KEY_DECISION : PS_KEY_METADATA;
  change = delta_find(d, entry->key, strlen(entry->key), kind);
  if (!change)
    return failure(e, LC_ERR_PROTOCOL,
                   "canonical record bypassed checkpoint admission");
  summary_view(change->slot,
               change->prepared ? &change->summary : &change->slot->summary,
               &prior);
  prior.seen = change->prepared || !change->created;
  if (d->body_replacement) {
    if (kind != PS_KEY_METADATA || change->created || change->prepared ||
        !entry->found || !entry->payload_span.present ||
        entry->version != prior.version ||
        entry->index_seq != prior.index_seq ||
        entry->lifetime != prior.lifetime)
      return failure(e, LC_ERR_INVALID,
                     "physical replacement changed logical key identity");
    accepted = 1;
    projected = *entry;
    projected.record_type = prior.record_type;
  } else if (kind == PS_KEY_DECISION) {
    accepted = lc_pouch_state_decision_entry_accepts(prior.seen,
                                                     prior.index_seq, entry);
    projected = *entry;
  } else {
    accepted = lc_pouch_state_entry_project(&prior, entry, &projected);
  }
  if (!accepted)
    return LC_OK;
  {
    uint64_t deletion = lc_pouch_state_entry_delete_bytes(&projected);
    if (deletion > change->slot->required_deletion)
      rc = reserve_slot(c, change->slot, deletion,
                        d->maintenance || (!d->writer && !d->metadata_only), e);
  }
  if (rc != LC_OK)
    return rc;
  rc = ps_checkpoint_record_encode(&c->allocator, &projected, &record, e);
  if (rc == LC_OK && record.length > change->bound)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "checkpoint metadata exceeds admitted bound");
  source = NULL;
  if (rc == LC_OK)
    source = source_get(c, &projected, &rc, e);
  if (rc == LC_OK) {
    if (source)
      ++source->prepared;
    release_prepared_source(c, change->body);
    ps_checkpoint_record_close(&change->record);
    change->record = record;
    memset(&record, 0, sizeof(record));
    summarize(&change->summary, &projected);
    change->body = source;
    change->prepared = 1;
    if (projected.query_index_seq > d->query_index)
      d->query_index = projected.query_index_seq;
  }
  ps_checkpoint_record_close(&record);
  return rc;
}
int ps_checkpoint_delta_prepare(ps_checkpoint_delta *d, lc_error *e) {
  ps_key_checkpoint *c;
  lc_pouch_state_entry entry = {0};
  lc_pouch_record_io io;
  uint64_t offset, next;
  int found = 0, truncated = 0, rc = LC_OK;
  if (!d || d->prepared || d->accepted || !d->writer || d->writer->failed ||
      !d->writer->writer)
    return failure(e, LC_ERR_INVALID, "checkpoint delta is not open");
  c = d->checkpoint;
  io = ps_record_output(d->writer);
  d->end = d->writer->tail;
  offset = d->start;
  if (offset == d->end)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint delta has no canonical records");
  pthread_mutex_lock(&c->mutex);
  while (rc == LC_OK && offset < d->end) {
    rc = lc_pouch_state_record_read(io, &c->allocator, d->leaf, offset, d->end,
                                    0, 0, NULL, &entry, &next, &found,
                                    &truncated, e);
    if (rc == LC_OK && (!found || truncated || next <= offset))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "checkpoint delta has an incomplete record");
    if (rc == LC_OK)
      rc = prepare_entry(d, &entry, e);
    if (rc == LC_OK)
      offset = next;
    lc_pouch_state_entry_cleanup(&c->allocator, &entry);
  }
  if (rc == LC_OK)
    if (d->count > LC_U64_MAX - c->body_epoch)
      rc = failure(e, LC_ERR_INVALID, "checkpoint body epoch exhausted");
  if (rc == LC_OK)
    d->prepared = 1;
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
static void dirty_add(ps_key_checkpoint *c, key_slot *s) {
  if (s->dirty_link)
    return;
  s->dirty_next = c->dirty;
  if (c->dirty)
    c->dirty->dirty_link = &s->dirty_next;
  c->dirty = s;
  s->dirty_link = &c->dirty;
  ++c->dirty_count;
}
static void dirty_remove(ps_key_checkpoint *c, key_slot *s) {
  *s->dirty_link = s->dirty_next;
  if (s->dirty_next)
    s->dirty_next->dirty_link = s->dirty_link;
  s->dirty_link = NULL;
  s->dirty_next = NULL;
  --c->dirty_count;
}
static void accept_delta(ps_checkpoint_delta *d, uint64_t bytes) {
  ps_key_checkpoint *c = d->checkpoint;
  slot_change *change;
  ps_budget_plan cost = {0};
  pthread_mutex_lock(&c->mutex);
  if (!c->claim.entries)
    (void)ps_budget_plan_value(&c->claim, CHECKPOINT_CONTROL_BYTES, NULL);
  for (change = d->changes; change; change = change->next) {
    key_slot *s = change->slot;
    if (!change->prepared)
      continue;
    if (change->created) {
      insert_slot(c, s);
      change->created = 0;
    }
    if (s->pending.bytes) {
      memset(&cost, 0, sizeof(cost));
      (void)ps_budget_plan_key_slot(&cost, s->kind, s->bound, 1, NULL);
      subtract_plan(&c->claim, &cost);
      c->dirty_bytes -= s->pending.length;
    }
    ps_checkpoint_record_close(&s->pending);
    s->pending = change->record;
    memset(&change->record, 0, sizeof(change->record));
    s->bound = change->bound;
    /* The newer stream owns retirement until its funded slot is durable. */
    retire_remove(s);
    bind_source(c, s, change->body, &change->summary);
    release_prepared_source(c, change->body);
    change->body = NULL;
    dirty_add(c, s);
    c->dirty_bytes += s->pending.length;
    memset(&cost, 0, sizeof(cost));
    (void)ps_budget_plan_key_slot(&cost, s->kind, s->bound, 1, NULL);
    (void)add_plan(&c->claim, &cost, NULL);
  }
  if (d->state_index > c->state_index)
    c->state_index = d->state_index;
  if (d->query_index > c->query_index)
    c->query_index = d->query_index;
  c->latest_segment = d->segment;
  c->latest_offset = d->end;
  if (bytes > LC_U64_MAX - c->uncovered_bytes)
    c->uncovered_bytes = LC_U64_MAX;
  else
    c->uncovered_bytes += bytes;
  c->changed = 1;
  if (c->logical_epoch != LC_U64_MAX)
    ++c->logical_epoch;
  d->accepted = 1;
  pthread_mutex_unlock(&c->mutex);
}
void ps_checkpoint_delta_accept(ps_checkpoint_delta *d) {
  accept_delta(d, d->end - d->start);
}
void ps_checkpoint_delta_close(ps_checkpoint_delta *d) {
  ps_key_checkpoint *c;
  slot_change *change, *next;
  if (!d)
    return;
  c = d->checkpoint;
  if (d->writer && d->writer->admission_context == d) {
    d->writer->admit = NULL;
    d->writer->admission_context = NULL;
  }
  pthread_mutex_lock(&c->mutex);
  for (change = d->changes; change; change = next) {
    next = change->next;
    if (change->slot && (!d->accepted || !change->prepared) && !change->created)
      (void)reserve_slot(c, change->slot, change->prior_deletion, 0, NULL);
    release_prepared_source(c, change->body);
    ps_checkpoint_record_close(&change->record);
    if (change->created)
      close_slot(c, change->slot);
    lc_free_with_allocator(&c->allocator, change);
  }
  lc_free_with_allocator(&c->allocator, d->buckets);
  settle(c);
  pthread_mutex_unlock(&c->mutex);
  lc_free_with_allocator(&c->allocator, d);
}
static int replay_superseded(ps_checkpoint_delta *d,
                             const lc_pouch_state_entry *entry) {
  slot_change *change;
  key_slot *slot;
  lc_pouch_state_entry prior, projected;
  unsigned int kind = entry->control ? PS_KEY_DECISION : PS_KEY_METADATA;
  if (!entry->key || (entry->control && !entry->decision))
    return 0;
  change = delta_find(d, entry->key, strlen(entry->key), kind);
  slot = change ? change->slot : find_key(d->checkpoint, entry->key, kind);
  if (!slot || (change && change->created && !change->prepared))
    return 0;
  summary_view(slot,
               change && change->prepared ? &change->summary : &slot->summary,
               &prior);
  return kind == PS_KEY_DECISION
             ? !lc_pouch_state_decision_entry_accepts(prior.seen,
                                                      prior.index_seq, entry)
             : !lc_pouch_state_entry_project(&prior, entry, &projected);
}
int ps_checkpoint_delta_replay(ps_checkpoint_delta *d,
                               const lc_pouch_state_entry *entry, lc_error *e) {
  ps_key_checkpoint *c;
  const char *key;
  uint64_t bytes = 0;
  int rc = LC_OK, superseded;
  if (!d || d->writer || d->metadata_only || d->accepted || !entry ||
      !entry->seen)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint replay requires a complete record");
  c = d->checkpoint;
  pthread_mutex_lock(&c->mutex);
  /* Forward slots and earlier records in this delta can supersede a replayed
   * value. Still account for its state high water and coverage. */
  superseded = replay_superseded(d, entry);
  if (!superseded)
    rc = lc_pouch_state_record_measure_snapshot(&c->allocator, NULL, entry, 0,
                                                &bytes, e);
  if (rc == LC_OK) {
    key =
        superseded || (entry->control && !entry->decision) ? NULL : entry->key;
    d->planning = 1;
    rc =
        admit_key(d, key, key ? strlen(key) : 0,
                  entry->control ? PS_KEY_DECISION : PS_KEY_METADATA, bytes, e);
    d->planning = 0;
  }
  if (rc == LC_OK) {
    if (!superseded)
      rc = prepare_entry(d, entry, e);
    else if (entry->index_seq > d->state_index)
      d->state_index = entry->index_seq;
  }
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
int ps_checkpoint_delta_replay_commit(ps_checkpoint_delta *d, uint64_t segment,
                                      uint64_t end, uint64_t bytes,
                                      lc_error *e) {
  ps_key_checkpoint *c;
  int rc = LC_OK;
  if (!d || d->writer || d->metadata_only || d->accepted)
    return failure(e, LC_ERR_INVALID, "checkpoint replay is not open");
  c = d->checkpoint;
  pthread_mutex_lock(&c->mutex);
  if (!d->claim.entries && bytes)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "checkpoint replay has bytes without records");
  else if (d->count > LC_U64_MAX - c->body_epoch)
    rc = failure(e, LC_ERR_INVALID, "checkpoint body epoch exhausted");
  if (rc == LC_OK && d->claim.entries)
    rc = fund(c, &d->claim, e);
  if (rc == LC_OK) {
    d->segment = segment;
    d->end = end;
    if (!d->claim.entries) {
      /* Empty selected successors advance the cursor without funding debt. */
      c->latest_segment = segment;
      c->latest_offset = end;
      d->accepted = 1;
    }
  }
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK && !d->accepted)
    accept_delta(d, bytes);
  return rc;
}
int ps_key_checkpoint_cover_empty(ps_key_checkpoint *c, uint64_t segment,
                                  lc_error *e) {
  ps_budget_plan plan;
  int rc = LC_OK;
  if (!c || !segment)
    return failure(e, LC_ERR_INVALID,
                   "empty coverage requires a selected head");
  pthread_mutex_lock(&c->mutex);
  if (c->selected.segment == segment && !c->selected.offset) {
    pthread_mutex_unlock(&c->mutex);
    return LC_OK;
  }
  if (segment < c->latest_segment ||
      (segment == c->latest_segment && c->latest_offset))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "empty coverage contradicts the canonical source tail");
  plan = c->claim;
  if (rc == LC_OK && !plan.entries)
    rc = ps_budget_plan_value(&plan, CHECKPOINT_CONTROL_BYTES, e);
  if (rc == LC_OK)
    rc = fund(c, &plan, e);
  if (rc == LC_OK) {
    c->claim = plan;
    c->latest_segment = segment;
    c->latest_offset = 0;
    c->changed = 1;
  }
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
int ps_key_checkpoint_due(ps_key_checkpoint *c) {
  int due;
  pthread_mutex_lock(&c->mutex);
  due = c->dirty_count >= CHECKPOINT_BATCH ||
        c->dirty_bytes >= CHECKPOINT_TARGET_BYTES ||
        c->uncovered_bytes >= CHECKPOINT_HISTORY_BYTES;
  pthread_mutex_unlock(&c->mutex);
  return due;
}

static int write_control_values(ps_key_checkpoint *c, ps_budget *budget,
                                const ps_checkpoint_info *info, ps_writer **out,
                                lc_error *e) {
  unsigned char bytes[CHECKPOINT_CONTROL_BYTES] = {0};
  int rc;
  memcpy(bytes, "LCPSCKPT", 8);
  ps_wire_put64(bytes + 8, c->ns->identity.owner);
  ps_wire_put64(bytes + 16, info->epoch);
  ps_wire_put64(bytes + 24, info->segment);
  ps_wire_put64(bytes + 32, info->offset);
  ps_wire_put64(bytes + 40, info->state_index);
  ps_wire_put64(bytes + 48, info->query_index);
  ps_wire_put64(bytes + 120, (uint64_t)crc32(0, bytes, 120));
  rc = ps_begin(c->ns->root->space, budget, control_key(c->ns), out, e);
  if (rc == LC_OK)
    rc = ps_write(*out, bytes, sizeof(bytes), e);
  return rc;
}
static int write_control(ps_key_checkpoint *c, uint64_t epoch, ps_writer **out,
                         lc_error *e) {
  ps_checkpoint_info info = {0};
  info.epoch = epoch;
  info.segment = c->latest_segment;
  info.offset = c->latest_offset;
  info.state_index = c->state_index;
  info.query_index = c->query_index;
  return write_control_values(c, c->budget, &info, out, e);
}
static int fund_completion_claim(ps_key_checkpoint *c,
                                 const ps_budget_plan *cost, lc_error *e) {
  ps_space *space = c->ns->root->space;
  ps_budget *parent = NULL;
  int rc;
  pthread_mutex_lock(&c->mutex);
  if (!c->budget)
    rc = ps_budget_open_completion(space, cost, &c->budget, e);
  else {
    rc = ps_budget_open(space, cost, &parent, e);
    if (rc == LC_OK)
      rc = ps_budget_transfer(parent, c->budget, cost, e);
  }
  pthread_mutex_unlock(&c->mutex);
  (void)ps_budget_close(parent, NULL);
  return rc;
}
int ps_key_metadata_begin(ps_key_checkpoint *c, int maintenance,
                          const lc_pouch_storage_batch *batch, uint64_t bytes,
                          ps_checkpoint_delta **out, lc_error *e) {
  ps_checkpoint_delta *d = NULL;
  ps_budget_plan cost;
  lc_pouch_record_header h;
  const void *key, *meta;
  size_t i;
  int rc;
  if (out)
    *out = NULL;
  if (!c || !out || !batch || !batch->prefix || !batch->count)
    return failure(e, LC_ERR_INVALID,
                   "native metadata requires prefixes and output");
  pthread_mutex_lock(&c->mutex);
  rc = c->changed || c->dirty
           ? failure(e, LC_ERR_INVALID,
                     "native metadata requires completed stream debt")
           : LC_OK;
  pthread_mutex_unlock(&c->mutex);
  for (i = 0; rc == LC_OK && i < batch->count; ++i) {
    memset(&h, 0, sizeof(h));
    key = meta = NULL;
    rc = batch->prefix(batch->context, i, &h, &key, &meta, e);
    if (rc == LC_OK && (h.payload_len || h.flags || h.payload_crc))
      rc = failure(e, LC_ERR_INVALID,
                   "native metadata cannot contain producer bytes");
  }
  if (rc == LC_OK)
    rc = ps_checkpoint_delta_open(c, maintenance, &d, e);
  if (rc == LC_OK)
    rc = ps_checkpoint_delta_plan(d, batch, bytes, &cost, e);
  if (rc == LC_OK && cost.metadata_bytes > PS_JOURNAL - 64)
    rc = failure(e, LC_ERR_INVALID,
                 "native metadata exceeds one revision descriptor window");
  if (rc == LC_OK) {
    cost.maintenance = maintenance;
    rc = fund_completion_claim(c, &cost, e);
  }
  if (rc == LC_OK) {
    d->metadata_only = 1;
    d->metadata_count = batch->count;
    d->metadata_bytes = bytes;
    *out = d;
  } else
    ps_checkpoint_delta_close(d);
  return rc;
}
typedef struct metadata_input {
  struct iovec parts[3];
  uint64_t bytes;
} metadata_input;
static int metadata_read(void *context, uint64_t offset,
                         const struct iovec *parts, int count, lc_error *e) {
  metadata_input *input = context;
  int i, j;
  for (i = 0; i < count; ++i) {
    unsigned char *to = parts[i].iov_base;
    size_t left = parts[i].iov_len;
    if (offset > input->bytes || left > input->bytes - offset)
      return failure(e, LC_ERR_PROTOCOL,
                     "native metadata prefix read exceeds its span");
    while (left) {
      uint64_t base = 0;
      for (j = 0; j < 3; ++j) {
        if (offset - base < input->parts[j].iov_len) {
          size_t at = (size_t)(offset - base);
          size_t take = input->parts[j].iov_len - at;
          if (take > left)
            take = left;
          memcpy(to, (const unsigned char *)input->parts[j].iov_base + at,
                 take);
          to += take;
          offset += take;
          left -= take;
          break;
        }
        base += input->parts[j].iov_len;
      }
      if (j == 3)
        return failure(e, LC_ERR_PROTOCOL,
                       "native metadata prefix is incomplete");
    }
  }
  return LC_OK;
}

static size_t prepare_completed_stages(ps_checkpoint_delta *d) {
  slot_change *decision, *stage;
  const char *key;
  size_t remove = 0;
  for (decision = d->changes; decision; decision = decision->next) {
    if (!decision->prepared || decision->slot->kind != PS_KEY_DECISION)
      continue;
    key = decision->slot->key;
    if (!lc_pouch_state_key_is_staged(key))
      continue;
    stage = delta_find(d, key, strlen(key), PS_KEY_METADATA);
    /* The prior stage is already covered. The same native revision selects
     * the accepted tombstone and the new coverage or neither of them. A
     * standalone recovery decision still needs a slot, as do decisions in a
     * stream whose checkpoint can publish rows before advancing coverage. */
    if (stage && stage->prepared && stage->slot->summary.found &&
        (stage->summary.type == LC_POUCH_STATE_RECORD_STATE_DELETE ||
         stage->summary.type == LC_POUCH_STATE_RECORD_OBJECT_DELETE) &&
        stage->summary.lifetime == stage->slot->summary.lifetime &&
        stage->slot->summary.index < decision->summary.index &&
        decision->summary.index < stage->summary.index) {
      decision->prepared = 0;
      /* A selected descriptor carries its standing deletion obligation
       * until the retirement frame is durable. Keep the ordinary tombstone
       * if an additional local obligation still belongs to the stage. */
      if (stage->slot->selected_incarnation && !stage->slot->reserve) {
        stage->remove = 1;
        ++remove;
      }
    }
  }
  return remove;
}

int ps_key_metadata_commit(ps_checkpoint_delta *d,
                           const lc_pouch_storage_batch *batch,
                           enum ps_commit_mode mode, lc_error *e) {
  static const lc_pouch_record_io_ops ops = {metadata_read, NULL, NULL, NULL,
                                             NULL};
  ps_key_checkpoint *c;
  ps_writer **writers = NULL;
  ps_revision_retirement *retirements = NULL;
  slot_change *change;
  ps_checkpoint_info selected = {0};
  lc_pouch_state_entry entry = {0};
  lc_pouch_record_header h;
  unsigned char header[LC_POUCH_RECORD_HEADER_BYTES];
  metadata_input input;
  lc_pouch_record_io io;
  const void *key, *meta;
  ps_ref control_ref, slot_ref;
  uint64_t next, bytes, total = 0, prefix = 0;
  size_t i, count = 0, retire_count = 0, retired = 0;
  int rc = LC_OK, found, truncated;
  if (!d || !d->metadata_only || d->accepted || d->prepared || !batch ||
      !batch->prefix || batch->count != d->metadata_count ||
      (mode != PS_COMMIT_DEFAULT && mode != PS_COMMIT_DEFER_SYNC))
    return failure(e, LC_ERR_INVALID, "native metadata plan is not open");
  c = d->checkpoint;
  io.ops = &ops;
  io.context = &input;
  pthread_mutex_lock(&c->mutex);
  for (i = 0; rc == LC_OK && i < batch->count; ++i) {
    memset(&h, 0, sizeof(h));
    key = meta = NULL;
    pthread_mutex_unlock(&c->mutex);
    rc = batch->prefix(batch->context, i, &h, &key, &meta, e);
    if (rc == LC_OK && (h.payload_len || h.flags || h.payload_crc))
      rc = failure(e, LC_ERR_PROTOCOL, "native metadata gained producer bytes");
    /* Borrowed providers and admission run outside the owner mutex. */
    if (rc == LC_OK)
      rc = admission(d, &h, key, meta, e);
    pthread_mutex_lock(&c->mutex);
    if (rc != LC_OK)
      break;
    bytes = LC_POUCH_RECORD_HEADER_BYTES + (uint64_t)h.key_len + h.meta_len;
    if (bytes > d->metadata_bytes - total) {
      rc = failure(e, LC_ERR_PROTOCOL,
                   "native metadata differs from its admitted span");
      break;
    }
    total += bytes;
    lc_pouch_record_header_encode(&h, header);
    input.parts[0].iov_base = header;
    input.parts[0].iov_len = sizeof(header);
    input.parts[1].iov_base = (void *)key;
    input.parts[1].iov_len = (size_t)h.key_len;
    input.parts[2].iov_base = (void *)meta;
    input.parts[2].iov_len = (size_t)h.meta_len;
    input.bytes = bytes;
    change = key ? delta_find(d, key, (size_t)h.key_len,
                              h.type == LC_POUCH_STATE_RECORD_DECISION
                                  ? PS_KEY_DECISION
                                  : PS_KEY_METADATA)
                 : NULL;
    found = truncated = 0;
    next = 0;
    rc = lc_pouch_state_record_read(
        io, &c->allocator,
        change ? change->slot->leaf : "snapshot-00000000000000000001.log", 0,
        bytes, 0, 0, NULL, &entry, &next, &found, &truncated, e);
    if (rc == LC_OK && (!found || truncated || next != bytes))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "native metadata has an incomplete record");
    if (rc == LC_OK) {
      rc = prepare_entry(d, &entry, e);
      if (rc == LC_OK && change && change->prepared &&
          change->summary.index == entry.index_seq)
        change->ordinal = i;
    }
    lc_pouch_state_entry_cleanup(&c->allocator, &entry);
  }
  if (rc == LC_OK &&
      (total != d->metadata_bytes || d->count > LC_U64_MAX - c->body_epoch))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "native metadata preparation exceeds its plan");
  if (rc == LC_OK) {
    retire_count = prepare_completed_stages(d);
    selected.segment = c->latest_segment;
    selected.offset = c->latest_offset;
    selected.state_index =
        d->state_index > c->state_index ? d->state_index : c->state_index;
    selected.query_index =
        d->query_index > c->query_index ? d->query_index : c->query_index;
    d->prepared = 1;
  }
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK)
    rc = ps_catalog_allocate_id(c->ns->root->space, &selected.epoch, e);
  if (rc == LC_OK && d->count >= (size_t)-1 / sizeof(*writers))
    rc = failure(e, LC_ERR_NOMEM,
                 "native metadata writer list exceeds local bounds");
  if (rc == LC_OK) {
    writers =
        lc_calloc_with_allocator(&c->allocator, d->count + 1, sizeof(*writers));
    if (!writers)
      rc = failure(e, LC_ERR_NOMEM, "cannot allocate native metadata writers");
  }
  if (rc == LC_OK && retire_count) {
    retirements = lc_calloc_with_allocator(&c->allocator, retire_count,
                                           sizeof(*retirements));
    if (!retirements)
      rc = failure(e, LC_ERR_NOMEM, "cannot allocate native stage retirements");
  }
  for (change = d->changes; rc == LC_OK && change; change = change->next) {
    ps_key physical;
    if (!change->prepared)
      continue;
    physical.owner = c->ns->identity.owner;
    physical.key = change->slot->id;
    physical.kind = change->slot->kind;
    if (change->remove) {
      retirements[retired].key = physical;
      retirements[retired++].incarnation = change->slot->selected_incarnation;
      continue;
    }
    rc = ps_checkpoint_record_write(c->ns->root->space, c->budget, physical,
                                    &change->record, &writers[count], e);
    if (rc == LC_OK) {
      rc = ps_revision_writer_info(writers[count], &slot_ref, &bytes, e);
      ++count;
      if (rc == LC_OK)
        change->selected_incarnation = slot_ref.incarnation;
    }
  }
  if (rc == LC_OK) {
    rc = write_control_values(c, c->budget, &selected, &writers[count], e);
    if (rc == LC_OK)
      rc = ps_revision_writer_info(writers[count], &control_ref, &bytes, e);
    if (writers[count])
      ++count;
  }
  if (rc == LC_OK) {
    int flags = d->body_replacement ? PS_REVISION_FORCE_DURABLE : 0;
    if (!d->maintenance && !d->body_replacement)
      flags |= PS_REVISION_ADMIT_RESERVE;
    rc = ps_revision_publish_retiring(writers, count, retirements, retired,
                                      flags, mode, &prefix, e);
  }
  if (rc == LC_OK) {
    pthread_mutex_lock(&c->mutex);
    for (change = d->changes; change; change = change->next) {
      key_slot *s = change->slot;
      if (!change->prepared)
        continue;
      if (change->remove) {
        --c->selected_count;
        drop_selected_slot(c, s);
        change->slot = NULL;
        continue;
      }
      if (change->created) {
        insert_slot(c, s);
        change->created = 0;
      }
      select_incarnation(c, s, change->selected_incarnation,
                         change->summary.deletion_bytes);
      bind_source(c, s, change->body, &change->summary);
      release_prepared_source(c, change->body);
      change->body = NULL;
      retire_note(c, s, prefix);
    }
    selected.control = control_ref.incarnation;
    c->selected = selected;
    c->state_index = selected.state_index;
    c->query_index = selected.query_index;
    if (!d->body_replacement && c->logical_epoch != LC_U64_MAX)
      ++c->logical_epoch;
    d->accepted = 1;
    (void)settle_retired(c, NULL);
    pthread_mutex_unlock(&c->mutex);
  }
  for (i = 0; i < count; ++i)
    ps_writer_close(writers[i]);
  lc_free_with_allocator(&c->allocator, retirements);
  lc_free_with_allocator(&c->allocator, writers);
  return rc;
}
void ps_key_metadata_visit_retired(const ps_checkpoint_delta *d,
                                   lc_pouch_storage_metadata_retired_fn visit,
                                   void *context) {
  const slot_change *change;
  if (d->accepted)
    for (change = d->changes; change; change = change->next)
      if (change->remove)
        visit(context, change->ordinal);
}
typedef struct body_replacement_prefix {
  const ps_checkpoint_record *record;
} body_replacement_prefix;
static int replacement_prefix(void *context, size_t index,
                              lc_pouch_record_header *header, const void **key,
                              const void **metadata, lc_error *e) {
  body_replacement_prefix *input = context;
  const ps_checkpoint_record *record = input->record;
  int rc;
  if (index)
    return failure(e, LC_ERR_INVALID, "physical replacement has one key");
  rc = lc_pouch_record_header_decode(record->bytes, header, e);
  if (rc == LC_OK) {
    *key = record->bytes + LC_POUCH_RECORD_HEADER_BYTES;
    *metadata = (const unsigned char *)*key + header->key_len;
  }
  return rc;
}
static int replacement_expected(ps_key_checkpoint *c,
                                const lc_pouch_state_entry *expected,
                                const ps_checkpoint_record *record,
                                lc_error *e) {
  ps_reader *reader = NULL;
  key_slot *slot;
  uint64_t id = 0;
  size_t at = 0, got;
  unsigned char bytes[1024];
  int rc;
  pthread_mutex_lock(&c->mutex);
  slot = find_key(c, expected->key, PS_KEY_METADATA);
  if (slot)
    id = slot->id;
  rc = !slot || c->changed || c->dirty || c->observer.prepare
           ? failure(e, LC_ERR_INVALID,
                     "physical replacement requires offline selected metadata")
           : LC_OK;
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK)
    rc = ps_key_checkpoint_read(c, id, &reader, e);
  if (rc == LC_OK && ps_read_size(reader) != record->length)
    rc = failure(e, LC_ERR_INVALID, "physical replacement source changed");
  while (rc == LC_OK && at < record->length) {
    size_t take = record->length - at;
    if (take > sizeof(bytes))
      take = sizeof(bytes);
    got = 0;
    rc = ps_read(reader, bytes, take, &got, e);
    if (rc == LC_OK && (got != take || memcmp(bytes, record->bytes + at, take)))
      rc = failure(e, LC_ERR_INVALID, "physical replacement source changed");
    at += got;
  }
  ps_reader_close(reader);
  return rc;
}
struct ps_checkpoint_body {
  ps_key_checkpoint *checkpoint;
  ps_checkpoint_delta *delta;
  ps_checkpoint_record original;
  lc_pouch_state_entry source;
  ps_budget_plan cost;
  uint64_t maximum;
  int funded;
};
void ps_key_body_close(ps_checkpoint_body *body) {
  ps_key_checkpoint *c;
  if (!body)
    return;
  c = body->checkpoint;
  ps_checkpoint_delta_close(body->delta);
  ps_checkpoint_record_close(&body->original);
  lc_pouch_state_entry_cleanup(&c->allocator, &body->source);
  lc_free_with_allocator(&c->allocator, body);
}
int ps_key_body_plan(ps_key_checkpoint *c, const lc_pouch_state_entry *expected,
                     uint64_t maximum, ps_checkpoint_body **out,
                     ps_budget_plan *cost, lc_error *e) {
  static const lc_pouch_record_io_ops ops = {metadata_read, NULL, NULL, NULL,
                                             NULL};
  ps_checkpoint_body *body = NULL;
  metadata_input input;
  lc_pouch_record_io io;
  uint64_t next = 0;
  int rc, found = 0, truncated = 0;
  if (out)
    *out = NULL;
  if (!c || !expected || !expected->found || expected->control ||
      !expected->payload_span.present || !out || !cost || !maximum ||
      maximum > LC_POUCH_STATE_RECORD_META_MAX_BYTES +
                    LC_POUCH_STATE_RECORD_KEY_MAX_BYTES +
                    LC_POUCH_RECORD_HEADER_BYTES)
    return failure(
        e, LC_ERR_INVALID,
        "body replacement planning requires resolved bounded metadata");
  memset(cost, 0, sizeof(*cost));
  body = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*body));
  if (!body)
    return failure(e, LC_ERR_NOMEM, "cannot allocate body replacement plan");
  body->checkpoint = c;
  body->maximum = maximum;
  rc = ps_checkpoint_record_encode(&c->allocator, expected, &body->original, e);
  if (rc == LC_OK)
    rc = replacement_expected(c, expected, &body->original, e);
  memset(&input, 0, sizeof(input));
  input.parts[0].iov_base = body->original.bytes;
  input.parts[0].iov_len = body->original.length;
  input.bytes = body->original.length;
  io.ops = &ops;
  io.context = &input;
  if (rc == LC_OK)
    rc = lc_pouch_state_record_read(
        io, &c->allocator, "snapshot-00000000000000000001.log", 0, input.bytes,
        0, 0, NULL, &body->source, &next, &found, &truncated, e);
  if (rc == LC_OK && (!found || truncated || next != input.bytes))
    rc = failure(e, LC_ERR_PROTOCOL, "body replacement source is incomplete");
  if (rc == LC_OK)
    rc = ps_checkpoint_delta_open(c, 0, &body->delta, e);
  if (rc == LC_OK) {
    ps_checkpoint_delta *d = body->delta;
    d->planning = 1;
    pthread_mutex_lock(&c->mutex);
    rc = admit_key(d, body->source.key, strlen(body->source.key),
                   PS_KEY_METADATA, maximum + 512, e);
    pthread_mutex_unlock(&c->mutex);
    d->planning = 0;
    if (rc == LC_OK)
      rc = delta_plan_cost(d, &body->cost, e);
    if (rc == LC_OK && body->cost.metadata_bytes > PS_JOURNAL - 64)
      rc = failure(e, LC_ERR_INVALID,
                   "body replacement exceeds one revision descriptor window");
    d->metadata_only = d->body_replacement = 1;
    d->metadata_count = 1;
  }
  if (rc == LC_OK) {
    *out = body;
    *cost = body->cost;
  } else
    ps_key_body_close(body);
  return rc;
}
int ps_key_body_fund(ps_checkpoint_body *body, ps_budget *parent, lc_error *e) {
  int rc;
  if (!body || body->funded || !parent)
    return failure(e, LC_ERR_INVALID, "body replacement plan is not open");
  rc = ps_checkpoint_delta_fund(body->delta, parent, &body->cost, e);
  if (rc == LC_OK)
    body->funded = 1;
  return rc;
}
const lc_pouch_state_entry *ps_key_body_source(const ps_checkpoint_body *body) {
  return body ? &body->source : NULL;
}
int ps_key_body_commit(ps_checkpoint_body *body,
                       const lc_pouch_state_entry *replacement, lc_error *e) {
  ps_checkpoint_record identity = {0}, converted = {0};
  lc_pouch_state_entry entry;
  lc_pouch_storage_batch batch;
  body_replacement_prefix input;
  ps_key_checkpoint *c;
  const lc_pouch_state_entry *expected;
  int rc;
  if (!body || !body->funded || !replacement)
    return failure(e, LC_ERR_INVALID, "body replacement plan is not funded");
  c = body->checkpoint;
  expected = &body->source;
  if (!replacement->found || replacement->control ||
      !replacement->payload_span.present ||
      replacement->payload_span.payload_length != replacement->cipher_bytes ||
      replacement->record_type !=
          (expected->record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT ||
                   expected->object_link
               ? LC_POUCH_STATE_RECORD_OBJECT_PUT
               : LC_POUCH_STATE_RECORD_STATE_PUT))
    return failure(e, LC_ERR_INVALID,
                   "physical replacement requires a resolved converted body");
  /* Normalize only representation fields: all logical facts, including a
   * linked object's class, must encode identically to the expected key. */
  entry = *replacement;
  entry.payload_span = expected->payload_span;
  entry.cipher_bytes = expected->cipher_bytes;
  entry.descriptor = expected->descriptor;
  entry.record_type = expected->record_type;
  entry.object_link = expected->object_link;
  rc = ps_checkpoint_record_encode(&c->allocator, &entry, &identity, e);
  if (rc == LC_OK &&
      (identity.length != body->original.length ||
       memcmp(identity.bytes, body->original.bytes, body->original.length)))
    rc = failure(e, LC_ERR_INVALID,
                 "physical replacement changed logical record facts");
  if (rc == LC_OK)
    rc = replacement_expected(c, expected, &body->original, e);
  entry = *expected;
  entry.payload_span = replacement->payload_span;
  entry.cipher_bytes = replacement->cipher_bytes;
  entry.descriptor = replacement->descriptor;
  if (rc == LC_OK)
    rc = ps_checkpoint_record_encode(&c->allocator, &entry, &converted, e);
  input.record = &converted;
  memset(&batch, 0, sizeof(batch));
  batch.context = &input;
  batch.count = 1;
  batch.prefix = replacement_prefix;
  if (rc == LC_OK && converted.length > body->maximum)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "body replacement metadata exceeds its admitted bound");
  if (rc == LC_OK) {
    body->delta->metadata_bytes = converted.length;
    rc = ps_key_metadata_commit(body->delta, &batch, PS_COMMIT_DEFAULT, e);
  }
  ps_checkpoint_record_close(&converted);
  ps_checkpoint_record_close(&identity);
  return rc;
}
int ps_key_checkpoint_replace_body(ps_key_checkpoint *c,
                                   const lc_pouch_state_entry *expected,
                                   const lc_pouch_state_entry *replacement,
                                   lc_error *e) {
  ps_checkpoint_body *body = NULL;
  ps_checkpoint_record converted = {0};
  ps_budget_plan cost;
  int rc;
  if (!c || !expected || !replacement)
    return failure(e, LC_ERR_INVALID,
                   "physical replacement requires source and result");
  rc = ps_checkpoint_record_encode(&c->allocator, replacement, &converted, e);
  if (rc == LC_OK)
    rc = ps_key_body_plan(c, expected, converted.length, &body, &cost, e);
  if (rc == LC_OK)
    rc = fund_completion_claim(c, &cost, e);
  if (rc == LC_OK) {
    body->funded = 1;
    rc = ps_key_body_commit(body, replacement, e);
  }
  ps_key_body_close(body);
  ps_checkpoint_record_close(&converted);
  return rc;
}
struct ps_key_cursor {
  ps_key_checkpoint *checkpoint;
  key_slot *next;
  uint64_t epoch;
};
static int cursor_valid(ps_key_cursor *cursor, lc_error *e) {
  ps_key_checkpoint *c = cursor->checkpoint;
  return c->observer.prepare || c->dirty || c->changed ||
                 c->logical_epoch == LC_U64_MAX ||
                 c->logical_epoch != cursor->epoch
             ? failure(e, LC_ERR_INVALID,
                       "offline key cursor invalidated by logical mutation")
             : LC_OK;
}
int ps_key_cursor_check(ps_key_cursor *cursor, lc_error *e) {
  int rc;
  if (!cursor)
    return failure(e, LC_ERR_INVALID, "key cursor is not open");
  pthread_mutex_lock(&cursor->checkpoint->mutex);
  rc = cursor_valid(cursor, e);
  pthread_mutex_unlock(&cursor->checkpoint->mutex);
  return rc;
}
int ps_key_cursor_open(ps_key_checkpoint *c, ps_key_cursor **out, lc_error *e) {
  ps_key_cursor *cursor;
  int rc;
  if (out)
    *out = NULL;
  if (!c || !out)
    return failure(e, LC_ERR_INVALID, "key cursor requires owner and output");
  cursor = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*cursor));
  if (!cursor)
    return failure(e, LC_ERR_NOMEM, "cannot allocate key cursor");
  cursor->checkpoint = c;
  pthread_mutex_lock(&c->mutex);
  cursor->epoch = c->logical_epoch;
  cursor->next = c->slots;
  rc = cursor_valid(cursor, e);
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK) {
    ps_namespace_retain(c->ns);
    *out = cursor;
  } else
    lc_free_with_allocator(&c->allocator, cursor);
  return rc;
}
int ps_key_cursor_next(ps_key_cursor *cursor, lc_pouch_state_entry *out,
                       int *found, lc_error *e) {
  ps_key_checkpoint *c;
  ps_reader *reader = NULL;
  key_slot *slot;
  lc_pouch_state_entry entry = {0};
  char leaf[LC_POUCH_SNAPSHOT_LEAF_BYTES];
  ps_key key;
  int rc;
  if (found)
    *found = 0;
  if (!cursor || !out || !found)
    return failure(e, LC_ERR_INVALID, "key cursor requires record and output");
  c = cursor->checkpoint;
  pthread_mutex_lock(&c->mutex);
  rc = cursor_valid(cursor, e);
  slot = cursor->next;
  if (rc == LC_OK && slot) {
    memcpy(leaf, slot->leaf, sizeof(leaf));
    key.owner = c->ns->identity.owner;
    key.key = slot->id;
    key.kind = slot->kind;
    rc = ps_read_open(c->ns->root->space, key, NULL, &reader, e);
  }
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK && slot)
    rc = ps_checkpoint_record_read(reader, &c->allocator, leaf, &entry, e);
  ps_reader_close(reader);
  pthread_mutex_lock(&c->mutex);
  if (rc == LC_OK)
    rc = cursor_valid(cursor, e);
  if (rc == LC_OK && slot) {
    lc_pouch_state_entry_cleanup(&c->allocator, out);
    *out = entry;
    memset(&entry, 0, sizeof(entry));
    cursor->next = slot->next;
    *found = 1;
  }
  pthread_mutex_unlock(&c->mutex);
  lc_pouch_state_entry_cleanup(&c->allocator, &entry);
  return rc;
}
void ps_key_cursor_close(ps_key_cursor *cursor) {
  ps_namespace *ns;
  if (!cursor)
    return;
  ns = cursor->checkpoint->ns;
  lc_free_with_allocator(&cursor->checkpoint->allocator, cursor);
  ps_namespace_release(ns);
}
static int checkpoint_flush(ps_key_checkpoint *c, int force_durable,
                            lc_error *e) {
  ps_writer *writers[CHECKPOINT_BATCH + 1];
  key_slot *slots[CHECKPOINT_BATCH], *s;
  uint64_t incarnations[CHECKPOINT_BATCH];
  ps_checkpoint_ref refs[CHECKPOINT_BATCH];
  ps_checkpoint_observer observer;
  ps_budget_plan cost;
  void *plan;
  uint64_t descriptors, epoch = 0, control_bytes;
  ps_ref control_ref, slot_ref;
  uint64_t prefix;
  size_t count, ordinary, i, outputs;
  int rc = LC_OK, final;
  if (!c)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint flush requires namespace owner");
  pthread_mutex_lock(&c->mutex);
  rc = settle_retired(c, e);
  if (rc != LC_OK) {
    pthread_mutex_unlock(&c->mutex);
    return rc;
  }
  if (!c->changed) {
    pthread_mutex_unlock(&c->mutex);
    return LC_OK;
  }
  observer = c->observer;
  rc = fund(c, &c->claim, e);
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK)
    rc = ps_catalog_allocate_id(c->ns->root->space, &epoch, e);
  while (rc == LC_OK && c->changed) {
    memset(writers, 0, sizeof(writers));
    count = ordinary = outputs = 0;
    descriptors = 88; /* Reserve final coverage even in a partial batch. */
    plan = NULL;
    for (s = c->dirty; s && count < CHECKPOINT_BATCH; s = s->dirty_next) {
      memset(&cost, 0, sizeof(cost));
      rc = ps_budget_plan_key_slot(&cost, s->kind, s->pending.length, 0, e);
      if (rc != LC_OK || cost.metadata_bytes > PS_JOURNAL - 64 - descriptors)
        break;
      descriptors += cost.metadata_bytes;
      slots[count++] = s;
      if (s->kind == PS_KEY_METADATA) {
        refs[ordinary].key = s->key;
        refs[ordinary].leaf = s->leaf;
        refs[ordinary].index = s->summary.index;
        refs[ordinary].lifetime = s->summary.lifetime;
        ++ordinary;
      }
    }
    final = s == NULL;
    if (rc == LC_OK && !count && !final)
      rc = failure(e, LC_ERR_INVALID, "checkpoint slot cannot fit a revision");
    for (i = 0; rc == LC_OK && i < count; ++i) {
      ps_key key;
      key.owner = c->ns->identity.owner;
      key.key = slots[i]->id;
      key.kind = slots[i]->kind;
      rc = ps_checkpoint_record_write(c->ns->root->space, c->budget, key,
                                      &slots[i]->pending, &writers[outputs], e);
      if (rc == LC_OK) {
        rc = ps_revision_writer_info(writers[outputs], &slot_ref,
                                     &control_bytes, e);
        ++outputs;
        if (rc == LC_OK)
          incarnations[i] = slot_ref.incarnation;
      }
    }
    if (rc == LC_OK && final) {
      rc = write_control(c, epoch, &writers[outputs], e);
      if (rc == LC_OK)
        rc = ps_revision_writer_info(writers[outputs], &control_ref,
                                     &control_bytes, e);
      /* Include failed private control writer in cleanup too. */
      if (writers[outputs])
        ++outputs;
    }
    if (rc == LC_OK && observer.prepare && ordinary)
      rc = observer.prepare(observer.context, c->ns->name, refs, ordinary,
                            &plan, e);
    if (rc == LC_OK)
      rc = ps_revision_publish(
          writers, outputs, force_durable ? PS_REVISION_FORCE_DURABLE : 0,
          force_durable ? PS_COMMIT_DEFAULT : PS_COMMIT_DEFER_SYNC, NULL,
          &prefix, e);
    if (observer.finish && plan)
      observer.finish(observer.context, plan, rc == LC_OK);
    for (i = 0; i < outputs; ++i)
      ps_writer_close(writers[i]);
    pthread_mutex_lock(&c->mutex);
    if (rc == LC_OK) {
      for (i = 0; i < count; ++i) {
        s = slots[i];
        select_incarnation(c, s, incarnations[i], s->summary.deletion_bytes);
        memset(&cost, 0, sizeof(cost));
        (void)ps_budget_plan_key_slot(&cost, s->kind, s->bound, 1, NULL);
        subtract_plan(&c->claim, &cost);
        c->dirty_bytes -= s->pending.length;
        dirty_remove(c, s);
        ps_checkpoint_record_close(&s->pending);
        s->bound = 0;
        /* Retain the old obligation until the entire replacement prefix is
         * durable, including when this flush is folded into a caller's scope.
         */
        retire_note(c, s, prefix);
      }
      if (final) {
        c->selected.epoch = epoch;
        c->selected.segment = c->latest_segment;
        c->selected.offset = c->latest_offset;
        c->selected.state_index = c->state_index;
        c->selected.query_index = c->query_index;
        c->selected.control = control_ref.incarnation;
        memset(&c->claim, 0, sizeof(c->claim));
        c->changed = 0;
        c->uncovered_bytes = 0;
      }
      /* Actual replacement descriptor growth may spend less than planned.
       * Return that excess before continuing another bounded batch. */
      if (c->changed)
        rc = fund(c, &c->claim, e);
    }
    if (rc == LC_OK)
      (void)settle_retired(c, NULL);
    settle(c);
    pthread_mutex_unlock(&c->mutex);
  }
  return rc;
}
int ps_key_checkpoint_flush(ps_key_checkpoint *c, lc_error *e) {
  return checkpoint_flush(c, 1, e);
}
int ps_key_checkpoint_flush_deferred(ps_key_checkpoint *c, lc_error *e) {
  return checkpoint_flush(c, 0, e);
}
