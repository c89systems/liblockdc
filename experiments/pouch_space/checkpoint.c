#include "checkpoint.h"
#include "budget.h"
#include "catalog.h"
#include "frame.h"
#include "lc_api_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_storage.h"
#include "revision.h"
#include "root_internal.h"
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
  key_slot *ranges;
  source_count *next;
};
struct key_slot {
  char *key, *leaf;
  uint64_t id, bound;
  unsigned int kind;
  ps_reserve *reserve;
  uint64_t required_deletion;
  key_summary summary;
  ps_checkpoint_record pending;
  source_count *body;
  key_slot *next, *hash_next, *id_next;
  key_slot *dirty_next, **dirty_link;
  key_slot *left, *right;
  uint64_t range_end;
  unsigned int height;
};
struct slot_change {
  key_slot *slot;
  uint64_t bound;
  uint64_t prior_deletion;
  int created, prepared;
  key_summary summary;
  source_count *body;
  ps_checkpoint_record record;
  slot_change *next, *hash_next;
};
struct ps_key_checkpoint {
  ps_namespace *ns;
  lc_allocator allocator;
  pthread_mutex_t mutex;
  key_slot **keys, **ids, *slots, *dirty;
  source_count **sources;
  size_t capacity, count, source_capacity, source_count;
  uint64_t dirty_count, dirty_bytes, uncovered_bytes;
  uint64_t latest_segment, latest_offset, state_index, query_index;
  uint64_t body_epoch;
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
};

static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0, message, NULL, NULL, "pouch");
}
/* One intrusive AVL interval index per logical source. Metadata-only lease
 * updates leave it alone; selecting a piece visits only overlapping bodies.
 * Nodes are already funded key slots, so canonical accept cannot allocate. */
static unsigned int range_height(key_slot *s) { return s ? s->height : 0; }
static void range_update(key_slot *s) {
  unsigned int a = range_height(s->left), b = range_height(s->right);
  s->height = 1 + (a > b ? a : b);
  s->range_end = s->summary.body_end;
  if (s->left && s->left->range_end > s->range_end)
    s->range_end = s->left->range_end;
  if (s->right && s->right->range_end > s->range_end)
    s->range_end = s->right->range_end;
}
static key_slot *range_left(key_slot *s) {
  key_slot *r = s->right;
  s->right = r->left;
  r->left = s;
  range_update(s);
  range_update(r);
  return r;
}
static key_slot *range_right(key_slot *s) {
  key_slot *l = s->left;
  s->left = l->right;
  l->right = s;
  range_update(s);
  range_update(l);
  return l;
}
static key_slot *range_balance(key_slot *s) {
  if (!s)
    return NULL;
  range_update(s);
  if (range_height(s->left) > range_height(s->right) + 1) {
    if (range_height(s->left->right) > range_height(s->left->left))
      s->left = range_left(s->left);
    return range_right(s);
  }
  if (range_height(s->right) > range_height(s->left) + 1) {
    if (range_height(s->right->left) > range_height(s->right->right))
      s->right = range_right(s->right);
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
    s->left = s->right = NULL;
    range_update(s);
    return s;
  }
  if (range_before(s, root))
    root->left = range_insert(root->left, s);
  else
    root->right = range_insert(root->right, s);
  return range_balance(root);
}
static key_slot *range_detach_first(key_slot *root, key_slot **first) {
  if (!root->left) {
    *first = root;
    return root->right;
  }
  root->left = range_detach_first(root->left, first);
  return range_balance(root);
}
static key_slot *range_remove(key_slot *root, key_slot *s) {
  key_slot *next;
  if (root == s) {
    if (!root->right)
      return root->left;
    next = NULL;
    root->right = range_detach_first(root->right, &next);
    next->left = root->left;
    next->right = root->right;
    return range_balance(next);
  }
  if (range_before(s, root))
    root->left = range_remove(root->left, s);
  else
    root->right = range_remove(root->right, s);
  return range_balance(root);
}
static uint64_t hash_key(const void *key, size_t n, unsigned int kind) {
  const unsigned char *p = key;
  uint64_t h = UINT64_C(1469598103934665603) ^ kind;
  while (n--) {
    h ^= *p++;
    h *= UINT64_C(1099511628211);
  }
  return h;
}
static key_slot *find_key(ps_key_checkpoint *c, const char *key,
                          unsigned int kind) {
  key_slot *s;
  size_t bucket =
      (size_t)(hash_key(key, strlen(key), kind) & (c->capacity - 1));
  for (s = c->keys[bucket]; s; s = s->hash_next)
    if (s->kind == kind && !strcmp(s->key, key))
      return s;
  return NULL;
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
    bucket =
        (size_t)(hash_key(s->key, strlen(s->key), s->kind) & (capacity - 1));
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
  size_t bucket =
      (size_t)(hash_key(s->key, strlen(s->key), s->kind) & (c->capacity - 1));
  s->hash_next = c->keys[bucket];
  c->keys[bucket] = s;
  bucket = (size_t)(s->id & (c->capacity - 1));
  s->id_next = c->ids[bucket];
  c->ids[bucket] = s;
  s->next = c->slots;
  c->slots = s;
  ++c->count;
}
static void close_slot(ps_key_checkpoint *c, key_slot *s) {
  ps_reserve_close(s->reserve);
  ps_checkpoint_record_close(&s->pending);
  lc_free_with_allocator(&c->allocator, s->key);
  lc_free_with_allocator(&c->allocator, s->leaf);
  lc_free_with_allocator(&c->allocator, s);
}
static key_slot *new_slot(ps_key_checkpoint *c, const void *key, size_t bytes,
                          unsigned int kind, uint64_t id, lc_error *e) {
  key_slot *s = lc_calloc_with_allocator(&c->allocator, 1, sizeof(*s));
  if (s) {
    s->key = lc_alloc_with_allocator(&c->allocator, bytes + 1);
    s->leaf = lc_pouch_namespace_snapshot_leaf(&c->allocator, id);
  }
  if (!s || !s->key || !s->leaf) {
    if (s)
      close_slot(c, s);
    failure(e, LC_ERR_NOMEM, "cannot allocate checkpoint key slot");
    return NULL;
  }
  memcpy(s->key, key, bytes);
  s->key[bytes] = 0;
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
  if (!bytes) {
    ps_reserve_close(s->reserve);
    s->reserve = NULL;
    s->required_deletion = 0;
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
  if (r->payload_span.payload_offset > (uint64_t)INT64_MAX ||
      r->payload_span.payload_length >
          (uint64_t)INT64_MAX - r->payload_span.payload_offset) {
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
    if (s->summary.body_end > s->summary.body_start)
      prior->ranges = range_remove(prior->ranges, s);
    --prior->references;
    prior->epoch = c->body_epoch;
  }
  s->summary = *summary;
  s->body = source;
  if (source) {
    ++source->references;
    source->epoch = c->body_epoch;
    if (s->summary.body_end > s->summary.body_start)
      source->ranges = range_insert(source->ranges, s);
  }
  release_source(c, prior);
}
static void release_prepared_source(ps_key_checkpoint *c,
                                    source_count *source) {
  if (source)
    --source->prepared;
  release_source(c, source);
}
static void put64(unsigned char *p, uint64_t value) {
  unsigned int i;
  for (i = 0; i < 8; ++i)
    p[i] = (unsigned char)(value >> (i * 8));
}
static uint64_t get64(const unsigned char *p) {
  uint64_t value = 0;
  unsigned int i;
  for (i = 0; i < 8; ++i)
    value |= (uint64_t)p[i] << (i * 8);
  return value;
}
static ps_key control_key(ps_key_checkpoint *c) {
  ps_key key = {0, 1, PS_NAMESPACE_CHECKPOINT};
  key.owner = c->ns->identity.owner;
  return key;
}
static int read_control(ps_key_checkpoint *c, lc_error *e) {
  ps_reader *reader = NULL;
  unsigned char bytes[CHECKPOINT_CONTROL_BYTES];
  size_t got = 0, i;
  int rc = ps_read_open(c->ns->root->space, control_key(c), NULL, &reader, e);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(e);
    return LC_OK;
  }
  if (rc == LC_OK && ps_read_size(reader) != sizeof(bytes))
    rc = failure(e, LC_ERR_PROTOCOL, "checkpoint coverage has an invalid size");
  if (rc == LC_OK)
    rc = ps_read(reader, bytes, sizeof(bytes), &got, e);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (got != sizeof(bytes) || memcmp(bytes, "LCPSCKPT", 8) ||
      get64(bytes + 8) != c->ns->identity.owner || !get64(bytes + 16) ||
      !get64(bytes + 24) ||
      get64(bytes + 120) != (uint64_t)crc32(0, bytes, 120))
    return failure(e, LC_ERR_PROTOCOL,
                   "checkpoint coverage identity is invalid");
  for (i = 56; i < 120; ++i)
    if (bytes[i])
      return failure(e, LC_ERR_PROTOCOL,
                     "checkpoint coverage has unknown fields");
  c->selected.epoch = get64(bytes + 16);
  c->selected.segment = get64(bytes + 24);
  c->selected.offset = get64(bytes + 32);
  c->selected.state_index = get64(bytes + 40);
  c->selected.query_index = get64(bytes + 48);
  if (c->selected.query_index > c->selected.state_index)
    return failure(e, LC_ERR_PROTOCOL,
                   "checkpoint query boundary exceeds state");
  c->state_index = c->selected.state_index;
  c->query_index = c->selected.query_index;
  c->latest_segment = c->selected.segment;
  c->latest_offset = c->selected.offset;
  return LC_OK;
}
static int load_slots(ps_key_checkpoint *c, lc_error *e) {
  ps_values *values = NULL;
  ps_reader *reader = NULL;
  lc_pouch_state_entry entry, projected;
  ps_ref ref;
  ps_key key;
  uint64_t bytes;
  size_t i;
  int rc;
  memset(&entry, 0, sizeof(entry));
  rc = ps_values_capture(c->ns->root->space, c->ns->identity.owner,
                         PS_KEY_METADATA, PS_KEY_DECISION, &values, e);
  for (i = 0; rc == LC_OK && i < ps_values_count(values); ++i) {
    key_slot *slot = NULL;
    rc = ps_values_info(values, i, &key, &ref, &bytes, e);
    if (rc == LC_OK)
      rc = room(c, c->count + 1, e);
    if (rc == LC_OK) {
      slot = new_slot(c, "", 0, key.kind, key.key, e);
      if (!slot)
        rc = LC_ERR_NOMEM;
    }
    if (rc == LC_OK)
      rc = ps_values_read_open(values, i, &reader, e);
    if (rc == LC_OK)
      rc = ps_checkpoint_record_read(reader, &c->allocator, slot->leaf, &entry,
                                     e);
    ps_reader_close(reader);
    reader = NULL;
    if (rc == LC_OK &&
        ((!entry.key || !entry.seen) ||
         (key.kind == PS_KEY_DECISION ? !entry.control || !entry.decision
                                      : entry.control || entry.decision)))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "checkpoint slot has the wrong canonical domain");
    if (rc == LC_OK &&
        (find_key(c, entry.key, key.kind) || find_id(c, key.key)))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "checkpoint repeats a canonical key slot");
    if (rc == LC_OK) {
      projected = entry;
      /* Snapshot body links carry their object class in the canonical flag.
       * Summaries must use the same normalized type as the live projection;
       * later META records intentionally rely on that prior classification. */
      if (!entry.control &&
          !lc_pouch_state_entry_project(NULL, &entry, &projected))
        rc = failure(e, LC_ERR_PROTOCOL,
                     "checkpoint has no canonical key projection");
    }
    if (rc == LC_OK) {
      lc_free_with_allocator(&c->allocator, slot->key);
      slot->key = entry.key;
      entry.key = NULL;
      summarize(&slot->summary, &projected);
      slot->body = source_get(c, &entry, &rc, e);
      if (rc == LC_OK)
        rc = reserve_slot(c, slot, slot->summary.deletion_bytes, 1, e);
    }
    if (rc == LC_OK) {
      if (slot->body) {
        ++slot->body->references;
        slot->body->epoch = ++c->body_epoch;
        if (slot->summary.body_end > slot->summary.body_start)
          slot->body->ranges = range_insert(slot->body->ranges, slot);
      }
      insert_slot(c, slot);
      if (entry.index_seq > c->state_index)
        c->state_index = entry.index_seq;
      if (projected.query_index_seq > c->query_index)
        c->query_index = projected.query_index_seq;
    } else if (slot)
      close_slot(c, slot);
    lc_pouch_state_entry_cleanup(&c->allocator, &entry);
  }
  ps_values_close(values);
  return rc;
}
int ps_key_checkpoint_open(ps_namespace *ns, ps_key_checkpoint **out,
                           lc_error *e) {
  ps_key_checkpoint *c;
  int rc;
  if (out)
    *out = NULL;
  if (!ns || !out)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint owner requires namespace and output");
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
    rc = read_control(c, e);
  if (rc == LC_OK)
    rc = load_slots(c, e);
  if (rc == LC_OK)
    *out = c;
  else
    ps_key_checkpoint_close(c);
  return rc;
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
  if (!s || s->range_end <= base)
    return LC_OK;
  rc = range_collect(s->left, base, end, out, capacity, e);
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
  return range_collect(s->right, base, end, out, capacity, e);
}
int ps_key_checkpoint_ranges(ps_key_checkpoint *c, unsigned int kind,
                             uint64_t id, uint64_t base, uint64_t bytes,
                             ps_checkpoint_ranges *out, lc_error *e) {
  source_count *s;
  size_t capacity = 0;
  int rc = LC_OK;
  if (!c || !out || !bytes || bytes > PS_CHUNK ||
      base > (uint64_t)INT64_MAX - bytes)
    return failure(e, LC_ERR_INVALID, "body capture requires a bounded piece");
  memset(out, 0, sizeof(*out));
  pthread_mutex_lock(&c->mutex);
  s = source_find(c, kind, id);
  if (s && s->prepared)
    rc = range_busy(e);
  if (rc == LC_OK && s) {
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
  if (extra->payload_bytes > UINT64_MAX - p->payload_bytes ||
      extra->metadata_bytes > UINT64_MAX - p->metadata_bytes ||
      extra->entries > UINT64_MAX - p->entries)
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
      rc = ps_budget_plan_value_bound(&before, change->bound, e);
      if (rc != LC_OK)
        return rc;
    }
    rc = ps_budget_plan_value_bound(&after, bound, e);
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
      rc = reserve_slot(c, change->slot, deletion, 0, e);
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
  d->checkpoint = c;
  d->maintenance = maintenance;
  d->claim = c->claim;
  *out = d;
  return LC_OK;
}
int ps_checkpoint_delta_plan(ps_checkpoint_delta *d,
                             const lc_pouch_storage_batch *batch,
                             uint64_t minimum, ps_budget_plan *extra,
                             lc_error *e) {
  ps_key_checkpoint *c = d->checkpoint;
  ps_budget_plan empty = {0}, current;
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
  pthread_mutex_lock(&c->mutex);
  empty.maintenance = 1;
  if (rc == LC_OK && !c->budget)
    rc = ps_budget_open(c->ns->root->space, &empty, &c->budget, e);
  if (rc == LC_OK)
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
int ps_checkpoint_delta_fund(ps_checkpoint_delta *d, ps_budget *source,
                             const ps_budget_plan *extra, lc_error *e) {
  if (!extra->payload_bytes && !extra->metadata_bytes && !extra->entries)
    return LC_OK;
  return ps_budget_transfer(source, d->checkpoint->budget, extra, e);
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
  if (kind == PS_KEY_DECISION) {
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
      rc = reserve_slot(c, change->slot, deletion, !d->writer, e);
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
    if (d->count > UINT64_MAX - c->body_epoch)
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
void ps_checkpoint_delta_accept(ps_checkpoint_delta *d) {
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
      (void)ps_budget_plan_value_bound(&cost, s->bound, NULL);
      subtract_plan(&c->claim, &cost);
      c->dirty_bytes -= s->pending.length;
    }
    ps_checkpoint_record_close(&s->pending);
    s->pending = change->record;
    memset(&change->record, 0, sizeof(change->record));
    s->bound = change->bound;
    bind_source(c, s, change->body, &change->summary);
    release_prepared_source(c, change->body);
    change->body = NULL;
    dirty_add(c, s);
    c->dirty_bytes += s->pending.length;
    memset(&cost, 0, sizeof(cost));
    (void)ps_budget_plan_value_bound(&cost, s->bound, NULL);
    (void)add_plan(&c->claim, &cost, NULL);
  }
  if (d->state_index > c->state_index)
    c->state_index = d->state_index;
  if (d->query_index > c->query_index)
    c->query_index = d->query_index;
  c->latest_segment = d->segment;
  c->latest_offset = d->end;
  if (d->end - d->start > UINT64_MAX - c->uncovered_bytes)
    c->uncovered_bytes = UINT64_MAX;
  else
    c->uncovered_bytes += d->end - d->start;
  c->changed = 1;
  d->accepted = 1;
  pthread_mutex_unlock(&c->mutex);
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
    if ((!d->accepted || !change->prepared) && !change->created)
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
static int replay_superseded(ps_key_checkpoint *c,
                             const lc_pouch_state_entry *entry) {
  key_slot *slot;
  lc_pouch_state_entry prior, projected;
  unsigned int kind = entry->control ? PS_KEY_DECISION : PS_KEY_METADATA;
  if (!entry->key || (entry->control && !entry->decision))
    return 0;
  slot = find_key(c, entry->key, kind);
  if (!slot)
    return 0;
  summary_view(slot, &slot->summary, &prior);
  return kind == PS_KEY_DECISION
             ? !lc_pouch_state_decision_entry_accepts(prior.seen,
                                                      prior.index_seq, entry)
             : !lc_pouch_state_entry_project(&prior, entry, &projected);
}
int ps_key_checkpoint_replay(ps_key_checkpoint *c,
                             const lc_pouch_state_entry *entry, lc_error *e) {
  ps_checkpoint_delta d;
  uint64_t bytes = 0;
  int rc = LC_OK;
  if (!c || !entry || !entry->seen)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint replay requires a complete record");
  memset(&d, 0, sizeof(d));
  d.checkpoint = c;
  d.claim = c->claim;
  pthread_mutex_lock(&c->mutex);
  /* Forward slots from an interrupted checkpoint may already supersede this
   * canonical prefix. They need only the coverage control, not another value
   * reservation. Refunding a temporary large claim after projection is too
   * late when the root has exhausted ordinary capacity. */
  if (!replay_superseded(c, entry))
    rc = lc_pouch_state_record_measure_snapshot(&c->allocator, NULL, entry, 0,
                                                &bytes, e);
  if (rc == LC_OK)
    rc =
        admit_key(&d, entry->control && !entry->decision ? NULL : entry->key,
                  entry->key ? strlen(entry->key) : 0,
                  entry->control ? PS_KEY_DECISION : PS_KEY_METADATA, bytes, e);
  if (rc == LC_OK)
    rc = prepare_entry(&d, entry, e);
  if (rc == LC_OK && d.count > UINT64_MAX - c->body_epoch)
    rc = failure(e, LC_ERR_INVALID, "checkpoint body epoch exhausted");
  pthread_mutex_unlock(&c->mutex);
  if (rc == LC_OK) {
    d.prepared = 1;
    d.segment = c->latest_segment;
    d.start = d.end = c->latest_offset;
    ps_checkpoint_delta_accept(&d);
  }
  /* Stack delta uses the identical ownership cleanup without freeing itself. */
  pthread_mutex_lock(&c->mutex);
  {
    slot_change *change, *next;
    for (change = d.changes; change; change = next) {
      next = change->next;
      release_prepared_source(c, change->body);
      ps_checkpoint_record_close(&change->record);
      if (change->created)
        close_slot(c, change->slot);
      lc_free_with_allocator(&c->allocator, change);
    }
    lc_free_with_allocator(&c->allocator, d.buckets);
  }
  settle(c);
  pthread_mutex_unlock(&c->mutex);
  return rc;
}
void ps_key_checkpoint_replayed(ps_key_checkpoint *c, uint64_t segment,
                                uint64_t offset) {
  uint64_t bytes;
  pthread_mutex_lock(&c->mutex);
  bytes = segment == c->latest_segment && offset >= c->latest_offset
              ? offset - c->latest_offset
              : offset;
  if (bytes > UINT64_MAX - c->uncovered_bytes)
    c->uncovered_bytes = UINT64_MAX;
  else
    c->uncovered_bytes += bytes;
  c->latest_segment = segment;
  c->latest_offset = offset;
  pthread_mutex_unlock(&c->mutex);
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

static int write_control(ps_key_checkpoint *c, uint64_t epoch, ps_writer **out,
                         lc_error *e) {
  unsigned char bytes[CHECKPOINT_CONTROL_BYTES] = {0};
  int rc;
  memcpy(bytes, "LCPSCKPT", 8);
  put64(bytes + 8, c->ns->identity.owner);
  put64(bytes + 16, epoch);
  put64(bytes + 24, c->latest_segment);
  put64(bytes + 32, c->latest_offset);
  put64(bytes + 40, c->state_index);
  put64(bytes + 48, c->query_index);
  put64(bytes + 120, (uint64_t)crc32(0, bytes, 120));
  rc = ps_begin(c->ns->root->space, c->budget, control_key(c), out, e);
  if (rc == LC_OK)
    rc = ps_write(*out, bytes, sizeof(bytes), e);
  return rc;
}
int ps_key_checkpoint_flush(ps_key_checkpoint *c, lc_error *e) {
  ps_writer *writers[CHECKPOINT_BATCH + 1];
  key_slot *slots[CHECKPOINT_BATCH], *s;
  ps_checkpoint_ref refs[CHECKPOINT_BATCH];
  ps_checkpoint_observer observer;
  ps_budget_plan cost;
  void *plan;
  uint64_t descriptors, epoch = 0;
  size_t count, ordinary, i, outputs;
  int rc = LC_OK, final;
  if (!c)
    return failure(e, LC_ERR_INVALID,
                   "checkpoint flush requires namespace owner");
  pthread_mutex_lock(&c->mutex);
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
      rc = ps_budget_plan_value(&cost, s->pending.length, e);
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
      if (rc == LC_OK)
        ++outputs;
    }
    if (rc == LC_OK && final) {
      rc = write_control(c, epoch, &writers[outputs], e);
      /* Include failed private control writer in cleanup too. */
      if (writers[outputs])
        ++outputs;
    }
    if (rc == LC_OK && observer.prepare && ordinary)
      rc = observer.prepare(observer.context, c->ns->name, refs, ordinary,
                            &plan, e);
    if (rc == LC_OK)
      rc = ps_revision_publish(writers, outputs, 1, NULL, e);
    if (observer.finish && plan)
      observer.finish(observer.context, plan, rc == LC_OK);
    for (i = 0; i < outputs; ++i)
      ps_writer_close(writers[i]);
    pthread_mutex_lock(&c->mutex);
    if (rc == LC_OK) {
      for (i = 0; i < count; ++i) {
        s = slots[i];
        memset(&cost, 0, sizeof(cost));
        (void)ps_budget_plan_value_bound(&cost, s->bound, NULL);
        subtract_plan(&c->claim, &cost);
        c->dirty_bytes -= s->pending.length;
        dirty_remove(c, s);
        ps_checkpoint_record_close(&s->pending);
        s->bound = 0;
        /* The selected forward slot is now durable and canonical replay
         * rejects superseded prefixes. Old reader-held storage stays owned;
         * it is never counted as newly available reserve. */
        (void)reserve_slot(c, s, s->summary.deletion_bytes, 0, NULL);
      }
      if (final) {
        c->selected.epoch = epoch;
        c->selected.segment = c->latest_segment;
        c->selected.offset = c->latest_offset;
        c->selected.state_index = c->state_index;
        c->selected.query_index = c->query_index;
        memset(&c->claim, 0, sizeof(c->claim));
        c->changed = 0;
        c->uncovered_bytes = 0;
      }
      /* Actual replacement descriptor growth may spend less than planned.
       * Return that excess before continuing another bounded batch. */
      if (c->changed)
        rc = fund(c, &c->claim, e);
    }
    settle(c);
    pthread_mutex_unlock(&c->mutex);
  }
  return rc;
}
