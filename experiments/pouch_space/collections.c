#define _GNU_SOURCE
#include "collections.h"
#include "collection_reclaim.h"
#include "collection_segment.h"
#include "control.h"
#include "lc_api_internal.h"
#include "lc_collection_catalog_record.h"
#include "lc_collection_internal.h"
#include "lc_collection_query.h"
#include "namespace_work.h"
#include "pouch_internal.h"
#include "query_index.h"
#include "root_internal.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

typedef struct collection_segment {
  lc_u64 id;
  ps_collection_segment *segment;
  ps_owned_head *retiring;
  ps_catalog_info binding;
  int bound;
  struct collection_segment *next;
} collection_segment;
/* Generation/publication/upload pins belong to captured operations or writers,
 * rather than to the thread that admitted them. They may outlive an authority
 * scope or cross a handoff. The mutex protects only pin admission/release. */
typedef struct collection_gate {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  unsigned long readers;
  int writer;
} collection_gate;
typedef struct collection_topology {
  lc_u64 id;
  collection_gate gate;
  struct collection_topology *next;
} collection_topology;
struct ps_collection_owner {
  ps_namespace *ns;
  lc_allocator allocator;
  char *name;
  char id[LC_XID_STRING_SIZE];
  char query_id[LC_XID_STRING_SIZE];
  uint64_t numeric;
  ps_catalog_info binding;
  ps_collection_segment *identity;
  collection_segment *segments;
  collection_topology *topologies;
  pthread_mutex_t append, cache;
  pthread_rwlock_t metadata;
  collection_gate commit, streams, compaction;
  unsigned int initialized;
  unsigned long references;
  int published, linked;
  struct ps_collection_owner *next;
};
typedef struct ps_collection_owner ps_collection_owner;
typedef struct collection_receiver {
  ps_collection_owner *owner;
  lc_collection_handle *receiver;
} collection_receiver;

static int failure(lc_error *e, int code, const char *reason,
                   const char *message) {
  return lc_error_set(e, code, 0L, message, NULL, reason, "pouch");
}
static void clear(lc_error *e) {
  lc_error_cleanup(e);
  lc_error_init(e);
}
static int gate_init(collection_gate *gate, lc_error *e) {
  if (pthread_mutex_init(&gate->mutex, NULL) != 0)
    return failure(e, LC_ERR_TRANSPORT, NULL,
                   "cannot initialize collection pin admission");
  if (pthread_cond_init(&gate->changed, NULL) != 0) {
    pthread_mutex_destroy(&gate->mutex);
    return failure(e, LC_ERR_TRANSPORT, NULL,
                   "cannot initialize collection pin notification");
  }
  return LC_OK;
}
static void gate_destroy(collection_gate *gate) {
  pthread_cond_destroy(&gate->changed);
  pthread_mutex_destroy(&gate->mutex);
}
static ps_space *space(ps_collection_owner *owner) {
  return owner->ns->root->space;
}
static int shared_collection(ps_collection_owner *owner) {
  return ps_coordination_shared(ps_space_coordination(space(owner)));
}
static int catalog_enter(ps_namespace *ns, int wait, void **pin, lc_error *e) {
  ps_coordination *coordination = ps_space_coordination(ns->root->space);
  ps_coordination_guard *guard = NULL;
  int rc;
  *pin = NULL;
  if (ps_coordination_shared(coordination)) {
    rc = ps_coordination_lock(coordination, PS_COORDINATION_COLLECTION_CATALOG,
                              ns->identity.owner, wait, &guard, e);
    *pin = guard;
    return rc;
  }
  rc = wait ? pthread_mutex_lock(&ns->collection_catalog)
            : pthread_mutex_trylock(&ns->collection_catalog);
  if (rc != 0)
    return failure(e, rc == EBUSY ? LC_ERR_PENDING : LC_ERR_TRANSPORT, NULL,
                   "cannot enter collection catalog authority");
  *pin = &ns->collection_catalog;
  return LC_OK;
}
static void catalog_leave(ps_namespace *ns, void *pin) {
  if (pin == &ns->collection_catalog)
    pthread_mutex_unlock(&ns->collection_catalog);
  else
    ps_coordination_unlock(pin);
}
static int catalog_end(ps_namespace *ns, void *pin, int rc, lc_error *e) {
  catalog_leave(ns, pin);
  if (rc == LC_OK && pin && pin != &ns->collection_catalog)
    rc = ps_check(ns->root->space, e);
  return rc;
}
static ps_key key_for(ps_collection_owner *owner, uint64_t id) {
  ps_key key;
  key.owner = owner->numeric;
  key.key = id;
  key.kind = PS_STREAM;
  return key;
}
static int same_ref(const ps_ref *a, const ps_ref *b) {
  return a->incarnation == b->incarnation &&
         !memcmp(a->container, b->container, sizeof(a->container));
}
static unsigned int segment_kind(lc_u64 id) {
  return id & LC_COLLECTION_SNAPSHOT_BIT ? PS_CATALOG_SNAPSHOT
                                         : PS_CATALOG_SEGMENT;
}
static void segment_name(lc_u64 id, unsigned char name[8]) {
  size_t i;
  for (i = 0U; i < 8U; ++i)
    name[i] = (unsigned char)(id >> (8U * i));
}
int ps_collection_canonical_recover(lc_pouch_record_io input,
                                    uint64_t checkpoint, uint64_t capacity,
                                    uint64_t *accepted, void *context,
                                    lc_error *e) {
  const char *leaf = context;
  unsigned char prefix[LC_POUCH_RECORD_HEADER_BYTES], zeros[32] = {0};
  struct iovec part;
  lc_pouch_collection_record record;
  uint64_t at = 0U, body = 0U;
  int has_body = 0, rc;
  part.iov_base = prefix;
  part.iov_len = sizeof(prefix);
  while (capacity - at >= sizeof(prefix)) {
    rc = lc_pouch_record_io_read(input, at, &part, 1, e);
    if (rc != LC_OK)
      return rc;
    if (!memcmp(prefix, zeros, sizeof(prefix))) {
      if (at < checkpoint)
        return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                       "collection checkpoint prefix is incomplete");
      break;
    }
    rc = lc_pouch_collection_record_read(input, at, capacity, leaf, &record, e);
    if ((rc == LC_ERR_PENDING || rc == LC_ERR_PROTOCOL) && at >= checkpoint) {
      /* The owned checkpoint bounds promised framing. As with namespace
       * recovery, a failed/torn append can leave any prefix of its private
       * header in already granted zero capacity. It is not a published
       * suffix. Transport/allocation errors never authorize truncation. */
      clear(e);
      break;
    }
    if (rc != LC_OK)
      return rc;
    if (record.next_offset <= at)
      return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                     "collection recovery did not advance its boundary");
    if (at < checkpoint && record.next_offset > checkpoint)
      return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                     "collection checkpoint splits a canonical record");
    has_body = record.kind == LC_POUCH_COLLECTION_EDIT_BODY;
    if (has_body)
      body = at;
    at = record.next_offset;
  }
  if (at < checkpoint || (has_body && body < checkpoint))
    return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                   "collection checkpoint includes an incomplete publication");
  /* A complete provisional replacement remains private until its receipt. */
  *accepted = has_body ? body : at;
  return LC_OK;
}
static int protect_segment(ps_collection_owner *owner, collection_segment *s,
                           lc_error *e) {
  ps_catalog_view *view = NULL;
  ps_catalog_info current;
  ps_owned_status status;
  unsigned char name[8];
  int rc;
  segment_name(s->id, name);
  rc = ps_collection_segment_status(s->segment, &status, NULL, e);
  if (rc == LC_OK)
    rc = ps_catalog_get(space(owner), owner->numeric, segment_kind(s->id), name,
                        sizeof(name), &view, e);
  if (rc == LC_OK && view) {
    rc = ps_catalog_view_info(view, &current, e);
    if (rc == LC_OK && (!current.stream.owner || !current.stream.key ||
                        current.stream.kind != PS_STREAM ||
                        !same_ref(&current.target, &status.ref)))
      rc = failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                   "collection segment binding has a different incarnation");
    if (rc == LC_OK)
      s->binding = current;
  } else if (rc == LC_OK)
    rc = ps_catalog_put(space(owner), NULL, owner->numeric, segment_kind(s->id),
                        name, sizeof(name), NULL, key_for(owner, s->id),
                        &status.ref, &s->binding, e);
  ps_catalog_view_close(view);
  if (rc == LC_OK)
    s->bound = 1;
  return rc;
}
static int segment_binding(ps_collection_owner *owner, lc_u64 id, int create,
                           ps_catalog_view **view, ps_catalog_info *info,
                           lc_error *e) {
  unsigned char name[8];
  int rc;
  segment_name(id, name);
  rc = ps_catalog_get(space(owner), owner->numeric, segment_kind(id), name,
                      sizeof(name), view, e);
  if (rc == LC_OK && *view) {
    rc = ps_catalog_view_info(*view, info, e);
    if (rc == LC_OK && (!info->stream.owner || !info->stream.key ||
                        info->stream.kind != PS_STREAM))
      rc = failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                   "collection segment catalog points outside its owner");
  } else if (rc == LC_OK && !create)
    rc = failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                 "collection selected segment has no durable binding");
  return rc;
}
static int open_segment(ps_collection_owner *owner, lc_u64 id, int create,
                        int recover, collection_segment *s, lc_error *e) {
  ps_catalog_view *prior = NULL;
  ps_catalog_info identity;
  ps_owned_status status;
  char leaf[64];
  int rc = lc_collection_record_leaf(id, leaf, e);
  s->id = id;
  if (rc == LC_OK)
    rc = segment_binding(owner, id, create, &prior, &identity, e);
  if (rc == LC_OK) {
    /* A prior failure can leave an unbound, empty successor. An exact binding
     * is required before a selected segment's suffix may be repaired. */
    rc = ps_collection_segment_open(
        space(owner), prior ? identity.stream : key_for(owner, id), 0,
        prior ? &identity.target : NULL,
        prior && recover ? ps_collection_canonical_recover : NULL, leaf,
        &s->segment, e);
    if (!prior && create && rc == LC_ERR_INVALID) {
      clear(e);
      rc = ps_collection_segment_open(space(owner), key_for(owner, id), 1, NULL,
                                      NULL, NULL, &s->segment, e);
    }
    if (rc == LC_OK && create) {
      rc = ps_collection_segment_status(s->segment, &status, NULL, e);
      if (rc == LC_OK && (status.tail || status.sealed))
        rc = failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                     "unpublished collection successor is not empty");
      if (rc == LC_OK && status.capacity)
        rc = ps_collection_segment_truncate(s->segment, 0U, e);
    }
    if (rc == LC_OK && prior) {
      s->binding = identity;
      s->bound = 1;
    }
  }
  ps_catalog_view_close(prior);
  if (rc == LC_OK && !s->bound)
    rc = protect_segment(owner, s, e);
  if (rc != LC_OK) {
    ps_collection_segment_close(s->segment);
    s->segment = NULL;
  }
  return rc;
}
/* Shared data handles follow the APPEND, upload, commit, or capture lifetime.
 * The owner registry may retain only scalar metadata and retirement retries. */
static int get_segment(ps_collection_owner *owner, lc_u64 id, int create,
                       ps_collection_segment **out, lc_error *e) {
  collection_segment *s;
  int rc = LC_OK;
  *out = NULL;
  if (shared_collection(owner)) {
    collection_segment opened = {0};
    rc = open_segment(owner, id, create, 0, &opened, e);
    *out = opened.segment;
    return rc;
  }
  pthread_mutex_lock(&owner->cache);
  for (s = owner->segments; s && s->id != id; s = s->next) {
  }
  if (!s) {
    s = lc_calloc_with_allocator(&owner->allocator, 1U, sizeof(*s));
    if (!s)
      rc = failure(e, LC_ERR_NOMEM, NULL,
                   "cannot allocate collection segment admission");
    if (rc == LC_OK)
      rc = open_segment(owner, id, create, 1, s, e);
    if (rc == LC_OK) {
      s->next = owner->segments;
      owner->segments = s;
    } else if (s) {
      lc_free_with_allocator(&owner->allocator, s);
      s = NULL;
    }
  }
  if (rc == LC_OK && !s->segment)
    rc = failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                 "collection selected a retiring segment");
  if (rc == LC_OK) {
    ps_collection_segment_retain(s->segment);
    *out = s->segment;
  }
  pthread_mutex_unlock(&owner->cache);
  return rc;
}
static void owner_free(ps_collection_owner *owner, int retire) {
  collection_segment *s, *next;
  collection_topology *t, *next_topology;
  ps_owned_head *identity = NULL;
  ps_owned_status status;
  lc_error e;
  int rc = LC_OK;
  lc_error_init(&e);
  retire = retire && ps_check(space(owner), &e) == LC_OK;
  for (s = owner->segments; s; s = next) {
    next = s->next;
    ps_collection_segment_close(s->segment);
    ps_owned_close(s->retiring);
    lc_free_with_allocator(&owner->allocator, s);
  }
  if (retire && owner->numeric &&
      (owner->identity || owner->binding.target.incarnation)) {
    if (owner->identity)
      rc = ps_collection_segment_claim_orphan(owner->identity, &e);
    else {
      rc = ps_owned_open(space(owner), NULL, owner->binding.stream, 0,
                         &owner->binding.target, &identity, &status, &e);
      if (rc == LC_OK)
        rc = ps_owned_claim_orphan(identity, &e);
    }
    if (rc == LC_OK)
      rc = ps_collection_reclaim_children(space(owner), owner->numeric, &e);
    if (rc == LC_OK && owner->identity)
      (void)ps_collection_segment_retire(owner->identity, &e);
    else if (rc == LC_OK)
      (void)ps_owned_retire(identity, &e);
    /* Failed cleanup retains an unbound identity for explicit root reclaim;
     * it never strands protected children by removing their retry anchor. */
  }
  ps_owned_close(identity);
  ps_collection_segment_close(owner->identity);
  for (t = owner->topologies; t; t = next_topology) {
    next_topology = t->next;
    gate_destroy(&t->gate);
    lc_free_with_allocator(&owner->allocator, t);
  }
  if (owner->initialized & 16U)
    gate_destroy(&owner->streams);
  if (owner->initialized & 32U)
    gate_destroy(&owner->compaction);
  if (owner->initialized & 8U)
    gate_destroy(&owner->commit);
  if (owner->initialized & 4U)
    pthread_rwlock_destroy(&owner->metadata);
  if (owner->initialized & 2U)
    pthread_mutex_destroy(&owner->cache);
  if (owner->initialized & 1U)
    pthread_mutex_destroy(&owner->append);
  lc_error_cleanup(&e);
  lc_free_with_allocator(&owner->allocator, owner->name);
  lc_free_with_allocator(&owner->allocator, owner);
}
void ps_collection_cache_destroy(ps_collection_owner *owner) {
  ps_collection_owner *next;
  while (owner) {
    next = owner->next;
    owner_free(owner, 0);
    owner = next;
  }
}
int ps_collection_conversion_release_cache(ps_root *root, uint64_t numeric,
                                           lc_error *e) {
  ps_namespace *ns;
  ps_collection_owner *owner = NULL;
  collection_segment *s, *next;
  int rc = LC_OK;
  pthread_mutex_lock(&root->mutex);
  for (ns = root->namespaces; ns && !owner; ns = ns->next)
    for (owner = ns->collections; owner && owner->numeric != numeric;
         owner = owner->next) {
    }
  if (owner && owner->references)
    rc = failure(e, LC_ERR_PENDING, "collection_conversion_busy",
                 "release collection receivers before conversion");
  s = rc == LC_OK && owner ? owner->segments : NULL;
  if (rc == LC_OK && owner)
    owner->segments = NULL;
  pthread_mutex_unlock(&root->mutex);
  while (s) {
    next = s->next;
    ps_collection_segment_close(s->segment);
    ps_owned_close(s->retiring);
    lc_free_with_allocator(&owner->allocator, s);
    s = next;
  }
  return rc;
}
static void pin_locked(ps_collection_owner *owner) {
  ++owner->references;
  ++owner->ns->pins;
  ++owner->ns->root->pins;
}
static void owner_release(ps_collection_owner *owner) {
  ps_namespace *ns = owner->ns;
  ps_collection_owner **link;
  int retire;
  pthread_mutex_lock(&ns->root->mutex);
  --owner->references;
  retire = !owner->references && !owner->published;
  if (retire && owner->linked) {
    link = &ns->collections;
    while (*link != owner)
      link = &(*link)->next;
    *link = owner->next;
    owner->linked = 0;
  }
  pthread_mutex_unlock(&ns->root->mutex);
  if (retire)
    owner_free(owner, 1);
  ps_namespace_release(ns);
}

static int control_load(void *context, lc_collection_disk_head *head,
                        lc_error *e) {
  ps_collection_owner *owner = context;
  return ps_collection_head_load(space(owner), owner->numeric, NULL, head, e);
}
static int control_store(void *context, const lc_collection_disk_head *head,
                         lc_error *e) {
  ps_collection_owner *owner = context;
  return ps_collection_head_store(space(owner), owner->numeric, head, NULL, e);
}
static int segment_open(void *context, lc_u64 id, int create,
                        lc_collection_segment *out, lc_error *e) {
  ps_collection_segment *segment = NULL;
  int rc = get_segment(context, id, create, &segment, e);
  if (rc == LC_OK)
    ps_collection_segment_bind(segment, out);
  ps_collection_segment_close(segment);
  return rc;
}
static int read_open(void *context, lc_u64 id, lc_pouch_record_io *input,
                     void **pin, lc_error *e) {
  ps_collection_owner *owner = context;
  ps_collection_segment *segment = NULL;
  int rc;
  if (shared_collection(owner)) {
    ps_catalog_view *view = NULL;
    ps_catalog_info info;
    rc = segment_binding(owner, id, 0, &view, &info, e);
    if (rc == LC_OK)
      rc = ps_collection_segment_view(space(owner), info.stream, &info.target,
                                      &segment, e);
    ps_catalog_view_close(view);
  } else
    rc = get_segment(owner, id, 0, &segment, e);
  if (rc == LC_OK) {
    *input = ps_collection_segment_io(segment);
    *pin = segment;
  }
  return rc;
}
static int replay_open(void *context, lc_u64 id, lc_collection_segment *out,
                       lc_error *e) {
  lc_pouch_record_io input;
  void *pin = NULL;
  int rc = read_open(context, id, &input, &pin, e);
  if (rc == LC_OK) {
    ps_collection_segment_bind(pin, out);
    ps_collection_segment_close(pin);
  }
  return rc;
}
static void read_close(void *context, void *pin) {
  (void)context;
  ps_collection_segment_close(pin);
}
static int order_refresh(void *context, lc_collection_order *order,
                         lc_error *e) {
  collection_receiver *binding = context;
  ps_collection_owner *owner = binding->owner;
  lc_collection_handle *c = binding->receiver;
  ps_owned_status active;
  int shared = shared_collection(owner);
  int rc = lc_collection_order_is_open(order)
               ? lc_collection_order_refresh(order, e)
               : lc_collection_order_scratch_open(&owner->allocator, order, e);
  /* A peer compactor may publish after prepare, or while a live upload makes
   * prepare return the previous view. Receiver-local scratch does not follow
   * the native projection's rename. Rebuild only disposable metadata under the
   * captured topology's metadata pin; never repair a producer's canonical tail.
   */
  if (rc == LC_OK && shared) {
    ps_catalog_view *view = NULL;
    ps_catalog_info info;
    rc = segment_binding(owner, c->head.active, 0, &view, &info, e);
    if (rc == LC_OK)
      rc =
          ps_owned_inspect(space(owner), info.stream, &info.target, &active, e);
    ps_catalog_view_close(view);
  } else if (rc == LC_OK) {
    ps_collection_segment *segment = NULL;
    rc = get_segment(owner, c->head.active, 0, &segment, e);
    if (rc == LC_OK)
      rc = ps_collection_segment_status(segment, &active, NULL, e);
    ps_collection_segment_close(segment);
  }
  /* A peer can also complete a page without changing the snapshot. Its
   * canonical ready tail must be visible before validating a late index
   * publication against this receiver's disposable order. Private allocation
   * alone does not advance this watermark or require a replay. */
  if (rc == LC_OK && (order->snapshot != c->head.snapshot ||
                      order->replay_segment != c->head.active ||
                      order->replay_offset < active.tail))
    rc = shared ? lc_collection_projection_catchup(c, e)
                : lc_collection_projection_capture(c, e);
  return rc;
}
static int ids_refresh(void *context, lc_collection_ids *ids, lc_error *e) {
  ps_collection_owner *owner = ((collection_receiver *)context)->owner;
  return lc_collection_ids_is_open(ids)
             ? lc_collection_ids_refresh(ids, e)
             : lc_collection_ids_scratch_open(&owner->allocator, ids, e);
}
#define COLLECTION_QUERY_NAME_BYTES (LC_XID_STRING_SIZE - 1U + 8U)
static int query_name(const char *epoch, lc_u64 ordinal, unsigned char *name,
                      lc_error *e) {
  size_t i;
  if (!lc_xid_is_valid(epoch))
    return failure(e, LC_ERR_INVALID, NULL,
                   "collection query epoch is invalid");
  memcpy(name, epoch, LC_XID_STRING_SIZE - 1U);
  for (i = 0U; i < 8U; ++i)
    name[LC_XID_STRING_SIZE - 1U + i] = (unsigned char)(ordinal >> (8U * i));
  return LC_OK;
}
static int query_open(void *context, const char *epoch, lc_u64 ordinal,
                      lc_pouch_index_input *out, lc_error *e) {
  ps_collection_owner *owner = ((collection_receiver *)context)->owner;
  unsigned char name[COLLECTION_QUERY_NAME_BYTES];
  int rc = query_name(epoch, ordinal, name, e);
  memset(out, 0, sizeof(*out));
  return rc == LC_OK ? ps_query_artifact_open(owner->ns, owner->numeric, name,
                                              sizeof(name), out, e)
                     : rc;
}
static void query_close(void *context, lc_pouch_index_input *input) {
  ps_collection_owner *owner = ((collection_receiver *)context)->owner;
  ps_query_artifact_close(owner->ns->root, input);
}
typedef struct collection_query_producer {
  collection_receiver *receiver;
  lc_pouch_index_produce_fn produce;
  void *context;
} collection_query_producer;
static int query_produce(void *context, lc_sink *sink, lc_error *e) {
  collection_query_producer *p = context;
#ifdef LOCKDC_TEST_BUILD
  int rc;
  if (lc_collection_query_test_hook) {
    rc = lc_collection_query_test_hook(lc_collection_query_test_context,
                                       p->receiver->receiver, NULL, e);
    if (rc != LC_OK)
      return rc;
  }
#endif
  return p->produce(p->context, sink, e);
}
static int query_replace(void *context, const char *epoch, lc_u64 ordinal,
                         uint64_t bound, lc_pouch_index_produce_fn produce,
                         void *producer, lc_pouch_index_select_fn select,
                         void *selector, lc_error *e) {
  collection_receiver *receiver = context;
  ps_collection_owner *owner = receiver->owner;
  collection_query_producer p;
  unsigned char name[COLLECTION_QUERY_NAME_BYTES];
  int rc = query_name(epoch, ordinal, name, e);
  p.receiver = receiver;
  p.produce = produce;
  p.context = producer;
  return rc == LC_OK
             ? ps_query_artifact_replace(owner->ns, owner->numeric, name,
                                         sizeof(name), bound, query_produce, &p,
                                         select, selector, e)
             : rc;
}
typedef struct collection_query_cleanup {
  collection_receiver *receiver;
  lc_pouch_maintenance_result *result;
} collection_query_cleanup;
static int query_cleanup_one(void *context, const void *name, size_t length,
                             const ps_catalog_info *info, lc_error *e) {
  collection_query_cleanup *cleanup = context;
  collection_receiver *receiver = cleanup->receiver;
  lc_collection_handle *c = receiver->receiver;
  const unsigned char *bytes = name;
  char epoch[LC_XID_STRING_SIZE];
  int rc;
  if (length != COLLECTION_QUERY_NAME_BYTES)
    return failure(e, LC_ERR_PROTOCOL, NULL,
                   "collection query catalog identity is invalid");
  memcpy(epoch, bytes, LC_XID_STRING_SIZE - 1U);
  epoch[LC_XID_STRING_SIZE - 1U] = '\0';
  if (!lc_xid_is_valid(epoch))
    return failure(e, LC_ERR_PROTOCOL, NULL,
                   "collection query catalog epoch is invalid");
  rc = lc_collection_metadata_enter(c, 1, e);
  if (rc != LC_OK)
    return rc;
  rc = lc_collection_refresh_view(c, e);
  if (rc == LC_OK && strcmp(epoch, c->order.query_id)) {
    rc = ps_namespace_work_ensure(receiver->owner->ns,
                                  LC_POUCH_STORAGE_WORK_ORDINARY, 0, e);
    if (rc == LC_OK)
      rc = ps_query_artifact_remove(space(receiver->owner),
                                    receiver->owner->numeric, name, length,
                                    &info->revision, e);
    if (rc == LC_OK && cleanup->result)
      ++cleanup->result->cleanup_deleted_count;
  }
  lc_collection_metadata_leave(c);
  return rc;
}
static int query_cleanup(void *context, lc_pouch_maintenance_result *out,
                         lc_error *e) {
  collection_receiver *receiver = context;
  ps_collection_owner *owner = receiver->owner;
  collection_query_cleanup cleanup;
  cleanup.receiver = receiver;
  cleanup.result = out;
  return ps_catalog_visit(space(owner), owner->numeric, PS_CATALOG_PROJECTION,
                          query_cleanup_one, &cleanup, e);
}

static int projection_begin(void *context,
                            lc_collection_projection_build *build,
                            lc_error *e) {
  ps_collection_owner *owner = ((collection_receiver *)context)->owner;
  int rc =
      lc_collection_order_scratch_open(&owner->allocator, &build->order, e);
  if (rc == LC_OK) {
    /* Disposable receivers share one recovery epoch while this physical
     * owner lives. Reconstructing a receiver is not a history rollback. */
    memcpy(build->order.query_id, owner->query_id, sizeof(owner->query_id));
    rc = lc_collection_order_checkpoint(&build->order, 1U, 0U, e);
  }
  if (rc == LC_OK)
    rc = ids_refresh(context, &build->ids, e);
  return rc;
}
static int projection_publish(void *context,
                              lc_collection_projection_build *build,
                              lc_error *e) {
  ps_collection_owner *owner = ((collection_receiver *)context)->owner;
  int rc = ps_check(space(owner), e);
  if (rc == LC_OK)
    build->published = 1;
  if (rc == LC_OK && build->on_publish)
    rc = build->on_publish(build->publish_context,
                           LC_COLLECTION_PROJECTION_ORDER, e);
  if (rc == LC_OK && build->on_publish)
    rc = build->on_publish(build->publish_context, LC_COLLECTION_PROJECTION_IDS,
                           e);
  return rc;
}
static void projection_cleanup(void *context,
                               lc_collection_projection_build *build) {
  (void)context;
  lc_collection_order_close(&build->order);
  lc_collection_ids_close(&build->ids);
}
static int gate_enter(collection_gate *gate, int exclusive, int wait,
                      void **pin, lc_error *e) {
  int rc = LC_OK;
  pthread_mutex_lock(&gate->mutex);
  while (gate->writer || (exclusive && gate->readers)) {
    if (!wait) {
      rc = failure(e, LC_ERR_PENDING, NULL, "collection storage is pinned");
      break;
    }
    if (pthread_cond_wait(&gate->changed, &gate->mutex) != 0) {
      rc = failure(e, LC_ERR_TRANSPORT, NULL,
                   "cannot wait for collection storage pins");
      break;
    }
  }
  if (rc == LC_OK) {
    if (exclusive)
      gate->writer = 1;
    else
      ++gate->readers;
    *pin = gate;
  }
  pthread_mutex_unlock(&gate->mutex);
  return rc;
}
static void gate_leave(collection_gate *gate) {
  pthread_mutex_lock(&gate->mutex);
  if (gate->writer)
    gate->writer = 0;
  else
    --gate->readers;
  pthread_cond_broadcast(&gate->changed);
  pthread_mutex_unlock(&gate->mutex);
}
static int lock_enter(void *context, enum lc_collection_authority_kind kind,
                      lc_u64 snapshot, int exclusive, int wait, void **pin,
                      lc_error *e) {
  ps_collection_owner *owner = context;
  collection_topology *topology;
  pthread_mutex_t *mutex = NULL;
  pthread_rwlock_t *gate = NULL;
  int rc = ps_check(space(owner), e);
  *pin = NULL;
  if (rc != LC_OK)
    return rc;
  if (kind != LC_COLLECTION_AUTH_CATALOG && shared_collection(owner)) {
    static const enum ps_coordination_domain domains[] = {
        PS_COORDINATION_COLLECTION_CATALOG,
        PS_COORDINATION_COLLECTION_METADATA,
        PS_COORDINATION_COLLECTION_APPEND,
        PS_COORDINATION_COLLECTION_COMMIT,
        PS_COORDINATION_COLLECTION_READERS,
        PS_COORDINATION_COLLECTION_STREAMS,
        PS_COORDINATION_COLLECTION_COMPACTION};
    ps_coordination_guard *guard = NULL;
    /* A pull producer may enter a different collection from its callback.
     * A colliding append byte must not wait for that same producer. */
    if (kind == LC_COLLECTION_AUTH_APPEND && wait &&
        lc_collection_source_callback_active())
      wait = 0;
    rc = ps_coordination_collection_lock(ps_space_coordination(space(owner)),
                                         domains[kind], owner->numeric,
                                         exclusive, wait, &guard, e);
    *pin = guard;
    return rc;
  }
  switch (kind) {
  case LC_COLLECTION_AUTH_CATALOG:
    return catalog_enter(owner->ns, wait, pin, e);
  case LC_COLLECTION_AUTH_APPEND:
    mutex = &owner->append;
    break;
  case LC_COLLECTION_AUTH_METADATA:
    gate = &owner->metadata;
    break;
  case LC_COLLECTION_AUTH_COMMIT:
    return gate_enter(&owner->commit, exclusive, wait, pin, e);
  case LC_COLLECTION_AUTH_STREAMS:
    return gate_enter(&owner->streams, exclusive, wait, pin, e);
  case LC_COLLECTION_AUTH_COMPACTION:
    return gate_enter(&owner->compaction, exclusive, wait, pin, e);
  case LC_COLLECTION_AUTH_READERS:
    pthread_mutex_lock(&owner->cache);
    for (topology = owner->topologies; topology && topology->id != snapshot;
         topology = topology->next) {
    }
    if (!topology) {
      topology =
          lc_calloc_with_allocator(&owner->allocator, 1U, sizeof(*topology));
      if (!topology)
        rc = failure(e, LC_ERR_NOMEM, NULL,
                     "cannot allocate captured collection topology");
      else if (gate_init(&topology->gate, e) != LC_OK) {
        lc_free_with_allocator(&owner->allocator, topology);
        topology = NULL;
        rc = e->code;
      } else {
        topology->id = snapshot;
        topology->next = owner->topologies;
        owner->topologies = topology;
      }
    }
    pthread_mutex_unlock(&owner->cache);
    return rc == LC_OK ? gate_enter(&topology->gate, exclusive, wait, pin, e)
                       : rc;
  }
  if (rc != LC_OK)
    return rc;
  if (mutex)
    rc = wait ? pthread_mutex_lock(mutex) : pthread_mutex_trylock(mutex);
  else if (gate && exclusive)
    rc = wait ? pthread_rwlock_wrlock(gate) : pthread_rwlock_trywrlock(gate);
  else if (gate)
    rc = wait ? pthread_rwlock_rdlock(gate) : pthread_rwlock_tryrdlock(gate);
  else
    rc = EINVAL;
  if (rc != 0)
    return failure(
        e, rc == EBUSY || rc == EAGAIN ? LC_ERR_PENDING : LC_ERR_TRANSPORT,
        NULL, "cannot enter collection storage authority");
  *pin = mutex ? (void *)mutex : (void *)gate;
  return LC_OK;
}
static void lock_leave(void *context, enum lc_collection_authority_kind kind,
                       void *pin) {
  ps_collection_owner *owner = context;
  if (kind == LC_COLLECTION_AUTH_CATALOG)
    catalog_leave(owner->ns, pin);
  else if (shared_collection(owner))
    ps_coordination_unlock(pin);
  else if (kind == LC_COLLECTION_AUTH_STREAMS ||
           kind == LC_COLLECTION_AUTH_READERS ||
           kind == LC_COLLECTION_AUTH_COMMIT ||
           kind == LC_COLLECTION_AUTH_COMPACTION)
    gate_leave(pin);
  else if (kind == LC_COLLECTION_AUTH_APPEND)
    pthread_mutex_unlock(pin);
  else
    pthread_rwlock_unlock(pin);
}
static int layout_active(void *context, lc_error *e) {
  collection_receiver *binding = context;
  lc_collection_handle *c = binding->receiver;
  ps_collection_owner *owner = binding->owner;
  ps_collection_segment *segment = NULL;
  lc_collection_segment active;
  int rc;
  if (c->active.ops && c->active_fd_segment == c->head.active)
    return LC_OK;
  if (shared_collection(owner)) {
    collection_segment opened = {0};
    rc = open_segment(owner, c->head.active, 0, 1, &opened, e);
    segment = opened.segment;
  } else
    rc = get_segment(owner, c->head.active, 0, &segment, e);
  if (rc == LC_OK) {
    ps_collection_segment_bind(segment, &active);
    lc_collection_segment_close(&c->active);
    c->active = active;
    c->active_fd_segment = c->head.active;
  }
  ps_collection_segment_close(segment);
  return rc;
}
static void layout_release_active(void *context) {
  lc_collection_handle *c = ((collection_receiver *)context)->receiver;
  lc_collection_segment_close(&c->active);
  c->active_fd_segment = 0;
}
static int layout_rotate(void *context, int force, lc_u64 limit, lc_error *e) {
  collection_receiver *binding = context;
  lc_collection_handle *c = binding->receiver;
  ps_collection_segment *old = NULL, *next = NULL;
  lc_collection_disk_head head;
  lc_u64 tail, id;
  int rc = lc_pouch_record_io_tail(c->active.io, &tail, e);
  if (rc != LC_OK || (!force && tail < limit))
    return rc;
  id = c->head.active + 1U;
  if (id >= LC_COLLECTION_SNAPSHOT_BIT)
    return failure(e, LC_ERR_INVALID, "sequence_exhausted",
                   "collection segment identities are exhausted");
  old = c->active.context;
  ps_collection_segment_retain(old);
  rc = ps_collection_segment_publish(old, PS_COMMIT_DEFAULT, e);
  if (rc == LC_OK)
    rc = get_segment(binding->owner, id, 1, &next, e);
  head = c->head;
  head.active = id;
  if (rc == LC_OK)
    rc = control_store(binding->owner, &head, e);
  if (rc == LC_OK) {
    lc_collection_segment_close(&c->active);
    ps_collection_segment_bind(next, &c->active);
    c->head = head;
    c->active_fd_segment = id;
    rc = ps_collection_segment_seal(old, e);
  }
  c->head_loaded = 0;
  ps_collection_segment_close(next);
  ps_collection_segment_close(old);
  return rc;
}
/* Metadata authority excludes admission of old read generations. Captured
 * operations release their lifetime pins without taking that authority. */
static int obsolete_readers(ps_collection_owner *owner, lc_u64 current,
                            int *busy, lc_error *e) {
  collection_topology *t;
  *busy = 0;
  if (shared_collection(owner)) {
    ps_coordination_guard *guard = NULL;
    int rc = ps_coordination_collection_lock(
        ps_space_coordination(space(owner)), PS_COORDINATION_COLLECTION_READERS,
        owner->numeric, 1, 0, &guard, e);
    if (rc == LC_ERR_PENDING) {
      clear(e);
      *busy = 1;
      return LC_OK;
    }
    ps_coordination_unlock(guard);
    return rc == LC_OK ? ps_check(space(owner), e) : rc;
  }
  pthread_mutex_lock(&owner->cache);
  for (t = owner->topologies; t; t = t->next) {
    if (t->id == current)
      continue;
    pthread_mutex_lock(&t->gate.mutex);
    *busy |= t->gate.readers != 0U || t->gate.writer;
    pthread_mutex_unlock(&t->gate.mutex);
  }
  pthread_mutex_unlock(&owner->cache);
  return LC_OK;
}
/* An uncached obsolete child needs only its physical identity, never canonical
 * replay. Retain failed retirements in the owner cache so a raw catalog view
 * can drain and a later cleanup can retry even after both names were removed.
 */
static int retire_resource(ps_collection_owner *owner,
                           ps_collection_resources *list, lc_u64 id,
                           const ps_catalog_info *identity, lc_error *e) {
  collection_segment *s, **link;
  ps_owned_status status;
  int rc = LC_OK;
  pthread_mutex_lock(&owner->cache);
  for (s = owner->segments; s && s->id != id; s = s->next) {
  }
  if (!s) {
    s = lc_calloc_with_allocator(&owner->allocator, 1U, sizeof(*s));
    if (!s)
      rc = failure(e, LC_ERR_NOMEM, NULL,
                   "cannot retain collection reclamation identity");
    if (rc == LC_OK)
      rc = ps_owned_open(space(owner), NULL, identity->stream, 0,
                         &identity->target, &s->retiring, &status, e);
    if (rc == LC_OK) {
      s->id = id;
      s->binding = *identity;
      s->bound = 1;
      s->next = owner->segments;
      owner->segments = s;
    } else {
      lc_free_with_allocator(&owner->allocator, s);
      s = NULL;
    }
  }
  if (rc == LC_OK)
    rc = ps_collection_resources_remove(space(owner), list, id, 0, e);
  if (rc == LC_OK) {
    s->bound = 0;
    rc = s->segment ? ps_collection_segment_retire(s->segment, e)
                    : ps_owned_retire(s->retiring, e);
    if (rc == LC_ERR_INVALID)
      rc = failure(e, LC_ERR_PENDING, NULL,
                   "obsolete collection storage has a catalog pin");
  }
  if (rc == LC_OK) {
    for (link = &owner->segments; *link != s; link = &(*link)->next) {
    }
    *link = s->next;
    ps_collection_segment_close(s->segment);
    ps_owned_close(s->retiring);
    lc_free_with_allocator(&owner->allocator, s);
  }
  pthread_mutex_unlock(&owner->cache);
  return rc;
}
static int mark_resource(ps_collection_owner *owner,
                         const ps_collection_resource *item, lc_u64 now,
                         lc_error *e) {
  unsigned char name[16];
  ps_catalog_info ignored;
  segment_name(item->id, name);
  segment_name(now, name + 8U);
  /* The catalog CRC covers the timestamp. The alias pins the same canonical
   * stream, so process death cannot strand an independent marker resource. */
  return ps_catalog_put(space(owner), NULL, owner->numeric, PS_CATALOG_CONTROL,
                        name, sizeof(name), NULL, item->binding.stream,
                        &item->binding.target, &ignored, e);
}
static int layout_cleanup(void *context, lc_pouch_maintenance_result *out,
                          lc_error *e) {
  collection_receiver *receiver = context;
  ps_collection_owner *owner = receiver->owner;
  lc_collection_handle *c = receiver->receiver;
  ps_collection_resources list;
  ps_collection_resource *item;
  collection_segment *s, *candidate;
  ps_catalog_info retry;
  lc_pouch_unix_seconds instant;
  lc_u64 marked, grace = c->pouch->compaction_delete_grace_seconds, retry_id;
  size_t i, j;
  int busy, pending, rc = obsolete_readers(owner, c->head.snapshot, &busy, e);
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&instant, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_collection_resources_capture(space(owner), owner->numeric, &list, e);
  for (i = 0U; rc == LC_OK && i < list.count; ++i) {
    item = &list.items[i];
    for (j = 0U; j < i && list.items[j].id != item->id; ++j) {
    }
    if (j != i)
      continue;
    if (lc_collection_segment_selected(&c->head, item->id)) {
      rc = ps_collection_resources_remove(space(owner), &list, item->id, 1, e);
      continue;
    }
    marked = item->marked;
    for (j = i + 1U; j < list.count; ++j)
      if (list.items[j].id == item->id && list.items[j].marked)
        marked = list.items[j].marked;
    pending = busy;
    if (grace && !marked && item->kind != PS_CATALOG_CONTROL) {
      rc = mark_resource(owner, item, (lc_u64)instant, e);
      pending = 1;
    } else if (grace && marked &&
               ((lc_u64)instant < marked || (lc_u64)instant - marked < grace))
      pending = 1;
    if (rc == LC_OK && !pending) {
      rc = retire_resource(owner, &list, item->id, &item->binding, e);
      if (rc == LC_ERR_PENDING) {
        pending = 1;
        clear(e);
        rc = LC_OK;
      } else if (rc == LC_OK && out)
        ++out->cleanup_deleted_count;
    }
    if (rc == LC_OK && pending && out)
      ++out->cleanup_pending_count;
  }
  /* Retry targets whose last catalog name was removed by a prior pass. Walk
   * by identity because successful retirement removes its cache node. */
  retry_id = 0U;
  while (rc == LC_OK && !busy) {
    pthread_mutex_lock(&owner->cache);
    candidate = NULL;
    for (s = owner->segments; s; s = s->next) {
      if (s->bound || s->id <= retry_id ||
          lc_collection_segment_selected(&c->head, s->id))
        continue;
      for (j = 0U; j < list.count && list.items[j].id != s->id; ++j) {
      }
      if (j == list.count && (!candidate || s->id < candidate->id))
        candidate = s;
    }
    if (candidate) {
      retry = candidate->binding;
      retry_id = candidate->id;
    }
    pthread_mutex_unlock(&owner->cache);
    if (!candidate)
      break;
    rc = retire_resource(owner, &list, retry_id, &retry, e);
    if (rc == LC_ERR_PENDING) {
      clear(e);
      rc = LC_OK;
      if (out)
        ++out->cleanup_pending_count;
    } else if (rc == LC_OK && out)
      ++out->cleanup_deleted_count;
  }
  ps_collection_resources_clear(&list);
  return rc;
}
static int layout_discard(void *context, lc_u64 id, lc_error *e) {
  collection_receiver *receiver = context;
  ps_collection_resources list;
  size_t i;
  int rc;
  if (lc_collection_segment_selected(&receiver->receiver->head, id))
    return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                   "cannot discard selected collection storage");
  rc = ps_collection_resources_capture(space(receiver->owner),
                                       receiver->owner->numeric, &list, e);
  for (i = 0U; rc == LC_OK && i < list.count; ++i)
    if (list.items[i].id == id) {
      rc = retire_resource(receiver->owner, &list, id, &list.items[i].binding,
                           e);
      break;
    }
  ps_collection_resources_clear(&list);
  return rc;
}
static int bound(void *context, lc_error *e) {
  collection_receiver *binding = context;
  ps_collection_owner *owner = binding->owner;
  ps_catalog_view *view = NULL;
  ps_catalog_info current;
  int rc = ps_catalog_get(space(owner), owner->ns->identity.owner,
                          PS_CATALOG_COLLECTION, owner->name,
                          strlen(owner->name), &view, e);
  if (rc == LC_OK && view)
    rc = ps_catalog_view_info(view, &current, e);
  if (rc == LC_OK &&
      (!view || !same_ref(&current.revision, &owner->binding.revision) ||
       !same_ref(&current.target, &owner->binding.target)))
    rc = failure(e, LC_ERR_INVALID, "collection_stale",
                 "collection name belongs to another incarnation");
  ps_catalog_view_close(view);
  return rc;
}
static int remove_name(void *context, lc_error *e) {
  collection_receiver *binding = context;
  ps_collection_owner *owner = binding->owner;
  int rc = ps_catalog_remove(space(owner), owner->ns->identity.owner,
                             PS_CATALOG_COLLECTION, owner->name,
                             strlen(owner->name), &owner->binding.revision, e);
  if (rc == LC_OK) {
    pthread_mutex_lock(&owner->ns->root->mutex);
    owner->published = 0;
    pthread_mutex_unlock(&owner->ns->root->mutex);
  }
  return rc;
}
static int receiver_open(ps_collection_owner *, lc_pouch *, lc_client_handle *,
                         lc_collection **, lc_error *);
static int receiver_clone(void *context, lc_collection **out, lc_error *e) {
  collection_receiver *binding = context;
  ps_collection_owner *owner = binding->owner;
  pthread_mutex_lock(&owner->ns->root->mutex);
  pin_locked(owner);
  pthread_mutex_unlock(&owner->ns->root->mutex);
  return receiver_open(owner, binding->receiver->pouch,
                       binding->receiver->client, out, e);
}
static int receiver_finish(void *context, lc_error *e) {
  collection_receiver *binding = context;
  return ps_check(space(binding->owner), e);
}
static void receiver_release(void *context) {
  collection_receiver *binding = context;
  ps_collection_owner *owner = binding->owner;
  lc_free_with_allocator(&owner->allocator, binding);
  owner_release(owner);
}
int ps_pouch_collection_maintain(lc_collection *self, int force,
                                 int cleanup_only,
                                 lc_pouch_maintenance_result *out,
                                 lc_error *e) {
  lc_collection_handle *c = self ? self->impl : NULL;
  int dropped = 0, rc = LC_OK;
  if (out)
    memset(out, 0, sizeof(*out));
  if (!c || !ps_pouch_state_root(c->pouch))
    return failure(e, LC_ERR_INVALID, NULL,
                   "collection maintenance requires a container receiver");
  if (!cleanup_only)
    rc = lc_collection_retention_maintain(self, &dropped, out, e);
  if (rc == LC_OK && !dropped)
    rc = lc_collection_compact(self, force, cleanup_only, out, e);
  if (rc == LC_OK && !dropped)
    rc = lc_collection_query_cleanup(self, out, e);
  if (rc == LC_OK && !dropped && !cleanup_only)
    rc = lc_collection_query_maintain(self, e);
  /* Journaled retirement preserves the previous recoverable root's extents.
   * Publish the existing metadata checkpoint before admitting their reuse. */
  if (rc == LC_OK)
    rc = ps_checkpoint(ps_root_space(ps_pouch_state_root(c->pouch)), e);
  return rc;
}
static int receiver_attach(void *context, lc_collection_handle *c,
                           lc_error *e) {
  static const lc_collection_authority_ops locks = {lock_enter, lock_leave};
  collection_receiver *binding = context;
  ps_collection_owner *owner = binding->owner;
  binding->receiver = c;
  c->reads.open = read_open;
  c->reads.close = read_close;
  c->reads.context = owner;
  c->segments.open = segment_open;
  if (shared_collection(owner)) {
    c->segments.replay = replay_open;
    c->segments.publish_reservations = 1;
  }
  c->segments.context = owner;
  c->segments.private_limit = PS_MIB;
  c->control.load = control_load;
  c->control.store = control_store;
  c->control.context = owner;
  c->authority.ops = &locks;
  c->authority.context = owner;
  c->projections.order_refresh = order_refresh;
  c->projections.ids_refresh = ids_refresh;
  c->projections.begin = projection_begin;
  c->projections.publish = projection_publish;
  c->projections.cleanup = projection_cleanup;
  c->projections.context = binding;
  c->queries.open = query_open;
  c->queries.close = query_close;
  c->queries.replace = query_replace;
  c->queries.cleanup = query_cleanup;
  c->queries.context = binding;
  c->layout.active = layout_active;
  c->layout.rotate = layout_rotate;
  c->layout.discard = layout_discard;
  c->layout.cleanup = layout_cleanup;
  if (shared_collection(owner))
    c->layout.release_active = layout_release_active;
  c->layout.context = binding;
  return ps_check(space(owner), e);
}
/* Consumes one owner reference on every result. */
static int receiver_open(ps_collection_owner *owner, lc_pouch *pouch,
                         lc_client_handle *client, lc_collection **out,
                         lc_error *e) {
  static const lc_collection_owner_ops operations = {
      receiver_attach, receiver_clone,  bound,
      remove_name,     receiver_finish, receiver_release};
  lc_collection_owner access;
  collection_receiver *binding =
      lc_calloc_with_allocator(&owner->allocator, 1U, sizeof(*binding));
  if (!binding) {
    owner_release(owner);
    return failure(e, LC_ERR_NOMEM, NULL,
                   "cannot allocate collection receiver binding");
  }
  binding->owner = owner;
  access.ops = &operations;
  access.context = binding;
  return lc_collection_owned_open(pouch, client, owner->ns->name, owner->name,
                                  owner->id, access, out, e);
}

static int initialize_locks(ps_collection_owner *owner, lc_error *e) {
  if (pthread_mutex_init(&owner->append, NULL) != 0)
    goto failed;
  owner->initialized |= 1U;
  if (pthread_mutex_init(&owner->cache, NULL) != 0)
    goto failed;
  owner->initialized |= 2U;
  if (pthread_rwlock_init(&owner->metadata, NULL) != 0)
    goto failed;
  owner->initialized |= 4U;
  if (gate_init(&owner->commit, e) != LC_OK)
    return e->code;
  owner->initialized |= 8U;
  if (gate_init(&owner->streams, e) != LC_OK)
    return e->code;
  owner->initialized |= 16U;
  if (gate_init(&owner->compaction, e) != LC_OK)
    return e->code;
  owner->initialized |= 32U;
  return LC_OK;
failed:
  return failure(e, LC_ERR_TRANSPORT, NULL,
                 "cannot initialize collection owner authority");
}
static ps_collection_owner *allocate_owner(ps_namespace *ns, const char *name,
                                           const lc_allocator *allocator,
                                           lc_error *e) {
  ps_collection_owner *owner =
      lc_calloc_with_allocator(allocator, 1U, sizeof(*owner));
  if (!owner) {
    failure(e, LC_ERR_NOMEM, NULL, "cannot allocate collection owner");
    return NULL;
  }
  owner->allocator = *allocator;
  owner->ns = ns;
  owner->name = lc_strdup_with_allocator(allocator, name);
  if (!owner->name) {
    failure(e, LC_ERR_NOMEM, NULL, "cannot retain collection owner name");
    owner_free(owner, 0);
    return NULL;
  }
  if (initialize_locks(owner, e) != LC_OK) {
    owner_free(owner, 0);
    return NULL;
  }
  if (lc_xid_new(owner->query_id, e) != LC_OK) {
    owner_free(owner, 0);
    return NULL;
  }
  return owner;
}
static void cache_owner(ps_collection_owner *owner) {
  ps_root *root = owner->ns->root;
  pthread_mutex_lock(&root->mutex);
  owner->next = owner->ns->collections;
  owner->ns->collections = owner;
  owner->linked = 1;
  pin_locked(owner);
  pthread_mutex_unlock(&root->mutex);
}
/* Caller serializes namespace admission. The catalog target remains pinned
 * until the immutable owner has admitted its own physical identity handle. */
static int load_owner(ps_namespace *ns, const char *name,
                      const ps_catalog_info *info,
                      const lc_allocator *allocator, ps_collection_owner **out,
                      lc_error *e) {
  ps_collection_owner *owner;
  ps_owned_status status;
  unsigned char bytes[LC_COLLECTION_CATALOG_BYTES];
  char decoded[LC_COLLECTION_NAME_BYTES];
  struct iovec part;
  int rc;
  *out = NULL;
  {
    ps_catalog_view *conversion = NULL;
    rc = ps_catalog_get(ns->root->space, info->stream.owner,
                        PS_CATALOG_CONVERSION, NULL, 0U, &conversion, e);
    if (rc == LC_OK && conversion)
      rc = failure(
          e, LC_ERR_PENDING, "collection_conversion_in_progress",
          "collection conversion must complete before receiver admission");
    ps_catalog_view_close(conversion);
    if (rc != LC_OK)
      return rc;
  }
  pthread_mutex_lock(&ns->root->mutex);
  for (owner = ns->collections; owner && owner->numeric != info->stream.owner;
       owner = owner->next) {
  }
  if (owner)
    pin_locked(owner);
  pthread_mutex_unlock(&ns->root->mutex);
  if (owner) {
    if (!same_ref(&info->revision, &owner->binding.revision) ||
        strcmp(owner->name, name)) {
      owner_release(owner);
      return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                     "collection catalog changed its immutable identity");
    }
    *out = owner;
    return LC_OK;
  }
  if (!info->stream.owner || info->stream.key || info->stream.kind != PS_STREAM)
    return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                   "collection catalog has an invalid identity target");
  owner = allocate_owner(ns, name, allocator, e);
  if (!owner)
    return e->code;
  owner->numeric = info->stream.owner;
  owner->binding = *info;
  owner->published = 1;
  rc = ps_collection_segment_open(space(owner), key_for(owner, 0U), 0,
                                  &info->target, NULL, NULL, &owner->identity,
                                  e);
  if (rc == LC_OK)
    rc = ps_collection_segment_status(owner->identity, &status, NULL, e);
  if (rc == LC_OK && (!status.sealed || status.tail != sizeof(bytes) ||
                      !same_ref(&status.ref, &info->target)))
    rc = failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                 "collection catalog identity is incomplete or stale");
  part.iov_base = bytes;
  part.iov_len = sizeof(bytes);
  if (rc == LC_OK)
    rc = lc_pouch_record_io_read(ps_collection_segment_io(owner->identity), 0U,
                                 &part, 1, e);
  if (rc == LC_OK)
    rc = lc_collection_catalog_decode(bytes, owner->id, decoded, e);
  if (rc == LC_OK && strcmp(name, decoded))
    rc = failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                 "collection catalog target has a different name");
  if (rc == LC_OK) {
    if (shared_collection(owner)) {
      ps_collection_segment_close(owner->identity);
      owner->identity = NULL;
    }
    cache_owner(owner);
    *out = owner;
  } else
    owner_free(owner, 0);
  return rc;
}
static int create_owner(ps_namespace *ns, const lc_collection_config *config,
                        const lc_allocator *allocator,
                        ps_collection_owner **out, lc_error *e) {
  ps_collection_owner *owner;
  ps_collection_segment *segment = NULL;
  lc_collection_disk_head head;
  lc_pouch_unix_seconds now;
  unsigned char bytes[LC_COLLECTION_CATALOG_BYTES];
  lc_pouch_record_io output;
  struct iovec part;
  int rc;
  *out = NULL;
  owner = allocate_owner(ns, config->name, allocator, e);
  if (!owner)
    return e->code;
  rc = ps_catalog_allocate_id(space(owner), &owner->numeric, e);
  if (rc == LC_OK)
    rc = lc_xid_new(owner->id, e);
  if (rc == LC_OK)
    rc = lc_collection_catalog_encode(owner->id, owner->name, bytes, e);
  if (rc == LC_OK)
    rc = ps_collection_segment_open(space(owner), key_for(owner, 0U), 1, NULL,
                                    NULL, NULL, &owner->identity, e);
  part.iov_base = bytes;
  part.iov_len = sizeof(bytes);
  if (rc == LC_OK) {
    output = ps_collection_segment_io(owner->identity);
    rc = lc_pouch_record_io_append(output, &part, 1, e);
  }
  if (rc == LC_OK)
    rc = ps_collection_segment_seal(owner->identity, e);
  if (rc == LC_OK)
    rc = get_segment(owner, 1U, 1, &segment, e);
  memset(&head, 0, sizeof(head));
  head.active = head.tail_segment = 1U;
  head.max_entries = config->has_max_entries ? config->max_entries : 0U;
  if (rc == LC_OK)
    rc = lc_pouch_now_unix(&now, e);
  if (rc == LC_OK)
    rc = lc_pouch_retention_resolve(
        config->has_retention_seconds
            ? -(lc_pouch_unix_seconds)config->retention_seconds
            : 0,
        now, &head.retention, e);
  if (rc == LC_OK)
    rc = control_store(owner, &head, e);
  ps_collection_segment_close(segment);
  if (rc == LC_OK) {
    cache_owner(owner);
    *out = owner;
  } else
    owner_free(owner, 1);
  return rc;
}
static int stale(int rc, const lc_error *e) {
  return rc == LC_ERR_INVALID && e->server_code &&
         !strcmp(e->server_code, "collection_stale");
}
int ps_collection_client_open(ps_root *root, lc_client *client,
                              const char *ns_name,
                              const lc_collection_config *config, int create,
                              lc_collection **out, lc_error *e) {
  lc_client_handle *logical = (lc_client_handle *)client;
  ps_namespace *ns = NULL;
  ps_collection_owner *owner = NULL;
  ps_catalog_view *view = NULL;
  ps_catalog_info info;
  ps_owned_status status;
  lc_collection_disk_head head;
  collection_receiver temporary;
  void *catalog;
  int found = 0, rc;
  *out = NULL;
  rc = ps_root_namespace_acquire(root, ns_name, create, &ns, e);
  if (rc == LC_ERR_INVALID && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_not_found"))
    return failure(e, LC_ERR_INVALID, "collection_not_found",
                   "collection does not exist");
  if (rc != LC_OK)
    return rc;
  rc = catalog_enter(ns, 1, &catalog, e);
  if (rc == LC_OK)
    rc = ps_catalog_get(root->space, ns->identity.owner, PS_CATALOG_COLLECTION,
                        config->name, strlen(config->name), &view, e);
  if (rc == LC_OK && view) {
    rc = ps_catalog_view_info(view, &info, e);
    if (rc == LC_OK)
      rc = load_owner(ns, config->name, &info, &logical->allocator, &owner, e);
    if (rc == LC_OK)
      rc = control_load(owner, &head, e);
    if (rc == LC_OK && head.dropped) {
      temporary.owner = owner;
      rc = remove_name(&temporary, e);
      ps_catalog_view_close(view);
      view = NULL;
      owner_release(owner);
      owner = NULL;
    } else if (rc == LC_OK)
      found = 1;
  }
  ps_catalog_view_close(view);
  if (rc == LC_OK && found && create)
    rc = failure(e, LC_ERR_INVALID, "collection_exists",
                 "collection name already exists");
  if (rc == LC_OK && !found && !create)
    rc = failure(e, LC_ERR_INVALID, "collection_not_found",
                 "collection does not exist");
  if (rc == LC_OK && !found)
    rc = ps_namespace_work_ensure(ns, LC_POUCH_STORAGE_WORK_COLLECTION, 0, e);
  if (rc == LC_OK && !found)
    rc = create_owner(ns, config, &logical->allocator, &owner, e);
  if (rc == LC_OK) {
    /* Keep the owner's construction reference through name publication. */
    pthread_mutex_lock(&root->mutex);
    pin_locked(owner);
    pthread_mutex_unlock(&root->mutex);
    rc = receiver_open(owner, logical->pouch, logical, out, e);
    if (rc == LC_OK && !found && config->lease)
      rc = lc_collection_lease_initialize((*out)->impl, config->lease, e);
    if (rc == LC_OK && !found)
      rc = ps_collection_segment_status(owner->identity, &status, NULL, e);
    if (rc == LC_OK && !found)
      rc = ps_catalog_put(root->space, NULL, ns->identity.owner,
                          PS_CATALOG_COLLECTION, owner->name,
                          strlen(owner->name), NULL, key_for(owner, 0U),
                          &status.ref, &owner->binding, e);
    if (rc == LC_OK && !found) {
      if (shared_collection(owner)) {
        ps_collection_segment_close(owner->identity);
        owner->identity = NULL;
      }
      pthread_mutex_lock(&root->mutex);
      owner->published = 1;
      pthread_mutex_unlock(&root->mutex);
    }
    if (rc != LC_OK && *out) {
      (*out)->close(*out);
      *out = NULL;
    }
  }
  if (owner)
    owner_release(owner);
  rc = catalog_end(ns, catalog, rc, e);
  if (rc != LC_OK && *out) {
    (*out)->close(*out);
    *out = NULL;
  }
  if (rc == LC_OK && !found)
    lc_pouch_compaction_note_mutation(logical->pouch, ns_name, 0);
  ps_namespace_release(ns);
  return rc;
}

typedef struct collection_listing {
  ps_namespace *ns;
  const lc_allocator *allocator;
  ps_collection_owner **items;
  size_t count, capacity;
} collection_listing;
static int capture_collection(void *context, const void *name, size_t length,
                              const ps_catalog_info *info, lc_error *e) {
  collection_listing *list = context;
  ps_collection_owner **grown;
  char text[LC_COLLECTION_NAME_BYTES];
  size_t next;
  int rc;
  if (!length || length >= sizeof(text) || memchr(name, 0, length))
    return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                   "collection catalog contains an invalid name");
  memcpy(text, name, length);
  text[length] = 0;
  if (list->count == list->capacity) {
    next = list->capacity ? list->capacity * 2U : 8U;
    if (next < list->capacity || next > (size_t)-1 / sizeof(*grown))
      return failure(e, LC_ERR_NOMEM, NULL, "collection listing is too large");
    grown = lc_realloc_with_allocator(list->allocator, list->items,
                                      next * sizeof(*grown));
    if (!grown)
      return failure(e, LC_ERR_NOMEM, NULL,
                     "cannot allocate collection listing");
    list->items = grown;
    list->capacity = next;
  }
  rc = load_owner(list->ns, text, info, list->allocator,
                  &list->items[list->count], e);
  if (rc == LC_OK)
    ++list->count;
  return rc;
}
static int compare_collections(const void *a, const void *b) {
  const ps_collection_owner *const *left = a, *const *right = b;
  return strcmp((*left)->name, (*right)->name);
}
int ps_collection_client_visit(ps_root *root, lc_client *client,
                               const char *ns_name,
                               lc_collection_visit_fn visitor, void *context,
                               lc_error *e) {
  lc_client_handle *logical = (lc_client_handle *)client;
  collection_listing list;
  lc_collection *collection;
  lc_collection_info info;
  lc_collection_call scope;
  lc_collection_guard guard;
  void *catalog;
  size_t i;
  int rc;
  memset(&list, 0, sizeof(list));
  list.allocator = &logical->allocator;
  rc = lc_collection_root_begin(logical->pouch, &scope, &guard, e);
  if (rc != LC_OK)
    return rc;
  rc = ps_root_namespace_acquire(root, ns_name, 0, &list.ns, e);
  if (rc == LC_ERR_INVALID && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_not_found")) {
    clear(e);
    rc = LC_OK;
  }
  if (rc == LC_OK && list.ns) {
    rc = catalog_enter(list.ns, 1, &catalog, e);
    if (rc == LC_OK)
      rc =
          ps_catalog_visit(root->space, list.ns->identity.owner,
                           PS_CATALOG_COLLECTION, capture_collection, &list, e);
    rc = catalog_end(list.ns, catalog, rc, e);
  }
  lc_collection_root_end(&scope, &guard);
  if (rc == LC_OK && list.count > 1U)
    qsort(list.items, list.count, sizeof(*list.items), compare_collections);
  for (i = 0U; i < list.count; ++i) {
    collection = NULL;
    if (rc == LC_OK) {
      rc =
          receiver_open(list.items[i], logical->pouch, logical, &collection, e);
      list.items[i] = NULL;
      if (rc == LC_OK)
        rc = collection->describe(collection, &info, e);
      if (stale(rc, e)) {
        clear(e);
        rc = LC_OK;
      } else if (rc == LC_OK)
        rc = visitor(context, &info, e);
      lc_collection_close(collection);
    }
    if (list.items[i])
      owner_release(list.items[i]);
  }
  lc_free_with_allocator(list.allocator, list.items);
  ps_namespace_release(list.ns);
  return rc;
}

typedef struct maintenance_item {
  char name[LC_COLLECTION_NAME_BYTES];
  ps_catalog_info identity;
} maintenance_item;
typedef struct maintenance_items {
  const lc_allocator *allocator;
  maintenance_item *items;
  size_t count, capacity;
} maintenance_items;
static int capture_maintenance(void *context, const void *name, size_t length,
                               const ps_catalog_info *info, lc_error *e) {
  maintenance_items *list = context;
  maintenance_item *grown, *item;
  size_t capacity;
  if (!length || length >= LC_COLLECTION_NAME_BYTES || memchr(name, 0, length))
    return failure(e, LC_ERR_PROTOCOL, "collection_corrupt",
                   "collection work catalog has an invalid name");
  if (list->count == list->capacity) {
    capacity = list->capacity ? list->capacity * 2U : 8U;
    if (capacity < list->capacity || capacity > (size_t)-1 / sizeof(*grown))
      return failure(e, LC_ERR_NOMEM, NULL,
                     "collection work list is too large");
    grown = lc_realloc_with_allocator(list->allocator, list->items,
                                      capacity * sizeof(*grown));
    if (!grown)
      return failure(e, LC_ERR_NOMEM, NULL, "cannot capture collection work");
    list->items = grown;
    list->capacity = capacity;
  }
  item = &list->items[list->count++];
  memcpy(item->name, name, length);
  item->name[length] = 0;
  item->identity = *info;
  return LC_OK;
}
static int maintenance_admit(ps_namespace *ns, lc_pouch *pouch,
                             const maintenance_item *item, lc_collection **out,
                             lc_error *e) {
  ps_catalog_view *view = NULL;
  ps_catalog_info current;
  ps_collection_owner *owner = NULL;
  void *catalog;
  int rc;
  *out = NULL;
  rc = catalog_enter(ns, 1, &catalog, e);
  if (rc == LC_OK)
    rc = ps_catalog_get(ns->root->space, ns->identity.owner,
                        PS_CATALOG_COLLECTION, item->name, strlen(item->name),
                        &view, e);
  if (rc == LC_OK && view)
    rc = ps_catalog_view_info(view, &current, e);
  if (rc == LC_OK && view &&
      same_ref(&current.revision, &item->identity.revision) &&
      same_ref(&current.target, &item->identity.target))
    rc = load_owner(ns, item->name, &current, &pouch->allocator, &owner, e);
  ps_catalog_view_close(view);
  rc = catalog_end(ns, catalog, rc, e);
  if (rc == LC_OK && owner)
    rc = receiver_open(owner, pouch, NULL, out, e);
  else if (owner)
    owner_release(owner);
  return rc;
}
static int count_binding(void *context, const void *name, size_t length,
                         const ps_catalog_info *info, lc_error *e) {
  int *present = context;
  (void)name;
  (void)length;
  (void)info;
  (void)e;
  *present = 1;
  return LC_OK;
}
int ps_collection_maintenance_run(ps_root *root, lc_pouch *pouch,
                                  const lc_pouch_maintenance_options *options,
                                  lc_pouch_maintenance_result *out,
                                  lc_error *e) {
  maintenance_items list;
  ps_namespace *ns = NULL;
  lc_collection *collection;
  lc_error local, first;
  uint64_t reclaimed, skipped;
  size_t i;
  void *catalog;
  int present = 0, pending = 0, scan_collections, dropped, rc, first_rc;
  memset(&list, 0, sizeof(list));
  list.allocator = &pouch->allocator;
  rc = ps_root_namespace_acquire(root, options->ns, 0, &ns, e);
  if (rc == LC_ERR_INVALID && e && e->server_code &&
      !strcmp(e->server_code, "pouch_namespace_not_found")) {
    clear(e);
    return LC_OK;
  }
  if (rc != LC_OK)
    return rc;
  rc = ps_namespace_work_get(ns, LC_POUCH_STORAGE_WORK_COLLECTION, &present, e);
  if (rc != LC_OK) {
    ps_namespace_release(ns);
    return rc;
  }
  scan_collections = present || !options->background || options->force;
  present = 0;
  /* A durable catalog prefix, not the resident receiver cache, selects work.
   * Background discovery can bypass the namespace catalog until its durable
   * collection hint is published. Root orphan reclamation still runs.
   */
  if (scan_collections) {
    rc = catalog_enter(ns, 1, &catalog, e);
    if (rc == LC_OK)
      rc = ps_catalog_visit(root->space, ns->identity.owner,
                            PS_CATALOG_COLLECTION, capture_maintenance, &list,
                            e);
    rc = catalog_end(ns, catalog, rc, e);
  }
  lc_error_init(&local);
  lc_error_init(&first);
  first_rc = rc;
  if (rc != LC_OK && e) {
    first = *e;
    lc_error_init(e);
  }
  for (i = 0U; i < list.count; ++i) {
    collection = NULL;
    dropped = 0;
    rc = maintenance_admit(ns, pouch, &list.items[i], &collection, &local);
    if (rc == LC_OK && collection && !options->cleanup_only)
      rc = lc_collection_retention_maintain(collection, &dropped, out, &local);
    if (rc == LC_OK && collection && !dropped)
      rc = lc_collection_compact(collection, options->force,
                                 options->cleanup_only, out, &local);
    if (rc == LC_OK && collection && !dropped)
      rc = lc_collection_query_cleanup(collection, out, &local);
    if (rc == LC_OK && collection && !dropped && !options->cleanup_only)
      rc = lc_collection_query_maintain(collection, &local);
    if (stale(rc, &local)) {
      clear(&local);
      rc = LC_OK;
      if (out)
        ++out->cleanup_pending_count;
    }
    lc_collection_close(collection);
    if (rc != LC_OK && first_rc == LC_OK) {
      first_rc = rc;
      first = local;
      lc_error_init(&local);
    }
    clear(&local);
  }
  lc_free_with_allocator(list.allocator, list.items);
  if (first_rc == LC_OK) {
    rc = ps_root_reclaim_orphans(root, &reclaimed, &skipped, &local);
    if (rc == LC_OK && scan_collections) {
      rc = catalog_enter(ns, 1, &catalog, &local);
      if (rc == LC_OK)
        rc = ps_catalog_visit(root->space, ns->identity.owner,
                              PS_CATALOG_COLLECTION, count_binding, &present,
                              &local);
      if (rc == LC_OK)
        rc = ps_catalog_orphans_pending(root->space, &pending, &local);
      if (rc == LC_OK && !present && !pending)
        rc = ps_namespace_work_remove(ns, LC_POUCH_STORAGE_WORK_COLLECTION,
                                      &local);
      rc = catalog_end(ns, catalog, rc, &local);
    } else if (rc == LC_OK)
      rc = ps_catalog_orphans_pending(root->space, &pending, &local);
    if (rc == LC_OK && pending && out)
      ++out->cleanup_pending_count;
    /* On a shared root, discovery without collection work adds no checkpoint
     * obligation. Capacity admission and journal rollover settle unrelated
     * work. Collection maintenance and orphan retirement drain their work. */
    if (rc == LC_OK &&
        (scan_collections || reclaimed ||
         !ps_coordination_shared(ps_space_coordination(root->space))))
      rc = options->cleanup_only && !options->force
               ? ps_checkpoint_cleanup(root->space, &local)
               : ps_checkpoint(root->space, &local);
    if (rc != LC_OK) {
      first_rc = rc;
      first = local;
      lc_error_init(&local);
    }
  }
  ps_namespace_release(ns);
  if (first_rc != LC_OK && e) {
    lc_error_cleanup(e);
    *e = first;
    lc_error_init(&first);
  }
  lc_error_cleanup(&local);
  lc_error_cleanup(&first);
  return first_rc;
}
