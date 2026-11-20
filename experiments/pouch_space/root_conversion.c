#include "collection_transform.h"
#include "control.h"
#include "lc_api_internal.h"
#include "lc_collection_catalog_record.h"
#include "lc_pouch_internal.h"
#include "namespace.h"
#include "query_index.h"
#include "root_internal.h"
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define INTENT_BYTES 192U
typedef struct conversion_item {
  char *name;
  ps_catalog_info info;
} conversion_item;
typedef struct conversion_items {
  conversion_item *items;
  size_t count, capacity;
  const lc_allocator *allocator;
} conversion_items;
typedef struct root_conversion {
  ps_root *root;
  lc_pouch *pouch;
  lc_pouch_crypto *target;
  const ps_root_options *options;
  const char *path;
  char key_id[80];
  uint64_t source, policy, phase, ns, completed, active;
  ps_ref head, intent;
} root_conversion;
#ifdef LOCKDC_TEST_BUILD
int (*ps_root_conversion_test_hook)(void *, const char *, lc_error *);
void *ps_root_conversion_test_context;
#endif
static int boundary(const char *event, lc_error *e) {
#ifdef LOCKDC_TEST_BUILD
  if (ps_root_conversion_test_hook)
    return ps_root_conversion_test_hook(ps_root_conversion_test_context, event,
                                        e);
#endif
  (void)event;
  (void)e;
  return LC_OK;
}
static int failure(lc_error *e, int code, const char *message) {
  return lc_error_set(e, code, 0L, message, NULL,
                      code == LC_ERR_PROTOCOL ? "pouch_root_corrupt" : NULL,
                      "pouch");
}
static uint64_t get(const unsigned char *p) {
  uint64_t n = 0;
  size_t i;
  for (i = 0; i < 8U; ++i)
    n |= (uint64_t)p[i] << (8U * i);
  return n;
}
static void put(unsigned char *p, uint64_t n) {
  size_t i;
  for (i = 0; i < 8U; ++i)
    p[i] = (unsigned char)(n >> (8U * i));
}
static ps_key intent_key(void) {
  ps_key key = {0, 2, PS_CONTROL};
  return key;
}
static int same_ref(const ps_ref *a, const ps_ref *b) {
  return a->incarnation == b->incarnation &&
         !memcmp(a->container, b->container, sizeof(a->container));
}
static int store(root_conversion *c, lc_error *e) {
  unsigned char bytes[INTENT_BYTES];
  ps_writer *writer = NULL;
  int rc;
  memset(bytes, 0, sizeof(bytes));
  memcpy(bytes, "LCPSRCVT", 8U);
  put(bytes + 8U, c->policy);
  put(bytes + 16U, c->source);
  put(bytes + 24U, c->phase);
  put(bytes + 32U, c->ns);
  put(bytes + 40U, c->completed);
  put(bytes + 48U, c->active);
  put(bytes + 56U, c->head.incarnation);
  memcpy(bytes + 64U, c->head.container, 16U);
  put(bytes + 80U, lc_pouch_crypto_enabled(c->root->crypto));
  memcpy(bytes + 96U, c->key_id, sizeof(c->key_id));
  put(bytes + 184U, crc32(0L, bytes, 184U));
  rc = boundary("before-progress", e);
  if (rc == LC_OK)
    rc = ps_begin(c->root->space, NULL, intent_key(), &writer, e);
  if (rc == LC_OK)
    rc = ps_write(writer, bytes, sizeof(bytes), e);
  if (rc == LC_OK)
    rc = ps_publish(writer, &c->intent, e);
  ps_writer_close(writer);
  if (rc == LC_OK)
    rc = boundary("progress", e);
  return rc;
}
static int load(root_conversion *c, int *found, lc_error *e) {
  unsigned char bytes[INTENT_BYTES], zero[16] = {0};
  ps_reader *reader = NULL;
  uint64_t length;
  int rc = ps_value_info(c->root->space, intent_key(), &c->intent, &length,
                         found, e);
  if (rc != LC_OK || !*found)
    return rc;
  if (length != sizeof(bytes))
    return failure(e, LC_ERR_PROTOCOL,
                   "root conversion intent length is invalid");
  rc = ps_read_open(c->root->space, intent_key(), &c->intent, &reader, e);
  if (rc == LC_OK)
    rc = ps_read_at(reader, 0U, bytes, sizeof(bytes), e);
  ps_reader_close(reader);
  if (rc != LC_OK)
    return rc;
  if (memcmp(bytes, "LCPSRCVT", 8U) || get(bytes + 8U) > 1U ||
      get(bytes + 16U) > 1U || get(bytes + 8U) == get(bytes + 16U) ||
      get(bytes + 24U) > 2U || get(bytes + 88U) || get(bytes + 176U) ||
      get(bytes + 80U) != (uint64_t)lc_pouch_crypto_enabled(c->root->crypto) ||
      memcmp(bytes + 96U, c->key_id, sizeof(c->key_id)) ||
      get(bytes + 184U) != (uint64_t)crc32(0L, bytes, 184U))
    return failure(e, LC_ERR_PROTOCOL, "root conversion intent is invalid");
  c->policy = get(bytes + 8U);
  c->source = get(bytes + 16U);
  c->phase = get(bytes + 24U);
  c->ns = get(bytes + 32U);
  c->completed = get(bytes + 40U);
  c->active = get(bytes + 48U);
  c->head.incarnation = get(bytes + 56U);
  memcpy(c->head.container, bytes + 64U, 16U);
  if ((c->phase < 2U &&
       c->source !=
           (uint64_t)lc_pouch_crypto_compression_enabled(c->root->crypto)) ||
      (!c->ns && (c->phase == 1U || c->completed || c->active)) ||
      (c->phase != 1U && (c->completed || c->active)) ||
      (c->active && c->active <= c->completed) ||
      (!!c->active != !!c->head.incarnation) ||
      (!!c->active != !!memcmp(c->head.container, zero, sizeof(zero))))
    return failure(e, LC_ERR_PROTOCOL,
                   "root conversion cursor is inconsistent");
  return LC_OK;
}
static void items_clear(conversion_items *items) {
  size_t i;
  for (i = 0; i < items->count; ++i)
    lc_free_with_allocator(items->allocator, items->items[i].name);
  lc_free_with_allocator(items->allocator, items->items);
}
static int capture(void *context, const void *name, size_t length,
                   const ps_catalog_info *info, lc_error *e) {
  conversion_items *items = context;
  conversion_item *grown, *item;
  size_t capacity;
  if (!info->id || !length || memchr(name, 0, length))
    return failure(e, LC_ERR_PROTOCOL,
                   "conversion catalog identity is invalid");
  if (items->count == items->capacity) {
    capacity = items->capacity ? items->capacity * 2U : 8U;
    if (capacity < items->capacity || capacity > (size_t)-1 / sizeof(*grown))
      return failure(e, LC_ERR_NOMEM, "conversion catalog is too large");
    grown = lc_realloc_with_allocator(items->allocator, items->items,
                                      capacity * sizeof(*grown));
    if (!grown)
      return failure(e, LC_ERR_NOMEM, "cannot capture conversion catalog");
    items->items = grown;
    items->capacity = capacity;
  }
  item = &items->items[items->count];
  item->name = lc_alloc_with_allocator(items->allocator, length + 1U);
  if (!item->name)
    return failure(e, LC_ERR_NOMEM, "cannot capture conversion name");
  memcpy(item->name, name, length);
  item->name[length] = 0;
  item->info = *info;
  ++items->count;
  return LC_OK;
}
static int item_order(const void *a, const void *b) {
  const conversion_item *x = a, *y = b;
  return x->info.id < y->info.id ? -1 : x->info.id != y->info.id;
}
static int items_load(root_conversion *c, uint64_t owner, unsigned int kind,
                      conversion_items *items, lc_error *e) {
  size_t i;
  int rc;
  memset(items, 0, sizeof(*items));
  items->allocator = &c->root->crypto_allocator;
  rc = boundary("before-catalog", e);
  if (rc == LC_OK)
    rc = ps_catalog_visit(c->root->space, owner, kind, capture, items, e);
  if (rc == LC_OK && items->count > 1U)
    qsort(items->items, items->count, sizeof(*items->items), item_order);
  for (i = 1U; rc == LC_OK && i < items->count; ++i)
    if (items->items[i - 1U].info.id == items->items[i].info.id)
      rc = failure(e, LC_ERR_PROTOCOL, "conversion catalog IDs are duplicated");
  if (rc == LC_OK)
    rc = boundary("catalog", e);
  return rc;
}
static int collection_id(root_conversion *c, const conversion_item *item,
                         char *id, lc_error *e) {
  ps_collection_segment *identity = NULL;
  ps_owned_status status;
  unsigned char bytes[LC_COLLECTION_CATALOG_BYTES];
  char name[LC_COLLECTION_NAME_BYTES];
  struct iovec part;
  int rc;
  if (!item->info.stream.owner || item->info.stream.key ||
      item->info.stream.kind != PS_STREAM)
    return failure(e, LC_ERR_PROTOCOL,
                   "conversion collection target is invalid");
  rc = ps_collection_segment_open(c->root->space, item->info.stream, 0,
                                  &item->info.target, NULL, NULL, &identity, e);
  if (rc == LC_OK)
    rc = ps_collection_segment_status(identity, &status, NULL, e);
  if (rc == LC_OK && (!status.sealed || status.tail != sizeof(bytes)))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "conversion collection identity is incomplete");
  part.iov_base = bytes;
  part.iov_len = sizeof(bytes);
  if (rc == LC_OK)
    rc = lc_pouch_record_io_read(ps_collection_segment_io(identity), 0U, &part,
                                 1, e);
  if (rc == LC_OK)
    rc = lc_collection_catalog_decode(bytes, id, name, e);
  if (rc == LC_OK && strcmp(name, item->name))
    rc = failure(e, LC_ERR_PROTOCOL, "conversion collection name has changed");
  ps_collection_segment_close(identity);
  return rc;
}
static int collection(root_conversion *c, const conversion_item *item,
                      lc_error *e) {
  char id[LC_XID_STRING_SIZE];
  ps_ref current;
  ps_key key;
  uint64_t bytes;
  ps_catalog_view *anchor = NULL;
  ps_collection_conversion *work = NULL;
  ps_collection_transform *transform = NULL;
  ps_collection_transform_result result;
  int done = 0, selected = 0;
  int rc = collection_id(c, item, id, e);
  key.owner = item->info.stream.owner;
  key.key = 1U;
  key.kind = PS_CONTROL;
  if (rc == LC_OK)
    rc = ps_value_info(c->root->space, key, &current, &bytes, &selected, e);
  if (rc == LC_OK && (!selected || bytes != LC_COLLECTION_HEAD_BYTES))
    rc = failure(e, LC_ERR_PROTOCOL, "conversion collection has no head");
  if (rc == LC_OK && c->active == item->info.id) {
    if (memcmp(current.container, c->head.container, sizeof(current.container)))
      return failure(
          e, LC_ERR_PROTOCOL,
          "conversion collection cursor belongs to another container");
    rc = ps_catalog_get(c->root->space, item->info.stream.owner,
                        PS_CATALOG_CONVERSION, NULL, 0U, &anchor, e);
    selected = rc == LC_OK && !anchor && !same_ref(&current, &c->head);
    ps_catalog_view_close(anchor);
  } else if (rc == LC_OK) {
    c->active = item->info.id;
    c->head = current;
    rc = store(c, e);
    selected = 0;
  }
  if (rc == LC_OK && !selected)
    rc = ps_collection_conversion_open(c->root->space, item->info.stream.owner,
                                       c->target, 1, &work, e);
  if (rc == LC_OK && !selected)
    rc = ps_collection_transform_open(c->root, c->pouch, c->target, work, id,
                                      65536U, &transform, e);
  while (rc == LC_OK && !selected && !done)
    rc = ps_collection_transform_step(transform, 256U, &done, &result, e);
  ps_collection_transform_close(transform);
  ps_collection_conversion_close(work);
  if (rc == LC_OK)
    rc = boundary("collection-selected", e);
  if (rc == LC_OK) {
    c->completed = item->info.id;
    c->active = 0U;
    memset(&c->head, 0, sizeof(c->head));
    rc = store(c, e);
  }
  return rc;
}
static int convert_namespace(root_conversion *c, conversion_item *item,
                             lc_error *e) {
  ps_namespace *ns = NULL;
  ps_namespace_conversion *work = NULL;
  ps_namespace_conversion_result result = {0};
  conversion_items collections;
  size_t i;
  int active_found, completed_found, rc;
  rc = ps_root_namespace_acquire(c->root, item->name, 0, &ns, e);
  if (rc == LC_OK &&
      (ns->identity.owner != item->info.id ||
       !same_ref(&ns->identity.incarnation, &item->info.revision)))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "conversion namespace identity has changed");
  if (rc == LC_OK && c->ns != item->info.id) {
    c->ns = item->info.id;
    c->phase = c->completed = c->active = 0U;
    memset(&c->head, 0, sizeof(c->head));
    rc = store(c, e);
  }
  if (rc == LC_OK && !c->phase)
    rc = ps_namespace_conversion_open(ns, c->target, &work, e);
  while (rc == LC_OK && !c->phase && !result.finished) {
    rc = ps_namespace_conversion_step(work, 32U, 32U, &result, e);
    if (rc == LC_OK && result.waiting)
      rc = failure(e, LC_ERR_PENDING,
                   "root conversion has pinned namespace work");
  }
  ps_namespace_conversion_close(work);
  if (rc == LC_OK && !c->phase)
    rc = boundary("namespace", e);
  if (rc == LC_OK && !c->phase)
    rc = ps_query_artifacts_reclaim(c->root->space, ns->identity.owner, e);
  if (rc == LC_OK && !c->phase) {
    c->phase = 1U;
    rc = store(c, e);
  }
  memset(&collections, 0, sizeof(collections));
  active_found = !c->active;
  completed_found = !c->completed;
  if (rc == LC_OK)
    rc = items_load(c, ns->identity.owner, PS_CATALOG_COLLECTION, &collections,
                    e);
  for (i = 0; rc == LC_OK && i < collections.count; ++i) {
    if (collections.items[i].info.id == c->active)
      active_found = 1;
    if (collections.items[i].info.id == c->completed)
      completed_found = 1;
  }
  if (rc == LC_OK && (!active_found || !completed_found))
    rc = failure(e, LC_ERR_PROTOCOL,
                 "root conversion collection cursor is missing");
  for (i = 0; rc == LC_OK && i < collections.count; ++i)
    if (collections.items[i].info.id > c->completed)
      rc = collection(c, &collections.items[i], e);
  items_clear(&collections);
  ps_namespace_release(ns);
  return rc;
}
static int execute(root_conversion *c, lc_error *e) {
  lc_pouch_crypto_open_options crypto = c->options->crypto;
  lc_pouch_open_options logical = {0};
  conversion_items namespaces;
  uint64_t reclaimed, skipped;
  size_t i;
  int found, rc;
  crypto.compression_enabled = (int)c->policy;
  crypto.generate_key_file = 0;
  rc = boundary("before-setup", e);
  if (rc == LC_OK)
    rc = lc_pouch_crypto_open(&c->root->crypto_allocator, &crypto, &c->target,
                              NULL, e);
  logical.background_compaction_enabled_set =
      logical.query_indexing_enabled_set = 1;
  logical.compression = "none";
  logical.durable_sync = c->options->storage.durable_sync;
  if (rc == LC_OK)
    rc = ps_root_crypto_matches(c->root, c->target, e);
  if (rc == LC_OK)
    rc = lc_pouch_runtime_open(c->path, &c->root->crypto_allocator, &logical,
                               &c->pouch, e);
  if (rc == LC_OK)
    c->pouch->crypto = c->root->crypto;
  if (rc == LC_OK)
    rc = boundary("setup", e);
  memset(&namespaces, 0, sizeof(namespaces));
  if (rc == LC_OK && c->phase < 2U)
    rc = items_load(c, 0U, PS_CATALOG_NAMESPACE, &namespaces, e);
  found = !c->ns;
  for (i = 0; rc == LC_OK && i < namespaces.count; ++i)
    if (namespaces.items[i].info.id == c->ns)
      found = 1;
  if (rc == LC_OK && c->phase < 2U && !found)
    rc = failure(e, LC_ERR_PROTOCOL,
                 "root conversion namespace cursor is missing");
  for (i = 0; rc == LC_OK && i < namespaces.count; ++i)
    if (namespaces.items[i].info.id >= c->ns)
      rc = convert_namespace(c, &namespaces.items[i], e);
  items_clear(&namespaces);
  if (rc == LC_OK && c->phase < 2U) {
    c->phase = 2U;
    c->completed = c->active = 0U;
    memset(&c->head, 0, sizeof(c->head));
    rc = store(c, e);
  }
  if (rc == LC_OK)
    rc = boundary("before-cleanup", e);
  if (rc == LC_OK)
    rc = ps_root_reclaim_orphans(c->root, &reclaimed, &skipped, e);
  if (rc == LC_OK)
    rc = boundary("cleanup", e);
  if (rc == LC_OK)
    rc = boundary("before-checkpoint", e);
  if (rc == LC_OK)
    rc = ps_checkpoint(c->root->space, e);
  if (rc == LC_OK)
    rc = boundary("checkpoint", e);
  if (rc == LC_OK)
    rc = boundary("before-policy", e);
  if (rc == LC_OK && c->policy != (uint64_t)lc_pouch_crypto_compression_enabled(
                                      c->root->crypto))
    rc = ps_root_config_write(c->root, c->target, e);
  if (rc == LC_OK)
    rc = boundary("policy", e);
  if (rc == LC_OK)
    rc = boundary("before-complete", e);
  if (rc == LC_OK)
    rc = ps_value_remove(c->root->space, intent_key(), &c->intent, e);
  if (rc == LC_OK)
    rc = boundary("complete", e);
  if (c->pouch)
    c->pouch->crypto = NULL;
  lc_pouch_close(c->pouch);
  c->pouch = NULL;
  if (rc == LC_OK) {
    lc_pouch_crypto_close(c->root->crypto);
    c->root->crypto = c->target;
    c->target = NULL;
  }
  lc_pouch_crypto_close(c->target);
  c->target = NULL;
  return rc;
}
int ps_root_conversion_run(ps_root *root, const char *path,
                           const ps_root_options *options, lc_error *e) {
  root_conversion c;
  char *key_id = NULL;
  int found, rc;
  memset(&c, 0, sizeof(c));
  c.root = root;
  c.options = options;
  c.path = path;
  rc = lc_pouch_crypto_enabled(root->crypto)
           ? lc_pouch_crypto_key_id(root->crypto, &key_id, e)
           : LC_OK;
  if (rc == LC_OK && key_id) {
    if (strlen(key_id) >= sizeof(c.key_id))
      rc = failure(e, LC_ERR_PROTOCOL,
                   "root conversion key identity is too long");
    else
      strcpy(c.key_id, key_id);
  }
  lc_free_with_allocator(NULL, key_id);
  if (rc == LC_OK)
    rc = load(&c, &found, e);
  if (rc == LC_OK &&
      ps_coordination_shared(ps_space_coordination(root->space))) {
    if (found || (options->compression_explicit &&
                  !!options->crypto.compression_enabled !=
                      lc_pouch_crypto_compression_enabled(root->crypto)))
      return lc_error_set(
          e, LC_ERR_UNSUPPORTED, 0,
          "container codec conversion requires an exclusive open", NULL,
          "pouch_root_conversion_exclusive", "pouch");
    return LC_OK;
  }
  if (rc == LC_OK && found)
    rc = execute(&c, e);
  if (rc == LC_OK && options->compression_explicit &&
      !!options->crypto.compression_enabled !=
          lc_pouch_crypto_compression_enabled(root->crypto)) {
    c.source = lc_pouch_crypto_compression_enabled(root->crypto);
    c.policy = !c.source;
    c.phase = c.ns = c.completed = c.active = 0U;
    memset(&c.head, 0, sizeof(c.head));
    rc = store(&c, e);
    if (rc == LC_OK)
      rc = execute(&c, e);
  }
  return rc;
}
