#include "collection_reclaim.h"
#include "collections.h"
#include "lc_api_internal.h"
#include "lc_collection_storage.h"
#include "owned.h"
#include "query_index.h"
#include <string.h>

static int corrupt(lc_error *e) {
  return lc_error_set(e, LC_ERR_PROTOCOL, 0L,
                      "collection reclamation catalog has an invalid identity",
                      NULL, "collection_corrupt", "pouch");
}
static uint64_t number(const unsigned char *bytes) {
  uint64_t value = 0U;
  size_t i;
  for (i = 0U; i < 8U; ++i)
    value |= (uint64_t)bytes[i] << (8U * i);
  return value;
}
static int capture(void *context, const void *name, size_t length,
                   const ps_catalog_info *info, lc_error *e) {
  ps_collection_resources *list = context;
  ps_collection_resource *item, *grown;
  const unsigned char *bytes = name;
  uint64_t id, marked;
  size_t i, next;
  if (length != (list->kind == PS_CATALOG_CONTROL ? 16U : 8U))
    return corrupt(e);
  id = number(bytes);
  marked = length == 16U ? number(bytes + 8U) : 0U;
  if (!(id & ~LC_COLLECTION_SNAPSHOT_BIT) ||
      (length == 8U &&
       list->kind != (id & LC_COLLECTION_SNAPSHOT_BIT ? PS_CATALOG_SNAPSHOT
                                                      : PS_CATALOG_SEGMENT)) ||
      (length == 16U && (!marked || marked > UINT64_C(0x7fffffffffffffff))) ||
      info->stream.owner != list->owner || info->stream.key != id ||
      info->stream.kind != PS_STREAM)
    return corrupt(e);
  for (i = 0U; i < list->count; ++i)
    if (list->items[i].id == id &&
        (list->items[i].binding.target.incarnation !=
             info->target.incarnation ||
         memcmp(list->items[i].binding.target.container, info->target.container,
                sizeof(info->target.container)) ||
         (marked && list->items[i].marked)))
      return corrupt(e);
  if (list->count == list->capacity) {
    next = list->capacity ? list->capacity * 2U : 8U;
    if (next < list->capacity || next > (size_t)-1 / sizeof(*grown))
      return lc_error_set(e, LC_ERR_NOMEM, 0L,
                          "collection reclamation metadata is too large", NULL,
                          NULL, "pouch");
    grown = lc_realloc_with_allocator(NULL, list->items, next * sizeof(*grown));
    if (!grown)
      return lc_error_set(e, LC_ERR_NOMEM, 0L,
                          "cannot capture collection reclamation metadata",
                          NULL, NULL, "pouch");
    list->items = grown;
    list->capacity = next;
  }
  item = &list->items[list->count++];
  memset(item, 0, sizeof(*item));
  memcpy(item->name, name, length);
  item->length = length;
  item->kind = list->kind;
  item->id = id;
  item->marked = marked;
  item->binding = *info;
  return LC_OK;
}
int ps_collection_resources_capture(ps_space *storage, uint64_t owner,
                                    ps_collection_resources *list,
                                    lc_error *e) {
  static const unsigned int kinds[] = {PS_CATALOG_SEGMENT, PS_CATALOG_SNAPSHOT,
                                       PS_CATALOG_CONTROL};
  size_t i;
  int rc = LC_OK;
  memset(list, 0, sizeof(*list));
  list->owner = owner;
  for (i = 0U; rc == LC_OK && i < sizeof(kinds) / sizeof(kinds[0]); ++i) {
    list->kind = kinds[i];
    rc = ps_catalog_visit(storage, owner, kinds[i], capture, list, e);
  }
  return rc;
}
void ps_collection_resources_clear(ps_collection_resources *list) {
  lc_free_with_allocator(NULL, list->items);
  memset(list, 0, sizeof(*list));
}
int ps_collection_resources_remove(ps_space *storage,
                                   ps_collection_resources *list, uint64_t id,
                                   int markers_only, lc_error *e) {
  ps_collection_resource *item;
  size_t i;
  unsigned int marker;
  int rc = LC_OK;
  for (marker = markers_only ? 1U : 0U; rc == LC_OK && marker < 2U; ++marker)
    for (i = 0U; rc == LC_OK && i < list->count; ++i) {
      item = &list->items[i];
      if (item->id != id || !item->length ||
          (item->kind == PS_CATALOG_CONTROL) != (int)marker)
        continue;
      rc = ps_catalog_remove(storage, list->owner, item->kind, item->name,
                             item->length, &item->binding.revision, e);
      if (rc == LC_OK)
        item->length = 0U;
    }
  return rc;
}
int ps_collection_reclaim_children(ps_space *storage, uint64_t owner,
                                   lc_error *e) {
  ps_collection_resources list;
  ps_owned_head *head;
  ps_owned_status status;
  ps_key control;
  ps_reader *value = NULL;
  size_t i, j;
  int rc = ps_collection_resources_capture(storage, owner, &list, e);
  for (i = 0U; rc == LC_OK && i < list.count; ++i) {
    for (j = 0U; j < i && list.items[j].id != list.items[i].id; ++j) {
    }
    if (j != i)
      continue;
    head = NULL;
    rc = ps_owned_open(storage, NULL, list.items[i].binding.stream, 0,
                       &list.items[i].binding.target, &head, &status, e);
    if (rc == LC_OK)
      rc = ps_collection_resources_remove(storage, &list, list.items[i].id, 0,
                                          e);
    if (rc == LC_OK)
      rc = ps_owned_retire(head, e);
    ps_owned_close(head);
  }
  ps_collection_resources_clear(&list);
  if (rc == LC_OK)
    rc = ps_query_artifacts_reclaim(storage, owner, e);
  control.owner = owner;
  control.key = 1U;
  control.kind = PS_CONTROL;
  if (rc == LC_OK) {
    rc = ps_read_open(storage, control, NULL, &value, e);
    if (rc == LC_ERR_INVALID) {
      /* A prior pass may have removed the head control before identity
       * retirement failed. Unpublished construction can also have no head. */
      lc_error_cleanup(e);
      rc = LC_OK;
    } else if (rc == LC_OK)
      rc = ps_delete(storage, control, e);
  }
  ps_reader_close(value);
  return rc;
}
