#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <stdlib.h>
#include <string.h>

void lc_pouch_index_result_key_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *list) {
  size_t index;

  if (list == NULL) {
    return;
  }
  for (index = 0U; index < list->count; ++index) {
    lc_free_with_allocator(allocator, list->items[index].key_hex);
  }
  lc_free_with_allocator(allocator, list->items);
  memset(list, 0, sizeof(*list));
}

static int
lc_pouch_index_result_key_list_reserve(const lc_allocator *allocator,
                                       lc_pouch_index_result_key_list *list,
                                       size_t needed, lc_error *error) {
  lc_pouch_index_result_key *next_items;
  size_t next_capacity;

  if (list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index result list reserve requires list", NULL,
                        NULL, NULL);
  }
  if (needed <= list->capacity) {
    return LC_OK;
  }
  next_capacity = list->capacity == 0U ? 16U : list->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index result list exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_result_key *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index result list", NULL,
                        NULL, NULL);
  }
  if (list->items != NULL) {
    memcpy(next_items, list->items, list->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, list->items);
  }
  memset(next_items + list->count, 0,
         (next_capacity - list->count) * sizeof(*next_items));
  list->items = next_items;
  list->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_result_key_list_add(const lc_allocator *allocator,
                                       lc_pouch_index_result_key_list *list,
                                       const char *key_hex,
                                       unsigned long doc_id, uint64_t version,
                                       uint64_t bytes, int has_query_hidden,
                                       int query_hidden, size_t value_index,
                                       lc_error *error) {
  lc_pouch_index_result_key *item;
  int rc;

  if (list == NULL || key_hex == NULL || key_hex[0] == '\0') {
    return LC_OK;
  }
  rc = lc_pouch_index_result_key_list_reserve(allocator, list, list->count + 1U,
                                              error);
  if (rc != LC_OK) {
    return rc;
  }
  item = &list->items[list->count];
  memset(item, 0, sizeof(*item));
  item->key_hex = lc_strdup_with_allocator(allocator, key_hex);
  if (item->key_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index result key", NULL, NULL,
                        NULL);
  }
  item->doc_id = doc_id;
  item->version = version;
  item->bytes = bytes;
  item->has_query_hidden = has_query_hidden ? 1 : 0;
  item->query_hidden = query_hidden ? 1 : 0;
  item->value_index = value_index;
  ++list->count;
  return LC_OK;
}

static int lc_pouch_index_result_key_compare(const void *left,
                                             const void *right) {
  const lc_pouch_index_result_key *a;
  const lc_pouch_index_result_key *b;
  int cmp;

  a = (const lc_pouch_index_result_key *)left;
  b = (const lc_pouch_index_result_key *)right;
  if (a->doc_id < b->doc_id) {
    return -1;
  }
  if (a->doc_id > b->doc_id) {
    return 1;
  }
  if (a->key_hex == NULL || b->key_hex == NULL) {
    return a->key_hex == b->key_hex ? 0 : (a->key_hex == NULL ? -1 : 1);
  }
  cmp = strcmp(a->key_hex, b->key_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (a->value_index < b->value_index) {
    return -1;
  }
  if (a->value_index > b->value_index) {
    return 1;
  }
  return 0;
}

int lc_pouch_index_result_key_list_sort_compact_docids(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *list,
    lc_error *error) {
  size_t index;
  size_t write_index;

  (void)error;
  if (list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index result list compaction requires list",
                        NULL, NULL, NULL);
  }
  if (list->count <= 1U) {
    return LC_OK;
  }
  qsort(list->items, list->count, sizeof(list->items[0]),
        lc_pouch_index_result_key_compare);
  write_index = 0U;
  for (index = 0U; index < list->count; ++index) {
    if (write_index > 0U &&
        list->items[index].doc_id == list->items[write_index - 1U].doc_id) {
      lc_free_with_allocator(allocator, list->items[index].key_hex);
      memset(&list->items[index], 0, sizeof(list->items[index]));
      continue;
    }
    if (write_index != index) {
      list->items[write_index] = list->items[index];
      memset(&list->items[index], 0, sizeof(list->items[index]));
    }
    ++write_index;
  }
  list->count = write_index;
  return LC_OK;
}

void lc_pouch_index_result_docid_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *list) {
  if (list == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, list->items);
  memset(list, 0, sizeof(*list));
}

static int
lc_pouch_index_result_docid_list_reserve(const lc_allocator *allocator,
                                         lc_pouch_index_result_docid_list *list,
                                         size_t needed, lc_error *error) {
  lc_pouch_index_result_docid *next_items;
  size_t next_capacity;

  if (list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index result docID list reserve requires list",
                        NULL, NULL, NULL);
  }
  if (needed <= list->capacity) {
    return LC_OK;
  }
  next_capacity = list->capacity == 0U ? 16U : list->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index result docID list exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_result_docid *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index result docID list",
                        NULL, NULL, NULL);
  }
  if (list->items != NULL) {
    memcpy(next_items, list->items, list->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, list->items);
  }
  memset(next_items + list->count, 0,
         (next_capacity - list->count) * sizeof(*next_items));
  list->items = next_items;
  list->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_result_docid_list_add(const lc_allocator *allocator,
                                         lc_pouch_index_result_docid_list *list,
                                         unsigned long doc_id,
                                         size_t value_index, lc_error *error) {
  lc_pouch_index_result_docid *item;
  int rc;

  if (list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index result docID add requires list", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_result_docid_list_reserve(allocator, list,
                                                list->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  item = &list->items[list->count++];
  item->doc_id = doc_id;
  item->value_index = value_index;
  return LC_OK;
}

static int lc_pouch_index_result_docid_compare(const void *left,
                                               const void *right) {
  const lc_pouch_index_result_docid *a;
  const lc_pouch_index_result_docid *b;

  a = (const lc_pouch_index_result_docid *)left;
  b = (const lc_pouch_index_result_docid *)right;
  if (a->doc_id < b->doc_id) {
    return -1;
  }
  if (a->doc_id > b->doc_id) {
    return 1;
  }
  if (a->value_index < b->value_index) {
    return -1;
  }
  if (a->value_index > b->value_index) {
    return 1;
  }
  return 0;
}

int lc_pouch_index_result_docid_list_sort_compact(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *list,
    lc_error *error) {
  size_t index;
  size_t write_index;
  int sorted_unique;

  (void)allocator;
  if (list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index result docID compaction requires list",
                        NULL, NULL, NULL);
  }
  if (list->count <= 1U) {
    return LC_OK;
  }
  sorted_unique = 1;
  for (index = 1U; index < list->count; ++index) {
    if (list->items[index - 1U].doc_id >= list->items[index].doc_id) {
      sorted_unique = 0;
      break;
    }
  }
  if (sorted_unique) {
    return LC_OK;
  }
  qsort(list->items, list->count, sizeof(list->items[0]),
        lc_pouch_index_result_docid_compare);
  write_index = 0U;
  for (index = 0U; index < list->count; ++index) {
    if (write_index > 0U &&
        list->items[index].doc_id == list->items[write_index - 1U].doc_id) {
      continue;
    }
    if (write_index != index) {
      list->items[write_index] = list->items[index];
    }
    ++write_index;
  }
  list->count = write_index;
  return LC_OK;
}

void lc_pouch_index_result_row_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *list) {
  size_t index;

  if (list == NULL) {
    return;
  }
  for (index = 0U; index < list->count; ++index) {
    if (list->items[index].owns_key) {
      lc_free_with_allocator(allocator, list->items[index].key);
    }
    if (list->items[index].owns_key_hex) {
      lc_free_with_allocator(allocator, list->items[index].key_hex);
    }
  }
  lc_free_with_allocator(allocator, list->items);
  memset(list, 0, sizeof(*list));
}

static int
lc_pouch_index_result_row_list_reserve(const lc_allocator *allocator,
                                       lc_pouch_index_result_row_list *list,
                                       size_t needed, lc_error *error) {
  lc_pouch_index_result_row *next_items;
  size_t next_capacity;

  if (list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index result row list reserve requires list",
                        NULL, NULL, NULL);
  }
  if (needed <= list->capacity) {
    return LC_OK;
  }
  next_capacity = list->capacity == 0U ? 16U : list->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index result row list exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_result_row *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index result row list", NULL,
                        NULL, NULL);
  }
  if (list->items != NULL) {
    memcpy(next_items, list->items, list->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, list->items);
  }
  memset(next_items + list->count, 0,
         (next_capacity - list->count) * sizeof(*next_items));
  list->items = next_items;
  list->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_result_row_list_add(const lc_allocator *allocator,
                                       lc_pouch_index_result_row_list *list,
                                       const char *key, const char *key_hex,
                                       unsigned long doc_id, uint64_t version,
                                       uint64_t bytes, int has_query_hidden,
                                       int query_hidden, size_t value_index,
                                       lc_error *error) {
  lc_pouch_index_result_row *item;
  int rc;

  if (list == NULL || key == NULL || key[0] == '\0') {
    return LC_OK;
  }
  rc = lc_pouch_index_result_row_list_reserve(allocator, list, list->count + 1U,
                                              error);
  if (rc != LC_OK) {
    return rc;
  }
  item = &list->items[list->count];
  memset(item, 0, sizeof(*item));
  item->key = lc_strdup_with_allocator(allocator, key);
  if (item->key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index result row key", NULL,
                        NULL, NULL);
  }
  item->owns_key = 1;
  if (key_hex != NULL) {
    item->key_hex = lc_strdup_with_allocator(allocator, key_hex);
    if (item->key_hex == NULL) {
      lc_free_with_allocator(allocator, item->key);
      memset(item, 0, sizeof(*item));
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch index result row key hex",
                          NULL, NULL, NULL);
    }
    item->owns_key_hex = 1;
  }
  item->doc_id = doc_id;
  item->version = version;
  item->bytes = bytes;
  item->has_query_hidden = has_query_hidden ? 1 : 0;
  item->query_hidden = query_hidden ? 1 : 0;
  item->value_index = value_index;
  ++list->count;
  return LC_OK;
}
