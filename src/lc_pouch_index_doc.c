#include "lc_pouch_index.h"

#include "lc_api_internal.h"
#include "lc_intcompat.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_DOC_TABLE_GENERATION_FORMAT "pouch-doc-table-generation"
#define LC_POUCH_INDEX_DOC_TABLE_GENERATION_VERSION 1UL

void lc_pouch_index_docid_set_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_docid_set *set) {
  if (set == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_index_docid_set_reserve(lc_pouch_index_docid_set *set,
                                            size_t needed,
                                            const lc_allocator *allocator,
                                            lc_error *error) {
  unsigned long *next_items;
  size_t next_capacity;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set reserve requires set", NULL,
                        NULL, NULL);
  }
  if (needed <= set->capacity) {
    return LC_OK;
  }
  next_capacity = set->capacity == 0U ? 16U : set->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index docID set exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (unsigned long *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index docID set", NULL, NULL,
                        NULL);
  }
  if (set->items != NULL) {
    memcpy(next_items, set->items, set->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, set->items);
  }
  set->items = next_items;
  set->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_sorted_unique(lc_pouch_index_docid_set *set,
                                                  unsigned long doc_id,
                                                  int *added,
                                                  const lc_allocator *allocator,
                                                  lc_error *error) {
  int rc;

  if (set == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set append requires set and added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  if (set->count > 0U) {
    if (doc_id < set->items[set->count - 1U]) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index docID set append requires sorted input",
                          NULL, NULL, "pouch");
    }
    if (doc_id == set->items[set->count - 1U]) {
      return LC_OK;
    }
  }
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count++] = doc_id;
  *added = 1;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_unique(lc_pouch_index_docid_set *set,
                                           unsigned long doc_id, int *added,
                                           const lc_allocator *allocator,
                                           lc_error *error) {
  size_t index;
  int rc;

  if (set == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set append requires set and added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  for (index = 0U; index < set->count; ++index) {
    if (set->items[index] == doc_id) {
      return LC_OK;
    }
  }
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count++] = doc_id;
  *added = 1;
  return LC_OK;
}

static int lc_pouch_index_docid_set_validate_sorted_unique(
    const lc_pouch_index_docid_set *set, const char *role, lc_error *error) {
  size_t index;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge requires inputs", NULL,
                        NULL, role);
  }
  for (index = 1U; index < set->count; ++index) {
    if (set->items[index] <= set->items[index - 1U]) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index docID set merge requires sorted unique "
                          "inputs",
                          NULL, NULL, role);
    }
  }
  return LC_OK;
}

static int lc_pouch_index_docid_set_prepare_merge(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, lc_error *error) {
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge requires output", NULL,
                        NULL, NULL);
  }
  if (out == left || out == right) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge output must not alias "
                        "inputs",
                        NULL, NULL, "pouch");
  }
  if (out->count != 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge requires empty output",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_index_docid_set_validate_sorted_unique(left, "left", error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_index_docid_set_validate_sorted_unique(right, "right", error);
}

int lc_pouch_index_docid_set_union_sorted(const lc_pouch_index_docid_set *left,
                                          const lc_pouch_index_docid_set *right,
                                          lc_pouch_index_docid_set *out,
                                          const lc_allocator *allocator,
                                          lc_error *error) {
  size_t left_index;
  size_t right_index;
  unsigned long doc_id;
  int added;
  int rc;

  rc = lc_pouch_index_docid_set_prepare_merge(left, right, out, error);
  if (rc != LC_OK) {
    return rc;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count || right_index < right->count) {
    if (right_index >= right->count ||
        (left_index < left->count &&
         left->items[left_index] < right->items[right_index])) {
      doc_id = left->items[left_index++];
    } else if (left_index >= left->count ||
               right->items[right_index] < left->items[left_index]) {
      doc_id = right->items[right_index++];
    } else {
      doc_id = left->items[left_index];
      ++left_index;
      ++right_index;
    }
    rc = lc_pouch_index_docid_set_append_sorted_unique(out, doc_id, &added,
                                                       allocator, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

int lc_pouch_index_docid_set_intersect_sorted(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, const lc_allocator *allocator,
    lc_error *error) {
  size_t left_index;
  size_t right_index;
  int added;
  int rc;

  rc = lc_pouch_index_docid_set_prepare_merge(left, right, out, error);
  if (rc != LC_OK) {
    return rc;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count && right_index < right->count) {
    if (left->items[left_index] < right->items[right_index]) {
      ++left_index;
      continue;
    }
    if (right->items[right_index] < left->items[left_index]) {
      ++right_index;
      continue;
    }
    rc = lc_pouch_index_docid_set_append_sorted_unique(
        out, left->items[left_index], &added, allocator, error);
    if (rc != LC_OK) {
      return rc;
    }
    ++left_index;
    ++right_index;
  }
  return LC_OK;
}

int lc_pouch_index_docid_set_subtract_sorted(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, const lc_allocator *allocator,
    lc_error *error) {
  size_t left_index;
  size_t right_index;
  int added;
  int rc;

  rc = lc_pouch_index_docid_set_prepare_merge(left, right, out, error);
  if (rc != LC_OK) {
    return rc;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count) {
    while (right_index < right->count &&
           right->items[right_index] < left->items[left_index]) {
      ++right_index;
    }
    if (right_index < right->count &&
        right->items[right_index] == left->items[left_index]) {
      ++left_index;
      continue;
    }
    rc = lc_pouch_index_docid_set_append_sorted_unique(
        out, left->items[left_index], &added, allocator, error);
    if (rc != LC_OK) {
      return rc;
    }
    ++left_index;
  }
  return LC_OK;
}

void lc_pouch_index_doc_table_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_doc_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    if (table->items[index].owns_key_hex) {
      lc_free_with_allocator(allocator, (char *)table->items[index].key_hex);
    }
  }
  lc_free_with_allocator(allocator, table->items);
  lc_free_with_allocator(allocator, table->owned_generation_bytes);
  memset(table, 0, sizeof(*table));
}

static int lc_pouch_index_doc_table_reserve(lc_pouch_index_doc_table *table,
                                            size_t needed,
                                            const lc_allocator *allocator,
                                            lc_error *error) {
  lc_pouch_index_doc *next_items;
  size_t next_capacity;

  if (table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table reserve requires table", NULL,
                        NULL, NULL);
  }
  if (needed <= table->capacity) {
    return LC_OK;
  }
  next_capacity = table->capacity == 0U ? 64U : table->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index doc table exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_doc *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index doc table", NULL, NULL,
                        NULL);
  }
  if (table->items != NULL) {
    memcpy(next_items, table->items, table->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, table->items);
  }
  table->items = next_items;
  table->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_doc_table_append_sorted_unique(
    lc_pouch_index_doc_table *table, const char *key_hex, uint64_t version,
    uint64_t bytes, int has_query_hidden, int query_hidden,
    unsigned long *doc_id, const lc_allocator *allocator, lc_error *error) {
  lc_pouch_index_doc *doc;
  int cmp;
  int rc;

  if (table == NULL || key_hex == NULL || key_hex[0] == '\0' ||
      doc_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table append requires table, key, and "
                        "docID output",
                        NULL, NULL, NULL);
  }
  if (table->count > 0U) {
    cmp = strcmp(key_hex, table->items[table->count - 1U].key_hex);
    if (cmp <= 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          cmp == 0 ? "pouch index doc table rejects duplicate "
                                     "keys"
                                   : "pouch index doc table append requires "
                                     "sorted keys",
                          NULL, NULL, "pouch");
    }
  }
  rc = lc_pouch_index_doc_table_reserve(table, table->count + 1U, allocator,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  *doc_id = (unsigned long)table->count;
  doc = &table->items[table->count++];
  doc->key_hex = key_hex;
  doc->version = version;
  doc->bytes = bytes;
  doc->has_query_hidden = has_query_hidden ? 1 : 0;
  doc->query_hidden = query_hidden ? 1 : 0;
  doc->owns_key_hex = 0;
  table->sorted_by_key = 1;
  return LC_OK;
}

static int lc_pouch_index_doc_table_append_unique_impl(
    lc_pouch_index_doc_table *table, const char *key_hex, uint64_t version,
    uint64_t bytes, int has_query_hidden, int query_hidden, int copy_key_hex,
    int check_unique, unsigned long *doc_id, const lc_allocator *allocator,
    lc_error *error) {
  lc_pouch_index_doc *doc;
  char *owned_key_hex;
  size_t index;
  int rc;

  if (table == NULL || key_hex == NULL || key_hex[0] == '\0' ||
      doc_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table append requires table, key, and "
                        "docID output",
                        NULL, NULL, NULL);
  }
  if (check_unique) {
    for (index = 0U; index < table->count; ++index) {
      if (strcmp(table->items[index].key_hex, key_hex) == 0) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch index doc table rejects duplicate keys",
                            NULL, NULL, "pouch");
      }
    }
  }
  rc = lc_pouch_index_doc_table_reserve(table, table->count + 1U, allocator,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  owned_key_hex = NULL;
  if (copy_key_hex) {
    owned_key_hex = lc_strdup_with_allocator(allocator, key_hex);
    if (owned_key_hex == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch index doc table key", NULL,
                          NULL, NULL);
    }
  }
  *doc_id = (unsigned long)table->count;
  doc = &table->items[table->count++];
  doc->key_hex = copy_key_hex ? owned_key_hex : key_hex;
  doc->version = version;
  doc->bytes = bytes;
  doc->has_query_hidden = has_query_hidden ? 1 : 0;
  doc->query_hidden = query_hidden ? 1 : 0;
  doc->owns_key_hex = copy_key_hex ? 1 : 0;
  table->sorted_by_key = 0;
  return LC_OK;
}

int lc_pouch_index_doc_table_append_owned(
    lc_pouch_index_doc_table *table, const char *key_hex, uint64_t version,
    uint64_t bytes, int has_query_hidden, int query_hidden,
    unsigned long *doc_id, const lc_allocator *allocator, lc_error *error) {
  return lc_pouch_index_doc_table_append_unique_impl(
      table, key_hex, version, bytes, has_query_hidden, query_hidden, 1, 0,
      doc_id, allocator, error);
}

int lc_pouch_index_doc_table_find_key_hex(const lc_pouch_index_doc_table *table,
                                          const char *key_hex,
                                          unsigned long *doc_id, int *found,
                                          lc_error *error) {
  size_t index;

  if (table == NULL || key_hex == NULL || key_hex[0] == '\0' ||
      doc_id == NULL || found == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table lookup requires table, key, "
                        "docID output, and found output",
                        NULL, NULL, NULL);
  }
  *found = 0;
  if (table->sorted_by_key) {
    size_t low;
    size_t high;
    size_t mid;
    int cmp;

    low = 0U;
    high = table->count;
    while (low < high) {
      mid = low + ((high - low) / 2U);
      cmp = strcmp(key_hex, table->items[mid].key_hex);
      if (cmp == 0) {
        *doc_id = (unsigned long)mid;
        *found = 1;
        return LC_OK;
      }
      if (cmp < 0) {
        high = mid;
      } else {
        low = mid + 1U;
      }
    }
    return LC_OK;
  }
  for (index = 0U; index < table->count; ++index) {
    if (strcmp(key_hex, table->items[index].key_hex) == 0) {
      *doc_id = (unsigned long)index;
      *found = 1;
      break;
    }
  }
  return LC_OK;
}

int lc_pouch_index_doc_table_get(const lc_pouch_index_doc_table *table,
                                 unsigned long doc_id,
                                 const lc_pouch_index_doc **doc,
                                 lc_error *error) {
  if (table == NULL || doc == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table get requires table and output",
                        NULL, NULL, NULL);
  }
  *doc = NULL;
  if (doc_id >= (unsigned long)table->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table docID is out of range", NULL,
                        NULL, "pouch");
  }
  *doc = &table->items[doc_id];
  return LC_OK;
}

static int lc_pouch_index_doc_generation_reserve(const lc_allocator *allocator,
                                                 char **bytes, size_t *capacity,
                                                 size_t length, size_t needed,
                                                 lc_error *error) {
  char *next_bytes;
  size_t next_capacity;

  if (bytes == NULL || capacity == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation reserve requires output",
                        NULL, NULL, NULL);
  }
  if (needed <= *capacity) {
    return LC_OK;
  }
  next_capacity = *capacity == 0U ? 256U : *capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch doc table generation exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch doc table generation", NULL,
                        NULL, NULL);
  }
  if (*bytes != NULL) {
    memcpy(next_bytes, *bytes, length);
    lc_free_with_allocator(allocator, *bytes);
  }
  *bytes = next_bytes;
  *capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_index_doc_generation_append(
    const lc_allocator *allocator, char **bytes, size_t *length,
    size_t *capacity, const char *data, size_t data_length, lc_error *error) {
  int rc;

  if (length == NULL || data == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation append requires data", NULL,
                        NULL, NULL);
  }
  if (data_length > (size_t)-1 - *length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch doc table generation exceeds local limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_doc_generation_reserve(
      allocator, bytes, capacity, *length, *length + data_length + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(*bytes + *length, data, data_length);
  *length += data_length;
  (*bytes)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_index_doc_generation_append_cstr(
    const lc_allocator *allocator, char **bytes, size_t *length,
    size_t *capacity, const char *text, lc_error *error) {
  return lc_pouch_index_doc_generation_append(
      allocator, bytes, length, capacity, text, strlen(text), error);
}

int lc_pouch_index_doc_table_generation_encode(
    const lc_pouch_index_doc_table *table, uint64_t index_seq,
    unsigned long row_hash, const lc_allocator *allocator, char **out_bytes,
    size_t *out_length, lc_error *error) {
  char *bytes;
  size_t length;
  size_t capacity;
  char line[256];
  char index_seq_text[32];
  size_t index;
  int written;
  int rc;

  if (table == NULL || out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation encode requires table and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  bytes = NULL;
  length = 0U;
  capacity = 0U;
  rc = lc_pouch_index_doc_generation_append_cstr(
      allocator, &bytes, &length, &capacity,
      "format=" LC_POUCH_INDEX_DOC_TABLE_GENERATION_FORMAT "\n", error);
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line), "version=%lu\n",
                       LC_POUCH_INDEX_DOC_TABLE_GENERATION_VERSION);
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation version exceeds local "
                            "limit",
                            NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    if (lc_u64_format_base10((lc_u64)index_seq, index_seq_text,
                             sizeof(index_seq_text)) < 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation index exceeds local limit",
                        NULL, NULL, NULL);
    } else {
      written = snprintf(line, sizeof(line), "index_seq=%s\n", index_seq_text);
      rc = written >= 0 && (size_t)written < sizeof(line)
               ? lc_pouch_index_doc_generation_append(allocator, &bytes,
                                                      &length, &capacity, line,
                                                      (size_t)written, error)
               : lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch doc table generation index exceeds local "
                              "limit",
                              NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line), "row_count=%lu\n",
                       (unsigned long)table->count);
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation count exceeds local "
                            "limit",
                            NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line), "row_hash=%lu\n", row_hash);
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation hash exceeds local "
                            "limit",
                            NULL, NULL, NULL);
  }
  for (index = 0U; rc == LC_OK && index < table->count; ++index) {
    const lc_pouch_index_doc *doc;
    char version_text[32];
    char bytes_text[32];

    doc = &table->items[index];
    written = snprintf(line, sizeof(line), "doc ");
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation row exceeds local "
                            "limit",
                            NULL, NULL, NULL);
    if (rc == LC_OK) {
      rc = lc_pouch_index_doc_generation_append_cstr(
          allocator, &bytes, &length, &capacity, doc->key_hex, error);
    }
    if (rc == LC_OK) {
      if (lc_u64_format_base10((lc_u64)doc->version, version_text,
                               sizeof(version_text)) < 0 ||
          lc_u64_format_base10((lc_u64)doc->bytes, bytes_text,
                               sizeof(bytes_text)) < 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch doc table generation row exceeds local limit",
                          NULL, NULL, NULL);
      } else {
        written = snprintf(line, sizeof(line), " %s %s %d %d\n", version_text,
                           bytes_text, doc->has_query_hidden ? 1 : 0,
                           doc->query_hidden ? 1 : 0);
        rc = written >= 0 && (size_t)written < sizeof(line)
                 ? lc_pouch_index_doc_generation_append(
                       allocator, &bytes, &length, &capacity, line,
                       (size_t)written, error)
                 : lc_error_set(error, LC_ERR_INVALID, 0L,
                                "pouch doc table generation row exceeds local "
                                "limit",
                                NULL, NULL, NULL);
      }
    }
  }
  if (rc == LC_OK) {
    *out_bytes = bytes;
    *out_length = length;
    bytes = NULL;
  }
  lc_free_with_allocator(allocator, bytes);
  return rc;
}

static int lc_pouch_index_doc_generation_parse_ulong(const char *text,
                                                     unsigned long *out) {
  char *end;
  unsigned long value;

  if (text == NULL || text[0] == '\0' || out == NULL) {
    return 0;
  }
  errno = 0;
  value = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0') {
    return 0;
  }
  *out = value;
  return 1;
}

static int lc_pouch_index_doc_generation_header_ulong(const char *line,
                                                      const char *prefix,
                                                      unsigned long *out) {
  size_t prefix_len;

  prefix_len = strlen(prefix);
  if (strncmp(line, prefix, prefix_len) != 0) {
    return 0;
  }
  return lc_pouch_index_doc_generation_parse_ulong(line + prefix_len, out);
}

static int lc_pouch_index_doc_generation_header_u64(const char *line,
                                                    const char *prefix,
                                                    uint64_t *out) {
  size_t prefix_len;
  lc_u64 value;

  if (line == NULL || prefix == NULL || out == NULL) {
    return 0;
  }
  prefix_len = strlen(prefix);
  if (strncmp(line, prefix, prefix_len) != 0) {
    return 0;
  }
  if (!lc_u64_parse_base10(line + prefix_len, &value)) {
    return 0;
  }
  *out = (uint64_t)value;
  return 1;
}

static char *lc_pouch_index_doc_generation_next_line(char **cursor) {
  char *line;
  char *newline;

  if (cursor == NULL || *cursor == NULL || **cursor == '\0') {
    return NULL;
  }
  line = *cursor;
  newline = strchr(line, '\n');
  if (newline == NULL) {
    *cursor = line + strlen(line);
  } else {
    *newline = '\0';
    *cursor = newline + 1;
  }
  return line;
}

static int lc_pouch_index_doc_generation_hex_token_valid(const char *token) {
  size_t index;
  unsigned char ch;

  if (token == NULL || token[0] == '\0') {
    return 0;
  }
  for (index = 0U; token[index] != '\0'; ++index) {
    ch = (unsigned char)token[index];
    if (!((ch >= (unsigned char)'0' && ch <= (unsigned char)'9') ||
          (ch >= (unsigned char)'a' && ch <= (unsigned char)'f') ||
          (ch >= (unsigned char)'A' && ch <= (unsigned char)'F'))) {
      return 0;
    }
  }
  return index % 2U == 0U;
}

static char *lc_pouch_index_doc_generation_next_token(char **cursor) {
  char *token;
  char *space;

  if (cursor == NULL || *cursor == NULL || **cursor == '\0') {
    return NULL;
  }
  token = *cursor;
  space = strchr(token, ' ');
  if (space == NULL) {
    *cursor = token + strlen(token);
  } else {
    *space = '\0';
    *cursor = space + 1U;
  }
  return token;
}

static int lc_pouch_index_doc_generation_parse_bytes(
    const lc_allocator *allocator, char *bytes, uint64_t expected_index_seq,
    unsigned long expected_row_count, unsigned long expected_row_hash,
    lc_pouch_index_doc_table *table, int *valid, lc_error *error) {
  char *cursor;
  char *line;
  uint64_t version;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long actual_count;
  uint64_t doc_bytes;
  unsigned long parsed_has_hidden;
  unsigned long parsed_hidden;
  int has_hidden;
  int hidden;
  int rc;
  unsigned long doc_id;

  if (valid == NULL || bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation parse requires bytes and "
                        "valid output",
                        NULL, NULL, NULL);
  }
  *valid = 0;
  cursor = bytes;
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      strcmp(line, "format=" LC_POUCH_INDEX_DOC_TABLE_GENERATION_FORMAT) != 0) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_u64(line, "version=", &version) ||
      version != LC_POUCH_INDEX_DOC_TABLE_GENERATION_VERSION) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_u64(line, "index_seq=", &version) ||
      version != expected_index_seq) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_ulong(line,
                                                  "row_count=", &row_count) ||
      row_count != expected_row_count) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_ulong(line,
                                                  "row_hash=", &row_hash) ||
      row_hash != expected_row_hash) {
    goto done;
  }
  actual_count = 0UL;
  while ((line = lc_pouch_index_doc_generation_next_line(&cursor)) != NULL) {
    char *key;
    char *rest;

    if (line[0] == '\0') {
      continue;
    }
    if (strncmp(line, "doc ", 4U) != 0) {
      goto done;
    }
    key = line + 4U;
    rest = strchr(key, ' ');
    if (rest == NULL || rest == key) {
      goto done;
    }
    *rest++ = '\0';
    if (!lc_pouch_index_doc_generation_hex_token_valid(key)) {
      goto done;
    }
    if (!lc_pouch_index_doc_generation_header_u64(
            lc_pouch_index_doc_generation_next_token(&rest), "", &version) ||
        !lc_pouch_index_doc_generation_header_u64(
            lc_pouch_index_doc_generation_next_token(&rest), "", &doc_bytes) ||
        !lc_pouch_index_doc_generation_parse_ulong(
            lc_pouch_index_doc_generation_next_token(&rest),
            &parsed_has_hidden) ||
        !lc_pouch_index_doc_generation_parse_ulong(
            lc_pouch_index_doc_generation_next_token(&rest), &parsed_hidden) ||
        lc_pouch_index_doc_generation_next_token(&rest) != NULL ||
        parsed_has_hidden > 1UL || parsed_hidden > 1UL) {
      goto done;
    }
    has_hidden = (int)parsed_has_hidden;
    hidden = (int)parsed_hidden;
    if (table != NULL) {
      rc = lc_pouch_index_doc_table_append_unique_impl(
          table, key, version, doc_bytes, has_hidden, hidden, 0, 1, &doc_id,
          allocator, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    ++actual_count;
  }
  *valid = actual_count == row_count;

done:
  (void)allocator;
  return LC_OK;
}

static int lc_pouch_index_doc_generation_read_file(
    const lc_allocator *allocator, const char *path, char **out_bytes,
    size_t *out_length, int *present, lc_error *error) {
  FILE *fp;
  char *bytes;
  size_t length;
  size_t capacity;
  size_t got;

  if (path == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation read requires path "
                        "and outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch doc table generation",
                        strerror(errno), NULL, NULL);
  }
  *present = 1;
  bytes = NULL;
  length = 0U;
  capacity = 0U;
  for (;;) {
    if (capacity - length <= 1U) {
      char *next_bytes;
      size_t next_capacity;

      next_capacity = capacity == 0U ? 4096U : capacity * 2U;
      if (next_capacity <= capacity || next_capacity == (size_t)-1) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch doc table generation exceeds local limit",
                            NULL, NULL, NULL);
      }
      next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
      if (next_bytes == NULL) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch doc table generation",
                            NULL, NULL, NULL);
      }
      if (bytes != NULL) {
        memcpy(next_bytes, bytes, length);
        lc_free_with_allocator(allocator, bytes);
      }
      bytes = next_bytes;
      capacity = next_capacity;
    }
    got = fread(bytes + length, 1U, capacity - length - 1U, fp);
    length += got;
    if (got == 0U) {
      if (ferror(fp)) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to read pouch doc table generation",
                            strerror(errno), NULL, NULL);
      }
      break;
    }
  }
  if (fclose(fp) != 0) {
    lc_free_with_allocator(allocator, bytes);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch doc table generation",
                        strerror(errno), NULL, NULL);
  }
  bytes[length] = '\0';
  *out_bytes = bytes;
  *out_length = length;
  return LC_OK;
}

int lc_pouch_index_doc_table_generation_load_bytes(
    const lc_allocator *allocator, char **bytes_inout, size_t length,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *valid, lc_error *error) {
  int rc;

  if (bytes_inout == NULL || *bytes_inout == NULL || table == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation load requires bytes, "
                        "table, and valid output",
                        NULL, NULL, NULL);
  }
  memset(table, 0, sizeof(*table));
  *valid = 0;
  rc = lc_pouch_index_doc_generation_parse_bytes(
      allocator, *bytes_inout, expected_index_seq, expected_row_count,
      expected_row_hash, table, valid, error);
  if (rc == LC_OK && *valid) {
    table->owned_generation_bytes = *bytes_inout;
    *bytes_inout = NULL;
  } else {
    lc_pouch_index_doc_table_cleanup(allocator, table);
  }
  (void)length;
  return rc;
}

int lc_pouch_index_doc_table_generation_load_file(
    const lc_allocator *allocator, const char *path,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *present, int *valid, lc_error *error) {
  char *bytes;
  size_t length;
  int rc;

  if (table == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation load requires table and "
                        "valid output",
                        NULL, NULL, NULL);
  }
  memset(table, 0, sizeof(*table));
  *valid = 0;
  bytes = NULL;
  length = 0U;
  rc = lc_pouch_index_doc_generation_read_file(allocator, path, &bytes, &length,
                                               present, error);
  if (rc == LC_OK && present != NULL && *present) {
    rc = lc_pouch_index_doc_table_generation_load_bytes(
        allocator, &bytes, length, expected_index_seq, expected_row_count,
        expected_row_hash, table, valid, error);
  }
  lc_free_with_allocator(allocator, bytes);
  return rc;
}
