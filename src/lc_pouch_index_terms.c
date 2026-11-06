#include "lc_pouch_index.h"

#include "lc_api_internal.h"
#include "lc_intcompat.h"

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_TERM_GENERATION_MAGIC "LPITGEN1"
#define LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN 8U
#define LC_POUCH_INDEX_TERM_GENERATION_VERSION_V1 ((uint64_t)1U)
#define LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 ((uint64_t)2U)
#define LC_POUCH_INDEX_TERM_GENERATION_VERSION                                 \
  LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2
#define LC_POUCH_INDEX_TERM_VALUE_STRING ((unsigned char)0U)
#define LC_POUCH_INDEX_TERM_VALUE_TRIGRAM ((unsigned char)1U)

static int lc_pouch_index_term_hex_token_valid(const char *token) {
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

static char *lc_pouch_index_term_next_token(char **cursor, int final_token) {
  char *start;
  char *space;

  if (cursor == NULL || *cursor == NULL) {
    return NULL;
  }
  start = *cursor;
  if (final_token) {
    space = strchr(start, '\n');
    if (space != NULL) {
      *space = '\0';
    }
    if (strchr(start, ' ') != NULL) {
      return NULL;
    }
    *cursor = NULL;
    return start;
  }
  space = strchr(start, ' ');
  if (space == NULL) {
    return NULL;
  }
  *space = '\0';
  *cursor = space + 1;
  return start;
}

static int lc_pouch_index_term_parse_ulong_token(const char *token,
                                                 unsigned long *out,
                                                 const char *message,
                                                 lc_error *error) {
  char *end;
  unsigned long parsed;

  if (token == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  errno = 0;
  parsed = strtoul(token, &end, 10);
  if (errno != 0 || end == token || *end != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  *out = parsed;
  return LC_OK;
}

static int lc_pouch_index_term_parse_u64_token(const char *token, uint64_t *out,
                                               const char *message,
                                               lc_error *error) {
  lc_u64 parsed;

  if (token == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  if (!lc_u64_parse_base10(token, &parsed)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  *out = (uint64_t)parsed;
  return LC_OK;
}

static char *lc_pouch_index_term_hex_encode_bytes(const lc_allocator *allocator,
                                                  const char *value,
                                                  size_t length) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *encoded;
  char *dst;

  if (value == NULL || length == 0U) {
    encoded = (char *)lc_alloc_with_allocator(allocator, 2U);
    if (encoded != NULL) {
      encoded[0] = '-';
      encoded[1] = '\0';
    }
    return encoded;
  }
  if (length > ((size_t)-1 - 1U) / 2U) {
    return NULL;
  }
  encoded = (char *)lc_alloc_with_allocator(allocator, (length * 2U) + 1U);
  if (encoded == NULL) {
    return NULL;
  }
  src = (const unsigned char *)value;
  dst = encoded;
  while (length-- > 0U) {
    *dst++ = hex[*src >> 4];
    *dst++ = hex[*src & 0x0fU];
    ++src;
  }
  *dst = '\0';
  return encoded;
}

static char *lc_pouch_index_term_hex_encode(const lc_allocator *allocator,
                                            const char *value) {
  return lc_pouch_index_term_hex_encode_bytes(
      allocator, value, value != NULL ? strlen(value) : 0U);
}

static int lc_pouch_index_term_parse_number_value(const char *text,
                                                  double *out) {
  char *endptr;
  double value;

  if (text == NULL || text[0] == '\0' || out == NULL) {
    return 0;
  }
  errno = 0;
  value = strtod(text, &endptr);
  if (errno != 0 || endptr == text || *endptr != '\0' || !isfinite(value)) {
    return 0;
  }
  *out = value;
  return 1;
}

static char *lc_pouch_index_term_value_hex_encode(const lc_allocator *allocator,
                                                  const char *value,
                                                  char value_type,
                                                  lc_error *error) {
  char canonical[64];
  double number;
  int written;

  if (value_type != 'n') {
    return lc_pouch_index_term_hex_encode(allocator, value);
  }
  if (!lc_pouch_index_term_parse_number_value(value, &number)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch index exact numeric term requires a finite JSON "
                       "number",
                       NULL, NULL, "pouch");
    return NULL;
  }
  if (number == 0.0) {
    written = snprintf(canonical, sizeof(canonical), "0");
  } else {
    written = snprintf(canonical, sizeof(canonical), "%.17g", number);
  }
  if (written < 0 || (size_t)written >= sizeof(canonical)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch index exact numeric term exceeds local limit",
                       NULL, NULL, "pouch");
    return NULL;
  }
  return lc_pouch_index_term_hex_encode(allocator, canonical);
}

void lc_pouch_index_term_fields_cleanup(const lc_allocator *allocator,
                                        lc_pouch_index_term_field *fields,
                                        size_t count) {
  size_t index;

  if (fields == NULL) {
    return;
  }
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(allocator, fields[index].field_hex);
  }
  lc_free_with_allocator(allocator, fields);
}

void lc_pouch_index_term_values_cleanup(const lc_allocator *allocator,
                                        lc_pouch_index_term_value *values,
                                        size_t count) {
  size_t index;

  if (values == NULL) {
    return;
  }
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(allocator, values[index].field_hex);
    lc_free_with_allocator(allocator, values[index].value_hex);
  }
  lc_free_with_allocator(allocator, values);
}

int lc_pouch_index_term_fields_find(const lc_pouch_index_term_field *fields,
                                    size_t count, const char *field_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (fields == NULL || field_hex == NULL || first_line == NULL ||
      line_count == NULL) {
    return 0;
  }
  low = 0U;
  high = count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = strcmp(field_hex, fields[mid].field_hex);
    if (cmp == 0) {
      *first_line = fields[mid].first_line;
      *line_count = fields[mid].line_count;
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

int lc_pouch_index_term_values_find(const lc_pouch_index_term_value *values,
                                    size_t count, const char *field_hex,
                                    const char *value_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (values == NULL || field_hex == NULL || value_hex == NULL ||
      first_line == NULL || line_count == NULL) {
    return 0;
  }
  low = 0U;
  high = count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = strcmp(field_hex, values[mid].field_hex);
    if (cmp == 0) {
      cmp = strcmp(value_hex, values[mid].value_hex);
    }
    if (cmp == 0) {
      *first_line = values[mid].first_line;
      *line_count = values[mid].line_count;
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

void lc_pouch_index_term_keys_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_term_key *terms,
                                      size_t count) {
  size_t index;

  if (terms == NULL) {
    return;
  }
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(allocator, (char *)terms[index].field_hex);
    lc_free_with_allocator(allocator, (char *)terms[index].value_hex);
  }
  lc_free_with_allocator(allocator, terms);
}

int lc_pouch_index_term_key_compare_items(
    const lc_pouch_index_term_key *left, const lc_pouch_index_term_key *right) {
  int cmp;

  if (left == NULL && right == NULL) {
    return 0;
  }
  if (left == NULL) {
    return -1;
  }
  if (right == NULL) {
    return 1;
  }
  cmp = strcmp(left->field_hex, right->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(left->value_hex, right->value_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (left->value_type != right->value_type) {
    return left->value_type < right->value_type ? -1 : 1;
  }
  return 0;
}

int lc_pouch_index_term_key_compare(const void *left, const void *right) {
  return lc_pouch_index_term_key_compare_items(
      (const lc_pouch_index_term_key *)left,
      (const lc_pouch_index_term_key *)right);
}

int lc_pouch_index_term_key_compare_pair(const char *field_hex,
                                         const char *value_hex,
                                         const lc_pouch_index_term_key *term) {
  int cmp;

  if (field_hex == NULL || value_hex == NULL || term == NULL) {
    return field_hex == NULL && value_hex == NULL && term == NULL ? 0 : -1;
  }
  cmp = strcmp(field_hex, term->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(value_hex, term->value_hex);
}

int lc_pouch_index_term_keys_find(const lc_pouch_index_term_key *terms,
                                  size_t count, const char *field_hex,
                                  const char *value_hex, char value_type,
                                  size_t *index_out) {
  size_t low;
  size_t high;
  size_t mid;
  size_t scan;
  int cmp;

  if (terms == NULL || field_hex == NULL || value_hex == NULL ||
      value_type == '\0' || count == 0U) {
    return 0;
  }
  low = 0U;
  high = count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp =
        lc_pouch_index_term_key_compare_pair(field_hex, value_hex, &terms[mid]);
    if (cmp == 0) {
      scan = mid;
      while (scan > 0U && lc_pouch_index_term_key_compare_pair(
                              field_hex, value_hex, &terms[scan - 1U]) == 0) {
        --scan;
      }
      for (; scan < count && lc_pouch_index_term_key_compare_pair(
                                 field_hex, value_hex, &terms[scan]) == 0;
           ++scan) {
        if (terms[scan].value_type == value_type) {
          if (index_out != NULL) {
            *index_out = scan;
          }
          return 1;
        }
      }
      return 0;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

int lc_pouch_index_term_keys_build_exact(const lc_pouch_index_plain_term *terms,
                                         size_t term_count,
                                         lc_pouch_index_term_key **out_terms,
                                         size_t *out_count,
                                         const lc_allocator *allocator,
                                         lc_error *error) {
  lc_pouch_index_term_key *keys;
  size_t index;
  size_t write_index;
  int rc;

  if (out_terms == NULL || out_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact term-key build requires outputs",
                        NULL, NULL, NULL);
  }
  *out_terms = NULL;
  *out_count = 0U;
  if (terms == NULL || term_count == 0U) {
    return LC_OK;
  }
  keys = (lc_pouch_index_term_key *)lc_alloc_with_allocator(
      allocator, term_count * sizeof(*keys));
  if (keys == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index exact term keys", NULL,
                        NULL, NULL);
  }
  memset(keys, 0, term_count * sizeof(*keys));
  rc = LC_OK;
  for (index = 0U; index < term_count; ++index) {
    if (terms[index].field == NULL || terms[index].field[0] == '\0' ||
        terms[index].value == NULL ||
        (terms[index].value_type != 's' && terms[index].value_type != 'n' &&
         terms[index].value_type != 'b' && terms[index].value_type != 'z')) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact term-key build requires non-empty "
                        "fields, non-null values, and scalar value types",
                        NULL, NULL, NULL);
      break;
    }
    keys[index].field_hex =
        lc_pouch_index_term_hex_encode(allocator, terms[index].field);
    keys[index].value_hex = lc_pouch_index_term_value_hex_encode(
        allocator, terms[index].value, terms[index].value_type, error);
    if (keys[index].field_hex == NULL || keys[index].value_hex == NULL) {
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch index exact term key",
                              NULL, NULL, NULL);
      break;
    }
    keys[index].value_type = terms[index].value_type;
  }
  if (rc == LC_OK) {
    qsort(keys, term_count, sizeof(keys[0]), lc_pouch_index_term_key_compare);
    write_index = 0U;
    for (index = 0U; index < term_count; ++index) {
      if (write_index > 0U && lc_pouch_index_term_key_compare_items(
                                  &keys[write_index - 1U], &keys[index]) == 0) {
        lc_free_with_allocator(allocator, (char *)keys[index].field_hex);
        lc_free_with_allocator(allocator, (char *)keys[index].value_hex);
        memset(&keys[index], 0, sizeof(keys[index]));
        continue;
      }
      if (write_index != index) {
        keys[write_index] = keys[index];
        memset(&keys[index], 0, sizeof(keys[index]));
      }
      ++write_index;
    }
    *out_terms = keys;
    *out_count = write_index;
    return LC_OK;
  }
  lc_pouch_index_term_keys_cleanup(allocator, keys, term_count);
  return rc;
}

int lc_pouch_index_term_keys_build_exact_for_field(
    const char *field, const char *const *values, const char *value_types,
    size_t value_count, lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  lc_pouch_index_plain_term *terms;
  size_t index;
  int rc;

  if (out_terms == NULL || out_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact field term-key build requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_terms = NULL;
  *out_count = 0U;
  if (field == NULL || field[0] == '\0' ||
      (values == NULL && value_count > 0U) ||
      (value_types == NULL && value_count > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact field term-key build requires a "
                        "field, values, and value types",
                        NULL, NULL, NULL);
  }
  if (value_count == 0U) {
    return LC_OK;
  }
  terms = (lc_pouch_index_plain_term *)lc_alloc_with_allocator(
      allocator, value_count * sizeof(*terms));
  if (terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index exact field terms",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < value_count; ++index) {
    terms[index].field = field;
    terms[index].value = values[index];
    terms[index].value_type = value_types[index];
  }
  rc = lc_pouch_index_term_keys_build_exact(terms, value_count, out_terms,
                                            out_count, allocator, error);
  lc_free_with_allocator(allocator, terms);
  return rc;
}

static int lc_pouch_index_term_value_hex_token_valid(const char *token) {
  return token != NULL && (strcmp(token, "-") == 0 ||
                           lc_pouch_index_term_hex_token_valid(token));
}

static int lc_pouch_index_term_value_type_valid(char value_type) {
  return value_type == 's' || value_type == 'n' || value_type == 'b' ||
         value_type == 'z' || value_type == 'h' || value_type == 'p' ||
         value_type == 'w';
}

static void lc_pouch_index_term_trigram_hex(unsigned long key, char out[7]) {
  static const char hex[] = "0123456789abcdef";

  out[0] = hex[(key >> 20U) & 0x0fUL];
  out[1] = hex[(key >> 16U) & 0x0fUL];
  out[2] = hex[(key >> 12U) & 0x0fUL];
  out[3] = hex[(key >> 8U) & 0x0fUL];
  out[4] = hex[(key >> 4U) & 0x0fUL];
  out[5] = hex[key & 0x0fUL];
  out[6] = '\0';
}

static const char *
lc_pouch_index_term_entry_value_hex(const lc_pouch_index_term_entry *entry,
                                    char value_buffer[7]) {
  if (entry != NULL && entry->value_is_trigram_key) {
    lc_pouch_index_term_trigram_hex(entry->trigram_key, value_buffer);
    return value_buffer;
  }
  return entry != NULL ? entry->value_hex : NULL;
}

static int lc_pouch_index_term_entry_compare_parts(const char *left_field_hex,
                                                   const char *left_value_hex,
                                                   char left_value_type,
                                                   const char *right_field_hex,
                                                   const char *right_value_hex,
                                                   char right_value_type) {
  int cmp;

  cmp = strcmp(left_field_hex, right_field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(left_value_hex, right_value_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (left_value_type != right_value_type) {
    return left_value_type < right_value_type ? -1 : 1;
  }
  return 0;
}

static int lc_pouch_index_term_entry_compare(const void *left,
                                             const void *right) {
  const lc_pouch_index_term_entry *a;
  const lc_pouch_index_term_entry *b;
  char a_value_buffer[7];
  char b_value_buffer[7];

  a = (const lc_pouch_index_term_entry *)left;
  b = (const lc_pouch_index_term_entry *)right;
  return lc_pouch_index_term_entry_compare_parts(
      a->field_hex, lc_pouch_index_term_entry_value_hex(a, a_value_buffer),
      a->value_type, b->field_hex,
      lc_pouch_index_term_entry_value_hex(b, b_value_buffer), b->value_type);
}

static int lc_pouch_index_term_table_find_position(
    const lc_pouch_index_term_table *table, const char *field_hex,
    const char *value_hex, char value_type, size_t *position_out) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (table == NULL || field_hex == NULL || value_hex == NULL ||
      !lc_pouch_index_term_value_type_valid(value_type)) {
    return 0;
  }
  low = 0U;
  high = table->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_index_term_entry_compare_parts(
        table->items[mid].field_hex, table->items[mid].value_hex,
        table->items[mid].value_type, field_hex, value_hex, value_type);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < table->count &&
         lc_pouch_index_term_entry_compare_parts(
             table->items[low].field_hex, table->items[low].value_hex,
             table->items[low].value_type, field_hex, value_hex,
             value_type) == 0;
}

static int lc_pouch_index_term_table_reserve(const lc_allocator *allocator,
                                             lc_pouch_index_term_table *table,
                                             size_t needed, lc_error *error) {
  lc_pouch_index_term_entry *next_items;
  size_t next_capacity;

  if (table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table reserve requires table", NULL,
                        NULL, NULL);
  }
  if (needed <= table->capacity) {
    return LC_OK;
  }
  next_capacity = table->capacity == 0U ? 16U : table->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index term table exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_term_entry *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term table", NULL, NULL,
                        NULL);
  }
  if (table->items != NULL) {
    memcpy(next_items, table->items, table->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, table->items);
  }
  memset(next_items + table->count, 0,
         (next_capacity - table->count) * sizeof(*next_items));
  table->items = next_items;
  table->capacity = next_capacity;
  return LC_OK;
}

void lc_pouch_index_term_table_cleanup(const lc_allocator *allocator,
                                       lc_pouch_index_term_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    if (table->items[index].owns_field_hex) {
      lc_free_with_allocator(allocator, table->items[index].field_hex);
    }
    lc_free_with_allocator(allocator, table->items[index].value_hex);
  }
  lc_free_with_allocator(allocator, table->items);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_term_table_find(const lc_pouch_index_term_table *table,
                                   const char *field_hex, const char *value_hex,
                                   char value_type,
                                   unsigned long *term_id_out) {
  size_t position;

  if (!lc_pouch_index_term_table_find_position(table, field_hex, value_hex,
                                               value_type, &position)) {
    return 0;
  }
  if (term_id_out != NULL) {
    *term_id_out = table->items[position].term_id;
  }
  return 1;
}

static int
lc_pouch_index_term_table_contains_id(const lc_pouch_index_term_table *table,
                                      unsigned long term_id) {
  size_t index;

  if (table == NULL || term_id == 0UL) {
    return 0;
  }
  for (index = 0U; index < table->count; ++index) {
    if (table->items[index].term_id == term_id) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_index_term_table_has_dense_ids(
    const lc_pouch_index_term_table *table) {
  if (table == NULL) {
    return 0;
  }
  if (table->count == 0U) {
    return table->next_term_id == 0UL || table->next_term_id == 1UL;
  }
  if (table->count > (size_t)((unsigned long)-1 - 1UL)) {
    return 0;
  }
  return table->next_term_id == (unsigned long)table->count + 1UL;
}

static int lc_pouch_index_term_table_contains_id_for_encode(
    const lc_pouch_index_term_table *table, int dense_ids,
    unsigned long term_id) {
  if (table == NULL || term_id == 0UL) {
    return 0;
  }
  if (dense_ids) {
    return term_id <= (unsigned long)table->count;
  }
  return lc_pouch_index_term_table_contains_id(table, term_id);
}

static int lc_pouch_index_term_table_add_entry(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long term_id, unsigned long *term_id_out, lc_error *error) {
  lc_pouch_index_term_entry entry;
  size_t position;
  int rc;

  if (table == NULL || field_hex == NULL || value_hex == NULL ||
      !lc_pouch_index_term_hex_token_valid(field_hex) ||
      !lc_pouch_index_term_value_hex_token_valid(value_hex) ||
      !lc_pouch_index_term_value_type_valid(value_type) || term_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table entry requires typed exact "
                        "hex term",
                        NULL, NULL, "pouch");
  }
  if (lc_pouch_index_term_table_contains_id(table, term_id) ||
      lc_pouch_index_term_table_find_position(table, field_hex, value_hex,
                                              value_type, &position)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table entry must be unique", NULL,
                        NULL, "pouch");
  }
  memset(&entry, 0, sizeof(entry));
  entry.field_hex = lc_strdup_with_allocator(allocator, field_hex);
  entry.value_hex = lc_strdup_with_allocator(allocator, value_hex);
  if (entry.field_hex == NULL || entry.value_hex == NULL) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term entry", NULL, NULL,
                        NULL);
  }
  entry.value_type = value_type;
  entry.term_id = term_id;
  entry.owns_field_hex = 1;
  rc = lc_pouch_index_term_table_reserve(allocator, table, table->count + 1U,
                                         error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return rc;
  }
  if (position < table->count) {
    memmove(&table->items[position + 1U], &table->items[position],
            (table->count - position) * sizeof(table->items[0]));
  }
  table->items[position] = entry;
  ++table->count;
  if (term_id >= table->next_term_id) {
    if (term_id == (unsigned long)-1) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index term table id exceeds local limit", NULL,
                          NULL, "pouch");
    }
    table->next_term_id = term_id + 1UL;
  }
  if (term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return LC_OK;
}

static int lc_pouch_index_term_table_find_or_add_impl(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, int validate_hex, lc_error *error) {
  lc_pouch_index_term_entry entry;
  unsigned long term_id;
  size_t position;
  int rc;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (table == NULL || field_hex == NULL || value_hex == NULL ||
      !lc_pouch_index_term_value_type_valid(value_type)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table add requires table and typed "
                        "exact hex term",
                        NULL, NULL, "pouch");
  }
  if (validate_hex && (!lc_pouch_index_term_hex_token_valid(field_hex) ||
                       !lc_pouch_index_term_value_hex_token_valid(value_hex))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table add requires valid hex term",
                        NULL, NULL, "pouch");
  }
  if (lc_pouch_index_term_table_find_position(table, field_hex, value_hex,
                                              value_type, &position)) {
    if (term_id_out != NULL) {
      *term_id_out = table->items[position].term_id;
    }
    return LC_OK;
  }
  if (table->next_term_id == 0UL) {
    table->next_term_id = 1UL;
  }
  if (table->next_term_id == (unsigned long)-1) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table id exceeds local limit", NULL,
                        NULL, "pouch");
  }
  term_id = table->next_term_id;
  memset(&entry, 0, sizeof(entry));
  entry.field_hex = lc_strdup_with_allocator(allocator, field_hex);
  entry.value_hex = lc_strdup_with_allocator(allocator, value_hex);
  if (entry.field_hex == NULL || entry.value_hex == NULL) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term entry", NULL, NULL,
                        NULL);
  }
  entry.value_type = value_type;
  entry.term_id = term_id;
  entry.owns_field_hex = 1;
  rc = lc_pouch_index_term_table_reserve(allocator, table, table->count + 1U,
                                         error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return rc;
  }
  if (position < table->count) {
    memmove(&table->items[position + 1U], &table->items[position],
            (table->count - position) * sizeof(table->items[0]));
  }
  table->items[position] = entry;
  ++table->count;
  table->next_term_id = term_id + 1UL;
  if (term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return LC_OK;
}

int lc_pouch_index_term_table_find_or_add(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, lc_error *error) {
  return lc_pouch_index_term_table_find_or_add_impl(allocator, table, field_hex,
                                                    value_hex, value_type,
                                                    term_id_out, 1, error);
}

int lc_pouch_index_term_table_find_or_add_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, lc_error *error) {
  return lc_pouch_index_term_table_find_or_add_impl(allocator, table, field_hex,
                                                    value_hex, value_type,
                                                    term_id_out, 0, error);
}

int lc_pouch_index_term_table_append_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, lc_error *error) {
  lc_pouch_index_term_entry entry;
  unsigned long term_id;
  int rc;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (table == NULL || field_hex == NULL || value_hex == NULL ||
      !lc_pouch_index_term_value_type_valid(value_type)) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch index term table append requires table and typed "
        "exact hex term",
        NULL, NULL, "pouch");
  }
  if (table->next_term_id == 0UL) {
    table->next_term_id = 1UL;
  }
  if (table->next_term_id == (unsigned long)-1) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table id exceeds local limit", NULL,
                        NULL, "pouch");
  }
  term_id = table->next_term_id;
  memset(&entry, 0, sizeof(entry));
  entry.field_hex = lc_strdup_with_allocator(allocator, field_hex);
  entry.value_hex = lc_strdup_with_allocator(allocator, value_hex);
  if (entry.field_hex == NULL || entry.value_hex == NULL) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term entry", NULL, NULL,
                        NULL);
  }
  entry.value_type = value_type;
  entry.term_id = term_id;
  entry.owns_field_hex = 1;
  rc = lc_pouch_index_term_table_reserve(allocator, table, table->count + 1U,
                                         error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return rc;
  }
  table->items[table->count++] = entry;
  table->next_term_id = term_id + 1UL;
  if (term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return LC_OK;
}

int lc_pouch_index_term_table_append_trigram_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, unsigned long trigram_key,
    unsigned long *term_id_out, lc_error *error) {
  lc_pouch_index_term_entry entry;
  unsigned long term_id;
  int rc;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (table == NULL || field_hex == NULL || trigram_key > 0xffffffUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index trigram term requires table, field, and "
                        "24-bit key",
                        NULL, NULL, "pouch");
  }
  if (table->next_term_id == 0UL) {
    table->next_term_id = 1UL;
  }
  if (table->next_term_id == (unsigned long)-1) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table id exceeds local limit", NULL,
                        NULL, "pouch");
  }
  term_id = table->next_term_id;
  memset(&entry, 0, sizeof(entry));
  entry.field_hex = (char *)field_hex;
  entry.trigram_key = trigram_key;
  entry.value_type = 's';
  entry.term_id = term_id;
  entry.value_is_trigram_key = 1;
  rc = lc_pouch_index_term_table_reserve(allocator, table, table->count + 1U,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  table->items[table->count++] = entry;
  table->next_term_id = term_id + 1UL;
  if (term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return LC_OK;
}

static int lc_pouch_index_term_posting_table_find_position(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    size_t *position_out) {
  size_t low;
  size_t high;
  size_t mid;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (table == NULL || term_id == 0UL) {
    return 0;
  }
  low = 0U;
  high = table->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    if (table->items[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < table->count && table->items[low].term_id == term_id;
}

int lc_pouch_index_term_posting_table_reserve(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    size_t needed, lc_error *error) {
  lc_pouch_index_term_posting_entry *next_items;
  size_t next_capacity;
  size_t index;

  if (table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting table reserve requires "
                        "table",
                        NULL, NULL, NULL);
  }
  if (needed <= table->capacity) {
    return LC_OK;
  }
  next_capacity = table->capacity == 0U ? 16U : table->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index term posting table exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  if (next_capacity > (size_t)-1 / sizeof(*next_items)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index term posting table exceeds local limit",
                        NULL, NULL, NULL);
  }
  next_items = (lc_pouch_index_term_posting_entry *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term posting table",
                        NULL, NULL, NULL);
  }
  if (table->items != NULL) {
    memcpy(next_items, table->items, table->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, table->items);
  }
  memset(next_items + table->count, 0,
         (next_capacity - table->count) * sizeof(*next_items));
  table->items = next_items;
  table->capacity = next_capacity;
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_index_adaptive_posting_rebind_inline_storage(
        &table->items[index].posting);
  }
  return LC_OK;
}

static void lc_pouch_index_term_posting_table_rebind_inline_storage(
    lc_pouch_index_term_posting_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_index_adaptive_posting_rebind_inline_storage(
        &table->items[index].posting);
  }
}

void lc_pouch_index_term_posting_table_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_index_adaptive_posting_cleanup(allocator,
                                            &table->items[index].posting);
  }
  lc_free_with_allocator(allocator, table->items);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_term_posting_table_put(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    unsigned long term_id, const unsigned long *doc_ids, size_t doc_id_count,
    lc_error *error) {
  lc_pouch_index_adaptive_posting posting;
  size_t position;
  size_t index;
  int added;
  int rc;

  if (table == NULL || term_id == 0UL ||
      (doc_ids == NULL && doc_id_count > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting put requires table, term, "
                        "and docIDs",
                        NULL, NULL, NULL);
  }
  memset(&posting, 0, sizeof(posting));
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < doc_id_count; ++index) {
    rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
        &posting, doc_ids[index], &added, allocator, error);
  }
  if (rc != LC_OK) {
    lc_pouch_index_adaptive_posting_cleanup(allocator, &posting);
    return rc;
  }
  if (lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                      &position)) {
    lc_pouch_index_adaptive_posting_cleanup(allocator,
                                            &table->items[position].posting);
    table->items[position].posting = posting;
    lc_pouch_index_adaptive_posting_rebind_inline_storage(
        &table->items[position].posting);
    return LC_OK;
  }
  rc = lc_pouch_index_term_posting_table_reserve(allocator, table,
                                                 table->count + 1U, error);
  if (rc != LC_OK) {
    lc_pouch_index_adaptive_posting_cleanup(allocator, &posting);
    return rc;
  }
  if (position < table->count) {
    memmove(&table->items[position + 1U], &table->items[position],
            (table->count - position) * sizeof(table->items[0]));
  }
  table->items[position].term_id = term_id;
  table->items[position].posting = posting;
  ++table->count;
  lc_pouch_index_term_posting_table_rebind_inline_storage(table);
  return LC_OK;
}

int lc_pouch_index_term_posting_table_append_sorted_unique_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    unsigned long term_id, const unsigned long *doc_ids, size_t doc_id_count,
    lc_error *error) {
  lc_pouch_index_adaptive_posting posting;
  int rc;

  if (table == NULL || term_id == 0UL ||
      (doc_ids == NULL && doc_id_count > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting append requires table, term, "
                        "and docIDs",
                        NULL, NULL, NULL);
  }
  if (table->count > 0U && term_id <= table->items[table->count - 1U].term_id) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting append requires increasing "
                        "term IDs",
                        NULL, NULL, "pouch");
  }
  memset(&posting, 0, sizeof(posting));
  rc = lc_pouch_index_adaptive_posting_build_sorted_unique_trusted(
      &posting, doc_ids, doc_id_count, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_posting_table_reserve(allocator, table,
                                                 table->count + 1U, error);
  if (rc != LC_OK) {
    lc_pouch_index_adaptive_posting_cleanup(allocator, &posting);
    return rc;
  }
  table->items[table->count].term_id = term_id;
  table->items[table->count].posting = posting;
  ++table->count;
  lc_pouch_index_adaptive_posting_rebind_inline_storage(
      &table->items[table->count - 1U].posting);
  return LC_OK;
}

int lc_pouch_index_term_posting_table_append_to_set(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    lc_pouch_index_docid_set *set, const lc_allocator *allocator,
    lc_error *error) {
  size_t position;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting decode requires output set",
                        NULL, NULL, NULL);
  }
  if (!lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                       &position)) {
    return LC_OK;
  }
  return lc_pouch_index_adaptive_posting_append_to_set(
      &table->items[position].posting, set, allocator, error);
}

int lc_pouch_index_term_posting_table_visit(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    lc_pouch_index_docid_visit_fn visit, void *context, lc_error *error) {
  size_t position;

  if (visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting visit requires visitor", NULL,
                        NULL, NULL);
  }
  if (!lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                       &position)) {
    return LC_OK;
  }
  return lc_pouch_index_adaptive_posting_visit(&table->items[position].posting,
                                               visit, context, error);
}

int lc_pouch_index_term_posting_table_or_dense_bits(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    unsigned char *bits, size_t length, int *applied, lc_error *error) {
  const lc_pouch_index_dense_posting *dense;
  size_t index;
  size_t position;

  if (applied != NULL) {
    *applied = 0;
  }
  if (table == NULL || bits == NULL || applied == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dense posting bitmap requires inputs", NULL,
                        NULL, NULL);
  }
  if (!lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                       &position) ||
      lc_pouch_index_adaptive_posting_selected_kind(
          &table->items[position].posting) !=
          LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE) {
    return LC_OK;
  }
  dense = &table->items[position].posting.dense;
  if (dense->length > length || (dense->length > 0U && dense->bits == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dense posting bitmap is invalid", NULL, NULL,
                        "pouch");
  }
  for (index = 0U; index < dense->length; ++index) {
    bits[index] = (unsigned char)(bits[index] | dense->bits[index]);
  }
  *applied = 1;
  return LC_OK;
}

typedef struct lc_pouch_index_term_generation_buffer {
  char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_index_term_generation_buffer;

static void lc_pouch_index_term_generation_buffer_cleanup(
    const lc_allocator *allocator,
    lc_pouch_index_term_generation_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, buffer->bytes);
  memset(buffer, 0, sizeof(*buffer));
}

static int lc_pouch_index_term_generation_size_add(size_t *length, size_t added,
                                                   lc_error *error) {
  if (added > (size_t)-1 - *length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index term generation exceeds local limit", NULL,
                        NULL, NULL);
  }
  *length += added;
  return LC_OK;
}

static int lc_pouch_index_term_generation_buffer_append(
    lc_pouch_index_term_generation_buffer *buffer, const char *bytes,
    size_t length, lc_error *error) {
  if (buffer == NULL || (bytes == NULL && length > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation append requires buffer",
                        NULL, NULL, NULL);
  }
  if (length >= buffer->capacity ||
      buffer->length >= buffer->capacity - length) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index term generation exceeds measured size",
                        NULL, NULL, NULL);
  }
  if (length > 0U) {
    memcpy(buffer->bytes + buffer->length, bytes, length);
  }
  buffer->length += length;
  buffer->bytes[buffer->length] = '\0';
  return LC_OK;
}

static int lc_pouch_index_term_generation_buffer_append_u8(
    lc_pouch_index_term_generation_buffer *buffer, unsigned char value,
    lc_error *error) {
  if (buffer->length >= buffer->capacity - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index term generation exceeds measured size",
                        NULL, NULL, NULL);
  }
  buffer->bytes[buffer->length++] = (char)value;
  return LC_OK;
}

static int lc_pouch_index_term_generation_buffer_append_u64(
    lc_pouch_index_term_generation_buffer *buffer, uint64_t value,
    lc_error *error) {
  size_t index;

  if (buffer->capacity - buffer->length <= 8U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index term generation exceeds measured size",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < 8U; ++index) {
    buffer->bytes[buffer->length + index] =
        (char)((value >> (index * 8U)) & 0xffU);
  }
  buffer->length += 8U;
  return LC_OK;
}

static int lc_pouch_index_term_generation_buffer_append_string(
    lc_pouch_index_term_generation_buffer *buffer, const char *value,
    lc_error *error) {
  size_t length;
  int rc;

  if (value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation string requires value",
                        NULL, NULL, NULL);
  }
  length = strlen(value);
  rc = lc_pouch_index_term_generation_buffer_append_u64(
      buffer, (uint64_t)length, error);
  if (rc == LC_OK && length > 0U) {
    rc = lc_pouch_index_term_generation_buffer_append(buffer, value, length,
                                                      error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append(buffer, "\0", 1U, error);
  }
  return rc;
}

void lc_pouch_index_term_generation_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation) {
  if (generation == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, generation->ns);
  lc_pouch_index_term_posting_table_cleanup(allocator, &generation->postings);
  lc_pouch_index_term_table_cleanup(allocator, &generation->terms);
  memset(generation, 0, sizeof(*generation));
}

void lc_pouch_index_term_generation_sort_terms(
    lc_pouch_index_term_generation *generation) {
  if (generation == NULL) {
    return;
  }
  if (generation->terms.count > 1U) {
    qsort(generation->terms.items, generation->terms.count,
          sizeof(generation->terms.items[0]),
          lc_pouch_index_term_entry_compare);
  }
  generation->terms_sorted = 1;
}

int lc_pouch_index_term_generation_encode(
    const lc_pouch_index_term_generation *generation,
    const lc_allocator *allocator, char **out_bytes, size_t *out_length,
    lc_error *error) {
  lc_pouch_index_term_generation_buffer buffer;
  lc_pouch_index_term_entry *sorted_terms;
  const char *previous_field_hex;
  unsigned long field_count;
  unsigned long field_id;
  size_t index;
  size_t encoded_length;
  int dense_term_ids;
  int rc;

  if (out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation encode requires outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  if (generation == NULL || generation->ns == NULL ||
      generation->ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation encode requires "
                        "namespace and generation",
                        NULL, NULL, NULL);
  }
  memset(&buffer, 0, sizeof(buffer));
  sorted_terms = NULL;
  if (generation->terms.count > 0U && !generation->terms_sorted) {
    sorted_terms = (lc_pouch_index_term_entry *)lc_alloc_with_allocator(
        allocator, generation->terms.count * sizeof(sorted_terms[0]));
    if (sorted_terms == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch index term generation sort",
                          NULL, NULL, NULL);
    }
    memcpy(sorted_terms, generation->terms.items,
           generation->terms.count * sizeof(sorted_terms[0]));
    qsort(sorted_terms, generation->terms.count, sizeof(sorted_terms[0]),
          lc_pouch_index_term_entry_compare);
  }
  /* Measure the unchanged binary layout before allocating. A single output
   * buffer avoids growth copies for each of the six packed term families. */
  encoded_length = LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN + 7U * 8U;
  rc = lc_pouch_index_term_generation_size_add(&encoded_length, 9U, error);
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_size_add(&encoded_length,
                                                 strlen(generation->ns), error);
  }
  field_count = 0UL;
  previous_field_hex = NULL;
  for (index = 0U; rc == LC_OK && index < generation->terms.count; ++index) {
    const lc_pouch_index_term_entry *term;
    const char *value_hex;
    int new_field;

    term = generation->terms_sorted ? &generation->terms.items[index]
                                    : &sorted_terms[index];
    value_hex = term->value_hex;
    if (term->term_id == 0UL || term->field_hex == NULL ||
        (!term->value_is_trigram_key &&
         !lc_pouch_index_term_value_hex_token_valid(value_hex)) ||
        !lc_pouch_index_term_value_type_valid(term->value_type) ||
        (term->value_is_trigram_key && term->trigram_key > 0xffffffUL)) {
      lc_free_with_allocator(allocator, sorted_terms);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index term generation contains invalid term",
                          NULL, NULL, "pouch");
    }
    new_field = previous_field_hex == NULL ||
                (previous_field_hex != term->field_hex &&
                 strcmp(previous_field_hex, term->field_hex) != 0);
    if (new_field) {
      /* Lexically grouped terms share a field token. Validate that token once
       * instead of rescanning it for every trigram in the field. */
      if (!lc_pouch_index_term_hex_token_valid(term->field_hex)) {
        lc_free_with_allocator(allocator, sorted_terms);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch index term generation contains invalid term",
                            NULL, NULL, "pouch");
      }
      if (field_count == ULONG_MAX) {
        lc_free_with_allocator(allocator, sorted_terms);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch index term generation field count exceeds "
                            "local limit",
                            NULL, NULL, "pouch");
      }
      ++field_count;
      rc = lc_pouch_index_term_generation_size_add(&encoded_length, 9U, error);
      if (rc == LC_OK) {
        rc = lc_pouch_index_term_generation_size_add(
            &encoded_length, strlen(term->field_hex), error);
      }
      previous_field_hex = term->field_hex;
    }
    if (rc == LC_OK) {
      /* ID, value type, field ID, encoding, and the encoded value. */
      rc = lc_pouch_index_term_generation_size_add(
          &encoded_length, term->value_is_trigram_key ? 21U : 27U, error);
    }
    if (rc == LC_OK && !term->value_is_trigram_key) {
      rc = lc_pouch_index_term_generation_size_add(&encoded_length,
                                                   strlen(value_hex), error);
    }
  }
  for (index = 0U; rc == LC_OK && index < generation->postings.count; ++index) {
    const lc_pouch_index_adaptive_posting *posting;
    size_t payload_length;

    posting = &generation->postings.items[index].posting;
    payload_length = lc_pouch_index_adaptive_posting_selected_kind(posting) ==
                             LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE
                         ? posting->dense.length
                         : posting->sparse.length;
    /* ID, kind, count, maximum document ID, and payload length. */
    rc = lc_pouch_index_term_generation_size_add(&encoded_length, 33U, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_size_add(&encoded_length,
                                                   payload_length, error);
    }
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, sorted_terms);
    return rc;
  }
  buffer.capacity = encoded_length + 1U;
  buffer.bytes = (char *)lc_alloc_with_allocator(allocator, buffer.capacity);
  if (buffer.bytes == NULL) {
    lc_free_with_allocator(allocator, sorted_terms);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term generation", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_term_generation_buffer_append(
      &buffer, LC_POUCH_INDEX_TERM_GENERATION_MAGIC,
      LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN, error);
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, LC_POUCH_INDEX_TERM_GENERATION_VERSION, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)generation->index_seq, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)generation->row_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)generation->row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_string(
        &buffer, generation->ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)field_count, error);
  }
  previous_field_hex = NULL;
  for (index = 0U; rc == LC_OK && index < generation->terms.count; ++index) {
    const lc_pouch_index_term_entry *term;

    term = generation->terms_sorted ? &generation->terms.items[index]
                                    : &sorted_terms[index];
    if (previous_field_hex == NULL ||
        (previous_field_hex != term->field_hex &&
         strcmp(previous_field_hex, term->field_hex) != 0)) {
      rc = lc_pouch_index_term_generation_buffer_append_string(
          &buffer, term->field_hex, error);
      previous_field_hex = term->field_hex;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)generation->terms.count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)generation->postings.count, error);
  }
  previous_field_hex = NULL;
  field_id = 0UL;
  for (index = 0U; rc == LC_OK && index < generation->terms.count; ++index) {
    const lc_pouch_index_term_entry *term;
    const char *value_hex;
    unsigned char trigram_bytes[3];
    unsigned char value_encoding;

    term = generation->terms_sorted ? &generation->terms.items[index]
                                    : &sorted_terms[index];
    value_hex = term->value_hex;
    if (previous_field_hex == NULL ||
        (previous_field_hex != term->field_hex &&
         strcmp(previous_field_hex, term->field_hex) != 0)) {
      ++field_id;
      previous_field_hex = term->field_hex;
    }
    value_encoding = term->value_is_trigram_key
                         ? LC_POUCH_INDEX_TERM_VALUE_TRIGRAM
                         : LC_POUCH_INDEX_TERM_VALUE_STRING;
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)term->term_id, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_u8(
          &buffer, (unsigned char)term->value_type, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_u64(
          &buffer, (uint64_t)field_id, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_u8(
          &buffer, value_encoding, error);
    }
    if (rc == LC_OK && value_encoding == LC_POUCH_INDEX_TERM_VALUE_TRIGRAM) {
      trigram_bytes[0] = (unsigned char)((term->trigram_key >> 16U) & 0xffUL);
      trigram_bytes[1] = (unsigned char)((term->trigram_key >> 8U) & 0xffUL);
      trigram_bytes[2] = (unsigned char)(term->trigram_key & 0xffUL);
      rc = lc_pouch_index_term_generation_buffer_append(
          &buffer, (const char *)trigram_bytes, sizeof(trigram_bytes), error);
    }
    if (rc == LC_OK && value_encoding == LC_POUCH_INDEX_TERM_VALUE_STRING) {
      rc = lc_pouch_index_term_generation_buffer_append_string(
          &buffer, value_hex, error);
    }
  }
  lc_free_with_allocator(allocator, sorted_terms);
  sorted_terms = NULL;
  dense_term_ids = lc_pouch_index_term_table_has_dense_ids(&generation->terms);
  for (index = 0U; rc == LC_OK && index < generation->postings.count; ++index) {
    const lc_pouch_index_term_posting_entry *entry;
    lc_pouch_index_adaptive_posting_kind kind;
    const lc_pouch_index_posting *sparse;
    const lc_pouch_index_dense_posting *dense;
    const unsigned char *payload_bytes;
    unsigned long count;
    unsigned long max_doc_id;
    size_t payload_length;

    entry = &generation->postings.items[index];
    if (!lc_pouch_index_term_table_contains_id_for_encode(
            &generation->terms, dense_term_ids, entry->term_id)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation posting references "
                        "unknown term",
                        NULL, NULL, "pouch");
      break;
    }
    kind = lc_pouch_index_adaptive_posting_selected_kind(&entry->posting);
    sparse = &entry->posting.sparse;
    dense = &entry->posting.dense;
    if (kind == LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE) {
      count = (unsigned long)dense->count;
      max_doc_id = dense->max_doc_id;
      payload_length = dense->length;
      payload_bytes = dense->bits;
    } else {
      count = (unsigned long)sparse->count;
      max_doc_id = sparse->has_last_doc_id ? sparse->last_doc_id : 0UL;
      payload_length = sparse->length;
      payload_bytes = sparse->bytes;
    }
    rc = lc_pouch_index_term_generation_buffer_append_u64(
        &buffer, (uint64_t)entry->term_id, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_u8(
          &buffer,
          kind == LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE ? (unsigned char)'d'
                                                        : (unsigned char)'s',
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_u64(
          &buffer, (uint64_t)count, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_u64(
          &buffer, (uint64_t)max_doc_id, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_u64(
          &buffer, (uint64_t)payload_length, error);
    }
    if (rc == LC_OK && payload_length > 0U) {
      rc = lc_pouch_index_term_generation_buffer_append(
          &buffer, (const char *)payload_bytes, payload_length, error);
    }
  }
  lc_free_with_allocator(allocator, sorted_terms);
  if (rc != LC_OK) {
    lc_pouch_index_term_generation_buffer_cleanup(allocator, &buffer);
    return rc;
  }
  *out_bytes = buffer.bytes;
  *out_length = buffer.length;
  buffer.bytes[buffer.length] = '\0';
  return LC_OK;
}

typedef struct lc_pouch_index_term_generation_cursor {
  const unsigned char *bytes;
  size_t length;
  size_t offset;
} lc_pouch_index_term_generation_cursor;

static int lc_pouch_index_term_generation_cursor_read(
    lc_pouch_index_term_generation_cursor *cursor, size_t length,
    const unsigned char **out, lc_error *error) {
  if (out != NULL) {
    *out = NULL;
  }
  if (cursor == NULL || (length > 0U && out == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation cursor requires output",
                        NULL, NULL, NULL);
  }
  if (length > cursor->length || cursor->offset > cursor->length - length) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation is truncated", NULL, NULL,
                        "pouch");
  }
  if (out != NULL) {
    *out = cursor->bytes + cursor->offset;
  }
  cursor->offset += length;
  return LC_OK;
}

static int lc_pouch_index_term_generation_cursor_u8(
    lc_pouch_index_term_generation_cursor *cursor, unsigned char *out,
    lc_error *error) {
  const unsigned char *bytes;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation u8 requires output", NULL,
                        NULL, NULL);
  }
  bytes = NULL;
  rc = lc_pouch_index_term_generation_cursor_read(cursor, 1U, &bytes, error);
  if (rc == LC_OK) {
    *out = bytes[0];
  }
  return rc;
}

static int lc_pouch_index_term_generation_cursor_u64(
    lc_pouch_index_term_generation_cursor *cursor, uint64_t *out,
    lc_error *error) {
  const unsigned char *bytes;
  uint64_t value;
  size_t index;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation u64 requires output", NULL,
                        NULL, NULL);
  }
  bytes = NULL;
  rc = lc_pouch_index_term_generation_cursor_read(cursor, 8U, &bytes, error);
  if (rc != LC_OK) {
    return rc;
  }
  value = (uint64_t)0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)bytes[index]) << (index * 8U);
  }
  *out = value;
  return LC_OK;
}

static int lc_pouch_index_term_generation_cursor_ulong(
    lc_pouch_index_term_generation_cursor *cursor, unsigned long *out,
    const char *message, lc_error *error) {
  uint64_t value;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  rc = lc_pouch_index_term_generation_cursor_u64(cursor, &value, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (value > (uint64_t)ULONG_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL,
                        "pouch");
  }
  *out = (unsigned long)value;
  return LC_OK;
}

static int lc_pouch_index_term_generation_cursor_string(
    lc_pouch_index_term_generation_cursor *cursor, const char **out,
    size_t *out_length, lc_error *error) {
  const unsigned char *bytes;
  uint64_t length64;
  size_t length;
  int rc;

  if (out == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation string requires output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  *out_length = 0U;
  rc = lc_pouch_index_term_generation_cursor_u64(cursor, &length64, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (length64 > (uint64_t)((size_t)-1)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation string exceeds local "
                        "limit",
                        NULL, NULL, "pouch");
  }
  length = (size_t)length64;
  if (length > cursor->length || cursor->offset > cursor->length - length ||
      cursor->offset + length >= cursor->length) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation string is truncated", NULL,
                        NULL, "pouch");
  }
  bytes = cursor->bytes + cursor->offset;
  if (memchr(bytes, '\0', length) != NULL ||
      cursor->bytes[cursor->offset + length] != (unsigned char)'\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation string is invalid", NULL,
                        NULL, "pouch");
  }
  cursor->offset += length + 1U;
  *out = (const char *)bytes;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_index_term_generation_decode_posting_bytes(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    unsigned long term_id, char kind, unsigned long count,
    unsigned long max_doc_id, const unsigned char *payload,
    size_t payload_length, lc_error *error) {
  lc_pouch_index_docid_set decoded;
  lc_pouch_index_posting sparse;
  lc_pouch_index_dense_posting dense;
  unsigned char *payload_copy;
  int rc;

  memset(&decoded, 0, sizeof(decoded));
  memset(&sparse, 0, sizeof(sparse));
  memset(&dense, 0, sizeof(dense));
  payload_copy = NULL;
  if ((payload == NULL && payload_length > 0U) ||
      (count == 0UL && payload_length != 0U) ||
      (count > 0UL && payload_length == 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation posting is invalid", NULL,
                        NULL, "pouch");
  }
  if (payload_length > 0U) {
    payload_copy =
        (unsigned char *)lc_alloc_with_allocator(allocator, payload_length);
    if (payload_copy == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch index term posting "
                          "payload",
                          NULL, NULL, NULL);
    }
    memcpy(payload_copy, payload, payload_length);
  }
  rc = LC_OK;
  if (kind == 's') {
    sparse.bytes = payload_copy;
    sparse.length = payload_length;
    sparse.capacity = payload_length;
    sparse.count = (size_t)count;
    sparse.last_doc_id = max_doc_id;
    sparse.has_last_doc_id = count > 0UL ? 1 : 0;
    payload_copy = NULL;
    rc = lc_pouch_index_posting_append_to_set(&sparse, &decoded, allocator,
                                              error);
  } else if (kind == 'd') {
    dense.bits = payload_copy;
    dense.length = payload_length;
    dense.capacity = payload_length;
    dense.count = (size_t)count;
    dense.max_doc_id = max_doc_id;
    dense.last_doc_id = max_doc_id;
    dense.has_last_doc_id = count > 0UL ? 1 : 0;
    payload_copy = NULL;
    rc = lc_pouch_index_dense_posting_append_to_set(&dense, &decoded, allocator,
                                                    error);
  } else {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has invalid posting kind",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK && decoded.count != (size_t)count) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation posting count mismatch",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK && count > 0UL &&
      decoded.items[decoded.count - 1U] != max_doc_id) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation posting max docID "
                      "mismatch",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_posting_table_put(allocator, &generation->postings,
                                               term_id, decoded.items,
                                               decoded.count, error);
  }
  lc_free_with_allocator(allocator, payload_copy);
  if (sparse.bytes != NULL) {
    lc_pouch_index_posting_cleanup(allocator, &sparse);
  }
  if (dense.bits != NULL) {
    lc_pouch_index_dense_posting_cleanup(allocator, &dense);
  }
  lc_pouch_index_docid_set_cleanup(allocator, &decoded);
  return rc;
}

int lc_pouch_index_term_generation_decode(
    const lc_allocator *allocator, const char *bytes, size_t length,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, const lc_pouch_index_term_limits *limits,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_index_term_generation decoded;
  lc_pouch_index_term_generation_cursor cursor;
  const unsigned char *magic;
  const char *ns;
  size_t namespace_length;
  size_t term_bytes;
  uint64_t version;
  const char **field_dictionary;
  unsigned long field_count;
  unsigned long term_count;
  unsigned long posting_count;
  unsigned long previous_posting_term_id;
  unsigned long index;
  int rc;

  if (generation == NULL || bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation decode requires bytes "
                        "and output",
                        NULL, NULL, NULL);
  }
  memset(&decoded, 0, sizeof(decoded));
  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = (const unsigned char *)bytes;
  cursor.length = length;
  rc = LC_OK;
  field_dictionary = NULL;
  field_count = 0UL;
  previous_posting_term_id = 0UL;
  magic = NULL;
  ns = NULL;
  namespace_length = 0U;
  term_bytes = 0U;
  rc = lc_pouch_index_term_generation_cursor_read(
      &cursor, LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN, &magic, error);
  if (rc == LC_OK && memcmp(magic, LC_POUCH_INDEX_TERM_GENERATION_MAGIC,
                            LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has invalid format", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_cursor_u64(&cursor, &version, error);
  }
  if (rc == LC_OK && version != LC_POUCH_INDEX_TERM_GENERATION_VERSION_V1 &&
      version != LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has invalid version", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_cursor_u64(&cursor, &decoded.index_seq,
                                                   error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_cursor_ulong(
        &cursor, &decoded.row_count,
        "pouch index term generation missing row count", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_cursor_ulong(
        &cursor, &decoded.row_hash,
        "pouch index term generation missing row hash", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_cursor_string(&cursor, &ns,
                                                      &namespace_length, error);
  }
  if (rc == LC_OK && namespace_length == 0U) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation missing namespace", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && (decoded.index_seq != expected_index_seq ||
                      decoded.row_count != expected_row_count ||
                      decoded.row_hash != expected_row_hash)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation identity mismatch", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    decoded.ns = lc_strdup_with_allocator(allocator, ns);
    if (decoded.ns == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) {
    rc = lc_pouch_index_term_generation_cursor_ulong(
        &cursor, &field_count,
        "pouch index term generation missing field count", error);
  }
  if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 &&
      field_count > 0UL) {
    if ((limits != NULL && field_count > limits->max_terms) ||
        field_count > cursor.length - cursor.offset ||
        field_count >
            (unsigned long)((size_t)-1 / sizeof(field_dictionary[0]))) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation field count exceeds "
                        "local limit",
                        NULL, NULL, "pouch");
    } else {
      field_dictionary = (const char **)lc_alloc_with_allocator(
          allocator, (size_t)field_count * sizeof(field_dictionary[0]));
      if (field_dictionary == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch index term generation "
                          "field dictionary",
                          NULL, NULL, NULL);
      }
    }
  }
  for (index = 0UL;
       rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 &&
       index < field_count;
       ++index) {
    const char *field_hex;
    size_t field_length;

    field_hex = NULL;
    field_length = 0U;
    rc = lc_pouch_index_term_generation_cursor_string(&cursor, &field_hex,
                                                      &field_length, error);
    if (rc == LC_OK && (field_length == 0U ||
                        !lc_pouch_index_term_hex_token_valid(field_hex) ||
                        (index > 0UL && strcmp(field_dictionary[index - 1UL],
                                               field_hex) >= 0))) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation field dictionary is "
                        "invalid",
                        NULL, NULL, "pouch");
    }
    if (rc == LC_OK) {
      field_dictionary[index] = field_hex;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_cursor_ulong(
        &cursor, &term_count, "pouch index term generation missing term count",
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_cursor_ulong(
        &cursor, &posting_count,
        "pouch index term generation missing posting count", error);
  }
  if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 &&
      ((term_count == 0UL && field_count != 0UL) || field_count > term_count)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation field dictionary does not "
                      "match terms",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK && limits != NULL &&
      (term_count > limits->max_terms || posting_count > limits->max_terms))
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation exceeds bounded term count",
                      NULL, NULL, "pouch");
  for (index = 0UL; rc == LC_OK && index < term_count; ++index) {
    const char *field_hex;
    const char *value_hex;
    const unsigned char *trigram_bytes;
    const lc_pouch_index_term_entry *previous_entry;
    size_t field_length;
    size_t value_length;
    unsigned long term_id;
    unsigned long field_id;
    unsigned char value_type;
    unsigned char value_encoding;
    char trigram_hex[7];

    field_hex = NULL;
    value_hex = NULL;
    trigram_bytes = NULL;
    field_length = 0U;
    value_length = 0U;
    field_id = 0UL;
    value_encoding = LC_POUCH_INDEX_TERM_VALUE_STRING;
    rc = lc_pouch_index_term_generation_cursor_ulong(
        &cursor, &term_id, "pouch index term generation term has invalid id",
        error);
    if (rc == LC_OK) {
      rc =
          lc_pouch_index_term_generation_cursor_u8(&cursor, &value_type, error);
    }
    if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V1) {
      rc = lc_pouch_index_term_generation_cursor_string(&cursor, &field_hex,
                                                        &field_length, error);
    }
    if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V1) {
      rc = lc_pouch_index_term_generation_cursor_string(&cursor, &value_hex,
                                                        &value_length, error);
    }
    if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) {
      rc = lc_pouch_index_term_generation_cursor_ulong(
          &cursor, &field_id,
          "pouch index term generation term has invalid field id", error);
    }
    if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) {
      rc = lc_pouch_index_term_generation_cursor_u8(&cursor, &value_encoding,
                                                    error);
    }
    if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) {
      if (field_id == 0UL || field_id > field_count ||
          (value_encoding != LC_POUCH_INDEX_TERM_VALUE_STRING &&
           value_encoding != LC_POUCH_INDEX_TERM_VALUE_TRIGRAM)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index term generation term is invalid", NULL,
                          NULL, "pouch");
      } else {
        field_hex = field_dictionary[field_id - 1UL];
        field_length = strlen(field_hex);
      }
    }
    if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 &&
        value_encoding == LC_POUCH_INDEX_TERM_VALUE_STRING) {
      rc = lc_pouch_index_term_generation_cursor_string(&cursor, &value_hex,
                                                        &value_length, error);
    }
    if (rc == LC_OK && version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 &&
        value_encoding == LC_POUCH_INDEX_TERM_VALUE_TRIGRAM) {
      rc = lc_pouch_index_term_generation_cursor_read(&cursor, 3U,
                                                      &trigram_bytes, error);
      if (rc == LC_OK) {
        lc_pouch_index_term_trigram_hex(
            ((unsigned long)trigram_bytes[0] << 16U) |
                ((unsigned long)trigram_bytes[1] << 8U) |
                (unsigned long)trigram_bytes[2],
            trigram_hex);
        value_hex = trigram_hex;
        value_length = 6U;
      }
    }
    if (rc != LC_OK) {
      break;
    }
    if (limits != NULL) {
      if (field_length > limits->max_term_bytes - term_bytes ||
          value_length > limits->max_term_bytes - term_bytes - field_length ||
          2U > limits->max_term_bytes - term_bytes - field_length -
                   value_length) {
        rc = lc_error_set(
            error, LC_ERR_INVALID, 0L,
            "pouch index term generation exceeds bounded term bytes", NULL,
            NULL, "pouch");
        break;
      }
      term_bytes += field_length + value_length + 2U;
    }
    previous_entry = decoded.terms.count > 0U
                         ? &decoded.terms.items[decoded.terms.count - 1U]
                         : NULL;
    if (previous_entry != NULL) {
      lc_pouch_index_term_key previous_key;
      lc_pouch_index_term_key current_key;

      memset(&previous_key, 0, sizeof(previous_key));
      memset(&current_key, 0, sizeof(current_key));
      previous_key.field_hex = previous_entry->field_hex;
      previous_key.value_hex = previous_entry->value_hex;
      previous_key.value_type = previous_entry->value_type;
      current_key.field_hex = field_hex;
      current_key.value_hex = value_hex;
      current_key.value_type = (char)value_type;
      if (lc_pouch_index_term_key_compare_items(&previous_key, &current_key) >=
          0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index term generation terms are unsorted",
                          NULL, NULL, "pouch");
        break;
      }
    }
    if (term_id == 0UL ||
        (value_encoding == LC_POUCH_INDEX_TERM_VALUE_TRIGRAM &&
         value_type != (unsigned char)'s') ||
        !lc_pouch_index_term_hex_token_valid(field_hex) ||
        !lc_pouch_index_term_value_hex_token_valid(value_hex) ||
        !lc_pouch_index_term_value_type_valid((char)value_type)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation term is invalid", NULL,
                        NULL, "pouch");
      break;
    }
    rc = lc_pouch_index_term_table_add_entry(
        allocator, &decoded.terms, field_hex, value_hex, (char)value_type,
        term_id, NULL, error);
  }
  for (index = 0UL; rc == LC_OK && index < posting_count; ++index) {
    const unsigned char *payload;
    unsigned long term_id;
    unsigned long count;
    unsigned long max_doc_id;
    unsigned long payload_length_ulong;
    unsigned char kind;

    payload = NULL;
    rc = lc_pouch_index_term_generation_cursor_ulong(
        &cursor, &term_id,
        "pouch index term generation posting has invalid term id", error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_cursor_u8(&cursor, &kind, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_cursor_ulong(
          &cursor, &count,
          "pouch index term generation posting has invalid count", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_cursor_ulong(
          &cursor, &max_doc_id,
          "pouch index term generation posting has invalid max docID", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_cursor_ulong(
          &cursor, &payload_length_ulong,
          "pouch index term generation posting has invalid payload length",
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_cursor_read(
          &cursor, (size_t)payload_length_ulong, &payload, error);
    }
    if (rc != LC_OK) {
      break;
    }
    if ((limits != NULL &&
         (count > decoded.row_count ||
          (count > 0UL && max_doc_id >= decoded.row_count))) ||
        term_id == 0UL ||
        (kind != (unsigned char)'s' && kind != (unsigned char)'d') ||
        (previous_posting_term_id != 0UL &&
         previous_posting_term_id >= term_id) ||
        !lc_pouch_index_term_table_contains_id(&decoded.terms, term_id)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation posting is invalid", NULL,
                        NULL, "pouch");
      break;
    }
    rc = lc_pouch_index_term_generation_decode_posting_bytes(
        allocator, &decoded, term_id, (char)kind, count, max_doc_id, payload,
        (size_t)payload_length_ulong, error);
    if (rc == LC_OK) {
      previous_posting_term_id = term_id;
    }
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has trailing records", NULL,
                      NULL, "pouch");
  }
  lc_free_with_allocator(allocator, field_dictionary);
  if (rc != LC_OK) {
    lc_pouch_index_term_generation_cleanup(allocator, &decoded);
    return rc;
  }
  lc_pouch_index_term_generation_cleanup(allocator, generation);
  *generation = decoded;
  return LC_OK;
}

int lc_pouch_index_term_generation_load_bytes(
    const lc_allocator *allocator, const char *bytes, size_t length,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_term_generation *generation,
    int *valid, lc_error *error) {
  lc_error decode_error;
  int rc;

  if (generation == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation load requires generation "
                        "and valid output",
                        NULL, NULL, NULL);
  }
  memset(generation, 0, sizeof(*generation));
  *valid = 0;
  lc_error_init(&decode_error);
  rc = lc_pouch_index_term_generation_decode(
      allocator, bytes, length, expected_index_seq, expected_row_count,
      expected_row_hash, NULL, generation, &decode_error);
  if (rc == LC_OK) {
    *valid = 1;
  } else if (rc == LC_ERR_INVALID) {
    lc_pouch_index_term_generation_cleanup(allocator, generation);
    rc = LC_OK;
  } else if (error != NULL && decode_error.code != LC_OK) {
    rc = lc_error_set(error, decode_error.code, 0L, decode_error.message,
                      decode_error.detail, decode_error.server_code,
                      decode_error.correlation_id);
  }
  lc_error_cleanup(&decode_error);
  return rc;
}

int lc_pouch_index_term_field_parse_line(char *line,
                                         lc_pouch_index_term_field *field,
                                         const lc_allocator *allocator,
                                         lc_error *error) {
  char *cursor;
  char *field_hex;
  char *first_token;
  char *count_token;
  char *first_byte_token;
  char *byte_count_token;
  unsigned long first_line;
  unsigned long line_count;
  uint64_t first_byte;
  uint64_t byte_count;
  int rc;

  if (line == NULL || field == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field requires line and output", NULL,
                        NULL, NULL);
  }
  first_byte = 0U;
  byte_count = 0U;
  if (strncmp(line, "term_field ", sizeof("term_field ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("term_field ") - 1U;
  field_hex = lc_pouch_index_term_next_token(&cursor, 0);
  first_token = lc_pouch_index_term_next_token(&cursor, 0);
  count_token = lc_pouch_index_term_next_token(&cursor, 0);
  first_byte_token = lc_pouch_index_term_next_token(&cursor, 0);
  byte_count_token = lc_pouch_index_term_next_token(&cursor, 1);
  if (field_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_index_term_hex_token_valid(field_hex) || first_token == NULL ||
      count_token == NULL || first_byte_token == NULL ||
      byte_count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field has invalid fields", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_token, &first_line, "pouch index term field has invalid first line",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      count_token, &line_count, "pouch index term field has invalid line count",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_u64_token(
      first_byte_token, &first_byte,
      "pouch index term field has invalid first byte", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_u64_token(
      byte_count_token, &byte_count,
      "pouch index term field has invalid byte count", error);
  if (rc != LC_OK) {
    return rc;
  }
  field->field_hex = lc_strdup_with_allocator(allocator, field_hex);
  if (field->field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term field", NULL, NULL,
                        NULL);
  }
  field->first_line = first_line;
  field->line_count = line_count;
  field->first_byte = first_byte;
  field->byte_count = byte_count;
  return LC_OK;
}

int lc_pouch_index_term_value_parse_line(char *line,
                                         lc_pouch_index_term_value *value,
                                         const lc_allocator *allocator,
                                         lc_error *error) {
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *first_token;
  char *count_token;
  char *first_byte_token;
  char *byte_count_token;
  unsigned long first_line;
  unsigned long line_count;
  uint64_t first_byte;
  uint64_t byte_count;
  int rc;

  if (line == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value requires line and output", NULL,
                        NULL, NULL);
  }
  first_byte = 0U;
  byte_count = 0U;
  if (strncmp(line, "term_value ", sizeof("term_value ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("term_value ") - 1U;
  field_hex = lc_pouch_index_term_next_token(&cursor, 0);
  value_hex = lc_pouch_index_term_next_token(&cursor, 0);
  first_token = lc_pouch_index_term_next_token(&cursor, 0);
  count_token = lc_pouch_index_term_next_token(&cursor, 0);
  first_byte_token = lc_pouch_index_term_next_token(&cursor, 0);
  byte_count_token = lc_pouch_index_term_next_token(&cursor, 1);
  if (field_hex == NULL || value_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_index_term_hex_token_valid(field_hex) ||
      !lc_pouch_index_term_hex_token_valid(value_hex) || first_token == NULL ||
      count_token == NULL || first_byte_token == NULL ||
      byte_count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value has invalid fields", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_token, &first_line, "pouch index term value has invalid first line",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      count_token, &line_count, "pouch index term value has invalid line count",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_u64_token(
      first_byte_token, &first_byte,
      "pouch index term value has invalid first byte", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_u64_token(
      byte_count_token, &byte_count,
      "pouch index term value has invalid byte count", error);
  if (rc != LC_OK) {
    return rc;
  }
  value->field_hex = lc_strdup_with_allocator(allocator, field_hex);
  value->value_hex = lc_strdup_with_allocator(allocator, value_hex);
  if (value->field_hex == NULL || value->value_hex == NULL) {
    lc_free_with_allocator(allocator, value->field_hex);
    lc_free_with_allocator(allocator, value->value_hex);
    memset(value, 0, sizeof(*value));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term value", NULL, NULL,
                        NULL);
  }
  value->first_line = first_line;
  value->line_count = line_count;
  value->first_byte = first_byte;
  value->byte_count = byte_count;
  return LC_OK;
}

int lc_pouch_index_term_fields_select_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const char *field_hex, const lc_pouch_index_term_key *terms,
    size_t term_count, unsigned long term_line_count, uint64_t term_byte_count,
    unsigned long *first_line, unsigned long *line_count, uint64_t *first_byte,
    uint64_t *byte_count) {
  unsigned long first;
  unsigned long count;
  unsigned long end;
  unsigned long min_first;
  unsigned long max_end;
  uint64_t min_byte;
  uint64_t max_byte_end;
  uint64_t byte_end;
  size_t index;
  size_t field_index;
  int found;

  if (first_line == NULL || line_count == NULL || first_byte == NULL ||
      byte_count == NULL) {
    return 0;
  }
  if (term_count == 0U) {
    if (!lc_pouch_index_term_fields_find(fields, field_count, field_hex,
                                         first_line, line_count)) {
      return 0;
    }
    for (index = 0U; index < field_count; ++index) {
      if (strcmp(fields[index].field_hex, field_hex) == 0) {
        *first_byte = fields[index].first_byte;
        *byte_count = fields[index].byte_count;
        return 1;
      }
    }
    return 0;
  }
  if (terms == NULL) {
    return 0;
  }
  found = 0;
  min_first = term_line_count;
  max_end = 0UL;
  min_byte = term_byte_count;
  max_byte_end = 0UL;
  for (index = 0U; index < term_count; ++index) {
    if (lc_pouch_index_term_fields_find(
            fields, field_count, terms[index].field_hex, &first, &count)) {
      end = first + count;
      if (!found || first < min_first) {
        min_first = first;
      }
      if (end > max_end) {
        max_end = end;
      }
      for (field_index = 0U; field_index < field_count; ++field_index) {
        if (strcmp(fields[field_index].field_hex, terms[index].field_hex) ==
            0) {
          if (fields[field_index].byte_count >
              LC_U64_MAX - fields[field_index].first_byte) {
            return 0;
          }
          byte_end =
              fields[field_index].first_byte + fields[field_index].byte_count;
          if (!found || fields[field_index].first_byte < min_byte) {
            min_byte = fields[field_index].first_byte;
          }
          if (byte_end > max_byte_end) {
            max_byte_end = byte_end;
          }
          break;
        }
      }
      found = 1;
    }
  }
  if (!found) {
    return 0;
  }
  *first_line = min_first;
  *line_count = max_end - min_first;
  *first_byte = min_byte;
  *byte_count = max_byte_end - min_byte;
  return 1;
}
