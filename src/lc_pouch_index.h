#ifndef LC_POUCH_INDEX_H
#define LC_POUCH_INDEX_H

#include "lc_internal.h"

#include <stdint.h>

typedef struct lc_pouch_index_instant {
  double seconds;
} lc_pouch_index_instant;

typedef struct lc_pouch_index_date_bounds {
  int has_gt;
  int has_gte;
  int has_lt;
  int has_lte;
  const char *gt;
  const char *gte;
  const char *lt;
  const char *lte;
} lc_pouch_index_date_bounds;

typedef struct lc_pouch_index_parsed_date_bounds {
  int has_gt;
  int has_gte;
  int has_lt;
  int has_lte;
  lc_pouch_index_instant gt;
  lc_pouch_index_instant gte;
  lc_pouch_index_instant lt;
  lc_pouch_index_instant lte;
} lc_pouch_index_parsed_date_bounds;

typedef struct lc_pouch_index_docid_set {
  unsigned long *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_docid_set;

typedef int (*lc_pouch_index_docid_visit_fn)(void *context,
                                             unsigned long doc_id,
                                             lc_error *error);

typedef struct lc_pouch_index_doc {
  const char *key_hex;
  uint64_t version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  int owns_key_hex;
} lc_pouch_index_doc;

typedef struct lc_pouch_index_doc_table {
  lc_pouch_index_doc *items;
  char *owned_generation_bytes;
  size_t count;
  size_t capacity;
  int sorted_by_key;
} lc_pouch_index_doc_table;

typedef struct lc_pouch_index_result_key {
  char *key_hex;
  unsigned long doc_id;
  uint64_t version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  size_t value_index;
} lc_pouch_index_result_key;

typedef struct lc_pouch_index_result_key_list {
  lc_pouch_index_result_key *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_result_key_list;

typedef struct lc_pouch_index_result_docid {
  unsigned long doc_id;
  size_t value_index;
} lc_pouch_index_result_docid;

typedef struct lc_pouch_index_result_docid_list {
  lc_pouch_index_result_docid *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_result_docid_list;

typedef struct lc_pouch_index_result_row {
  char *key;
  char *key_hex;
  unsigned long doc_id;
  uint64_t version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  int owns_key;
  int owns_key_hex;
  size_t value_index;
} lc_pouch_index_result_row;

typedef struct lc_pouch_index_result_row_list {
  lc_pouch_index_result_row *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_result_row_list;

typedef struct lc_pouch_index_term_field {
  char *field_hex;
  unsigned long first_line;
  unsigned long line_count;
  uint64_t first_byte;
  uint64_t byte_count;
} lc_pouch_index_term_field;

typedef struct lc_pouch_index_term_value {
  char *field_hex;
  char *value_hex;
  unsigned long first_line;
  unsigned long line_count;
  uint64_t first_byte;
  uint64_t byte_count;
} lc_pouch_index_term_value;

typedef struct lc_pouch_index_term_key {
  const char *field_hex;
  const char *value_hex;
  char value_type;
} lc_pouch_index_term_key;

typedef struct lc_pouch_index_plain_term {
  const char *field;
  const char *value;
  char value_type;
} lc_pouch_index_plain_term;

typedef struct lc_pouch_index_term_range {
  unsigned long first_line;
  unsigned long line_count;
  uint64_t first_byte;
  uint64_t byte_count;
} lc_pouch_index_term_range;

typedef struct lc_pouch_index_posting {
  unsigned char *bytes;
  size_t length;
  size_t capacity;
  size_t count;
  unsigned long last_doc_id;
  int has_last_doc_id;
  /* Most trigram terms occur in one document per immutable generation. Keep
   * that varint local so a flush does not allocate one heap block per term. */
  unsigned char inline_bytes[sizeof(unsigned long) * 2U];
  int using_inline_bytes;
} lc_pouch_index_posting;

typedef struct lc_pouch_index_dense_posting {
  unsigned char *bits;
  size_t length;
  size_t capacity;
  size_t count;
  unsigned long max_doc_id;
  unsigned long last_doc_id;
  int has_last_doc_id;
} lc_pouch_index_dense_posting;

typedef enum lc_pouch_index_adaptive_posting_kind {
  LC_POUCH_INDEX_ADAPTIVE_POSTING_SPARSE = 1,
  LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE = 2
} lc_pouch_index_adaptive_posting_kind;

typedef struct lc_pouch_index_adaptive_posting {
  lc_pouch_index_posting sparse;
  lc_pouch_index_dense_posting dense;
  int dense_disabled;
} lc_pouch_index_adaptive_posting;

typedef struct lc_pouch_index_term_entry {
  char *field_hex;
  char *value_hex;
  unsigned long trigram_key;
  uint64_t text_trigram_bloom;
  char value_type;
  unsigned long term_id;
  int owns_field_hex;
  int value_is_trigram_key;
  int has_text_trigram_bloom;
} lc_pouch_index_term_entry;

typedef struct lc_pouch_index_term_table {
  lc_pouch_index_term_entry *items;
  size_t count;
  size_t capacity;
  unsigned long next_term_id;
} lc_pouch_index_term_table;

typedef struct lc_pouch_index_term_posting_entry {
  unsigned long term_id;
  lc_pouch_index_adaptive_posting posting;
} lc_pouch_index_term_posting_entry;

typedef struct lc_pouch_index_term_posting_table {
  lc_pouch_index_term_posting_entry *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_term_posting_table;

typedef struct lc_pouch_index_term_generation {
  char *ns;
  uint64_t index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_term_table terms;
  lc_pouch_index_term_posting_table postings;
  int terms_sorted;
} lc_pouch_index_term_generation;

void lc_pouch_index_docid_set_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_docid_set *set);
int lc_pouch_index_docid_set_append_sorted_unique(lc_pouch_index_docid_set *set,
                                                  unsigned long doc_id,
                                                  int *added,
                                                  const lc_allocator *allocator,
                                                  lc_error *error);
int lc_pouch_index_docid_set_append_unique(lc_pouch_index_docid_set *set,
                                           unsigned long doc_id, int *added,
                                           const lc_allocator *allocator,
                                           lc_error *error);
int lc_pouch_index_docid_set_union_sorted(const lc_pouch_index_docid_set *left,
                                          const lc_pouch_index_docid_set *right,
                                          lc_pouch_index_docid_set *out,
                                          const lc_allocator *allocator,
                                          lc_error *error);
int lc_pouch_index_docid_set_intersect_sorted(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, const lc_allocator *allocator,
    lc_error *error);
int lc_pouch_index_docid_set_subtract_sorted(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, const lc_allocator *allocator,
    lc_error *error);
void lc_pouch_index_doc_table_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_doc_table *table);
int lc_pouch_index_doc_table_append_owned(
    lc_pouch_index_doc_table *table, const char *key_hex, uint64_t version,
    uint64_t bytes, int has_query_hidden, int query_hidden,
    unsigned long *doc_id, const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_doc_table_append_sorted_unique(
    lc_pouch_index_doc_table *table, const char *key_hex, uint64_t version,
    uint64_t bytes, int has_query_hidden, int query_hidden,
    unsigned long *doc_id, const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_doc_table_find_key_hex(const lc_pouch_index_doc_table *table,
                                          const char *key_hex,
                                          unsigned long *doc_id, int *found,
                                          lc_error *error);
int lc_pouch_index_doc_table_get(const lc_pouch_index_doc_table *table,
                                 unsigned long doc_id,
                                 const lc_pouch_index_doc **doc,
                                 lc_error *error);
int lc_pouch_index_doc_table_generation_encode(
    const lc_pouch_index_doc_table *table, uint64_t index_seq,
    unsigned long row_hash, const lc_allocator *allocator, char **out_bytes,
    size_t *out_length, lc_error *error);
int lc_pouch_index_doc_table_generation_load_bytes(
    const lc_allocator *allocator, char **bytes_inout, size_t length,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *valid, lc_error *error);
int lc_pouch_index_doc_table_generation_load_file(
    const lc_allocator *allocator, const char *path,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *present, int *valid, lc_error *error);
void lc_pouch_index_result_key_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *list);
int lc_pouch_index_result_key_list_add(const lc_allocator *allocator,
                                       lc_pouch_index_result_key_list *list,
                                       const char *key_hex,
                                       unsigned long doc_id, uint64_t version,
                                       uint64_t bytes, int has_query_hidden,
                                       int query_hidden, size_t value_index,
                                       lc_error *error);
int lc_pouch_index_result_key_list_sort_compact_docids(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *list,
    lc_error *error);
void lc_pouch_index_result_docid_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *list);
int lc_pouch_index_result_docid_list_add(const lc_allocator *allocator,
                                         lc_pouch_index_result_docid_list *list,
                                         unsigned long doc_id,
                                         size_t value_index, lc_error *error);
int lc_pouch_index_result_docid_list_sort_compact(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *list,
    lc_error *error);
void lc_pouch_index_result_row_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *list);
int lc_pouch_index_result_row_list_add(const lc_allocator *allocator,
                                       lc_pouch_index_result_row_list *list,
                                       const char *key, const char *key_hex,
                                       unsigned long doc_id, uint64_t version,
                                       uint64_t bytes, int has_query_hidden,
                                       int query_hidden, size_t value_index,
                                       lc_error *error);
void lc_pouch_index_term_fields_cleanup(const lc_allocator *allocator,
                                        lc_pouch_index_term_field *fields,
                                        size_t count);
void lc_pouch_index_term_values_cleanup(const lc_allocator *allocator,
                                        lc_pouch_index_term_value *values,
                                        size_t count);
int lc_pouch_index_term_fields_find(const lc_pouch_index_term_field *fields,
                                    size_t count, const char *field_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count);
int lc_pouch_index_term_values_find(const lc_pouch_index_term_value *values,
                                    size_t count, const char *field_hex,
                                    const char *value_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count);
void lc_pouch_index_term_keys_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_term_key *terms,
                                      size_t count);
int lc_pouch_index_term_key_compare_items(const lc_pouch_index_term_key *left,
                                          const lc_pouch_index_term_key *right);
int lc_pouch_index_term_key_compare(const void *left, const void *right);
int lc_pouch_index_term_key_compare_pair(const char *field_hex,
                                         const char *value_hex,
                                         const lc_pouch_index_term_key *term);
int lc_pouch_index_term_keys_find(const lc_pouch_index_term_key *terms,
                                  size_t count, const char *field_hex,
                                  const char *value_hex, char value_type,
                                  size_t *index_out);
int lc_pouch_index_term_keys_build_exact(const lc_pouch_index_plain_term *terms,
                                         size_t term_count,
                                         lc_pouch_index_term_key **out_terms,
                                         size_t *out_count,
                                         const lc_allocator *allocator,
                                         lc_error *error);
int lc_pouch_index_term_keys_build_exact_for_field(
    const char *field, const char *const *values, const char *value_types,
    size_t value_count, lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error);
void lc_pouch_index_term_table_cleanup(const lc_allocator *allocator,
                                       lc_pouch_index_term_table *table);
int lc_pouch_index_term_table_find(const lc_pouch_index_term_table *table,
                                   const char *field_hex, const char *value_hex,
                                   char value_type, unsigned long *term_id_out);
int lc_pouch_index_term_table_find_or_add(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, lc_error *error);
int lc_pouch_index_term_table_find_or_add_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, lc_error *error);
int lc_pouch_index_term_table_append_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, lc_error *error);
/** Appends a trusted raw trigram term without materializing its six-byte hex
 * spelling. `field_hex` is borrowed and must outlive the table; encoding
 * retains the normal on-disk term representation. */
int lc_pouch_index_term_table_append_trigram_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, unsigned long trigram_key,
    unsigned long *term_id_out, lc_error *error);
void lc_pouch_index_term_posting_table_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table);
/** Reserves posting slots without changing existing entries. Inline posting
 * storage is rebound after relocation; failure leaves the table intact. */
int lc_pouch_index_term_posting_table_reserve(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    size_t needed, lc_error *error);
int lc_pouch_index_term_posting_table_put(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    unsigned long term_id, const unsigned long *doc_ids, size_t doc_id_count,
    lc_error *error);
int lc_pouch_index_term_posting_table_append_sorted_unique_trusted(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    unsigned long term_id, const unsigned long *doc_ids, size_t doc_id_count,
    lc_error *error);
int lc_pouch_index_term_posting_table_append_to_set(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    lc_pouch_index_docid_set *set, const lc_allocator *allocator,
    lc_error *error);
int lc_pouch_index_term_posting_table_visit(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    lc_pouch_index_docid_visit_fn visit, void *context, lc_error *error);
/* Applies a dense posting directly to an equally bounded caller bitmap.
 * Returns success with applied == 0 for sparse or absent postings. This is an
 * internal fast path for validated in-memory index generations. */
int lc_pouch_index_term_posting_table_or_dense_bits(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    unsigned char *bits, size_t length, int *applied, lc_error *error);
void lc_pouch_index_term_generation_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation);
/** Orders terms for immutable generation encoding without changing term IDs or
 * their postings. Call only after the generation has stopped accepting terms.
 */
void lc_pouch_index_term_generation_sort_terms(
    lc_pouch_index_term_generation *generation);
/** Encodes the current compact term artifact: field names are dictionary
 * entries and trusted trigrams remain three-byte values. Decoders retain
 * compatibility with the legacy field-per-term artifact. */
int lc_pouch_index_term_generation_encode(
    const lc_pouch_index_term_generation *generation,
    const lc_allocator *allocator, char **out_bytes, size_t *out_length,
    lc_error *error);
typedef struct lc_pouch_index_term_limits {
  size_t max_terms;
  size_t max_term_bytes;
} lc_pouch_index_term_limits;
/* Optional limits bound decoded dictionary/posting cardinality and expanded
 * field/value bytes before allocation, including dictionary expansion. Bounded
 * pages also require posting slots within expected_row_count. NULL retains
 * ordinary namespace-generation limits and its independent docID domain. */
int lc_pouch_index_term_generation_decode(
    const lc_allocator *allocator, const char *bytes, size_t length,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, const lc_pouch_index_term_limits *limits,
    lc_pouch_index_term_generation *generation, lc_error *error);
int lc_pouch_index_term_generation_load_bytes(
    const lc_allocator *allocator, const char *bytes, size_t length,
    uint64_t expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_term_generation *generation,
    int *valid, lc_error *error);
int lc_pouch_index_term_field_parse_line(char *line,
                                         lc_pouch_index_term_field *field,
                                         const lc_allocator *allocator,
                                         lc_error *error);
int lc_pouch_index_term_value_parse_line(char *line,
                                         lc_pouch_index_term_value *value,
                                         const lc_allocator *allocator,
                                         lc_error *error);
int lc_pouch_index_term_fields_select_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const char *field_hex, const lc_pouch_index_term_key *terms,
    size_t term_count, unsigned long term_line_count, uint64_t term_byte_count,
    unsigned long *first_line, unsigned long *line_count, uint64_t *first_byte,
    uint64_t *byte_count);
void lc_pouch_index_posting_cleanup(const lc_allocator *allocator,
                                    lc_pouch_index_posting *posting);
int lc_pouch_index_posting_append_sorted_unique(lc_pouch_index_posting *posting,
                                                unsigned long doc_id,
                                                int *added,
                                                const lc_allocator *allocator,
                                                lc_error *error);
int lc_pouch_index_posting_append_to_set(const lc_pouch_index_posting *posting,
                                         lc_pouch_index_docid_set *set,
                                         const lc_allocator *allocator,
                                         lc_error *error);
int lc_pouch_index_posting_visit(const lc_pouch_index_posting *posting,
                                 lc_pouch_index_docid_visit_fn visit,
                                 void *context, lc_error *error);
void lc_pouch_index_dense_posting_cleanup(
    const lc_allocator *allocator, lc_pouch_index_dense_posting *posting);
int lc_pouch_index_dense_posting_append_sorted_unique(
    lc_pouch_index_dense_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_dense_posting_append_to_set(
    const lc_pouch_index_dense_posting *posting, lc_pouch_index_docid_set *set,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_dense_posting_visit(
    const lc_pouch_index_dense_posting *posting,
    lc_pouch_index_docid_visit_fn visit, void *context, lc_error *error);
void lc_pouch_index_adaptive_posting_cleanup(
    const lc_allocator *allocator, lc_pouch_index_adaptive_posting *posting);
void lc_pouch_index_adaptive_posting_rebind_inline_storage(
    lc_pouch_index_adaptive_posting *posting);
int lc_pouch_index_adaptive_posting_append_sorted_unique(
    lc_pouch_index_adaptive_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_adaptive_posting_build_sorted_unique_trusted(
    lc_pouch_index_adaptive_posting *posting, const unsigned long *doc_ids,
    size_t doc_id_count, const lc_allocator *allocator, lc_error *error);
lc_pouch_index_adaptive_posting_kind
lc_pouch_index_adaptive_posting_selected_kind(
    const lc_pouch_index_adaptive_posting *posting);
int lc_pouch_index_adaptive_posting_append_to_set(
    const lc_pouch_index_adaptive_posting *posting,
    lc_pouch_index_docid_set *set, const lc_allocator *allocator,
    lc_error *error);
int lc_pouch_index_adaptive_posting_visit(
    const lc_pouch_index_adaptive_posting *posting,
    lc_pouch_index_docid_visit_fn visit, void *context, lc_error *error);
int lc_pouch_index_parse_lql_datetime(const char *text,
                                      lc_pouch_index_instant *out);
int lc_pouch_index_parse_date_bounds(const lc_pouch_index_date_bounds *bounds,
                                     lc_pouch_index_parsed_date_bounds *out,
                                     lc_error *error);
int lc_pouch_index_date_contains_value(
    const lc_pouch_index_parsed_date_bounds *bounds,
    const lc_pouch_index_instant *value);

#endif
