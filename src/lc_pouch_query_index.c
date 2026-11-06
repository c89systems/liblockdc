#include "lc_pouch_query_index.h"

#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_log.h"
#include "lc_pouch_index_artifact.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_plan.h"
#include "lc_pouch_query_terms.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifdef LOCKDC_TEST_BUILD
lc_pouch_test_hook lc_pouch_test_after_doc_table_cache_borrow_hook = NULL;
void *lc_pouch_test_after_doc_table_cache_borrow_context = NULL;
lc_pouch_test_hook lc_pouch_test_after_doc_table_cache_rows_attached_hook =
    NULL;
void *lc_pouch_test_after_doc_table_cache_rows_attached_context = NULL;
lc_pouch_test_hook lc_pouch_test_before_doc_table_decoded_key_populate_hook =
    NULL;
void *lc_pouch_test_before_doc_table_decoded_key_populate_context = NULL;
size_t lc_pouch_test_doc_table_cache_capacity = 0U;
#endif

#define LC_POUCH_QUERY_INDEX_FORMAT "pouch-query-index"
#define LC_POUCH_QUERY_INDEX_VERSION 12UL
#define LC_POUCH_QUERY_INDEX_LEAF "query.index"
#define LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF "query.index.lcpdtg"
#define LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF "query.index.lcpttg"
#define LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF "query.index.lcppg"
#define LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF "query.index.lcprg"
#define LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF "query.index.lcptxg"
#define LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF "query.index.lcpt3g"
#define LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF "query.index.lcptdg"
#define LC_POUCH_QUERY_INDEX_DELETE_LEAF "query.index.lcpdel"
#define LC_POUCH_QUERY_INDEX_PACKED_LEAF "query.index.lcpseg"
#define LC_POUCH_QUERY_INDEX_DOC_TABLE_CACHE_MAX 64U
#define LC_POUCH_QUERY_INDEX_PACKED_MAGIC "LPQISEG1"
#define LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN 8U
#define LC_POUCH_QUERY_INDEX_PACKED_VERSION ((uint64_t)1U)
#define LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT 8U
#define LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE 0U
#define LC_POUCH_QUERY_INDEX_PACKED_EXACT 1U
#define LC_POUCH_QUERY_INDEX_PACKED_PRESENCE 2U
#define LC_POUCH_QUERY_INDEX_PACKED_RANGE 3U
#define LC_POUCH_QUERY_INDEX_PACKED_TEXT 4U
#define LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM 5U
#define LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL 6U
#define LC_POUCH_QUERY_INDEX_PACKED_DELETE 7U
#define LC_POUCH_QUERY_INDEX_MANIFEST_FORMAT "pouch-query-index-manifest"
#define LC_POUCH_QUERY_INDEX_MANIFEST_VERSION 3UL
#define LC_POUCH_QUERY_INDEX_MANIFEST_LEAF "query.manifest"
#define LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX ".lcpcrypto"
#define LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_MAGIC "LPQICRF1"
#define LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES 16U
#define LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD "/..."
#define LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX "2f2e2e2e"
#define LC_POUCH_QUERY_INDEX_MAX_CONTAINS_TRIGRAMS 8U
#define LC_POUCH_QUERY_INDEX_MAX_SEGMENTS 8U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT 1U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PACKED 0U
#define LC_POUCH_QUERY_INDEX_HASH_OFFSET 2166136261UL
#define LC_POUCH_QUERY_INDEX_HASH_PRIME 16777619UL
#define LC_POUCH_QUERY_INDEX_HASH_MASK 0xffffffffUL
#define LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES 256U
#define LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE 'h'
#define LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE 'p'
/* All-text word postings are a bounded positive accelerator for icontains.
 * They never decide a negative: non-word and partial-word searches retain the
 * trigram-plus-verifier path below. */
#define LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE 'w'
#define LC_POUCH_QUERY_INDEX_TEXT_TOKEN_SOURCE_BYTES_MAX 1024U
#define LC_POUCH_QUERY_INDEX_TEXT_TOKEN_BYTES_MAX 96U
#define LC_POUCH_QUERY_INDEX_TEXT_TOKEN_COUNT_MAX 64U
#define LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD 4096U
#define LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES 2097152U
/* Lucene-style bounded text indexing: exact hashes and a retained prefix keep
 * long values searchable while candidate verification avoids blob-sized
 * trigram posting lists on the write path. */
#define LC_POUCH_QUERY_INDEX_TRIGRAM_INDEXED_BYTES_MAX                         \
  LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES
/* Small writes feed the exclusive memtable synchronously. Larger bodies are
 * indexed from their durable span when the indexer publishes the segment. */
#define LC_POUCH_QUERY_INDEX_FOREGROUND_CAPTURE_BYTES_MAX (64U * 1024U)
/* UTF-8 JSON strings cannot produce this raw-byte trigram. It marks a large
 * value whose contains candidates must be verified against state. */
#define LC_POUCH_QUERY_INDEX_TRIGRAM_FALLBACK_KEY 0xffffffUL
#define LC_POUCH_QUERY_INDEX_TRIGRAM_FALLBACK_HEX "ffffff"
#define LC_POUCH_QUERY_INDEX_INLINE_DOCIDS 4U
#define LC_POUCH_QUERY_INDEX_PENDING_SOURCE_BYTES_MAX (8U * 1024U * 1024U)
#define LC_POUCH_QUERY_INDEX_PENDING_TERMS_MAX 262144U
#define LC_POUCH_INDEX_TERM_GENERATION_MAGIC "LPITGEN1"
#define LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN 8U
#define LC_POUCH_INDEX_TERM_GENERATION_VERSION_V1 ((uint64_t)1U)
#define LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 ((uint64_t)2U)
#define LC_POUCH_INDEX_TERM_VALUE_STRING ((unsigned char)0U)
#define LC_POUCH_INDEX_TERM_VALUE_TRIGRAM ((unsigned char)1U)

typedef struct lc_pouch_query_index_row {
  char *key;
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  int needs_extraction;
} lc_pouch_query_index_row;

typedef struct lc_pouch_query_index_term {
  char *field_hex;
  char *value_hex;
  char *canonical_value_hex;
  char *long_value;
  char *text_prefix_hex;
  char *temporal_value_hex;
  unsigned long *trigram_keys;
  size_t long_value_len;
  size_t trigram_key_count;
  size_t trigram_key_capacity;
  char *key_hex;
  char value_type;
  int derived_terms_ready;
  int trigram_keys_sorted;
  int trigram_keys_truncated;
  unsigned long doc_id;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_query_index_term;

typedef struct lc_pouch_query_index_presence {
  char *field_hex;
  char *key_hex;
} lc_pouch_query_index_presence;

typedef struct lc_pouch_query_index_summary {
  const lc_allocator *allocator;
  lc_pouch *pouch;
  const char *ns;
  lc_pouch_query_index_row *rows;
  size_t count;
  size_t capacity;
  lc_pouch_query_index_term *terms;
  size_t term_count;
  size_t term_capacity;
  int term_index_complete;
  lc_pouch_query_index_presence *presences;
  size_t presence_count;
  size_t presence_capacity;
  int presence_index_complete;
} lc_pouch_query_index_summary;

typedef struct lc_pouch_query_index_extract_batch {
  lc_pouch_query_index_summary *summary;
  const size_t *row_indices;
  size_t index;
} lc_pouch_query_index_extract_batch;

typedef struct lc_pouch_query_index_incremental_change {
  char *key;
  char *key_hex;
  char *content_type;
  char *etag;
  char *descriptor;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
  int has_payload;
  /* A metadata-only tail record retains the cached normalized body terms. */
  int preserves_cached_body;
} lc_pouch_query_index_incremental_change;

typedef struct lc_pouch_query_index_incremental_context {
  lc_pouch_query_index_summary *summary;
  lc_pouch_query_index_incremental_change *changes;
  size_t change_count;
  size_t change_capacity;
  const char **read_keys;
  size_t read_key_count;
  size_t read_key_capacity;
  size_t *read_row_indices;
  size_t read_row_index_capacity;
} lc_pouch_query_index_incremental_context;

typedef struct lc_pouch_query_index_text {
  const lc_allocator *allocator;
  char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_query_index_text;

typedef struct lc_pouch_query_index_docid_bitmap {
  const lc_allocator *allocator;
  unsigned char *bits;
  size_t length;
  unsigned long doc_count;
} lc_pouch_query_index_docid_bitmap;

typedef struct lc_pouch_query_index_direct_posting_append {
  const lc_allocator *allocator;
  const lc_pouch_index_result_docid_list *filter;
  lc_pouch_index_result_docid_list *docids;
  size_t value_index;
} lc_pouch_query_index_direct_posting_append;

typedef struct lc_pouch_query_index_direct_unique_posting_append {
  const lc_allocator *allocator;
  const lc_pouch_index_result_docid_list *filter;
  lc_pouch_query_index_docid_bitmap *seen;
  lc_pouch_index_result_docid_list *docids;
  size_t value_index;
  size_t *pending;
} lc_pouch_query_index_direct_unique_posting_append;

static int
lc_pouch_query_index_text_append_cstr(lc_pouch_query_index_text *text,
                                      const char *bytes, unsigned long *hash,
                                      lc_error *error);
static int lc_pouch_query_index_direct_posting_append_docid(
    void *context, unsigned long doc_id, lc_error *error);
static int lc_pouch_query_index_exact_generation_number_text_hex(
    const lc_allocator *allocator, const char *value, size_t value_len,
    char **out, lc_error *error);

typedef struct lc_pouch_query_index_read_result {
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  int term_index_complete;
  int presence_index_complete;
  int present;
  int valid;
} lc_pouch_query_index_read_result;

typedef struct lc_pouch_query_index_manifest_artifact_signature {
  uint64_t size;
  unsigned long mtime;
  unsigned long mtime_nsec;
  unsigned long ctime;
  unsigned long ctime_nsec;
  unsigned long inode;
  int present;
} lc_pouch_query_index_manifest_artifact_signature;

typedef struct lc_pouch_query_index_manifest_segment {
  char *id;
  lc_pouch_generation base_index_seq;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long delete_count;
  unsigned long delete_hash;
  int term_index_complete;
  int presence_index_complete;
  lc_pouch_query_index_manifest_artifact_signature
      artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT];
} lc_pouch_query_index_manifest_segment;

typedef struct lc_pouch_query_index_manifest {
  lc_pouch_generation index_seq;
  lc_pouch_query_index_manifest_segment *segments;
  size_t segment_count;
  size_t segment_capacity;
  int present;
  int valid;
  int intentionally_invalidated;
} lc_pouch_query_index_manifest;

typedef struct lc_pouch_query_index_generation_cache_term {
  char *field_hex;
  char *value_hex;
  char value_hex_inline[7];
  char value_type;
  unsigned long term_id;
  unsigned long ordinal;
} lc_pouch_query_index_generation_cache_term;

typedef struct lc_pouch_query_index_generation_cache_posting {
  unsigned long term_id;
  char kind;
  unsigned long count;
  unsigned long max_doc_id;
  unsigned long payload_length;
  const unsigned char *payload;
} lc_pouch_query_index_generation_cache_posting;

typedef struct lc_pouch_query_index_generation_cache_field {
  const char *field_hex;
} lc_pouch_query_index_generation_cache_field;

struct lc_pouch_query_index_generation_cache_entry {
  char *path;
  char *bytes;
  size_t length;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_query_index_generation_cache_term *terms;
  size_t term_count;
  lc_pouch_query_index_generation_cache_field *fields;
  size_t field_count;
  /* Direct generations retain field names in their term table. The compact
   * field directory borrows those names instead of duplicating them. */
  int fields_borrow_direct_generation;
  lc_pouch_query_index_generation_cache_posting *postings;
  size_t posting_count;
  /* A newly published single-writer trigram generation is already decoded.
   * Retain its body-free posting table instead of serializing and parsing it
   * again solely to populate this cache. */
  lc_pouch_index_term_generation direct_generation;
  int has_direct_generation;
  int direct_generation_is_text;
  size_t *direct_text_prefix_indices;
  size_t direct_text_prefix_count;
  struct lc_pouch_query_index_generation_cache_entry *next;
};

struct lc_pouch_query_index_doc_table_cache_entry {
  char *path;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_doc_table table;
  char **decoded_keys;
  size_t decoded_key_count;
  /* A query holds a borrow from lookup until every row view it built has been
   * emitted. Replaced and evicted entries retire first and are freed by the
   * final borrower, so publication can never invalidate an in-flight query. */
  size_t borrow_count;
  int retired;
  struct lc_pouch_query_index_doc_table_cache_entry *next;
};

typedef struct lc_pouch_query_index_key_hex_set {
  char **items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_key_hex_set;

typedef struct lc_pouch_query_index_doc_table_cache_borrow_list {
  struct lc_pouch_query_index_doc_table_cache_entry **items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_doc_table_cache_borrow_list;

typedef struct lc_pouch_query_index_memtable lc_pouch_query_index_memtable;

/* A publication guard is bound to one lease or transaction, not merely the
 * document key. This prevents a late completion of an earlier lease from
 * clearing a newer lease's provisional index boundary. */
struct lc_pouch_query_index_active_operation {
  char *ns;
  char *key;
  char *operation_id;
  lc_pouch_unix_seconds expires_at_unix;
  struct lc_pouch_query_index_active_operation *next;
};

/* A bounded single-writer memtable. It retains only normalized index
 * material, never source JSON or document bodies. A process restart simply
 * replays the durable tail. */
struct lc_pouch_query_index_pending_entry {
  char *ns;
  lc_pouch_generation base_index_seq;
  lc_pouch_generation last_index_seq;
  /* Set only when this handle observed every query-visible row since an empty
   * index generation. It permits a safe initial single-writer build. */
  int covers_namespace;
  /* A capture failure leaves this entry as a replay marker until publication.
   * It must never seed a partial index segment. */
  int incomplete;
  lc_pouch_query_index_summary summary;
  lc_pouch_query_index_key_hex_set deletes;
  /* Distinct keys whose bodies must be replayed. This preserves the normal
   * threshold policy even when the foreground path cannot retain a source. */
  lc_pouch_query_index_key_hex_set incomplete_keys;
  lc_pouch_query_index_memtable *memtable;
  struct lc_pouch_query_index_pending_entry *next;
};

typedef enum lc_pouch_query_index_segmented_collect_kind {
  LC_POUCH_QUERY_INDEX_SEGMENTED_ALL = 0,
  LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT = 1,
  LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE = 2,
  LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE = 3,
  LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT = 4,
  LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM = 5,
  LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL = 6
} lc_pouch_query_index_segmented_collect_kind;

typedef struct lc_pouch_query_index_row_reader {
  const lc_allocator *allocator;
  unsigned long row_index;
  lc_pouch_query_index_row_visit_fn visit;
  void *context;
} lc_pouch_query_index_row_reader;

typedef struct lc_pouch_query_index_term_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  const char *value_hex;
  const char *value_text;
  char value_type;
  int value_type_match;
  int prefix_match;
  int contains_match;
  int ignore_case;
  int string_values_only;
  int range_match;
  int date_match;
  int stop;
  lc_pouch_query_index_range_bounds range_bounds;
  lc_pouch_query_index_date_bounds date_bounds;
  lc_pouch_index_parsed_date_bounds parsed_date_bounds;
  char *value_scratch;
  size_t value_scratch_capacity;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_term_reader;

typedef struct lc_pouch_query_index_presence_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_presence_reader;

typedef struct lc_pouch_query_index_exact_generation_docids {
  unsigned long term_id;
  lc_pouch_index_docid_set docids;
  unsigned long inline_docids[LC_POUCH_QUERY_INDEX_INLINE_DOCIDS];
  int using_inline_docids;
  int docids_need_sort;
} lc_pouch_query_index_exact_generation_docids;

typedef struct lc_pouch_query_index_exact_generation_accumulator {
  lc_pouch_query_index_exact_generation_docids *items;
  size_t *term_positions;
  size_t count;
  size_t capacity;
  size_t term_position_capacity;
} lc_pouch_query_index_exact_generation_accumulator;

typedef struct lc_pouch_query_index_trigram_value_set {
  unsigned long *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_trigram_value_set;

/* One source of sorted trigram keys. The merge builder emits immutable term
 * and posting tables without duplicating every key into a global sort buffer.
 */
typedef struct lc_pouch_query_index_trigram_pair {
  unsigned long key;
  unsigned long doc_id;
} lc_pouch_query_index_trigram_pair;

static int lc_pouch_query_index_term_precompute_derived(
    lc_pouch_query_index_summary *summary, lc_pouch_query_index_term *term,
    const char *value, size_t value_len, lc_error *error);
typedef struct lc_pouch_query_index_trigram_term_cache_entry {
  const char *field_hex;
  unsigned long field_id;
  unsigned long field_hash;
  unsigned long trigram_key;
  unsigned long term_id;
  int used;
} lc_pouch_query_index_trigram_term_cache_entry;

typedef struct lc_pouch_query_index_trigram_term_cache {
  lc_pouch_query_index_trigram_term_cache_entry *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_trigram_term_cache;

typedef struct lc_pouch_query_index_field_cache_entry {
  char *field_hex;
  unsigned long field_hash;
  unsigned long field_id;
  int used;
} lc_pouch_query_index_field_cache_entry;

typedef struct lc_pouch_query_index_field_cache {
  lc_pouch_query_index_field_cache_entry *items;
  size_t count;
  size_t capacity;
  unsigned long next_field_id;
} lc_pouch_query_index_field_cache;

typedef struct lc_pouch_query_index_term_cache_entry {
  const char *field_hex;
  const char *value_hex;
  unsigned long hash;
  unsigned long term_id;
  char value_type;
  int used;
} lc_pouch_query_index_term_cache_entry;

typedef struct lc_pouch_query_index_term_cache {
  lc_pouch_query_index_term_cache_entry *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_term_cache;

/* The exclusive-writer memtable is deliberately limited to derived rows,
 * term dictionaries, and posting lists. It owns no source JSON and is never
 * consulted for recovery: a restart rebuilds it from the durable state tail. */
struct lc_pouch_query_index_memtable {
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation exact_generation;
  lc_pouch_index_term_generation presence_generation;
  lc_pouch_index_term_generation range_generation;
  lc_pouch_index_term_generation text_generation;
  lc_pouch_index_term_generation trigram_generation;
  lc_pouch_index_term_generation temporal_generation;
  lc_pouch_query_index_exact_generation_accumulator exact_accumulator;
  lc_pouch_query_index_exact_generation_accumulator presence_accumulator;
  lc_pouch_query_index_exact_generation_accumulator range_accumulator;
  lc_pouch_query_index_exact_generation_accumulator text_accumulator;
  lc_pouch_query_index_exact_generation_accumulator trigram_accumulator;
  lc_pouch_query_index_exact_generation_accumulator temporal_accumulator;
  lc_pouch_query_index_term_cache exact_term_cache;
  lc_pouch_query_index_term_cache presence_term_cache;
  lc_pouch_query_index_term_cache range_term_cache;
  lc_pouch_query_index_term_cache text_term_cache;
  lc_pouch_query_index_term_cache temporal_term_cache;
  lc_pouch_query_index_field_cache trigram_field_cache;
  lc_pouch_query_index_trigram_term_cache trigram_term_cache;
};

static void
lc_pouch_query_index_memtable_cleanup(const lc_allocator *allocator,
                                      lc_pouch_query_index_memtable *memtable);
static int lc_pouch_query_index_memtable_note_write(
    lc_pouch_query_index_pending_entry *pending,
    lc_pouch_query_index_summary *update, const char *key_hex, lc_error *error);
static int lc_pouch_query_index_memtable_note_delete(
    lc_pouch_query_index_pending_entry *pending, const char *key_hex,
    lc_error *error);
static int lc_pouch_query_index_memtable_tail_compatible(
    lc_pouch_query_index_memtable *memtable,
    const lc_pouch_query_index_summary *summary,
    const lc_pouch_query_index_incremental_context *incremental);
static int lc_pouch_query_index_memtable_encode(
    lc_pouch_query_index_memtable *memtable, lc_pouch_generation index_seq,
    lc_pouch_query_index_summary *summary, unsigned long *row_hash_out,
    unsigned long *term_count_out, unsigned long *term_hash_out,
    unsigned long *term_field_count_out, unsigned long *term_value_count_out,
    unsigned long *presence_count_out, unsigned long *presence_hash_out,
    char **doc_table_generation_out, size_t *doc_table_generation_length_out,
    char **exact_term_generation_out, size_t *exact_term_generation_length_out,
    char **presence_term_generation_out,
    size_t *presence_term_generation_length_out,
    char **range_term_generation_out, size_t *range_term_generation_length_out,
    char **text_term_generation_out, size_t *text_term_generation_length_out,
    char **trigram_term_generation_out,
    size_t *trigram_term_generation_length_out,
    char **temporal_term_generation_out,
    size_t *temporal_term_generation_length_out, lc_error *error);

static int lc_pouch_query_index_hex_value(unsigned char value);
static unsigned char lc_pouch_query_index_ascii_lower(unsigned char ch);
static void lc_pouch_query_index_trigram_value_set_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_trigram_value_set *set);
static void lc_pouch_query_index_trigram_value_set_sort_unique(
    lc_pouch_query_index_trigram_value_set *set);
static int lc_pouch_query_index_trigram_value_set_add(
    const lc_allocator *allocator, lc_pouch_query_index_trigram_value_set *set,
    unsigned long value, lc_error *error);
static int lc_pouch_query_index_trigram_generation_append_docid(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    unsigned long trigram_key, unsigned long doc_id, lc_error *error);

static int lc_pouch_query_index_trigram_seen_mark(unsigned char *seen,
                                                  unsigned long key) {
  unsigned long byte_index;
  unsigned char mask;

  if (seen == NULL) {
    return 1;
  }
  byte_index = key >> 3U;
  mask = (unsigned char)(1U << (key & 7U));
  if ((seen[byte_index] & mask) != 0U) {
    return 0;
  }
  seen[byte_index] = (unsigned char)(seen[byte_index] | mask);
  return 1;
}

static int lc_pouch_query_index_trigram_repeated_space(unsigned char a,
                                                       unsigned char b,
                                                       unsigned char c) {
  return a == b && b == c && (a == ' ' || a == '\t' || a == '\n' || a == '\r');
}

static int lc_pouch_query_index_trigram_generation_add_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    const char *value_hex, unsigned long doc_id, lc_error *error) {
  lc_pouch_query_index_trigram_value_set unique_terms;
  unsigned char *seen;
  size_t value_hex_len;
  size_t index;
  int rc;

  if (allocator == NULL || generation == NULL || accumulator == NULL ||
      field_cache == NULL || field_hex == NULL || value_hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram generation requires allocator, "
                        "generation, accumulator, field, and value",
                        NULL, NULL, NULL);
  }
  if (strcmp(value_hex, "-") == 0) {
    return LC_OK;
  }
  value_hex_len = strlen(value_hex);
  if ((value_hex_len % 2U) != 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram generation requires byte-aligned hex",
                        NULL, NULL, "pouch");
  }
  if (value_hex_len < 6U) {
    return LC_OK;
  }
  memset(&unique_terms, 0, sizeof(unique_terms));
  seen = NULL;
  if ((value_hex_len / 2U) >= LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD) {
    seen = (unsigned char *)lc_calloc_with_allocator(
        allocator, LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES, 1U);
    if (seen == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch trigram seen set", NULL,
                          NULL, NULL);
    }
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index + 6U <= value_hex_len; index += 2U) {
    unsigned char folded[3] = {0U, 0U, 0U};
    size_t byte_index;
    unsigned long folded_key;

    for (byte_index = 0U; rc == LC_OK && byte_index < 3U; ++byte_index) {
      int high;
      int low;
      unsigned char value;

      high = lc_pouch_query_index_hex_value(
          (unsigned char)value_hex[index + (byte_index * 2U)]);
      low = lc_pouch_query_index_hex_value(
          (unsigned char)value_hex[index + (byte_index * 2U) + 1U]);
      if (high < 0 || low < 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch trigram generation has invalid hex", NULL,
                          NULL, "pouch");
        break;
      }
      value = (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
      folded[byte_index] = lc_pouch_query_index_ascii_lower(value);
    }
    if (rc != LC_OK) {
      break;
    }
    folded_key = ((unsigned long)folded[0] << 16U) |
                 ((unsigned long)folded[1] << 8U) | (unsigned long)folded[2];
    if (lc_pouch_query_index_trigram_seen_mark(seen, folded_key)) {
      rc = lc_pouch_query_index_trigram_value_set_add(allocator, &unique_terms,
                                                      folded_key, error);
    }
  }
  if (rc == LC_OK && seen == NULL) {
    lc_pouch_query_index_trigram_value_set_sort_unique(&unique_terms);
  }
  for (index = 0U; rc == LC_OK && index < unique_terms.count; ++index) {
    rc = lc_pouch_query_index_trigram_generation_append_docid(
        allocator, generation, accumulator, field_cache, term_cache, field_hex,
        unique_terms.items[index], doc_id, error);
  }
  lc_pouch_query_index_trigram_value_set_cleanup(allocator, &unique_terms);
  lc_free_with_allocator(allocator, seen);
  return rc;
}

static int lc_pouch_query_index_trigram_generation_add_raw_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    const char *value, size_t value_len, unsigned long doc_id,
    lc_error *error) {
  lc_pouch_query_index_trigram_value_set unique_terms;
  unsigned char *seen;
  size_t index;
  int rc;

  if (allocator == NULL || generation == NULL || accumulator == NULL ||
      field_cache == NULL || field_hex == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch raw trigram generation requires allocator, "
                        "generation, accumulator, field, and value",
                        NULL, NULL, NULL);
  }
  if (value_len < 3U) {
    return LC_OK;
  }
  memset(&unique_terms, 0, sizeof(unique_terms));
  seen = NULL;
  if (value_len >= LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD) {
    seen = (unsigned char *)lc_calloc_with_allocator(
        allocator, LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES, 1U);
    if (seen == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch raw trigram seen set", NULL,
                          NULL, NULL);
    }
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index + 2U < value_len; ++index) {
    unsigned char bytes[3];
    unsigned char folded_bytes[3];
    size_t byte_index;
    unsigned long folded_key;

    for (byte_index = 0U; byte_index < 3U; ++byte_index) {
      unsigned char byte;

      byte = (unsigned char)value[index + byte_index];
      bytes[byte_index] = byte;
      folded_bytes[byte_index] =
          byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
    }
    folded_key = ((unsigned long)folded_bytes[0] << 16U) |
                 ((unsigned long)folded_bytes[1] << 8U) |
                 (unsigned long)folded_bytes[2];
    if (lc_pouch_query_index_trigram_seen_mark(seen, folded_key)) {
      rc = lc_pouch_query_index_trigram_value_set_add(allocator, &unique_terms,
                                                      folded_key, error);
    }
    if (lc_pouch_query_index_trigram_repeated_space(bytes[0], bytes[1],
                                                    bytes[2])) {
      size_t run_end;

      run_end = index + 3U;
      while (run_end < value_len && (unsigned char)value[run_end] == bytes[0]) {
        ++run_end;
      }
      if (run_end > index + 3U) {
        index = run_end >= 3U ? run_end - 3U : run_end;
      }
    }
  }
  if (rc == LC_OK && seen == NULL) {
    lc_pouch_query_index_trigram_value_set_sort_unique(&unique_terms);
  }
  for (index = 0U; rc == LC_OK && index < unique_terms.count; ++index) {
    rc = lc_pouch_query_index_trigram_generation_append_docid(
        allocator, generation, accumulator, field_cache, term_cache, field_hex,
        unique_terms.items[index], doc_id, error);
  }
  lc_pouch_query_index_trigram_value_set_cleanup(allocator, &unique_terms);
  lc_free_with_allocator(allocator, seen);
  return rc;
}

static int lc_pouch_query_index_text_generation_add_term(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long doc_id,
    lc_error *error);
static int lc_pouch_query_index_exact_generation_accumulator_flush(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_index_term_generation *generation, lc_error *error);

typedef struct lc_pouch_query_index_file_signature {
  uint64_t size;
  unsigned long mtime;
  unsigned long mtime_nsec;
  unsigned long ctime;
  unsigned long ctime_nsec;
  unsigned long inode;
  int present;
} lc_pouch_query_index_file_signature;

struct lc_pouch_query_index_packed_cache_entry {
  char *path;
  char *bytes;
  size_t length;
  lc_pouch_query_index_file_signature signature;
  struct lc_pouch_query_index_packed_cache_entry *next;
};

struct lc_pouch_query_index_manifest_trust_entry {
  char *ns;
  lc_pouch_generation index_seq;
  uint64_t writer_mode_epoch;
  lc_pouch_query_index_manifest manifest;
  int manifest_cached;
  struct lc_pouch_query_index_manifest_trust_entry *next;
};

#define LC_POUCH_QUERY_INDEX_PACKED_CACHE_MAX 128U

static int lc_pouch_query_index_field_hex_is_any_text(const char *field_hex);
static int lc_pouch_query_index_key_is_attachment(const char *key);
static char *
lc_pouch_query_index_exact_hash_value(const lc_allocator *allocator,
                                      const char *value, size_t value_len,
                                      lc_error *error);
static char *lc_pouch_query_index_exact_hash_format(
    const lc_allocator *allocator, unsigned long length, unsigned long hash,
    unsigned long folded_hash, lc_error *error);
static unsigned long lc_pouch_query_index_exact_hash_update(unsigned long hash,
                                                            unsigned char byte);
static int lc_pouch_query_index_visit_exact_generation(
    lc_pouch *pouch, const char *ns, const lc_pouch_index_term_key *exact_terms,
    size_t exact_term_count, lc_pouch_query_index_key_visit_fn visit,
    void *context, lc_pouch_generation *index_seq, lc_error *error);
static int lc_pouch_query_index_segmented_collect(
    lc_pouch *pouch, const char *ns,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    lc_pouch_index_result_row_list *rows,
    lc_pouch_query_index_doc_table_cache_borrow_list *doc_table_cache_borrows,
    const lc_pouch_generation *state_index_seq_snapshot,
    lc_pouch_generation *index_seq, int *text_complete_out, lc_error *error);
static int
lc_pouch_query_index_result_rows_reserve(const lc_allocator *allocator,
                                         lc_pouch_index_result_row_list *rows,
                                         size_t needed, lc_error *error);
static void lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
    lc_pouch *pouch, lc_pouch_query_index_doc_table_cache_borrow_list *list);
static int lc_pouch_query_index_parse_header_line(const char *line,
                                                  const char *name,
                                                  unsigned long *out);
static int lc_pouch_query_index_parse_header_u64(const char *line,
                                                 const char *name,
                                                 uint64_t *out);
static int lc_pouch_query_index_artifact_signature(
    const char *path, lc_pouch_query_index_file_signature *out);
static int
lc_pouch_query_index_manifest_trust_valid(const lc_pouch *pouch, const char *ns,
                                          lc_pouch_generation index_seq);
/* In exclusive-writer mode, this is the durable manifest sequence Pouch
 * published under the current ownership epoch.  It is intentionally not used
 * by validated flushes, which must still inspect the files. */
static int
lc_pouch_query_index_manifest_trust_seq(const lc_pouch *pouch, const char *ns,
                                        lc_pouch_generation *index_seq);
static void lc_pouch_query_index_manifest_trust_remember(
    lc_pouch *pouch, const char *ns, lc_pouch_generation index_seq,
    const lc_pouch_query_index_manifest *manifest);
static int lc_pouch_query_index_manifest_trust_snapshot(
    lc_pouch *pouch, const char *ns, lc_pouch_generation index_seq,
    lc_pouch_query_index_manifest *out);
static void lc_pouch_query_index_manifest_trust_forget(lc_pouch *pouch,
                                                       const char *ns);
static int lc_pouch_query_index_append_generation_posting(
    const lc_allocator *allocator, char kind, unsigned long count,
    unsigned long max_doc_id, unsigned long payload_length,
    const unsigned char *payload, size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, lc_error *error);
static int lc_pouch_query_index_read_sparse_posting_varint(
    const unsigned char *bytes, size_t length, size_t *offset,
    unsigned long *out, lc_error *error);
static int lc_pouch_query_index_docid_list_contains_sorted(
    const lc_pouch_index_result_docid_list *list, unsigned long doc_id);
static int lc_pouch_query_index_segmented_paths(
    lc_pouch *pouch, const char *ns, const char *segment_id, char **header_path,
    char **doc_table_path, char **exact_term_path, char **presence_term_path,
    char **range_term_path, char **text_term_path, char **trigram_term_path,
    char **temporal_term_path, char **delete_path, lc_error *error);
static void lc_pouch_query_index_manifest_segment_capture_artifacts(
    lc_pouch_query_index_manifest_segment *segment, const char *packed_path);
static int lc_pouch_query_index_manifest_segment_artifacts_match(
    const lc_pouch_query_index_manifest_segment *segment,
    const char *packed_path,
    lc_pouch_query_index_file_signature *actual_signatures);
static void lc_pouch_query_index_segmented_paths_cleanup(
    lc_pouch *pouch, char **header_path, char **doc_table_path,
    char **exact_term_path, char **presence_term_path, char **range_term_path,
    char **text_term_path, char **trigram_term_path, char **temporal_term_path,
    char **delete_path);

typedef struct lc_pouch_query_index_source_reader {
  lc_source *source;
  lc_error error;
} lc_pouch_query_index_source_reader;

static int lc_pouch_query_index_skip_lines(FILE *fp, unsigned long count,
                                           const lc_allocator *allocator,
                                           int *valid, lc_error *error);

typedef struct lc_pouch_query_index_extract_context {
  lc_pouch_query_index_summary *summary;
  const char *key_hex;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  char *field;
  size_t field_len;
  size_t field_capacity;
  char *array_field;
  size_t array_field_len;
  size_t array_field_capacity;
  char *value;
  size_t value_len;
  size_t value_capacity;
  unsigned long string_length;
  unsigned long string_hash;
  unsigned long string_folded_hash;
  int capturing_string;
  unsigned char *array_depths;
  size_t array_depth_capacity;
  int has_array_field;
  lc_pouch_query_term_fn visit;
  void *visit_context;
  lc_error visit_error;
  int visit_rc;
  int unrepresentable_number;
} lc_pouch_query_index_extract_context;

static int lc_pouch_query_index_parse_number_value(const char *text,
                                                   double *out);

static unsigned long lc_pouch_query_index_hash_init(void) {
  return LC_POUCH_QUERY_INDEX_HASH_OFFSET;
}

static int lc_pouch_query_index_hex_value(unsigned char value);

static void lc_pouch_query_index_hash_byte(unsigned long *hash,
                                           unsigned char byte) {
  *hash ^= (unsigned long)byte;
  *hash *= LC_POUCH_QUERY_INDEX_HASH_PRIME;
  *hash &= LC_POUCH_QUERY_INDEX_HASH_MASK;
}

static void lc_pouch_query_index_hash_bytes(unsigned long *hash,
                                            const char *bytes, size_t length) {
  size_t index;

  for (index = 0U; index < length; ++index) {
    lc_pouch_query_index_hash_byte(hash, (unsigned char)bytes[index]);
  }
}

static char *
lc_pouch_query_index_hex_encode_bytes(const lc_allocator *allocator,
                                      const char *value, size_t length) {
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

static void lc_pouch_query_index_hex_encode_byte(char *out,
                                                 unsigned char value) {
  static const char hex[] = "0123456789abcdef";

  out[0] = hex[value >> 4];
  out[1] = hex[value & 0x0fU];
}

static char *lc_pouch_query_index_hex_encode_folded_ascii_bytes(
    const lc_allocator *allocator, const char *value, size_t length) {
  char *encoded;
  size_t index;

  if (value == NULL || length == 0U) {
    return lc_pouch_query_index_hex_encode_bytes(allocator, value, length);
  }
  if (length > ((size_t)-1 - 1U) / 2U) {
    return NULL;
  }
  encoded = (char *)lc_alloc_with_allocator(allocator, (length * 2U) + 1U);
  if (encoded == NULL) {
    return NULL;
  }
  for (index = 0U; index < length; ++index) {
    unsigned char byte;

    byte = (unsigned char)value[index];
    if (byte >= 'A' && byte <= 'Z') {
      byte = (unsigned char)(byte - 'A' + 'a');
    }
    lc_pouch_query_index_hex_encode_byte(encoded + (index * 2U), byte);
  }
  encoded[length * 2U] = '\0';
  return encoded;
}

static char *lc_pouch_query_index_hex_encode(const lc_allocator *allocator,
                                             const char *value) {
  return lc_pouch_query_index_hex_encode_bytes(
      allocator, value, value != NULL ? strlen(value) : 0U);
}

static void
lc_pouch_query_index_summary_cleanup(lc_pouch_query_index_summary *summary) {
  size_t index;

  if (summary == NULL) {
    return;
  }
  for (index = 0U; index < summary->count; ++index) {
    lc_free_with_allocator(summary->allocator, summary->rows[index].key);
    lc_free_with_allocator(summary->allocator, summary->rows[index].key_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->rows[index].content_type_hex);
    lc_free_with_allocator(summary->allocator, summary->rows[index].etag_hex);
  }
  for (index = 0U; index < summary->term_count; ++index) {
    lc_free_with_allocator(summary->allocator, summary->terms[index].field_hex);
    lc_free_with_allocator(summary->allocator, summary->terms[index].value_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].canonical_value_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].long_value);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].text_prefix_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].temporal_value_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].trigram_keys);
    lc_free_with_allocator(summary->allocator, summary->terms[index].key_hex);
  }
  for (index = 0U; index < summary->presence_count; ++index) {
    lc_free_with_allocator(summary->allocator,
                           summary->presences[index].field_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->presences[index].key_hex);
  }
  lc_free_with_allocator(summary->allocator, summary->rows);
  lc_free_with_allocator(summary->allocator, summary->terms);
  lc_free_with_allocator(summary->allocator, summary->presences);
  memset(summary, 0, sizeof(*summary));
}

static int
lc_pouch_query_index_summary_reserve(lc_pouch_query_index_summary *summary,
                                     size_t needed, lc_error *error) {
  lc_pouch_query_index_row *next_rows;
  size_t next_capacity;

  if (needed <= summary->capacity) {
    return LC_OK;
  }
  next_capacity = summary->capacity == 0U ? 16U : summary->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index summary exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_rows = (lc_pouch_query_index_row *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_rows));
  if (next_rows == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index summary rows",
                        NULL, NULL, NULL);
  }
  if (summary->rows != NULL) {
    memcpy(next_rows, summary->rows, summary->count * sizeof(*next_rows));
    lc_free_with_allocator(summary->allocator, summary->rows);
  }
  memset(next_rows + summary->count, 0,
         (next_capacity - summary->count) * sizeof(*next_rows));
  summary->rows = next_rows;
  summary->capacity = next_capacity;
  return LC_OK;
}

static int
lc_pouch_query_index_term_reserve(lc_pouch_query_index_summary *summary,
                                  size_t needed, lc_error *error) {
  lc_pouch_query_index_term *next_terms;
  size_t next_capacity;

  if (needed <= summary->term_capacity) {
    return LC_OK;
  }
  next_capacity = summary->term_capacity == 0U ? 32U : summary->term_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index terms exceed local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_terms = (lc_pouch_query_index_term *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_terms));
  if (next_terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index terms", NULL,
                        NULL, NULL);
  }
  if (summary->terms != NULL) {
    memcpy(next_terms, summary->terms,
           summary->term_count * sizeof(*next_terms));
    lc_free_with_allocator(summary->allocator, summary->terms);
  }
  memset(next_terms + summary->term_count, 0,
         (next_capacity - summary->term_count) * sizeof(*next_terms));
  summary->terms = next_terms;
  summary->term_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_term_add(lc_pouch_query_index_summary *summary,
                                         const char *field, size_t field_len,
                                         const char *value, size_t value_len,
                                         const char *key_hex, char value_type,
                                         lc_pouch_generation version,
                                         uint64_t bytes, int has_query_hidden,
                                         int query_hidden, lc_error *error) {
  lc_pouch_query_index_term *term;
  int long_string;
  int rc;

  if (field == NULL || field_len == 0U || value == NULL || key_hex == NULL) {
    return LC_OK;
  }
  long_string = value_type == 's' &&
                value_len > LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
  rc = lc_pouch_query_index_term_reserve(summary, summary->term_count + 1U,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  term = &summary->terms[summary->term_count];
  memset(term, 0, sizeof(*term));
  term->field_hex = lc_pouch_query_index_hex_encode_bytes(summary->allocator,
                                                          field, field_len);
  term->value_hex =
      long_string ? lc_pouch_query_index_exact_hash_value(
                        summary->allocator, value, value_len, error)
                  : lc_pouch_query_index_hex_encode_bytes(summary->allocator,
                                                          value, value_len);
  if (value_type == 'n') {
    rc = lc_pouch_query_index_exact_generation_number_text_hex(
        summary->allocator, value, value_len, &term->canonical_value_hex,
        error);
    if (rc != LC_OK) {
      lc_free_with_allocator(summary->allocator, term->field_hex);
      lc_free_with_allocator(summary->allocator, term->value_hex);
      memset(term, 0, sizeof(*term));
      return rc;
    }
  }
  term->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (long_string) {
    term->long_value =
        (char *)lc_alloc_with_allocator(summary->allocator, value_len + 1U);
    if (term->long_value != NULL) {
      memcpy(term->long_value, value, value_len);
      term->long_value[value_len] = '\0';
      term->long_value_len = value_len;
    }
  }
  if (term->field_hex == NULL || term->value_hex == NULL ||
      term->key_hex == NULL || (long_string && term->long_value == NULL)) {
    lc_free_with_allocator(summary->allocator, term->field_hex);
    lc_free_with_allocator(summary->allocator, term->value_hex);
    lc_free_with_allocator(summary->allocator, term->canonical_value_hex);
    lc_free_with_allocator(summary->allocator, term->long_value);
    lc_free_with_allocator(summary->allocator, term->key_hex);
    memset(term, 0, sizeof(*term));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term", NULL, NULL,
                        NULL);
  }
  if (value_type == 's') {
    rc = lc_pouch_query_index_term_precompute_derived(summary, term, value,
                                                      value_len, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(summary->allocator, term->field_hex);
      lc_free_with_allocator(summary->allocator, term->value_hex);
      lc_free_with_allocator(summary->allocator, term->canonical_value_hex);
      lc_free_with_allocator(summary->allocator, term->long_value);
      lc_free_with_allocator(summary->allocator, term->text_prefix_hex);
      lc_free_with_allocator(summary->allocator, term->temporal_value_hex);
      lc_free_with_allocator(summary->allocator, term->trigram_keys);
      lc_free_with_allocator(summary->allocator, term->key_hex);
      memset(term, 0, sizeof(*term));
      return rc;
    }
    /* A derived term retains only index material. Never keep a full JSON
     * value in the mutable index path after its prefix, trigrams, and date
     * form have been produced. */
    lc_free_with_allocator(summary->allocator, term->long_value);
    term->long_value = NULL;
    term->long_value_len = 0U;
  }
  term->value_type = value_type;
  term->version = version;
  term->bytes = bytes;
  term->has_query_hidden = has_query_hidden;
  term->query_hidden = query_hidden;
  ++summary->term_count;
  return LC_OK;
}

/* JSON delivers strings in chunks.  Keep at most the part that can affect
 * derived postings, while hashing the complete value for exact matching. */
static int lc_pouch_query_index_term_add_streamed_long(
    lc_pouch_query_index_summary *summary, const char *field, size_t field_len,
    const char *value_prefix, size_t value_prefix_len,
    unsigned long value_length, unsigned long value_hash,
    unsigned long value_folded_hash, const char *key_hex,
    lc_pouch_generation version, uint64_t bytes, int has_query_hidden,
    int query_hidden, lc_error *error) {
  lc_pouch_query_index_term *term;
  int rc;

  if (summary == NULL || field == NULL || field_len == 0U ||
      value_prefix == NULL ||
      value_length <= LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES ||
      key_hex == NULL) {
    return LC_OK;
  }
  if (value_prefix_len < LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES ||
      (value_length <= LC_POUCH_QUERY_INDEX_TRIGRAM_INDEXED_BYTES_MAX &&
       value_prefix_len != (size_t)value_length)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index streamed string capture is "
                        "incomplete",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_query_index_term_reserve(summary, summary->term_count + 1U,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  term = &summary->terms[summary->term_count];
  memset(term, 0, sizeof(*term));
  term->field_hex = lc_pouch_query_index_hex_encode_bytes(summary->allocator,
                                                          field, field_len);
  term->value_hex = lc_pouch_query_index_exact_hash_format(
      summary->allocator, value_length, value_hash, value_folded_hash, error);
  term->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (term->field_hex == NULL || term->value_hex == NULL ||
      term->key_hex == NULL) {
    rc = error != NULL && error->code != LC_OK
             ? error->code
             : lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch "
                            "query-index term",
                            NULL, NULL, NULL);
    goto cleanup;
  }
  if (value_length > LC_POUCH_QUERY_INDEX_TRIGRAM_INDEXED_BYTES_MAX) {
    term->text_prefix_hex = lc_pouch_query_index_hex_encode_bytes(
        summary->allocator, value_prefix,
        LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES);
    if (term->text_prefix_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch text prefix", NULL, NULL,
                        NULL);
      goto cleanup;
    }
    term->trigram_keys_truncated = 1;
    term->derived_terms_ready = 1;
    term->trigram_keys_sorted = 1;
  } else {
    rc = lc_pouch_query_index_term_precompute_derived(
        summary, term, value_prefix, (size_t)value_length, error);
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  term->value_type = 's';
  term->version = version;
  term->bytes = bytes;
  term->has_query_hidden = has_query_hidden;
  term->query_hidden = query_hidden;
  ++summary->term_count;
  return LC_OK;

cleanup:
  lc_free_with_allocator(summary->allocator, term->field_hex);
  lc_free_with_allocator(summary->allocator, term->value_hex);
  lc_free_with_allocator(summary->allocator, term->text_prefix_hex);
  lc_free_with_allocator(summary->allocator, term->temporal_value_hex);
  lc_free_with_allocator(summary->allocator, term->trigram_keys);
  lc_free_with_allocator(summary->allocator, term->key_hex);
  memset(term, 0, sizeof(*term));
  return rc;
}

static int
lc_pouch_query_index_presence_reserve(lc_pouch_query_index_summary *summary,
                                      size_t needed, lc_error *error) {
  lc_pouch_query_index_presence *next_presences;
  size_t next_capacity;

  if (needed <= summary->presence_capacity) {
    return LC_OK;
  }
  next_capacity =
      summary->presence_capacity == 0U ? 32U : summary->presence_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index presences exceed local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_presences = (lc_pouch_query_index_presence *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_presences));
  if (next_presences == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presences", NULL,
                        NULL, NULL);
  }
  if (summary->presences != NULL) {
    memcpy(next_presences, summary->presences,
           summary->presence_count * sizeof(*next_presences));
    lc_free_with_allocator(summary->allocator, summary->presences);
  }
  memset(next_presences + summary->presence_count, 0,
         (next_capacity - summary->presence_count) * sizeof(*next_presences));
  summary->presences = next_presences;
  summary->presence_capacity = next_capacity;
  return LC_OK;
}

static int
lc_pouch_query_index_presence_add(lc_pouch_query_index_summary *summary,
                                  const char *field, size_t field_len,
                                  const char *key_hex, lc_error *error) {
  lc_pouch_query_index_presence *presence;
  int rc;

  if (field == NULL || field_len == 0U || key_hex == NULL) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_presence_reserve(
      summary, summary->presence_count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  presence = &summary->presences[summary->presence_count];
  memset(presence, 0, sizeof(*presence));
  presence->field_hex = lc_pouch_query_index_hex_encode_bytes(
      summary->allocator, field, field_len);
  presence->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (presence->field_hex == NULL || presence->key_hex == NULL) {
    lc_free_with_allocator(summary->allocator, presence->field_hex);
    lc_free_with_allocator(summary->allocator, presence->key_hex);
    memset(presence, 0, sizeof(*presence));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presence", NULL,
                        NULL, NULL);
  }
  ++summary->presence_count;
  return LC_OK;
}

static lonejson_read_result
lc_pouch_query_index_lonejson_read(void *user, unsigned char *buffer,
                                   size_t capacity) {
  lc_pouch_query_index_source_reader *reader;
  lonejson_read_result result;

  memset(&result, 0, sizeof(result));
  reader = (lc_pouch_query_index_source_reader *)user;
  if (reader == NULL || reader->source == NULL) {
    result.error_code = EINVAL;
    return result;
  }
  result.bytes_read =
      reader->source->read(reader->source, buffer, capacity, &reader->error);
  if (result.bytes_read > capacity) {
    lc_error_set(&reader->error, LC_ERR_INVALID, 0L,
                 "query-index source exceeded requested chunk size", NULL, NULL,
                 NULL);
    result.bytes_read = 0U;
  }
  if (result.bytes_read == 0U) {
    if (reader->error.code != LC_OK) {
      result.error_code = EIO;
    } else {
      result.eof = 1;
    }
  }
  return result;
}

static int lc_pouch_query_index_extract_reserve(
    lc_pouch_query_index_extract_context *context, int field, size_t extra,
    lc_error *error) {
  char **bytes;
  size_t *length;
  size_t *capacity;
  char *next;
  size_t needed;
  size_t next_capacity;

  if (field == 0) {
    bytes = &context->value;
    length = &context->value_len;
    capacity = &context->value_capacity;
  } else if (field == 1) {
    bytes = &context->field;
    length = &context->field_len;
    capacity = &context->field_capacity;
  } else {
    bytes = &context->array_field;
    length = &context->array_field_len;
    capacity = &context->array_field_capacity;
  }
  if (extra > (size_t)-1 - *length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index extracted term exceeds local limit",
                        NULL, NULL, NULL);
  }
  needed = *length + extra + 1U;
  if (needed <= *capacity) {
    return LC_OK;
  }
  next_capacity = *capacity == 0U ? 64U : *capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index extracted term exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (char *)lc_alloc_with_allocator(context->summary->allocator,
                                         next_capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index extracted term",
                        NULL, NULL, NULL);
  }
  if (*bytes != NULL) {
    memcpy(next, *bytes, *length);
    lc_free_with_allocator(context->summary->allocator, *bytes);
  }
  *bytes = next;
  *capacity = next_capacity;
  (*bytes)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_extract_append(
    lc_pouch_query_index_extract_context *context, int field, const char *bytes,
    size_t length, lc_error *error) {
  char **target;
  size_t *target_len;
  int rc;

  rc = lc_pouch_query_index_extract_reserve(context, field, length, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (field == 0) {
    target = &context->value;
    target_len = &context->value_len;
  } else if (field == 1) {
    target = &context->field;
    target_len = &context->field_len;
  } else {
    target = &context->array_field;
    target_len = &context->array_field_len;
  }
  memcpy(*target + *target_len, bytes, length);
  *target_len += length;
  (*target)[*target_len] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_extract_append_path_segment(
    lc_pouch_query_index_extract_context *context, int field,
    const lonejson_path_segment *segment, int array_item, lc_error *error) {
  size_t byte_index;
  int rc;

  rc = lc_pouch_query_index_extract_append(context, field, "/", 1U, error);
  if (rc != LC_OK || array_item) {
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_extract_append(context, field, "[]", 2U, error);
    }
    return rc;
  }
  for (byte_index = 0U; byte_index < segment->len; ++byte_index) {
    const char *replacement;
    char byte;

    byte = segment->data[byte_index];
    replacement = NULL;
    if (byte == '~') {
      replacement = "~0";
    } else if (byte == '/') {
      replacement = "~1";
    }
    if (replacement != NULL) {
      rc = lc_pouch_query_index_extract_append(context, field, replacement, 2U,
                                               error);
    } else {
      rc =
          lc_pouch_query_index_extract_append(context, field, &byte, 1U, error);
    }
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int lc_pouch_query_index_extract_set_field(
    lc_pouch_query_index_extract_context *context,
    const lonejson_value_path *path, lc_error *error) {
  size_t segment_index;

  context->field_len = 0U;
  if (context->field != NULL) {
    context->field[0] = '\0';
  }
  context->array_field_len = 0U;
  if (context->array_field != NULL) {
    context->array_field[0] = '\0';
  }
  context->has_array_field = 0;
  if (path == NULL || path->segment_count == 0U) {
    return LC_OK;
  }
  for (segment_index = 0U; segment_index < path->segment_count;
       ++segment_index) {
    const lonejson_path_segment *segment;
    size_t primary_prefix_len;
    int array_item;
    int rc;

    segment = &path->segments[segment_index];
    array_item = context->array_depths != NULL &&
                 segment_index < context->array_depth_capacity &&
                 context->array_depths[segment_index] != 0U;
    primary_prefix_len = context->field_len;
    rc = lc_pouch_query_index_extract_append_path_segment(context, 1, segment,
                                                          0, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (array_item && !context->has_array_field) {
      if (primary_prefix_len > 0U) {
        rc = lc_pouch_query_index_extract_append(context, 2, context->field,
                                                 primary_prefix_len, error);
        if (rc != LC_OK) {
          return rc;
        }
      }
      context->has_array_field = 1;
    }
    if (context->has_array_field) {
      rc = lc_pouch_query_index_extract_append_path_segment(context, 2, segment,
                                                            array_item, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  return LC_OK;
}

static int lc_pouch_query_index_extract_array_depth_reserve(
    lc_pouch_query_index_extract_context *context, size_t depth,
    lc_error *error) {
  unsigned char *next;
  size_t needed;
  size_t next_capacity;

  if (context == NULL || context->summary == NULL || depth == (size_t)-1) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index array depth is invalid", NULL, NULL,
                        NULL);
  }
  needed = depth + 1U;
  if (needed <= context->array_depth_capacity) {
    return LC_OK;
  }
  next_capacity =
      context->array_depth_capacity == 0U ? 8U : context->array_depth_capacity;
  while (next_capacity < needed) {
    if (next_capacity > (size_t)-1 / 2U) {
      next_capacity = needed;
      break;
    }
    next_capacity *= 2U;
  }
  next = (unsigned char *)lc_alloc_with_allocator(context->summary->allocator,
                                                  next_capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index array depth",
                        NULL, NULL, NULL);
  }
  memset(next, 0, next_capacity);
  if (context->array_depths != NULL) {
    memcpy(next, context->array_depths, context->array_depth_capacity);
    lc_free_with_allocator(context->summary->allocator, context->array_depths);
  }
  context->array_depths = next;
  context->array_depth_capacity = next_capacity;
  return LC_OK;
}

/* Consumes the callback's local owned error after copying its diagnostic. */
static lonejson_status
lc_pouch_query_index_lonejson_error(lonejson_error *lj_error, lc_error *error) {
  if (lj_error != NULL) {
    lonejson_error_init(lj_error);
    lj_error->code = error != NULL && error->code == LC_ERR_NOMEM
                         ? LONEJSON_STATUS_ALLOCATION_FAILED
                         : LONEJSON_STATUS_CALLBACK_FAILED;
    snprintf(lj_error->message, sizeof(lj_error->message), "%s",
             error != NULL && error->message != NULL ? error->message
                                                     : "pouch query-index "
                                                       "callback failed");
  }
  lc_error_cleanup(error);
  return lj_error != NULL ? lj_error->code : LONEJSON_STATUS_CALLBACK_FAILED;
}

static int
lc_pouch_query_index_emit_term(lc_pouch_query_index_extract_context *context,
                               const char *field, size_t field_len,
                               const char *value, size_t value_len,
                               char value_type, lc_error *error) {
  lc_pouch_index_term_key term;
  char *field_hex;
  char *value_hex;
  int rc;
  if (field_len == 0U)
    return LC_OK;
  field_hex = lc_pouch_query_index_hex_encode_bytes(context->summary->allocator,
                                                    field, field_len);
  value_hex = NULL;
  rc = LC_OK;
  if (value_type == 'n') {
    rc = lc_pouch_query_index_exact_generation_number_text_hex(
        context->summary->allocator, value, value_len, &value_hex, error);
  } else if (value_type == 'h') {
    value_hex = lc_pouch_query_index_exact_hash_format(
        context->summary->allocator, context->string_length,
        context->string_hash, context->string_folded_hash, error);
  } else {
    value_hex = lc_pouch_query_index_hex_encode_bytes(
        context->summary->allocator, value, value_len);
  }
  if (rc == LC_OK && (field_hex == NULL || value_hex == NULL))
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate streamed query term", NULL, NULL,
                      NULL);
  if (rc == LC_OK) {
    term.field_hex = field_hex;
    term.value_hex = value_hex;
    term.value_type = value_type == 'e' ? 's' : value_type;
    rc = context->visit(context->visit_context, &term, value_type == 'e',
                        &context->visit_error);
    if (rc != LC_OK) {
      context->visit_rc = rc;
      if (context->visit_error.code == LC_OK)
        lc_error_set(&context->visit_error, rc, 0L,
                     "streamed query term callback rejected a term", NULL, NULL,
                     NULL);
    }
  }
  lc_free_with_allocator(context->summary->allocator, field_hex);
  lc_free_with_allocator(context->summary->allocator, value_hex);
  return rc;
}

static int lc_pouch_query_index_extract_presence(
    lc_pouch_query_index_extract_context *context, const char *field,
    size_t length, lc_error *error) {
  if (context->visit != NULL)
    return lc_pouch_query_index_emit_term(context, field, length, "", 0U, 'e',
                                          error);
  return lc_pouch_query_index_presence_add(context->summary, field, length,
                                           context->key_hex, error);
}

static int lc_pouch_query_index_add_presence_for_field(
    lc_pouch_query_index_extract_context *context, const char *field,
    size_t field_len, lc_error *error) {
  size_t index;
  int rc;

  if (context == NULL || field == NULL || field_len == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_extract_presence(context, field, field_len, error);
  if (rc == LC_OK) {
    char *recursive_field;
    size_t recursive_len;

    recursive_len = field_len + 3U;
    recursive_field = (char *)lc_alloc_with_allocator(
        context->summary->allocator, recursive_len + 1U);
    if (recursive_field == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch recursive presence field",
                          NULL, NULL, NULL);
    }
    memcpy(recursive_field, field, field_len);
    memcpy(recursive_field + field_len, "/**", 4U);
    rc = lc_pouch_query_index_extract_presence(context, recursive_field,
                                               recursive_len, error);
    lc_free_with_allocator(context->summary->allocator, recursive_field);
  }
  for (index = 1U; rc == LC_OK && index < field_len; ++index) {
    char *recursive_field;
    size_t recursive_len;

    if (field[index] != '/') {
      continue;
    }
    recursive_len = index + 3U;
    recursive_field = (char *)lc_alloc_with_allocator(
        context->summary->allocator, recursive_len + 1U);
    if (recursive_field == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch recursive presence field",
                          NULL, NULL, NULL);
    }
    memcpy(recursive_field, field, index);
    memcpy(recursive_field + index, "/**", 4U);
    rc = lc_pouch_query_index_extract_presence(context, recursive_field,
                                               recursive_len, error);
    lc_free_with_allocator(context->summary->allocator, recursive_field);
  }
  return rc;
}

static int lc_pouch_query_index_add_presence_for_current_field(
    lc_pouch_query_index_extract_context *context, lc_error *error) {
  int rc;

  if (context == NULL) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_add_presence_for_field(context, context->field,
                                                   context->field_len, error);
  if (rc == LC_OK && context->has_array_field) {
    rc = lc_pouch_query_index_add_presence_for_field(
        context, context->array_field, context->array_field_len, error);
  }
  return rc;
}

static int lc_pouch_query_index_add_term_for_current_field(
    lc_pouch_query_index_extract_context *context, const char *value,
    size_t value_len, char value_type, lc_error *error) {
  int rc;

  if (context == NULL) {
    return LC_OK;
  }
  if (context->visit != NULL) {
    rc = lc_pouch_query_index_emit_term(context, context->field,
                                        context->field_len, value, value_len,
                                        value_type, error);
    if (rc == LC_OK && context->has_array_field)
      rc = lc_pouch_query_index_emit_term(context, context->array_field,
                                          context->array_field_len, value,
                                          value_len, value_type, error);
    return rc;
  }
  rc = lc_pouch_query_index_term_add(
      context->summary, context->field, context->field_len, value, value_len,
      context->key_hex, value_type, context->version, context->bytes,
      context->has_query_hidden, context->query_hidden, error);
  if (rc == LC_OK && context->has_array_field) {
    rc = lc_pouch_query_index_term_add(
        context->summary, context->array_field, context->array_field_len, value,
        value_len, context->key_hex, value_type, context->version,
        context->bytes, context->has_query_hidden, context->query_hidden,
        error);
  }
  return rc;
}

static int lc_pouch_query_index_add_streamed_long_for_current_field(
    lc_pouch_query_index_extract_context *context, lc_error *error) {
  int rc;

  if (context == NULL) {
    return LC_OK;
  }
  if (context->visit != NULL) {
    rc = lc_pouch_query_index_emit_term(
        context, context->field, context->field_len, NULL, 0U, 'h', error);
    if (rc == LC_OK && context->has_array_field)
      rc = lc_pouch_query_index_emit_term(context, context->array_field,
                                          context->array_field_len, NULL, 0U,
                                          'h', error);
    return rc;
  }
  rc = lc_pouch_query_index_term_add_streamed_long(
      context->summary, context->field, context->field_len, context->value,
      context->value_len, context->string_length, context->string_hash,
      context->string_folded_hash, context->key_hex, context->version,
      context->bytes, context->has_query_hidden, context->query_hidden, error);
  if (rc == LC_OK && context->has_array_field) {
    rc = lc_pouch_query_index_term_add_streamed_long(
        context->summary, context->array_field, context->array_field_len,
        context->value, context->value_len, context->string_length,
        context->string_hash, context->string_folded_hash, context->key_hex,
        context->version, context->bytes, context->has_query_hidden,
        context->query_hidden, error);
  }
  return rc;
}

static lonejson_status
lc_pouch_query_index_presence_value(void *user, const lonejson_value_path *path,
                                    lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_array_begin(void *user, const lonejson_value_path *path,
                                 lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  rc = lc_pouch_query_index_presence_value(user, path, lj_error);
  if (rc != LONEJSON_STATUS_OK) {
    return rc;
  }
  context = (lc_pouch_query_index_extract_context *)user;
  if (context == NULL || path == NULL) {
    return LONEJSON_STATUS_OK;
  }
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_array_depth_reserve(
      context, path->segment_count, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  context->array_depths[path->segment_count] = 1U;
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_array_end(void *user, const lonejson_value_path *path,
                               lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;

  (void)lj_error;
  context = (lc_pouch_query_index_extract_context *)user;
  if (context != NULL && path != NULL && context->array_depths != NULL &&
      path->segment_count < context->array_depth_capacity) {
    context->array_depths[path->segment_count] = 0U;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_begin(void *user, const lonejson_value_path *path,
                                  lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  context->value_len = 0U;
  context->string_length = 0UL;
  context->string_hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  context->string_folded_hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  context->capturing_string = 1;
  if (context->value != NULL) {
    context->value[0] = '\0';
  }
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_chunk(void *user, const lonejson_value_path *path,
                                  const char *data, size_t len,
                                  lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  size_t captured;
  size_t index;
  unsigned long string_hash;
  unsigned long string_folded_hash;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  if (!context->capturing_string) {
    rc = lc_pouch_query_index_extract_append(context, 0, data, len, &error);
  } else if ((uint64_t)len >
             (uint64_t)ULONG_MAX - (uint64_t)context->string_length) {
    rc = lc_error_set(&error, LC_ERR_INVALID, 0L,
                      "pouch query-index string exceeds exact-index limit",
                      NULL, NULL, "pouch");
  } else {
    /* Keep the streaming hash state in registers while parsing a chunk.
     * Pouch deliberately hashes the complete string even when it only retains
     * a bounded value prefix, so storing through the extraction context for
     * every byte would put needless traffic on the flush hot path. */
    string_hash = context->string_hash;
    string_folded_hash = context->string_folded_hash;
    for (index = 0U; index < len; ++index) {
      unsigned char byte;
      unsigned char folded;

      byte = (unsigned char)data[index];
      folded =
          byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
      string_hash = lc_pouch_query_index_exact_hash_update(string_hash, byte);
      string_folded_hash =
          lc_pouch_query_index_exact_hash_update(string_folded_hash, folded);
    }
    context->string_hash = string_hash;
    context->string_folded_hash = string_folded_hash;
    context->string_length += (unsigned long)len;
    captured = 0U;
    if (context->value_len < LC_POUCH_QUERY_INDEX_TRIGRAM_INDEXED_BYTES_MAX) {
      captured =
          LC_POUCH_QUERY_INDEX_TRIGRAM_INDEXED_BYTES_MAX - context->value_len;
      if (captured > len) {
        captured = len;
      }
    }
    rc = captured == 0U ? LC_OK
                        : lc_pouch_query_index_extract_append(context, 0, data,
                                                              captured, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_end(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = context->string_length > LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES
           ? lc_pouch_query_index_add_streamed_long_for_current_field(context,
                                                                      &error)
           : lc_pouch_query_index_add_term_for_current_field(
                 context, context->value, context->value_len, 's', &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_number_begin(void *user, const lonejson_value_path *path,
                                  lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lonejson_status status;

  status = lc_pouch_query_index_string_begin(user, path, lj_error);
  if (status == LONEJSON_STATUS_OK) {
    context = (lc_pouch_query_index_extract_context *)user;
    context->capturing_string = 0;
  }
  return status;
}

static lonejson_status
lc_pouch_query_index_number_chunk(void *user, const lonejson_value_path *path,
                                  const char *data, size_t len,
                                  lonejson_error *lj_error) {
  return lc_pouch_query_index_string_chunk(user, path, data, len, lj_error);
}

static lonejson_status
lc_pouch_query_index_number_end(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;
  double normalized;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  if (context->visit != NULL &&
      !lc_pouch_query_index_parse_number_value(context->value, &normalized)) {
    context->unrepresentable_number = 1;
    return LONEJSON_STATUS_OK;
  }
  lc_error_init(&error);
  rc = lc_pouch_query_index_add_term_for_current_field(
      context, context->value, context->value_len, 'n', &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_boolean_value(void *user, const lonejson_value_path *path,
                                   int value, lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  const char *text;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  text = value ? "true" : "false";
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_term_for_current_field(
        context, text, strlen(text), 'b', &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_null_value(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_term_for_current_field(context, "null", 4U,
                                                         'z', &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static void lc_pouch_query_index_extract_context_cleanup(
    lc_pouch_query_index_extract_context *context) {
  if (context == NULL || context->summary == NULL) {
    return;
  }
  lc_free_with_allocator(context->summary->allocator, context->field);
  lc_free_with_allocator(context->summary->allocator, context->array_field);
  lc_free_with_allocator(context->summary->allocator, context->value);
  lc_free_with_allocator(context->summary->allocator, context->array_depths);
  memset(context, 0, sizeof(*context));
}

static int lc_pouch_query_index_extract_terms(
    lc_pouch_query_index_summary *summary, const char *key,
    const lc_pouch_query_index_row *row, lc_source *body,
    lc_pouch_query_term_fn visit, void *visit_context, lc_error *error) {
  lc_pouch_query_index_source_reader reader;
  lc_pouch_query_index_extract_context context;
  lonejson_path_value_visitor visitor;
  lonejson_error lj_error;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  memset(&reader, 0, sizeof(reader));
  memset(&context, 0, sizeof(context));
  if (summary == NULL || key == NULL || row == NULL || body == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index extraction requires summary, key, "
                        "row, and body",
                        NULL, NULL, NULL);
  }
  if (visit == NULL && body->reset != NULL) {
    rc = body->reset(body, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch query-index JSON runtime",
                        NULL, NULL, NULL);
  }
  reader.source = body;
  lc_error_init(&reader.error);
  context.summary = summary;
  context.key_hex = row->key_hex;
  context.version = row->version;
  context.bytes = row->bytes;
  context.has_query_hidden = row->has_query_hidden;
  context.query_hidden = row->query_hidden;
  context.visit = visit;
  context.visit_context = visit_context;
  lc_error_init(&context.visit_error);
  visitor = lonejson_default_path_value_visitor();
  visitor.object_begin = lc_pouch_query_index_presence_value;
  visitor.array_begin = lc_pouch_query_index_array_begin;
  visitor.array_end = lc_pouch_query_index_array_end;
  visitor.string_begin = lc_pouch_query_index_string_begin;
  visitor.string_chunk = lc_pouch_query_index_string_chunk;
  visitor.string_end = lc_pouch_query_index_string_end;
  visitor.number_begin = lc_pouch_query_index_number_begin;
  visitor.number_chunk = lc_pouch_query_index_number_chunk;
  visitor.number_end = lc_pouch_query_index_number_end;
  visitor.boolean_value = lc_pouch_query_index_boolean_value;
  visitor.null_value = lc_pouch_query_index_null_value;
  lonejson_error_init(&lj_error);
  status = runtime->visit_path_value_reader(
      runtime, lc_pouch_query_index_lonejson_read, &reader, &visitor, &context,
      &lj_error);
  if (context.visit_rc != LC_OK) {
    rc = context.visit_rc;
    if (error != NULL) {
      lc_error_cleanup(error);
      *error = context.visit_error;
      memset(&context.visit_error, 0, sizeof(context.visit_error));
    }
  } else if (reader.error.code != LC_OK) {
    rc = reader.error.code;
    if (error != NULL) {
      lc_error_cleanup(error);
      *error = reader.error;
      memset(&reader.error, 0, sizeof(reader.error));
    }
  } else if (status == LONEJSON_STATUS_ALLOCATION_FAILED ||
             status == LONEJSON_STATUS_CALLBACK_FAILED) {
    rc = lc_lonejson_error_from_status(
        error, status, &lj_error, "failed to build pouch query-index terms");
  } else {
    if (status != LONEJSON_STATUS_OK) {
      summary->term_index_complete = 0;
      summary->presence_index_complete = 0;
    }
    rc = visit != NULL && status != LONEJSON_STATUS_OK
             ? lc_lonejson_error_from_status(
                   error, status, &lj_error,
                   "failed to extract streamed query terms")
             : LC_OK;
  }
  if (rc == LC_OK && status == LONEJSON_STATUS_OK &&
      context.unrepresentable_number)
    rc = lc_error_set(
        error, LC_ERR_UNSUPPORTED, 0L,
        "valid state number is outside the query index representation", NULL,
        LC_POUCH_QUERY_NUMBER_UNINDEXED, "pouch");
  lc_error_cleanup(&reader.error);
  lc_error_cleanup(&context.visit_error);
  lc_pouch_query_index_extract_context_cleanup(&context);
  return rc;
}

static int lc_pouch_query_index_extract_terms_from_body(
    lc_pouch_query_index_summary *summary, const char *key,
    const lc_pouch_query_index_row *row, lc_source *body, lc_error *error) {
  return lc_pouch_query_index_extract_terms(summary, key, row, body, NULL, NULL,
                                            error);
}

int lc_pouch_query_terms_visit(lc_source *source, const lc_allocator *allocator,
                               lc_pouch_query_term_fn visit, void *context,
                               lc_error *error) {
  lc_pouch_query_index_summary summary;
  lc_pouch_query_index_row row;
  if (source == NULL || source->read == NULL || visit == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "streamed query terms require a source and visitor",
                        NULL, NULL, NULL);
  memset(&summary, 0, sizeof(summary));
  memset(&row, 0, sizeof(row));
  summary.allocator = allocator;
  row.key_hex = "-";
  return lc_pouch_query_index_extract_terms(&summary, "-", &row, source, visit,
                                            context, error);
}

static int lc_pouch_query_index_extract_batch_visit(
    const char *key, const lc_pouch_state_read_result *read_result,
    void *context, lc_error *error) {
  lc_pouch_query_index_extract_batch *batch;
  lc_pouch_query_index_row *row;
  size_t row_index;

  batch = (lc_pouch_query_index_extract_batch *)context;
  if (batch == NULL || batch->summary == NULL || batch->row_indices == NULL ||
      batch->index >= batch->summary->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index batch extraction exceeded rows",
                        NULL, NULL, NULL);
  }
  row_index = batch->row_indices[batch->index++];
  if (row_index >= batch->summary->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index batch extraction row is invalid",
                        NULL, NULL, NULL);
  }
  row = &batch->summary->rows[row_index];
  if (read_result == NULL || !read_result->found || read_result->body == NULL) {
    return LC_OK;
  }
  return lc_pouch_query_index_extract_terms_from_body(
      batch->summary, key != NULL ? key : row->key, row, read_result->body,
      error);
}

static int
lc_pouch_query_index_summary_visit(const lc_pouch_state_visit_entry *entry,
                                   void *context, lc_error *error) {
  lc_pouch_query_index_summary *summary;
  lc_pouch_query_index_row *row;
  int rc;

  summary = (lc_pouch_query_index_summary *)context;
  if (entry == NULL || entry->key == NULL || !entry->has_payload ||
      entry->object_record ||
      lc_pouch_query_index_key_is_attachment(entry->key)) {
    return LC_OK;
  }
  if (strncmp(entry->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(entry->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  rc =
      lc_pouch_query_index_summary_reserve(summary, summary->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  row = &summary->rows[summary->count];
  memset(row, 0, sizeof(*row));
  row->key = lc_strdup_with_allocator(summary->allocator, entry->key);
  row->key_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->key);
  row->content_type_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->content_type);
  row->etag_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->etag);
  if (row->key == NULL || row->key_hex == NULL ||
      row->content_type_hex == NULL || row->etag_hex == NULL) {
    lc_free_with_allocator(summary->allocator, row->key);
    lc_free_with_allocator(summary->allocator, row->key_hex);
    lc_free_with_allocator(summary->allocator, row->content_type_hex);
    lc_free_with_allocator(summary->allocator, row->etag_hex);
    memset(row, 0, sizeof(*row));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index summary row",
                        NULL, NULL, NULL);
  }
  row->version = entry->version;
  row->bytes = entry->bytes;
  row->has_query_hidden = entry->has_query_hidden;
  row->query_hidden = entry->query_hidden;
  ++summary->count;
  return LC_OK;
}

static void
lc_pouch_query_index_summary_row_cleanup(lc_pouch_query_index_summary *summary,
                                         lc_pouch_query_index_row *row) {
  if (summary == NULL || row == NULL) {
    return;
  }
  lc_free_with_allocator(summary->allocator, row->key);
  lc_free_with_allocator(summary->allocator, row->key_hex);
  lc_free_with_allocator(summary->allocator, row->content_type_hex);
  lc_free_with_allocator(summary->allocator, row->etag_hex);
  memset(row, 0, sizeof(*row));
}

static void
lc_pouch_query_index_summary_term_cleanup(lc_pouch_query_index_summary *summary,
                                          lc_pouch_query_index_term *term) {
  if (summary == NULL || term == NULL) {
    return;
  }
  lc_free_with_allocator(summary->allocator, term->field_hex);
  lc_free_with_allocator(summary->allocator, term->value_hex);
  lc_free_with_allocator(summary->allocator, term->canonical_value_hex);
  lc_free_with_allocator(summary->allocator, term->long_value);
  lc_free_with_allocator(summary->allocator, term->text_prefix_hex);
  lc_free_with_allocator(summary->allocator, term->temporal_value_hex);
  lc_free_with_allocator(summary->allocator, term->trigram_keys);
  lc_free_with_allocator(summary->allocator, term->key_hex);
  memset(term, 0, sizeof(*term));
}

static void lc_pouch_query_index_summary_presence_cleanup(
    lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_presence *presence) {
  if (summary == NULL || presence == NULL) {
    return;
  }
  lc_free_with_allocator(summary->allocator, presence->field_hex);
  lc_free_with_allocator(summary->allocator, presence->key_hex);
  memset(presence, 0, sizeof(*presence));
}

static void lc_pouch_query_index_summary_clear_derived(
    lc_pouch_query_index_summary *summary) {
  size_t index;

  if (summary == NULL) {
    return;
  }
  for (index = 0U; index < summary->term_count; ++index) {
    lc_pouch_query_index_summary_term_cleanup(summary, &summary->terms[index]);
  }
  for (index = 0U; index < summary->presence_count; ++index) {
    lc_pouch_query_index_summary_presence_cleanup(summary,
                                                  &summary->presences[index]);
  }
  summary->term_count = 0U;
  summary->presence_count = 0U;
}

static int lc_pouch_query_index_summary_has_deferred_rows(
    const lc_pouch_query_index_summary *summary) {
  size_t index;

  if (summary == NULL) {
    return 0;
  }
  for (index = 0U; index < summary->count; ++index) {
    if (summary->rows[index].needs_extraction) {
      return 1;
    }
  }
  return 0;
}

static void lc_pouch_query_index_summary_mark_rows_deferred(
    lc_pouch_query_index_summary *summary) {
  size_t index;

  if (summary == NULL) {
    return;
  }
  lc_pouch_query_index_summary_clear_derived(summary);
  for (index = 0U; index < summary->count; ++index) {
    summary->rows[index].needs_extraction = 1;
  }
}

static void lc_pouch_query_index_incremental_change_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_incremental_change *change) {
  if (change == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, change->key);
  lc_free_with_allocator(allocator, change->key_hex);
  lc_free_with_allocator(allocator, change->content_type);
  lc_free_with_allocator(allocator, change->etag);
  lc_free_with_allocator(allocator, change->descriptor);
  memset(change, 0, sizeof(*change));
}

static void lc_pouch_query_index_incremental_context_cleanup(
    lc_pouch_query_index_incremental_context *context) {
  const lc_allocator *allocator;
  size_t index;

  if (context == NULL || context->summary == NULL) {
    return;
  }
  allocator = context->summary->allocator;
  for (index = 0U; index < context->change_count; ++index) {
    lc_pouch_query_index_incremental_change_cleanup(allocator,
                                                    &context->changes[index]);
  }
  lc_free_with_allocator(allocator, context->changes);
  lc_free_with_allocator(allocator, context->read_keys);
  lc_free_with_allocator(allocator, context->read_row_indices);
  memset(context, 0, sizeof(*context));
}

static int lc_pouch_query_index_flush_lock(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;

  if (pouch == NULL || !pouch->query_flush_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query flush requires an open pouch", NULL, NULL,
                        "pouch");
  }
  pthread_rc = pthread_mutex_lock(&pouch->query_flush_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch query flush",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void lc_pouch_query_index_flush_unlock(lc_pouch *pouch) {
  if (pouch != NULL && pouch->query_flush_mutex_initialized) {
    (void)pthread_mutex_unlock(&pouch->query_flush_mutex);
  }
}

/* Transaction staging is durable recovery state, never public query state. */
static int lc_pouch_query_index_key_is_staged(const char *key) {
  return key != NULL &&
         (strncmp(key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
          strstr(key, "/.staging/") != NULL);
}

/* Attachments share Pouch's physical state tree but are private binary objects,
 * not public documents. This matches the reserved-key boundary in the client
 * layer and keeps arbitrary attachment bytes out of JSON selector indexing. */
static int lc_pouch_query_index_key_is_attachment(const char *key) {
  return key != NULL && strncmp(key, "state/", sizeof("state/") - 1U) == 0 &&
         strstr(key, "/attachments/") != NULL;
}

static void lc_pouch_query_index_pending_note_write(
    lc_pouch *pouch, const char *ns, const char *key, const char *content_type,
    lc_source *body, const lc_pouch_state_write_result *result,
    int operation_active);
static void lc_pouch_query_index_pending_note_delete(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_state_write_result *result, int operation_active);

void lc_pouch_query_index_note_state_write(
    lc_pouch *pouch, const char *ns, const char *key, const char *content_type,
    lc_source *body, const lc_pouch_state_write_result *result,
    int operation_active) {
  if (pouch == NULL || ns == NULL || ns[0] == '.' ||
      lc_pouch_query_index_key_is_staged(key) ||
      lc_pouch_query_index_key_is_attachment(key)) {
    return;
  }
  lc_pouch_query_index_pending_note_write(pouch, ns, key, content_type, body,
                                          result, operation_active);
  lc_pouch_indexer_note_mutation(pouch, ns);
}

void lc_pouch_query_index_note_state_delete(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_state_write_result *result, int operation_active) {
  if (pouch == NULL || ns == NULL || ns[0] == '.' ||
      lc_pouch_query_index_key_is_staged(key) ||
      lc_pouch_query_index_key_is_attachment(key)) {
    return;
  }
  lc_pouch_query_index_pending_note_delete(pouch, ns, key, result,
                                           operation_active);
  lc_pouch_indexer_note_mutation(pouch, ns);
}

int lc_pouch_query_index_has_pending(lc_pouch *pouch, const char *ns) {
  lc_pouch_generation manifest_seq;
  lc_pouch_generation state_index_seq;
  lc_error error;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return 1;
  }
  manifest_seq = 0UL;
  state_index_seq = 0UL;
  lc_error_init(&error);
  rc = lc_pouch_state_query_index_seq(pouch, ns, &state_index_seq, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_manifest_seq(pouch, ns, &manifest_seq, &error);
  }
  lc_error_cleanup(&error);
  return rc != LC_OK || manifest_seq != state_index_seq;
}

static int
lc_pouch_query_index_incremental_change_compare_key(const void *left,
                                                    const void *right) {
  const lc_pouch_query_index_incremental_change *a;
  const lc_pouch_query_index_incremental_change *b;

  a = (const lc_pouch_query_index_incremental_change *)left;
  b = (const lc_pouch_query_index_incremental_change *)right;
  return strcmp(a->key != NULL ? a->key : "", b->key != NULL ? b->key : "");
}

static int
lc_pouch_query_index_incremental_change_compare_key_hex(const void *left,
                                                        const void *right) {
  const lc_pouch_query_index_incremental_change *a;
  const lc_pouch_query_index_incremental_change *b;

  a = (const lc_pouch_query_index_incremental_change *)left;
  b = (const lc_pouch_query_index_incremental_change *)right;
  return strcmp(a->key_hex != NULL ? a->key_hex : "",
                b->key_hex != NULL ? b->key_hex : "");
}

static int lc_pouch_query_index_incremental_change_find_key(
    const lc_pouch_query_index_incremental_change *changes, size_t change_count,
    const char *key) {
  size_t low;
  size_t high;

  if (changes == NULL || key == NULL) {
    return 0;
  }
  low = 0U;
  high = change_count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(key, changes[mid].key != NULL ? changes[mid].key : "");
    if (cmp == 0) {
      return !changes[mid].preserves_cached_body;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

static int lc_pouch_query_index_incremental_change_find_key_hex(
    const lc_pouch_query_index_incremental_change *changes, size_t change_count,
    const char *key_hex) {
  size_t low;
  size_t high;

  if (changes == NULL || key_hex == NULL) {
    return 0;
  }
  low = 0U;
  high = change_count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(key_hex,
                 changes[mid].key_hex != NULL ? changes[mid].key_hex : "");
    if (cmp == 0) {
      return !changes[mid].preserves_cached_body;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

static lc_pouch_query_index_row *
lc_pouch_query_index_summary_find_row(lc_pouch_query_index_summary *summary,
                                      const char *key) {
  size_t index;

  if (summary == NULL || key == NULL) {
    return NULL;
  }
  for (index = 0U; index < summary->count; ++index) {
    if (summary->rows[index].key != NULL &&
        strcmp(summary->rows[index].key, key) == 0) {
      return &summary->rows[index];
    }
  }
  return NULL;
}

static int lc_pouch_query_index_incremental_change_preserves_body(
    const lc_pouch_query_index_row *row,
    const lc_pouch_query_index_incremental_change *change) {
  return row != NULL && change != NULL && change->found &&
         row->version == change->version && row->bytes == change->bytes;
}

static int lc_pouch_query_index_summary_remove_changes(
    lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_incremental_change *changes, size_t change_count,
    lc_error *error) {
  size_t read_index;
  size_t write_index;
  (void)error;

  if (summary == NULL || changes == NULL || change_count == 0U) {
    return LC_OK;
  }
  qsort(changes, change_count, sizeof(changes[0]),
        lc_pouch_query_index_incremental_change_compare_key);
  write_index = 0U;
  for (read_index = 0U; read_index < summary->count; ++read_index) {
    if (lc_pouch_query_index_incremental_change_find_key(
            changes, change_count, summary->rows[read_index].key)) {
      lc_pouch_query_index_summary_row_cleanup(summary,
                                               &summary->rows[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->rows[write_index] = summary->rows[read_index];
      memset(&summary->rows[read_index], 0, sizeof(summary->rows[read_index]));
    }
    ++write_index;
  }
  summary->count = write_index;

  qsort(changes, change_count, sizeof(changes[0]),
        lc_pouch_query_index_incremental_change_compare_key_hex);
  write_index = 0U;
  for (read_index = 0U; read_index < summary->term_count; ++read_index) {
    if (lc_pouch_query_index_incremental_change_find_key_hex(
            changes, change_count, summary->terms[read_index].key_hex)) {
      lc_pouch_query_index_summary_term_cleanup(summary,
                                                &summary->terms[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->terms[write_index] = summary->terms[read_index];
      memset(&summary->terms[read_index], 0,
             sizeof(summary->terms[read_index]));
    }
    ++write_index;
  }
  summary->term_count = write_index;

  write_index = 0U;
  for (read_index = 0U; read_index < summary->presence_count; ++read_index) {
    if (lc_pouch_query_index_incremental_change_find_key_hex(
            changes, change_count, summary->presences[read_index].key_hex)) {
      lc_pouch_query_index_summary_presence_cleanup(
          summary, &summary->presences[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->presences[write_index] = summary->presences[read_index];
      memset(&summary->presences[read_index], 0,
             sizeof(summary->presences[read_index]));
    }
    ++write_index;
  }
  summary->presence_count = write_index;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_context_add_key(
    lc_pouch_query_index_incremental_context *context, const char *key,
    size_t row_index, lc_error *error) {
  const char **next;
  size_t *next_indices;
  size_t next_capacity;

  if (context == NULL || key == NULL) {
    return LC_OK;
  }
  if (context->read_key_count == context->read_key_capacity) {
    next_capacity = context->read_key_capacity == 0U
                        ? 16U
                        : context->read_key_capacity * 2U;
    next = (const char **)lc_alloc_with_allocator(
        context->summary->allocator, next_capacity * sizeof(*next));
    next_indices = (size_t *)lc_alloc_with_allocator(
        context->summary->allocator, next_capacity * sizeof(*next_indices));
    if (next == NULL || next_indices == NULL) {
      lc_free_with_allocator(context->summary->allocator, next);
      lc_free_with_allocator(context->summary->allocator, next_indices);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch incremental index keys",
                          NULL, NULL, NULL);
    }
    if (context->read_keys != NULL) {
      memcpy(next, context->read_keys, context->read_key_count * sizeof(*next));
      lc_free_with_allocator(context->summary->allocator, context->read_keys);
    }
    if (context->read_row_indices != NULL) {
      memcpy(next_indices, context->read_row_indices,
             context->read_key_count * sizeof(*next_indices));
      lc_free_with_allocator(context->summary->allocator,
                             context->read_row_indices);
    }
    context->read_keys = next;
    context->read_row_indices = next_indices;
    context->read_key_capacity = next_capacity;
    context->read_row_index_capacity = next_capacity;
  }
  context->read_keys[context->read_key_count] = key;
  context->read_row_indices[context->read_key_count] = row_index;
  ++context->read_key_count;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_context_reserve_changes(
    lc_pouch_query_index_incremental_context *context, size_t needed,
    lc_error *error) {
  lc_pouch_query_index_incremental_change *next;
  size_t next_capacity;

  if (context == NULL || context->summary == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch incremental index changes require context", NULL,
                        NULL, NULL);
  }
  if (needed <= context->change_capacity) {
    return LC_OK;
  }
  next_capacity =
      context->change_capacity == 0U ? 16U : context->change_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch incremental index changes exceed local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (lc_pouch_query_index_incremental_change *)lc_alloc_with_allocator(
      context->summary->allocator, next_capacity * sizeof(*next));
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch incremental index changes",
                        NULL, NULL, NULL);
  }
  if (context->changes != NULL) {
    memcpy(next, context->changes, context->change_count * sizeof(next[0]));
    lc_free_with_allocator(context->summary->allocator, context->changes);
  }
  memset(next + context->change_count, 0,
         (next_capacity - context->change_count) * sizeof(next[0]));
  context->changes = next;
  context->change_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_context_add_change(
    lc_pouch_query_index_incremental_context *context,
    const lc_pouch_state_change_visit_entry *entry, lc_error *error) {
  lc_pouch_query_index_incremental_change *change;
  int rc;

  if (context == NULL || context->summary == NULL || entry == NULL ||
      entry->key == NULL || entry->object_record ||
      lc_pouch_query_index_key_is_attachment(entry->key)) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_incremental_context_reserve_changes(
      context, context->change_count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  change = &context->changes[context->change_count];
  memset(change, 0, sizeof(*change));
  change->key =
      lc_strdup_with_allocator(context->summary->allocator, entry->key);
  change->key_hex =
      lc_pouch_query_index_hex_encode(context->summary->allocator, entry->key);
  change->content_type = lc_strdup_with_allocator(context->summary->allocator,
                                                  entry->content_type);
  change->etag =
      lc_strdup_with_allocator(context->summary->allocator, entry->etag);
  change->descriptor =
      lc_strdup_with_allocator(context->summary->allocator, entry->descriptor);
  if (change->key == NULL || change->key_hex == NULL ||
      (entry->content_type != NULL && change->content_type == NULL) ||
      (entry->etag != NULL && change->etag == NULL) ||
      (entry->descriptor != NULL && change->descriptor == NULL)) {
    lc_pouch_query_index_incremental_change_cleanup(context->summary->allocator,
                                                    change);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch incremental index change",
                        NULL, NULL, NULL);
  }
  change->version = entry->version;
  change->bytes = entry->bytes;
  change->cipher_bytes = entry->cipher_bytes;
  change->updated_at_unix = entry->updated_at_unix;
  change->has_query_hidden = entry->has_query_hidden;
  change->query_hidden = entry->query_hidden;
  change->found = entry->found;
  change->has_payload = entry->has_payload;
  ++context->change_count;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_visit(
    const lc_pouch_state_change_visit_entry *entry, void *context,
    lc_error *error) {
  lc_pouch_query_index_incremental_context *incremental;

  incremental = (lc_pouch_query_index_incremental_context *)context;
  return lc_pouch_query_index_incremental_context_add_change(incremental, entry,
                                                             error);
}

static int lc_pouch_query_index_incremental_apply_changes(
    lc_pouch_query_index_incremental_context *incremental, lc_error *error) {
  lc_pouch_state_visit_entry current;
  size_t change_index;
  int rc;

  if (incremental == NULL || incremental->summary == NULL ||
      incremental->change_count == 0U) {
    return LC_OK;
  }
  for (change_index = 0U; change_index < incremental->change_count;
       ++change_index) {
    lc_pouch_query_index_incremental_change *change;
    lc_pouch_query_index_row *row;

    change = &incremental->changes[change_index];
    row = lc_pouch_query_index_summary_find_row(incremental->summary,
                                                change->key);
    change->preserves_cached_body =
        lc_pouch_query_index_incremental_change_preserves_body(row, change);
  }
  rc = lc_pouch_query_index_summary_remove_changes(
      incremental->summary, incremental->changes, incremental->change_count,
      error);
  if (rc != LC_OK) {
    return rc;
  }
  qsort(incremental->changes, incremental->change_count,
        sizeof(incremental->changes[0]),
        lc_pouch_query_index_incremental_change_compare_key);
  for (change_index = 0U; change_index < incremental->change_count;
       ++change_index) {
    const lc_pouch_query_index_incremental_change *change;
    size_t prior_count;

    change = &incremental->changes[change_index];
    if (!change->found) {
      continue;
    }
    if (change->preserves_cached_body) {
      lc_pouch_query_index_row *row;

      row = lc_pouch_query_index_summary_find_row(incremental->summary,
                                                  change->key);
      if (row == NULL) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch cached metadata row is missing", NULL, NULL,
                            "pouch");
      }
      row->has_query_hidden = change->has_query_hidden;
      row->query_hidden = change->query_hidden;
      continue;
    }
    memset(&current, 0, sizeof(current));
    current.key = change->key;
    current.content_type = change->content_type;
    current.etag = change->etag;
    current.version = change->version;
    current.bytes = change->bytes;
    current.cipher_bytes = change->cipher_bytes;
    current.descriptor = change->descriptor;
    current.updated_at_unix = change->updated_at_unix;
    current.has_query_hidden = change->has_query_hidden;
    current.query_hidden = change->query_hidden;
    current.has_payload = change->has_payload;
    prior_count = incremental->summary->count;
    rc = lc_pouch_query_index_summary_visit(&current, incremental->summary,
                                            error);
    if (rc != LC_OK) {
      return rc;
    }
    if (incremental->summary->count > prior_count) {
      rc = lc_pouch_query_index_incremental_context_add_key(
          incremental, incremental->summary->rows[prior_count].key, prior_count,
          error);
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  return LC_OK;
}

static int lc_pouch_query_index_row_compare(const void *left,
                                            const void *right) {
  const lc_pouch_query_index_row *a;
  const lc_pouch_query_index_row *b;
  int cmp;

  a = (const lc_pouch_query_index_row *)left;
  b = (const lc_pouch_query_index_row *)right;
  cmp = strcmp(a->key_hex, b->key_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (a->version < b->version) {
    return -1;
  }
  if (a->version > b->version) {
    return 1;
  }
  return 0;
}

static int
lc_pouch_query_index_term_equal(const lc_pouch_query_index_term *left,
                                const lc_pouch_query_index_term *right) {
  return left != NULL && right != NULL &&
         strcmp(left->field_hex, right->field_hex) == 0 &&
         strcmp(left->value_hex, right->value_hex) == 0 &&
         left->value_type == right->value_type &&
         ((left->long_value == NULL && right->long_value == NULL) ||
          (left->long_value != NULL && right->long_value != NULL &&
           left->long_value_len == right->long_value_len &&
           memcmp(left->long_value, right->long_value, left->long_value_len) ==
               0)) &&
         strcmp(left->key_hex, right->key_hex) == 0;
}

static int lc_pouch_query_index_presence_equal(
    const lc_pouch_query_index_presence *left,
    const lc_pouch_query_index_presence *right) {
  return left != NULL && right != NULL &&
         strcmp(left->field_hex, right->field_hex) == 0 &&
         strcmp(left->key_hex, right->key_hex) == 0;
}

static int lc_pouch_query_index_text_reserve(lc_pouch_query_index_text *text,
                                             size_t extra, lc_error *error) {
  char *next_bytes;
  size_t needed;
  size_t next_capacity;

  if (extra > (size_t)-1 - text->length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index text exceeds local limit", NULL,
                        NULL, NULL);
  }
  needed = text->length + extra + 1U;
  if (needed <= text->capacity) {
    return LC_OK;
  }
  next_capacity = text->capacity == 0U ? 256U : text->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index text exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes = (char *)lc_alloc_with_allocator(text->allocator, next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index text", NULL, NULL,
                        NULL);
  }
  if (text->bytes != NULL) {
    memcpy(next_bytes, text->bytes, text->length);
    lc_free_with_allocator(text->allocator, text->bytes);
  }
  text->bytes = next_bytes;
  text->capacity = next_capacity;
  text->bytes[text->length] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_text_append(lc_pouch_query_index_text *text,
                                            const char *bytes, size_t length,
                                            unsigned long *hash,
                                            lc_error *error) {
  int rc;

  if (text != NULL) {
    rc = lc_pouch_query_index_text_reserve(text, length, error);
    if (rc != LC_OK) {
      return rc;
    }
    memcpy(text->bytes + text->length, bytes, length);
    text->length += length;
    text->bytes[text->length] = '\0';
  }
  if (hash != NULL) {
    lc_pouch_query_index_hash_bytes(hash, bytes, length);
  }
  return LC_OK;
}

static int
lc_pouch_query_index_text_append_cstr(lc_pouch_query_index_text *text,
                                      const char *bytes, unsigned long *hash,
                                      lc_error *error) {
  return lc_pouch_query_index_text_append(text, bytes, strlen(bytes), hash,
                                          error);
}

static int lc_pouch_query_index_text_append_ulong_line(
    lc_pouch_query_index_text *text, const char *name, unsigned long value,
    lc_error *error) {
  char line[96];
  int written;

  if (text == NULL || name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line requires text and name",
                        NULL, NULL, NULL);
  }
  written = snprintf(line, sizeof(line), "%s=%lu\n", name, value);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line exceeds local limit",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_text_append(text, line, (size_t)written, NULL,
                                          error);
}

static int
lc_pouch_query_index_text_append_u64_line(lc_pouch_query_index_text *text,
                                          const char *name, uint64_t value,
                                          lc_error *error) {
  char line[96];
  char value_text[32];
  int written;

  if (text == NULL || name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line requires text and name",
                        NULL, NULL, NULL);
  }
  if (lc_u64_format_base10((lc_u64)value, value_text, sizeof(value_text)) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line exceeds local limit",
                        NULL, NULL, NULL);
  }
  written = snprintf(line, sizeof(line), "%s=%s\n", name, value_text);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line exceeds local limit",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_text_append(text, line, (size_t)written, NULL,
                                          error);
}

static int lc_pouch_query_index_read_line(FILE *fp,
                                          lc_pouch_query_index_text *line,
                                          int *got_line, lc_error *error) {
  int ch;

  if (line == NULL || got_line == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index line read requires outputs", NULL,
                        NULL, NULL);
  }
  line->length = 0U;
  if (line->bytes != NULL) {
    line->bytes[0] = '\0';
  }
  *got_line = 0;
  while ((ch = fgetc(fp)) != EOF) {
    int rc;
    char byte;

    byte = (char)ch;
    rc = lc_pouch_query_index_text_append(line, &byte, 1U, NULL, error);
    if (rc != LC_OK) {
      return rc;
    }
    *got_line = 1;
    if (ch == '\n') {
      return LC_OK;
    }
  }
  if (ferror(fp)) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index row", strerror(errno),
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_hex_value(unsigned char value) {
  if (value >= '0' && value <= '9') {
    return (int)(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return (int)(value - 'a') + 10;
  }
  if (value >= 'A' && value <= 'F') {
    return (int)(value - 'A') + 10;
  }
  return -1;
}

static int lc_pouch_query_index_hex_token_valid(const char *token) {
  size_t index;
  size_t length;

  if (token == NULL || token[0] == '\0') {
    return 0;
  }
  if (strcmp(token, "-") == 0) {
    return 1;
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    return 0;
  }
  for (index = 0U; index < length; ++index) {
    if (lc_pouch_query_index_hex_value((unsigned char)token[index]) < 0) {
      return 0;
    }
  }
  return 1;
}

static char *lc_pouch_query_index_hex_decode(const lc_allocator *allocator,
                                             const char *token,
                                             lc_error *error) {
  char *decoded;
  size_t index;
  size_t length;

  if (token == NULL || token[0] == '\0' || strcmp(token, "-") == 0) {
    return lc_strdup_with_allocator(allocator, "");
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index row has invalid hex token", NULL, NULL,
                 NULL);
    return NULL;
  }
  decoded = (char *)lc_alloc_with_allocator(allocator, (length / 2U) + 1U);
  if (decoded == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index decoded key", NULL, NULL,
                 NULL);
    return NULL;
  }
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      lc_free_with_allocator(allocator, decoded);
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    decoded[index / 2U] = (char)(((unsigned int)high << 4) | (unsigned int)low);
  }
  decoded[length / 2U] = '\0';
  return decoded;
}

static char *lc_pouch_query_index_hex_decode_scratch(
    lc_pouch_query_index_term_reader *reader, const char *token,
    lc_error *error) {
  char *next;
  size_t index;
  size_t length;
  size_t decoded_length;

  if (reader == NULL || token == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index scratch decode requires reader and token",
                 NULL, NULL, NULL);
    return NULL;
  }
  if (token[0] == '\0' || strcmp(token, "-") == 0) {
    decoded_length = 0U;
  } else {
    length = strlen(token);
    if ((length % 2U) != 0U) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    decoded_length = length / 2U;
  }
  if (decoded_length + 1U > reader->value_scratch_capacity) {
    next =
        (char *)lc_alloc_with_allocator(reader->allocator, decoded_length + 1U);
    if (next == NULL) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch query-index decoded value", NULL,
                   NULL, NULL);
      return NULL;
    }
    lc_free_with_allocator(reader->allocator, reader->value_scratch);
    reader->value_scratch = next;
    reader->value_scratch_capacity = decoded_length + 1U;
  }
  if (decoded_length == 0U) {
    reader->value_scratch[0] = '\0';
    return reader->value_scratch;
  }
  length = decoded_length * 2U;
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    reader->value_scratch[index / 2U] =
        (char)(((unsigned int)high << 4) | (unsigned int)low);
  }
  reader->value_scratch[decoded_length] = '\0';
  return reader->value_scratch;
}

static int lc_pouch_query_index_parse_ulong_token(char **cursor,
                                                  unsigned long *out) {
  char *begin;
  char *end;

  if (cursor == NULL || *cursor == NULL || out == NULL) {
    return 0;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return 0;
  }
  errno = 0;
  *out = strtoul(begin, &end, 10);
  if (errno != 0 || end == begin || *end != ' ') {
    return 0;
  }
  *cursor = end + 1;
  return 1;
}

static int lc_pouch_query_index_parse_u64_token(char **cursor, uint64_t *out) {
  char *begin;
  char *end;
  lc_u64 value;

  if (cursor == NULL || *cursor == NULL || out == NULL) {
    return 0;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return 0;
  }
  end = begin;
  while (*end >= '0' && *end <= '9') {
    ++end;
  }
  if (end == begin || *end != ' ' ||
      !lc_parse_u64_base10_range_checked(begin, (size_t)(end - begin),
                                         &value)) {
    return 0;
  }
  *out = (uint64_t)value;
  *cursor = end + 1;
  return 1;
}

static int lc_pouch_query_index_parse_int_token(char **cursor, int *out) {
  unsigned long parsed;

  if (!lc_pouch_query_index_parse_ulong_token(cursor, &parsed) ||
      parsed > 1UL) {
    return 0;
  }
  *out = parsed != 0UL;
  return 1;
}

static char *lc_pouch_query_index_next_token(char **cursor, int final_token) {
  char *begin;
  char *end;

  if (cursor == NULL || *cursor == NULL) {
    return NULL;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return NULL;
  }
  end = begin;
  while (*end != '\0' && *end != '\n' && *end != ' ') {
    ++end;
  }
  if (final_token) {
    if (*end == ' ') {
      return NULL;
    }
  } else if (*end != ' ') {
    return NULL;
  }
  if (*end != '\0') {
    *end = '\0';
    *cursor = end + 1;
  } else {
    *cursor = end;
  }
  return begin;
}

static int lc_pouch_query_index_parse_and_visit_row(
    const char *line_bytes, lc_pouch_query_index_row_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_row_view row;
  char *line;
  char *cursor;
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  char *key;
  size_t line_len;
  int rc;

  if (line_bytes == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  line_len = strlen(line_bytes);
  line = (char *)lc_alloc_with_allocator(reader->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index row parser", NULL,
                        NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "row ", sizeof("row ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("row ") - 1U;
  memset(&row, 0, sizeof(row));
  key = NULL;
  key_hex = NULL;
  content_type_hex = NULL;
  etag_hex = NULL;
  rc = LC_OK;
  if (!lc_pouch_query_index_parse_u64_token(&cursor, &row.version) ||
      !lc_pouch_query_index_parse_u64_token(&cursor, &row.bytes) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &row.has_query_hidden) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &row.query_hidden)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index row has invalid numeric fields", NULL,
                      NULL, NULL);
  }
  if (rc == LC_OK) {
    key_hex = lc_pouch_query_index_next_token(&cursor, 0);
    content_type_hex = lc_pouch_query_index_next_token(&cursor, 0);
    etag_hex = lc_pouch_query_index_next_token(&cursor, 1);
    if (key_hex == NULL || content_type_hex == NULL || etag_hex == NULL ||
        strcmp(key_hex, "-") == 0 ||
        !lc_pouch_query_index_hex_token_valid(key_hex) ||
        !lc_pouch_query_index_hex_token_valid(content_type_hex) ||
        !lc_pouch_query_index_hex_token_valid(etag_hex)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row has invalid hex fields", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    row.doc_id = reader->row_index;
    row.key_hex = key_hex;
    row.key = key;
    rc = reader->visit(&row, reader->context, error);
  }
  lc_free_with_allocator(reader->allocator, key);
  lc_free_with_allocator(reader->allocator, line);
  return rc;
}

static int lc_pouch_query_index_parse_number_value(const char *text,
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

static int lc_pouch_query_index_exact_generation_value_hex(
    const lc_allocator *allocator, const char *value_hex, char value_type,
    char **out, lc_error *error) {
  char canonical[64];
  char *value_text;
  double number;
  int written;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation value requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (value_hex == NULL || (value_type != 's' && value_type != 'n' &&
                            value_type != 'b' && value_type != 'z')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation value requires a "
                        "typed scalar",
                        NULL, NULL, "pouch");
  }
  if (value_type != 'n') {
    *out = lc_strdup_with_allocator(allocator, value_hex);
    if (*out == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch exact generation value",
                          NULL, NULL, NULL);
    }
    return LC_OK;
  }
  value_text = lc_pouch_query_index_hex_decode(allocator, value_hex, error);
  if (value_text == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (!lc_pouch_query_index_parse_number_value(value_text, &number)) {
    lc_free_with_allocator(allocator, value_text);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number term is "
                        "invalid",
                        NULL, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, value_text);
  if (number == 0.0) {
    written = snprintf(canonical, sizeof(canonical), "0");
  } else {
    written = snprintf(canonical, sizeof(canonical), "%.17g", number);
  }
  if (written < 0 || (size_t)written >= sizeof(canonical)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number exceeds "
                        "local limit",
                        NULL, NULL, "pouch");
  }
  *out = lc_pouch_query_index_hex_encode(allocator, canonical);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact numeric generation "
                        "value",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_number_text_hex(
    const lc_allocator *allocator, const char *value, size_t value_len,
    char **out, lc_error *error) {
  char canonical[64];
  char *value_text;
  double number;
  int written;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index numeric generation value requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number term is "
                        "invalid",
                        NULL, NULL, "pouch");
  }
  value_text = (char *)lc_alloc_with_allocator(allocator, value_len + 1U);
  if (value_text == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact numeric generation "
                        "text",
                        NULL, NULL, NULL);
  }
  memcpy(value_text, value, value_len);
  value_text[value_len] = '\0';
  if (!lc_pouch_query_index_parse_number_value(value_text, &number)) {
    lc_free_with_allocator(allocator, value_text);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number term is "
                        "invalid",
                        NULL, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, value_text);
  if (number == 0.0) {
    written = snprintf(canonical, sizeof(canonical), "0");
  } else {
    written = snprintf(canonical, sizeof(canonical), "%.17g", number);
  }
  if (written < 0 || (size_t)written >= sizeof(canonical)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number exceeds "
                        "local limit",
                        NULL, NULL, "pouch");
  }
  *out = lc_pouch_query_index_hex_encode(allocator, canonical);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact numeric generation "
                        "value",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static unsigned long
lc_pouch_query_index_exact_hash_update(unsigned long hash, unsigned char byte) {
  hash ^= (unsigned long)byte;
  return (hash * LC_POUCH_QUERY_INDEX_HASH_PRIME) &
         LC_POUCH_QUERY_INDEX_HASH_MASK;
}

static char *lc_pouch_query_index_exact_hash_format(
    const lc_allocator *allocator, unsigned long length, unsigned long hash,
    unsigned long folded_hash, lc_error *error) {
  char value[25];
  int written;

  written =
      snprintf(value, sizeof(value), "%08lx%08lx%08lx",
               length & LC_POUCH_QUERY_INDEX_HASH_MASK, hash, folded_hash);
  if (written < 0 || (size_t)written >= sizeof(value)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index exact hash exceeds local limit", NULL, NULL,
                 "pouch");
    return NULL;
  }
  return lc_strdup_with_allocator(allocator, value);
}

static char *
lc_pouch_query_index_exact_hash_value(const lc_allocator *allocator,
                                      const char *value, size_t value_len,
                                      lc_error *error) {
  unsigned long hash;
  unsigned long folded_hash;
  size_t index;

  hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  folded_hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  for (index = 0U; index < value_len; ++index) {
    unsigned char byte;
    unsigned char folded;

    byte = (unsigned char)value[index];
    folded =
        byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
    hash = lc_pouch_query_index_exact_hash_update(hash, byte);
    folded_hash = lc_pouch_query_index_exact_hash_update(folded_hash, folded);
  }
  return lc_pouch_query_index_exact_hash_format(
      allocator, (unsigned long)value_len, hash, folded_hash, error);
}

static char *lc_pouch_query_index_exact_hash_value_hex(
    const lc_allocator *allocator, const char *value_hex, lc_error *error) {
  unsigned long hash;
  unsigned long folded_hash;
  size_t value_hex_len;
  size_t index;

  if (value_hex == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index exact hash requires value", NULL, NULL,
                 NULL);
    return NULL;
  }
  value_hex_len = strlen(value_hex);
  if ((value_hex_len % 2U) != 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index exact hash requires byte-aligned hex", NULL,
                 NULL, "pouch");
    return NULL;
  }
  hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  folded_hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  for (index = 0U; index < value_hex_len; index += 2U) {
    int high;
    int low;
    unsigned char value;
    unsigned char folded;

    high = lc_pouch_query_index_hex_value((unsigned char)value_hex[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)value_hex[index + 1U]);
    if (high < 0 || low < 0) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index exact hash has invalid hex", NULL, NULL,
                   "pouch");
      return NULL;
    }
    value = (unsigned char)(((unsigned int)high << 4U) | (unsigned int)low);
    folded = value >= 'A' && value <= 'Z' ? (unsigned char)(value - 'A' + 'a')
                                          : value;
    hash = lc_pouch_query_index_exact_hash_update(hash, value);
    folded_hash = lc_pouch_query_index_exact_hash_update(folded_hash, folded);
  }
  return lc_pouch_query_index_exact_hash_format(
      allocator, (unsigned long)(value_hex_len / 2U), hash, folded_hash, error);
}

static int
lc_pouch_query_index_exact_string_is_long_hex(const char *value_hex) {
  return value_hex != NULL &&
         strlen(value_hex) >
             (LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES * 2U);
}

int lc_pouch_query_index_scalar_candidates_exact(const char *value,
                                                 char value_type) {
  return value_type != 's' || value == NULL ||
         strlen(value) <= LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
}

static int lc_pouch_query_index_rewrite_long_exact_terms(
    const lc_allocator *allocator, lc_pouch_index_term_key *terms,
    size_t *term_count, lc_error *error) {
  size_t index;
  size_t write_index;
  int changed;
  int rc;

  if (terms == NULL || term_count == NULL) {
    return LC_OK;
  }
  changed = 0;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < *term_count; ++index) {
    if (terms[index].value_type == 's' &&
        lc_pouch_query_index_exact_string_is_long_hex(terms[index].value_hex)) {
      char *hash_value_hex;

      hash_value_hex = lc_pouch_query_index_exact_hash_value_hex(
          allocator, terms[index].value_hex, error);
      if (hash_value_hex == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
        break;
      }
      lc_free_with_allocator(allocator, (char *)terms[index].value_hex);
      terms[index].value_hex = hash_value_hex;
      terms[index].value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
      changed = 1;
    }
  }
  if (rc != LC_OK || !changed || *term_count <= 1U) {
    return rc;
  }
  qsort(terms, *term_count, sizeof(terms[0]), lc_pouch_index_term_key_compare);
  write_index = 0U;
  for (index = 0U; index < *term_count; ++index) {
    if (write_index > 0U && lc_pouch_index_term_key_compare_items(
                                &terms[write_index - 1U], &terms[index]) == 0) {
      lc_free_with_allocator(allocator, (char *)terms[index].field_hex);
      lc_free_with_allocator(allocator, (char *)terms[index].value_hex);
      memset(&terms[index], 0, sizeof(terms[index]));
      continue;
    }
    if (write_index != index) {
      terms[write_index] = terms[index];
      memset(&terms[index], 0, sizeof(terms[index]));
    }
    ++write_index;
  }
  *term_count = write_index;
  return LC_OK;
}

int lc_pouch_query_index_build_exact_terms(
    const lc_pouch_index_plain_term *terms, size_t term_count,
    lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  int rc;

  rc = lc_pouch_index_term_keys_build_exact(terms, term_count, out_terms,
                                            out_count, allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rewrite_long_exact_terms(allocator, *out_terms,
                                                       out_count, error);
  }
  return rc;
}

static int lc_pouch_query_index_build_exact_terms_for_field(
    const char *field, const char *const *values, const char *value_types,
    size_t value_count, lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  int rc;

  rc = lc_pouch_index_term_keys_build_exact_for_field(
      field, values, value_types, value_count, out_terms, out_count, allocator,
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rewrite_long_exact_terms(allocator, *out_terms,
                                                       out_count, error);
  }
  return rc;
}

static int
lc_pouch_query_index_hex_string_may_be_datetime(const char *value_hex) {
  size_t index;

  if (value_hex == NULL) {
    return 0;
  }
  for (index = 0U; index < 20U; ++index) {
    if (value_hex[index] == '\0') {
      return 0;
    }
  }
  return value_hex[0] == '3' && value_hex[2] == '3' && value_hex[4] == '3' &&
         value_hex[6] == '3' && value_hex[1] >= '0' && value_hex[1] <= '9' &&
         value_hex[3] >= '0' && value_hex[3] <= '9' && value_hex[5] >= '0' &&
         value_hex[5] <= '9' && value_hex[7] >= '0' && value_hex[7] <= '9' &&
         value_hex[8] == '2' && (value_hex[9] == 'd' || value_hex[9] == 'D') &&
         value_hex[10] == '3' && value_hex[12] == '3' && value_hex[11] >= '0' &&
         value_hex[11] <= '9' && value_hex[13] >= '0' && value_hex[13] <= '9' &&
         value_hex[14] == '2' &&
         (value_hex[15] == 'd' || value_hex[15] == 'D') &&
         value_hex[16] == '3' && value_hex[18] == '3' && value_hex[17] >= '0' &&
         value_hex[17] <= '9' && value_hex[19] >= '0' && value_hex[19] <= '9';
}

static int lc_pouch_query_index_parse_integer_hex_value(const char *token,
                                                        double *out) {
  double value;
  size_t index;
  size_t length;
  int negative;
  int saw_digit;

  if (token == NULL || token[0] == '\0' || out == NULL) {
    return 0;
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    return 0;
  }
  value = 0.0;
  negative = 0;
  saw_digit = 0;
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;
    unsigned char ch;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      return 0;
    }
    ch = (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
    if (index == 0U && ch == '-') {
      negative = 1;
      continue;
    }
    if (ch < '0' || ch > '9') {
      return 0;
    }
    saw_digit = 1;
    value = (value * 10.0) + (double)(ch - '0');
    if (!isfinite(value)) {
      return 0;
    }
  }
  if (!saw_digit) {
    return 0;
  }
  *out = negative ? -value : value;
  return 1;
}

static int lc_pouch_query_index_range_contains_value(
    const lc_pouch_query_index_range_bounds *bounds, double value) {
  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return 0;
  }
  if (bounds->has_gt && !(value > bounds->gt)) {
    return 0;
  }
  if (bounds->has_gte && !(value >= bounds->gte)) {
    return 0;
  }
  if (bounds->has_lt && !(value < bounds->lt)) {
    return 0;
  }
  if (bounds->has_lte && !(value <= bounds->lte)) {
    return 0;
  }
  return 1;
}

static int lc_pouch_query_index_hex_value(unsigned char value);

static unsigned char lc_pouch_query_index_ascii_lower(unsigned char ch) {
  if (ch >= 'A' && ch <= 'Z') {
    return (unsigned char)(ch - 'A' + 'a');
  }
  return ch;
}

static int lc_pouch_query_index_hex_text_has_prefix(const char *value_hex,
                                                    const char *prefix,
                                                    int ignore_case) {
  size_t index;

  if (value_hex == NULL || prefix == NULL) {
    return 0;
  }
  for (index = 0U; prefix[index] != '\0'; ++index) {
    int high;
    int low;
    unsigned char actual;
    unsigned char expected;

    if (value_hex[index * 2U] == '\0' || value_hex[(index * 2U) + 1U] == '\0') {
      return 0;
    }
    high = lc_pouch_query_index_hex_value((unsigned char)value_hex[index * 2U]);
    low = lc_pouch_query_index_hex_value(
        (unsigned char)value_hex[(index * 2U) + 1U]);
    if (high < 0 || low < 0) {
      return 0;
    }
    actual = (unsigned char)((high << 4) | low);
    if (actual == '\0') {
      return 0;
    }
    expected = (unsigned char)prefix[index];
    if (ignore_case) {
      actual = lc_pouch_query_index_ascii_lower(actual);
      expected = lc_pouch_query_index_ascii_lower(expected);
    }
    if (actual != expected) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_hex_text_contains(const char *value_hex,
                                                  const char *needle,
                                                  int ignore_case) {
  size_t value_index;
  size_t needle_len;
  size_t value_hex_len;
  size_t value_len;

  if (value_hex == NULL || needle == NULL) {
    return 0;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U) {
    return 1;
  }
  value_hex_len = strlen(value_hex);
  value_len = value_hex_len / 2U;
  if (value_hex_len % 2U != 0U || needle_len > value_len) {
    return 0;
  }
  for (value_index = 0U; value_index + needle_len <= value_len; ++value_index) {
    if (lc_pouch_query_index_hex_text_has_prefix(value_hex + (value_index * 2U),
                                                 needle, ignore_case)) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_hex_contains_aligned(const char *value_hex,
                                                     const char *needle_hex) {
  const char *match;
  size_t value_len;
  size_t needle_len;

  if (value_hex == NULL || needle_hex == NULL) {
    return 0;
  }
  needle_len = strlen(needle_hex);
  if (needle_len == 0U) {
    return 1;
  }
  value_len = strlen(value_hex);
  if (value_len % 2U != 0U || needle_len % 2U != 0U || needle_len > value_len) {
    return 0;
  }
  match = value_hex;
  while ((match = strstr(match, needle_hex)) != NULL) {
    if (((size_t)(match - value_hex) % 2U) == 0U) {
      return 1;
    }
    ++match;
  }
  return 0;
}

static int lc_pouch_query_index_term_reader_matches_value(
    lc_pouch_query_index_term_reader *reader, const char *value_hex,
    int *matched, lc_error *error) {
  char *value_text;
  double number;
  lc_pouch_index_instant instant;

  if (matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term match requires output", NULL,
                        NULL, NULL);
  }
  *matched = 0;
  if (reader == NULL || value_hex == NULL) {
    return LC_OK;
  }
  if (reader->prefix_match) {
    if (reader->ignore_case) {
      *matched = lc_pouch_query_index_hex_text_has_prefix(
          value_hex, reader->value_text, 1);
    } else {
      *matched =
          strncmp(value_hex, reader->value_hex, strlen(reader->value_hex)) == 0;
    }
    return LC_OK;
  }
  if (reader->contains_match) {
    if (reader->ignore_case) {
      *matched = lc_pouch_query_index_hex_text_contains(value_hex,
                                                        reader->value_text, 1);
    } else {
      *matched = lc_pouch_query_index_hex_contains_aligned(value_hex,
                                                           reader->value_hex);
    }
    return LC_OK;
  }
  if (reader->range_match) {
    if (lc_pouch_query_index_parse_integer_hex_value(value_hex, &number)) {
      *matched = lc_pouch_query_index_range_contains_value(
          &reader->range_bounds, number);
    } else {
      value_text =
          lc_pouch_query_index_hex_decode_scratch(reader, value_hex, error);
      if (value_text == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      if (lc_pouch_query_index_parse_number_value(value_text, &number)) {
        *matched = lc_pouch_query_index_range_contains_value(
            &reader->range_bounds, number);
      }
    }
    return LC_OK;
  }
  if (reader->date_match) {
    value_text =
        lc_pouch_query_index_hex_decode_scratch(reader, value_hex, error);
    if (value_text == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (lc_pouch_index_parse_lql_datetime(value_text, &instant)) {
      *matched = lc_pouch_index_date_contains_value(&reader->parsed_date_bounds,
                                                    &instant);
    }
    return LC_OK;
  }
  *matched = strcmp(value_hex, reader->value_hex) == 0;
  return LC_OK;
}

static int lc_pouch_query_index_parse_and_visit_term(
    char *line, lc_pouch_query_index_term_reader *reader, lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *key_hex;
  char *type_token;
  char *key;
  char value_type;
  int matched;
  int field_cmp;
  int value_cmp;
  int rc;

  if (line == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  if (strncmp(line, "term ", sizeof("term ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("term ") - 1U;
  key = NULL;
  rc = LC_OK;
  memset(&key_view, 0, sizeof(key_view));
  if (!lc_pouch_query_index_parse_u64_token(&cursor, &key_view.version) ||
      !lc_pouch_query_index_parse_u64_token(&cursor, &key_view.bytes) ||
      !lc_pouch_query_index_parse_int_token(&cursor,
                                            &key_view.has_query_hidden) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &key_view.query_hidden) ||
      !lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.doc_id)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index term has invalid numeric fields", NULL,
                      NULL, NULL);
  }
  field_hex = NULL;
  value_hex = NULL;
  key_hex = NULL;
  type_token = NULL;
  value_type = '\0';
  if (rc == LC_OK) {
    field_hex = lc_pouch_query_index_next_token(&cursor, 0);
    value_hex = lc_pouch_query_index_next_token(&cursor, 0);
    key_hex = lc_pouch_query_index_next_token(&cursor, 0);
    type_token = lc_pouch_query_index_next_token(&cursor, 1);
    if (field_hex == NULL || value_hex == NULL || key_hex == NULL ||
        type_token == NULL || type_token[0] == '\0' || type_token[1] != '\0' ||
        (type_token[0] != 's' && type_token[0] != 'n' && type_token[0] != 'b' &&
         type_token[0] != 'z') ||
        strcmp(field_hex, "-") == 0 || strcmp(key_hex, "-") == 0 ||
        !lc_pouch_query_index_hex_token_valid(field_hex) ||
        !lc_pouch_query_index_hex_token_valid(value_hex) ||
        !lc_pouch_query_index_hex_token_valid(key_hex)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid hex fields", NULL,
                        NULL, NULL);
    } else {
      value_type = type_token[0];
    }
  }
  matched = 0;
  field_cmp = 0;
  if (rc == LC_OK &&
      lc_pouch_query_index_field_hex_is_any_text(reader->field_hex)) {
    field_cmp = 0;
  } else if (rc == LC_OK &&
             (field_cmp = strcmp(field_hex, reader->field_hex)) > 0) {
    reader->stop = 1;
  } else if (rc == LC_OK && field_cmp == 0) {
    if (reader->value_type_match && value_type != reader->value_type) {
      matched = 0;
    } else if (reader->string_values_only && value_type != 's') {
      matched = 0;
    } else if (!reader->prefix_match && !reader->contains_match &&
               !reader->range_match && !reader->date_match &&
               !reader->ignore_case) {
      value_cmp = strcmp(value_hex, reader->value_hex);
      if (value_cmp > 0) {
        reader->stop = 1;
      } else if (value_cmp == 0) {
        rc = lc_pouch_query_index_term_reader_matches_value(reader, value_hex,
                                                            &matched, error);
      }
    } else {
      rc = lc_pouch_query_index_term_reader_matches_value(reader, value_hex,
                                                          &matched, error);
    }
  }
  if (rc == LC_OK && matched) {
    key_view.key_hex = key_hex;
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      key_view.key = key;
      rc = reader->visit(&key_view, reader->context, error);
    }
  }
  lc_free_with_allocator(reader->allocator, key);
  return rc;
}

static int lc_pouch_query_index_parse_and_visit_presence(
    const char *line_bytes, lc_pouch_query_index_presence_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *line;
  char *cursor;
  char *field_hex;
  char *key_hex;
  char *key;
  size_t line_len;
  int rc;

  if (line_bytes == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  line_len = strlen(line_bytes);
  line = (char *)lc_alloc_with_allocator(reader->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presence parser",
                        NULL, NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "present ", sizeof("present ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("present ") - 1U;
  key = NULL;
  field_hex = lc_pouch_query_index_next_token(&cursor, 0);
  key_hex = lc_pouch_query_index_next_token(&cursor, 1);
  if (field_hex == NULL || key_hex == NULL || strcmp(field_hex, "-") == 0 ||
      strcmp(key_hex, "-") == 0 ||
      !lc_pouch_query_index_hex_token_valid(field_hex) ||
      !lc_pouch_query_index_hex_token_valid(key_hex)) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence has invalid hex fields",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  if (strcmp(field_hex, reader->field_hex) == 0) {
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      key_view.key = key;
      rc = reader->visit(&key_view, reader->context, error);
    }
  }
  lc_free_with_allocator(reader->allocator, key);
  lc_free_with_allocator(reader->allocator, line);
  return rc;
}

static char *lc_pouch_query_index_path(lc_pouch *pouch, const char *ns,
                                       lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *header_path;

  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  if (namespace_path == NULL) {
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  header_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                   LC_POUCH_QUERY_INDEX_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (header_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index header artifact path",
                 NULL, NULL, NULL);
  }
  return header_path;
}

static char *lc_pouch_query_index_manifest_path(lc_pouch *pouch, const char *ns,
                                                lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *manifest_path;

  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  if (namespace_path == NULL) {
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  manifest_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                     LC_POUCH_QUERY_INDEX_MANIFEST_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (manifest_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index manifest path", NULL,
                 NULL, NULL);
  }
  return manifest_path;
}

static char *lc_pouch_query_index_segment_artifact_path(
    lc_pouch *pouch, const char *ns, const char *segment_id,
    const char *artifact_leaf, lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *leaf;
  char *path;
  size_t segment_len;
  size_t artifact_len;

  if (segment_id == NULL || segment_id[0] == '\0' || artifact_leaf == NULL ||
      artifact_leaf[0] == '\0') {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index segment path requires segment and "
                 "artifact",
                 NULL, NULL, "pouch");
    return NULL;
  }
  segment_len = strlen(segment_id);
  artifact_len = strlen(artifact_leaf);
  if (segment_len > (size_t)-1 - artifact_len - sizeof("query..")) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index segment artifact path exceeds local limit",
                 NULL, NULL, NULL);
    return NULL;
  }
  leaf = (char *)lc_alloc_with_allocator(&pouch->allocator,
                                         sizeof("query.") - 1U + segment_len +
                                             1U + artifact_len + 1U);
  if (leaf == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index segment artifact leaf",
                 NULL, NULL, NULL);
    return NULL;
  }
  memcpy(leaf, "query.", sizeof("query.") - 1U);
  memcpy(leaf + sizeof("query.") - 1U, segment_id, segment_len);
  leaf[sizeof("query.") - 1U + segment_len] = '.';
  memcpy(leaf + sizeof("query.") + segment_len, artifact_leaf,
         artifact_len + 1U);
  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  if (namespace_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, leaf);
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, leaf);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  path = lc_pouch_path_join(&pouch->allocator, index_path, leaf);
  lc_free_with_allocator(&pouch->allocator, index_path);
  lc_free_with_allocator(&pouch->allocator, leaf);
  if (path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index segment artifact path",
                 NULL, NULL, NULL);
  }
  return path;
}

static int lc_pouch_query_index_file_present(const char *path, int *present,
                                             lc_error *error) {
  struct stat st;

  if (path == NULL || present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index file probe requires path and "
                        "present output",
                        NULL, NULL, NULL);
  }
  *present = 0;
  if (stat(path, &st) == 0) {
    *present = S_ISREG(st.st_mode) ? 1 : 0;
    return LC_OK;
  }
  if (errno == ENOENT) {
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to probe pouch query-index artifact",
                      strerror(errno), NULL, "pouch");
}

static char *
lc_pouch_query_index_crypto_descriptor_path(const lc_allocator *allocator,
                                            const char *path, lc_error *error) {
  char *descriptor_path;
  size_t path_len;
  size_t suffix_len;

  if (path == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index crypto descriptor requires path", NULL,
                 NULL, "pouch");
    return NULL;
  }
  path_len = strlen(path);
  suffix_len = strlen(LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX);
  if (path_len > (size_t)-1 - suffix_len - 1U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index crypto descriptor path exceeds local "
                 "limit",
                 NULL, NULL, NULL);
    return NULL;
  }
  descriptor_path =
      (char *)lc_alloc_with_allocator(allocator, path_len + suffix_len + 1U);
  if (descriptor_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index crypto descriptor path",
                 NULL, NULL, NULL);
    return NULL;
  }
  memcpy(descriptor_path, path, path_len);
  memcpy(descriptor_path + path_len, LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX,
         suffix_len + 1U);
  return descriptor_path;
}

static char *
lc_pouch_query_index_artifact_context(const lc_allocator *allocator,
                                      const char *ns, const char *path,
                                      lc_error *error) {
  const char prefix[] = "query-index/";
  const char *leaf;
  char *context;
  size_t prefix_len;
  size_t namespace_len;
  size_t leaf_len;
  size_t length;

  if (ns == NULL || path == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index crypto context requires namespace and "
                 "path",
                 NULL, NULL, "pouch");
    return NULL;
  }
  leaf = strrchr(path, '/');
  leaf = leaf != NULL ? leaf + 1 : path;
  prefix_len = sizeof(prefix) - 1U;
  namespace_len = strlen(ns);
  leaf_len = strlen(leaf);
  if (namespace_len > (size_t)-1 - prefix_len - leaf_len - 2U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index crypto context exceeds local limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  length = prefix_len + namespace_len + 1U + leaf_len;
  context = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (context == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index crypto context", NULL,
                 NULL, NULL);
    return NULL;
  }
  memcpy(context, prefix, prefix_len);
  memcpy(context + prefix_len, ns, namespace_len);
  context[prefix_len + namespace_len] = '/';
  memcpy(context + prefix_len + namespace_len + 1U, leaf, leaf_len + 1U);
  return context;
}

static int lc_pouch_query_index_read_file_bytes(const lc_allocator *allocator,
                                                const char *path,
                                                char **out_bytes,
                                                size_t *out_length,
                                                int *present, lc_error *error) {
  FILE *fp;
  char *bytes;
  size_t length;
  size_t capacity;
  size_t got;

  if (path == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index file read requires path and "
                        "outputs",
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
                        "failed to open pouch query-index file",
                        strerror(errno), NULL, "pouch");
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
                            "pouch query-index file exceeds local limit", NULL,
                            NULL, NULL);
      }
      next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
      if (next_bytes == NULL) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch query-index file", NULL,
                            NULL, NULL);
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
                            "failed to read pouch query-index file",
                            strerror(errno), NULL, "pouch");
      }
      break;
    }
  }
  if (fclose(fp) != 0) {
    lc_free_with_allocator(allocator, bytes);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index file",
                        strerror(errno), NULL, "pouch");
  }
  bytes[length] = '\0';
  *out_bytes = bytes;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_query_index_write_bytes_direct_relaxed(const char *path,
                                                           const char *bytes,
                                                           size_t length,
                                                           lc_error *error) {
  if (path == NULL || (bytes == NULL && length > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index artifact write requires path and "
                        "bytes",
                        NULL, NULL, NULL);
  }
  return lc_pouch_path_write_bytes_file_relaxed(
      path, bytes != NULL ? bytes : "", length, error);
}

/* A segment artifact that the current manifest cannot name does not need a
 * temporary rename. A failed write leaves an unreferenced path only; publish
 * still occurs atomically when the manifest is replaced after all artifacts
 * have been completed. */
static int lc_pouch_query_index_write_unpublished_bytes_relaxed(
    const char *path, const char *bytes, size_t length, lc_error *error) {
  size_t offset;
  int fd;

  if (path == NULL || (bytes == NULL && length > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch unpublished artifact write requires path and "
                        "bytes",
                        NULL, NULL, NULL);
  }
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch unpublished artifact",
                        strerror(errno), NULL, "pouch");
  }
  offset = 0U;
  while (offset < length) {
    ssize_t written;

    written = write(fd, bytes + offset, length - offset);
    if (written > 0) {
      offset += (size_t)written;
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    (void)close(fd);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch unpublished artifact",
                        written < 0 ? strerror(errno) : NULL, NULL, "pouch");
  }
  if (close(fd) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch unpublished artifact",
                        strerror(errno), NULL, "pouch");
  }
  return LC_OK;
}

static void lc_pouch_query_index_put_u64_le(unsigned char *out,
                                            uint64_t value) {
  size_t index;

  if (out == NULL) {
    return;
  }
  for (index = 0U; index < 8U; ++index) {
    out[index] = (unsigned char)((value >> (index * 8U)) & 0xffU);
  }
}

static uint64_t lc_pouch_query_index_get_u64_le(const unsigned char *in) {
  uint64_t value;
  size_t index;

  value = (uint64_t)0U;
  if (in == NULL) {
    return value;
  }
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)in[index]) << (index * 8U);
  }
  return value;
}

int lc_pouch_query_index_append_crypto_footer_fd(int fd, const char *descriptor,
                                                 lc_error *error) {
  unsigned char footer[LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES];
  const char *cursor;
  size_t descriptor_length;
  size_t remaining;
  int rc;

  if (fd < 0 || descriptor == NULL || descriptor[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact requires fd "
                        "and descriptor",
                        NULL, NULL, NULL);
  }
  descriptor_length = strlen(descriptor);
  rc = LC_OK;
  cursor = descriptor;
  remaining = descriptor_length;
  while (remaining > 0U) {
    ssize_t written;

    written = write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch query-index descriptor footer",
                        strerror(errno), NULL, "pouch");
      break;
    }
    if (written == 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to make progress writing pouch query-index "
                        "descriptor footer",
                        NULL, NULL, "pouch");
      break;
    }
    cursor += (size_t)written;
    remaining -= (size_t)written;
  }
  if (rc == LC_OK) {
    memcpy(footer, LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_MAGIC, 8U);
    lc_pouch_query_index_put_u64_le(footer + 8U, (uint64_t)descriptor_length);
    cursor = (const char *)footer;
    remaining = sizeof(footer);
    while (remaining > 0U) {
      ssize_t written;

      written = write(fd, cursor, remaining);
      if (written < 0) {
        if (errno == EINTR) {
          continue;
        }
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch query-index crypto footer",
                          strerror(errno), NULL, "pouch");
        break;
      }
      if (written == 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to make progress writing pouch query-index "
                          "crypto footer",
                          NULL, NULL, "pouch");
        break;
      }
      cursor += (size_t)written;
      remaining -= (size_t)written;
    }
  }
  return rc;
}

int lc_pouch_query_index_read_crypto_footer(const lc_allocator *allocator,
                                            const char *path,
                                            size_t max_descriptor_bytes,
                                            char **descriptor_out,
                                            uint64_t *cipher_length_out,
                                            lc_error *error) {
  unsigned char footer[LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES];
  char *descriptor;
  struct stat st;
  uint64_t descriptor_length64;
  size_t descriptor_length;
  off_t descriptor_offset;
  ssize_t got;
  int fd;

  if (allocator == NULL || path == NULL || descriptor_out == NULL ||
      cipher_length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index footer read requires "
                        "allocator, path, and outputs",
                        NULL, NULL, NULL);
  }
  *descriptor_out = NULL;
  *cipher_length_out = 0U;
  if (stat(path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch encrypted query-index artifact",
                        strerror(errno), NULL, "pouch");
  }
  if (st.st_size < 0 ||
      st.st_size < (off_t)LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact is missing "
                        "crypto footer",
                        NULL, NULL, "pouch");
  }
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch encrypted query-index artifact",
                        strerror(errno), NULL, "pouch");
  }
  got = pread(fd, footer, sizeof(footer), st.st_size - (off_t)sizeof(footer));
  if (got < 0) {
    int saved_errno;

    saved_errno = errno;
    close(fd);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index crypto footer",
                        strerror(saved_errno), NULL, "pouch");
  }
  if ((size_t)got != sizeof(footer) ||
      memcmp(footer, LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_MAGIC, 8U) != 0) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact has invalid "
                        "crypto footer",
                        NULL, NULL, "pouch");
  }
  descriptor_length64 = lc_pouch_query_index_get_u64_le(footer + 8U);
  if (descriptor_length64 == 0U ||
      (max_descriptor_bytes != 0U &&
       descriptor_length64 > max_descriptor_bytes) ||
      descriptor_length64 >
          (uint64_t)(st.st_size -
                     (off_t)LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES) ||
      descriptor_length64 > (uint64_t)((size_t)-1) - 1U) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact has invalid "
                        "descriptor length",
                        NULL, NULL, "pouch");
  }
  descriptor_length = (size_t)descriptor_length64;
  descriptor_offset = st.st_size -
                      (off_t)LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES -
                      (off_t)descriptor_length;
  if (descriptor_offset < 0) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact has invalid "
                        "ciphertext span",
                        NULL, NULL, "pouch");
  }
  descriptor =
      (char *)lc_alloc_with_allocator(allocator, descriptor_length + 1U);
  if (descriptor == NULL) {
    close(fd);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index descriptor", NULL,
                        NULL, NULL);
  }
  got = pread(fd, descriptor, descriptor_length, descriptor_offset);
  if (got < 0) {
    int saved_errno;

    saved_errno = errno;
    close(fd);
    lc_free_with_allocator(allocator, descriptor);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index descriptor",
                        strerror(saved_errno), NULL, "pouch");
  }
  close(fd);
  if ((size_t)got != descriptor_length) {
    lc_free_with_allocator(allocator, descriptor);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index descriptor is truncated",
                        NULL, NULL, "pouch");
  }
  descriptor[descriptor_length] = '\0';
  *descriptor_out = descriptor;
  *cipher_length_out = (uint64_t)descriptor_offset;
  return LC_OK;
}

static int lc_pouch_query_index_source_to_bytes(const lc_allocator *allocator,
                                                lc_source *source,
                                                char **out_bytes,
                                                size_t *out_length,
                                                lc_error *error) {
  lc_sink *sink;
  const void *bytes;
  char *copy;
  size_t length;
  int rc;

  if (source == NULL || out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index source read requires source and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(source, sink, NULL, error);
  }
  bytes = NULL;
  length = 0U;
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  copy = NULL;
  if (rc == LC_OK) {
    if (length == (size_t)-1) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index source exceeds local limit", NULL,
                        NULL, NULL);
    } else {
      copy = (char *)lc_alloc_with_allocator(allocator, length + 1U);
      if (copy == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index source bytes",
                          NULL, NULL, NULL);
      } else {
        if (length > 0U) {
          memcpy(copy, bytes, length);
        }
        copy[length] = '\0';
      }
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, copy);
    return rc;
  }
  *out_bytes = copy;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_query_index_read_encrypted_artifact_bytes(
    lc_pouch *pouch, const char *ns, const char *path, int strict,
    char **out_bytes, size_t *out_length, int *present, int *valid,
    lc_error *error) {
  lc_source *source;
  char *context;
  char *descriptor;
  uint64_t cipher_length;
  int rc;

  if (out_bytes == NULL || out_length == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact read requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  *valid = 0;
  rc = lc_pouch_query_index_file_present(path, present, error);
  if (rc != LC_OK || !*present) {
    return rc;
  }
  descriptor = NULL;
  cipher_length = 0U;
  rc = lc_pouch_query_index_read_crypto_footer(
      &pouch->allocator, path, 0U, &descriptor, &cipher_length, error);
  if (rc != LC_OK) {
    if (!strict && rc != LC_ERR_NOMEM) {
      if (error != NULL) {
        lc_error_cleanup(error);
      }
      return LC_OK;
    }
    return rc;
  }
  context =
      lc_pouch_query_index_artifact_context(&pouch->allocator, ns, path, error);
  if (context == NULL) {
    lc_free_with_allocator(&pouch->allocator, descriptor);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  rc = lc_pouch_crypto_source_from_file_span(pouch->crypto, context, context,
                                             path, 0UL, cipher_length,
                                             descriptor, &source, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_source_to_bytes(&pouch->allocator, source,
                                              out_bytes, out_length, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  lc_free_with_allocator(&pouch->allocator, context);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  if (rc != LC_OK) {
    if (!strict && rc != LC_ERR_NOMEM) {
      if (error != NULL) {
        lc_error_cleanup(error);
      }
      return LC_OK;
    }
    return rc;
  }
  *valid = 1;
  return LC_OK;
}

static const char *
lc_pouch_query_index_packed_leaf_for_component(size_t component) {
  switch (component) {
  case LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE:
    return LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_EXACT:
    return LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_PRESENCE:
    return LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_RANGE:
    return LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_TEXT:
    return LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM:
    return LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL:
    return LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_DELETE:
    return LC_POUCH_QUERY_INDEX_DELETE_LEAF;
  default:
    return NULL;
  }
}

static size_t lc_pouch_query_index_packed_component_for_path(const char *path) {
  size_t path_len;
  size_t component;

  if (path == NULL) {
    return LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
  }
  path_len = strlen(path);
  for (component = 0U; component < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++component) {
    const char *leaf;
    size_t leaf_len;

    leaf = lc_pouch_query_index_packed_leaf_for_component(component);
    if (leaf == NULL) {
      continue;
    }
    leaf_len = strlen(leaf);
    if (path_len >= leaf_len && strcmp(path + path_len - leaf_len, leaf) == 0) {
      return component;
    }
  }
  return LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
}

static char *lc_pouch_query_index_packed_path_from_component(
    const lc_allocator *allocator, const char *component_path, size_t component,
    lc_error *error) {
  const char *leaf;
  char *packed_path;
  size_t path_len;
  size_t leaf_len;
  size_t packed_leaf_len;
  size_t prefix_len;

  leaf = lc_pouch_query_index_packed_leaf_for_component(component);
  if (component_path == NULL || leaf == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch packed query-index path requires component path", NULL,
                 NULL, "pouch");
    return NULL;
  }
  path_len = strlen(component_path);
  leaf_len = strlen(leaf);
  packed_leaf_len = strlen(LC_POUCH_QUERY_INDEX_PACKED_LEAF);
  if (path_len < leaf_len ||
      strcmp(component_path + path_len - leaf_len, leaf) != 0) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch packed query-index path has unexpected component", NULL,
                 NULL, "pouch");
    return NULL;
  }
  prefix_len = path_len - leaf_len;
  if (prefix_len > (size_t)-1 - packed_leaf_len - 1U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch packed query-index path exceeds local limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  packed_path = (char *)lc_alloc_with_allocator(
      allocator, prefix_len + packed_leaf_len + 1U);
  if (packed_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch packed query-index path", NULL, NULL,
                 NULL);
    return NULL;
  }
  memcpy(packed_path, component_path, prefix_len);
  memcpy(packed_path + prefix_len, LC_POUCH_QUERY_INDEX_PACKED_LEAF,
         packed_leaf_len + 1U);
  return packed_path;
}

static int lc_pouch_query_index_pack_components(
    const lc_allocator *allocator, const char *doc_table, size_t doc_table_len,
    const char *exact, size_t exact_len, const char *presence,
    size_t presence_len, const char *range, size_t range_len, const char *text,
    size_t text_len, const char *trigram, size_t trigram_len,
    const char *temporal, size_t temporal_len, const char *deletes,
    size_t deletes_len, char **out, size_t *out_len, lc_error *error) {
  const char *components[LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT];
  size_t lengths[LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT];
  unsigned char *bytes;
  size_t header_len;
  size_t total_len;
  size_t offset;
  size_t index;

  if (out == NULL || out_len == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch packed query-index encode requires outputs",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  *out_len = 0U;
  components[LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE] = doc_table;
  components[LC_POUCH_QUERY_INDEX_PACKED_EXACT] = exact;
  components[LC_POUCH_QUERY_INDEX_PACKED_PRESENCE] = presence;
  components[LC_POUCH_QUERY_INDEX_PACKED_RANGE] = range;
  components[LC_POUCH_QUERY_INDEX_PACKED_TEXT] = text;
  components[LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM] = trigram;
  components[LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL] = temporal;
  components[LC_POUCH_QUERY_INDEX_PACKED_DELETE] = deletes;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE] = doc_table_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_EXACT] = exact_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_PRESENCE] = presence_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_RANGE] = range_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_TEXT] = text_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM] = trigram_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL] = temporal_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_DELETE] = deletes_len;

  header_len = LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U +
               (LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT * 8U);
  total_len = header_len;
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    if (lengths[index] > 0U && components[index] == NULL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch packed query-index component missing bytes",
                          NULL, NULL, "pouch");
    }
    if (lengths[index] > (size_t)-1 - total_len) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch packed query-index segment exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    total_len += lengths[index];
  }
  bytes = (unsigned char *)lc_alloc_with_allocator(allocator, total_len);
  if (bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index segment",
                        NULL, NULL, NULL);
  }
  memcpy(bytes, LC_POUCH_QUERY_INDEX_PACKED_MAGIC,
         LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN);
  lc_pouch_query_index_put_u64_le(bytes + LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN,
                                  LC_POUCH_QUERY_INDEX_PACKED_VERSION);
  lc_pouch_query_index_put_u64_le(
      bytes + LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 8U,
      (uint64_t)LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT);
  offset = LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U;
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    lc_pouch_query_index_put_u64_le(bytes + offset, (uint64_t)lengths[index]);
    offset += 8U;
  }
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    if (lengths[index] > 0U) {
      memcpy(bytes + offset, components[index], lengths[index]);
      offset += lengths[index];
    }
  }
  *out = (char *)bytes;
  *out_len = total_len;
  return LC_OK;
}

static void lc_pouch_query_index_packed_cache_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_packed_cache_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->path);
  lc_free_with_allocator(allocator, entry->bytes);
  memset(entry, 0, sizeof(*entry));
}

static int lc_pouch_query_index_artifact_signature_equal(
    const lc_pouch_query_index_file_signature *left,
    const lc_pouch_query_index_file_signature *right) {
  return left != NULL && right != NULL && left->present && right->present &&
         left->size == right->size && left->mtime == right->mtime &&
         left->mtime_nsec == right->mtime_nsec && left->ctime == right->ctime &&
         left->ctime_nsec == right->ctime_nsec && left->inode == right->inode;
}

static int lc_pouch_query_index_packed_cache_get(
    lc_pouch *pouch, const char *ns, const char *packed_path,
    lc_pouch_query_index_packed_cache_entry **out, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_packed_cache_entry *entry;
  lc_pouch_query_index_packed_cache_entry *previous;
  lc_pouch_query_index_file_signature signature;
  char *bytes;
  size_t length;
  int artifact_valid;
  int rc;

  if (pouch == NULL || packed_path == NULL || out == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch packed query-index cache requires inputs", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  *present = 0;
  *valid = 0;
  if (!lc_pouch_query_index_artifact_signature(packed_path, &signature)) {
    *valid = 1;
    return LC_OK;
  }
  previous = NULL;
  entry = pouch->query_packed_cache;
  while (entry != NULL) {
    if (strcmp(entry->path, packed_path) == 0 &&
        lc_pouch_query_index_artifact_signature_equal(&entry->signature,
                                                      &signature)) {
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_packed_cache;
        pouch->query_packed_cache = entry;
      }
      *out = entry;
      *present = 1;
      *valid = 1;
      return LC_OK;
    }
    previous = entry;
    entry = entry->next;
  }

  bytes = NULL;
  length = 0U;
  artifact_valid = 1;
  if (lc_pouch_crypto_enabled(pouch->crypto)) {
    artifact_valid = 0;
    rc = lc_pouch_query_index_read_encrypted_artifact_bytes(
        pouch, ns, packed_path, 0, &bytes, &length, present, &artifact_valid,
        error);
  } else {
    rc = lc_pouch_query_index_read_file_bytes(&pouch->allocator, packed_path,
                                              &bytes, &length, present, error);
  }
  if (rc != LC_OK || !*present || !artifact_valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    *valid = artifact_valid;
    return rc;
  }
  entry = (lc_pouch_query_index_packed_cache_entry *)lc_alloc_with_allocator(
      &pouch->allocator, sizeof(*entry));
  if (entry == NULL) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index cache",
                        NULL, NULL, NULL);
  }
  memset(entry, 0, sizeof(*entry));
  entry->path = lc_strdup_with_allocator(&pouch->allocator, packed_path);
  entry->bytes = bytes;
  entry->length = length;
  entry->signature = signature;
  if (entry->path == NULL) {
    lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator, entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index path",
                        NULL, NULL, NULL);
  }
  entry->next = pouch->query_packed_cache;
  pouch->query_packed_cache = entry;
  ++pouch->query_packed_cache_count;
  while (pouch->query_packed_cache_count >
         LC_POUCH_QUERY_INDEX_PACKED_CACHE_MAX) {
    lc_pouch_query_index_packed_cache_entry *victim_prev;
    lc_pouch_query_index_packed_cache_entry *victim;

    victim_prev = NULL;
    victim = pouch->query_packed_cache;
    while (victim != NULL && victim->next != NULL) {
      victim_prev = victim;
      victim = victim->next;
    }
    if (victim == NULL) {
      break;
    }
    if (victim_prev != NULL) {
      victim_prev->next = NULL;
    } else {
      pouch->query_packed_cache = NULL;
    }
    lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator, victim);
    lc_free_with_allocator(&pouch->allocator, victim);
    --pouch->query_packed_cache_count;
  }
  *out = entry;
  *valid = 1;
  return LC_OK;
}

static void lc_pouch_query_index_packed_cache_forget(lc_pouch *pouch,
                                                     const char *packed_path) {
  lc_pouch_query_index_packed_cache_entry *entry;
  lc_pouch_query_index_packed_cache_entry *previous;

  if (pouch == NULL || packed_path == NULL) {
    return;
  }
  previous = NULL;
  entry = pouch->query_packed_cache;
  while (entry != NULL) {
    lc_pouch_query_index_packed_cache_entry *next;

    next = entry->next;
    if (entry->path != NULL && strcmp(entry->path, packed_path) == 0) {
      if (previous != NULL) {
        previous->next = next;
      } else {
        pouch->query_packed_cache = next;
      }
      if (pouch->query_packed_cache_count > 0U) {
        --pouch->query_packed_cache_count;
      }
      lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator, entry);
      lc_free_with_allocator(&pouch->allocator, entry);
      entry = next;
      continue;
    }
    previous = entry;
    entry = next;
  }
}

/* A synchronous flush has just written this immutable generation and still
 * owns its verified plaintext.  Retain that buffer for the immediately
 * following warm instead of decrypting and decompressing the same artifact
 * from disk.  The normal signature check remains in the cache lookup, so a
 * later replacement or a peer writer cannot observe stale bytes. */
static int lc_pouch_query_index_read_packed_component_bytes(
    lc_pouch *pouch, const char *ns, const char *component_path,
    size_t component, char **out_bytes, size_t *out_length, int *present,
    int *valid, lc_error *error) {
  char *packed_path;
  lc_pouch_query_index_packed_cache_entry *packed;
  char *component_bytes;
  size_t header_len;
  size_t offset;
  size_t component_offset;
  size_t index;
  size_t component_length;
  int rc;

  if (pouch == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL || valid == NULL ||
      component >= LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch packed query-index component read requires "
                        "inputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  *valid = 0;
  packed_path = lc_pouch_query_index_packed_path_from_component(
      &pouch->allocator, component_path, component, error);
  if (packed_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  packed = NULL;
  rc = lc_pouch_query_index_packed_cache_get(pouch, ns, packed_path, &packed,
                                             present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    lc_free_with_allocator(&pouch->allocator, packed_path);
    return rc;
  }
  header_len = LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U +
               (LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT * 8U);
  if (packed == NULL || packed->length < header_len ||
      memcmp(packed->bytes, LC_POUCH_QUERY_INDEX_PACKED_MAGIC,
             LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN) != 0 ||
      lc_pouch_query_index_get_u64_le((const unsigned char *)packed->bytes +
                                      LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN) !=
          LC_POUCH_QUERY_INDEX_PACKED_VERSION ||
      lc_pouch_query_index_get_u64_le((const unsigned char *)packed->bytes +
                                      LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN +
                                      8U) !=
          LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT) {
    *valid = 0;
    lc_free_with_allocator(&pouch->allocator, packed_path);
    return LC_OK;
  }
  offset = header_len;
  component_offset = 0U;
  component_length = 0U;
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    uint64_t length64;
    size_t length;

    length64 = lc_pouch_query_index_get_u64_le(
        (const unsigned char *)packed->bytes +
        LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U + (index * 8U));
    if (length64 > (uint64_t)((size_t)-1)) {
      *valid = 0;
      lc_free_with_allocator(&pouch->allocator, packed_path);
      return LC_OK;
    }
    length = (size_t)length64;
    if (offset > packed->length || length > packed->length - offset) {
      *valid = 0;
      lc_free_with_allocator(&pouch->allocator, packed_path);
      return LC_OK;
    }
    if (index == component) {
      component_offset = offset;
      component_length = length;
    }
    offset += length;
  }
  if (offset != packed->length) {
    *valid = 0;
    lc_free_with_allocator(&pouch->allocator, packed_path);
    return LC_OK;
  }
  lc_free_with_allocator(&pouch->allocator, packed_path);
  if (component_offset > packed->length ||
      component_length > packed->length - component_offset) {
    *valid = 0;
    return LC_OK;
  }
  component_bytes =
      (char *)lc_alloc_with_allocator(&pouch->allocator, component_length + 1U);
  if (component_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index "
                        "component",
                        NULL, NULL, NULL);
  }
  if (component_length > 0U) {
    memcpy(component_bytes, packed->bytes + component_offset, component_length);
  }
  component_bytes[component_length] = '\0';
  *out_bytes = component_bytes;
  *out_length = component_length;
  *valid = 1;
  return LC_OK;
}

static int
lc_pouch_query_index_open_temp_artifact(const lc_allocator *allocator,
                                        const char *path, char **temp_path_out,
                                        int *fd_out, lc_error *error) {
  size_t path_length;
  unsigned int attempt;
  int fd;

  if (path == NULL || temp_path_out == NULL || fd_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index temp artifact requires path and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *temp_path_out = NULL;
  *fd_out = -1;
  path_length = strlen(path);
  for (attempt = 0U; attempt < 100U; ++attempt) {
    char suffix[64];
    char *temp_path;
    int written;

    written = snprintf(suffix, sizeof(suffix), ".tmp.%ld.%u", (long)getpid(),
                       attempt);
    if (written < 0 || (size_t)written >= sizeof(suffix) ||
        path_length > (size_t)-1 - (size_t)written - 1U) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index temp artifact path exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    temp_path = (char *)lc_alloc_with_allocator(
        allocator, path_length + (size_t)written + 1U);
    if (temp_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index temp artifact "
                          "path",
                          NULL, NULL, NULL);
    }
    memcpy(temp_path, path, path_length);
    memcpy(temp_path + path_length, suffix, (size_t)written + 1U);
    fd = open(temp_path, O_CREAT | O_EXCL | O_WRONLY, 0666);
    if (fd >= 0) {
      *temp_path_out = temp_path;
      *fd_out = fd;
      return LC_OK;
    }
    lc_free_with_allocator(allocator, temp_path);
    if (errno != EEXIST) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch query-index temp artifact",
                          strerror(errno), NULL, "pouch");
    }
  }
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to allocate a unique pouch query-index temp "
                      "artifact",
                      NULL, NULL, "pouch");
}

static int lc_pouch_query_index_write_artifact_bytes(
    lc_pouch *pouch, const char *ns, const char *path, const char *bytes,
    size_t length, int unpublished_path, lc_error *error) {
  lc_source *source;
  char *context;
  char *descriptor;
  char *temp_path;
  uint64_t plain_bytes;
  uint64_t cipher_bytes;
  int fd;
  int rc;

  if (pouch == NULL || path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index artifact write requires pouch and "
                        "path",
                        NULL, NULL, NULL);
  }
  lc_pouch_query_index_packed_cache_forget(pouch, path);
  if (!lc_pouch_crypto_enabled(pouch->crypto)) {
    if (unpublished_path) {
      return lc_pouch_query_index_write_unpublished_bytes_relaxed(
          path, bytes != NULL ? bytes : "", length, error);
    }
    return lc_pouch_query_index_write_bytes_direct_relaxed(
        path, bytes != NULL ? bytes : "", length, error);
  }
  context =
      lc_pouch_query_index_artifact_context(&pouch->allocator, ns, path, error);
  if (context == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  descriptor = NULL;
  temp_path = NULL;
  fd = -1;
  rc =
      lc_source_from_memory(bytes != NULL ? bytes : "", length, &source, error);
  if (rc == LC_OK && unpublished_path) {
    fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
    if (fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch encrypted query-index artifact",
                        strerror(errno), NULL, "pouch");
    }
  }
  if (rc == LC_OK && !unpublished_path) {
    rc = lc_pouch_query_index_open_temp_artifact(&pouch->allocator, path,
                                                 &temp_path, &fd, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_stream_to_fd_crc_with_compression(
        pouch->crypto, context, fd, source, &plain_bytes, &cipher_bytes, NULL,
        &descriptor, 0, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_append_crypto_footer_fd(fd, descriptor, error);
  }
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch encrypted query-index artifact",
                      strerror(errno), NULL, "pouch");
  }
  if (rc == LC_OK && !unpublished_path && rename(temp_path, path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to install pouch encrypted query-index artifact",
                      strerror(errno), NULL, "pouch");
  }
  if (rc != LC_OK && temp_path != NULL) {
    (void)unlink(temp_path);
  }
  lc_free_with_allocator(&pouch->allocator, temp_path);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  lc_free_with_allocator(&pouch->allocator, context);
  return rc;
}

static void
lc_pouch_query_index_manifest_cleanup(const lc_allocator *allocator,
                                      lc_pouch_query_index_manifest *manifest) {
  size_t index;

  if (manifest == NULL) {
    return;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    lc_free_with_allocator(allocator, manifest->segments[index].id);
  }
  lc_free_with_allocator(allocator, manifest->segments);
  memset(manifest, 0, sizeof(*manifest));
}

static int lc_pouch_query_index_manifest_clone(
    const lc_allocator *allocator, lc_pouch_query_index_manifest *out,
    const lc_pouch_query_index_manifest *source) {
  size_t index;

  if (out == NULL || source == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  out->index_seq = source->index_seq;
  out->present = source->present;
  out->valid = source->valid;
  /* Keep the source's reserved slots. A trusted snapshot is immediately
   * extended on the incremental flush path; preserving its capacity avoids a
   * second allocation and copy before the new segment is published. */
  out->segment_capacity = source->segment_capacity >= source->segment_count
                              ? source->segment_capacity
                              : source->segment_count;
  if (source->segment_count == 0U) {
    return LC_OK;
  }
  out->segments =
      (lc_pouch_query_index_manifest_segment *)lc_calloc_with_allocator(
          allocator, out->segment_capacity, sizeof(out->segments[0]));
  if (out->segments == NULL) {
    return LC_ERR_NOMEM;
  }
  for (index = 0U; index < source->segment_count; ++index) {
    out->segments[index] = source->segments[index];
    out->segments[index].id =
        lc_strdup_with_allocator(allocator, source->segments[index].id);
    if (out->segments[index].id == NULL) {
      lc_pouch_query_index_manifest_cleanup(allocator, out);
      return LC_ERR_NOMEM;
    }
  }
  out->segment_count = source->segment_count;
  return LC_OK;
}

static int
lc_pouch_query_index_manifest_reserve(const lc_allocator *allocator,
                                      lc_pouch_query_index_manifest *manifest,
                                      size_t needed, lc_error *error) {
  lc_pouch_query_index_manifest_segment *next;
  size_t next_capacity;

  if (manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest reserve requires manifest",
                        NULL, NULL, NULL);
  }
  if (needed <= manifest->segment_capacity) {
    return LC_OK;
  }
  next_capacity =
      manifest->segment_capacity == 0U ? 8U : manifest->segment_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index manifest exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (lc_pouch_query_index_manifest_segment *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(next[0]));
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index manifest", NULL,
                        NULL, NULL);
  }
  if (manifest->segments != NULL) {
    memcpy(next, manifest->segments, manifest->segment_count * sizeof(next[0]));
    lc_free_with_allocator(allocator, manifest->segments);
  }
  memset(next + manifest->segment_count, 0,
         (next_capacity - manifest->segment_count) * sizeof(next[0]));
  manifest->segments = next;
  manifest->segment_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_segment_id(lc_pouch_generation index_seq,
                                           char *out, size_t out_size,
                                           lc_error *error) {
  if (out == NULL || out_size == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment id requires output", NULL,
                        NULL, NULL);
  }
  if (lc_u64_format_base10_padded((lc_u64)index_seq, 20U, out, out_size) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment id exceeds local limit",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_manifest_add_front(
    const lc_allocator *allocator, lc_pouch_query_index_manifest *manifest,
    const char *segment_id, lc_pouch_generation base_index_seq,
    lc_pouch_generation index_seq, unsigned long row_count,
    unsigned long row_hash, unsigned long delete_count,
    unsigned long delete_hash, int term_index_complete,
    int presence_index_complete, lc_error *error) {
  lc_pouch_query_index_manifest_segment *segment;
  char *segment_id_copy;
  int rc;

  if (manifest == NULL || segment_id == NULL || segment_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest append requires segment",
                        NULL, NULL, NULL);
  }
  segment_id_copy = lc_strdup_with_allocator(allocator, segment_id);
  if (segment_id_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index manifest "
                        "segment",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_manifest_reserve(
      allocator, manifest, manifest->segment_count + 1U, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, segment_id_copy);
    return rc;
  }
  if (manifest->segment_count > 0U) {
    memmove(&manifest->segments[1], &manifest->segments[0],
            manifest->segment_count * sizeof(manifest->segments[0]));
    memset(&manifest->segments[0], 0, sizeof(manifest->segments[0]));
  }
  segment = &manifest->segments[0];
  segment->id = segment_id_copy;
  segment->base_index_seq = base_index_seq;
  segment->index_seq = index_seq;
  segment->row_count = row_count;
  segment->row_hash = row_hash;
  segment->delete_count = delete_count;
  segment->delete_hash = delete_hash;
  segment->term_index_complete = term_index_complete != 0 ? 1 : 0;
  segment->presence_index_complete = presence_index_complete != 0 ? 1 : 0;
  ++manifest->segment_count;
  manifest->index_seq = index_seq;
  manifest->present = 1;
  manifest->valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_manifest_coherent(
    const lc_pouch_query_index_manifest *manifest) {
  size_t index;

  if (manifest == NULL || manifest->segment_count == 0U) {
    return 0;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    char expected_id[32];

    if (manifest->segments[index].id == NULL ||
        manifest->segments[index].base_index_seq >
            manifest->segments[index].index_seq ||
        (manifest->segments[index].index_seq > 0UL &&
         manifest->segments[index].base_index_seq ==
             manifest->segments[index].index_seq) ||
        lc_pouch_query_index_segment_id(manifest->segments[index].index_seq,
                                        expected_id, sizeof(expected_id),
                                        NULL) != LC_OK ||
        strcmp(manifest->segments[index].id, expected_id) != 0) {
      return 0;
    }
    if (index == 0U) {
      if (manifest->segments[index].index_seq != manifest->index_seq) {
        return 0;
      }
    } else {
      if (manifest->segments[index - 1U].base_index_seq !=
              manifest->segments[index].index_seq ||
          manifest->segments[index - 1U].index_seq <=
              manifest->segments[index].index_seq) {
        return 0;
      }
    }
  }
  return manifest->segments[manifest->segment_count - 1U].base_index_seq == 0UL;
}

static char *lc_pouch_query_index_manifest_next_line(char **cursor) {
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

static char *lc_pouch_query_index_manifest_next_token(char **cursor) {
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

static int lc_pouch_query_index_manifest_parse_u64(char **cursor,
                                                   uint64_t *out) {
  char *token;
  lc_u64 value;

  token = lc_pouch_query_index_manifest_next_token(cursor);
  if (token == NULL || !lc_u64_parse_base10(token, &value)) {
    return 0;
  }
  *out = (uint64_t)value;
  return 1;
}

static int lc_pouch_query_index_manifest_parse_ulong(char **cursor,
                                                     unsigned long *out) {
  char *token;

  token = lc_pouch_query_index_manifest_next_token(cursor);
  return token != NULL && lc_parse_ulong_base10_checked(token, out);
}

static lc_pouch_query_index_manifest_segment *
lc_pouch_query_index_manifest_find_segment(
    lc_pouch_query_index_manifest *manifest, const char *segment_id) {
  size_t index;

  if (manifest == NULL || segment_id == NULL || segment_id[0] == '\0') {
    return NULL;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    if (manifest->segments[index].id != NULL &&
        strcmp(manifest->segments[index].id, segment_id) == 0) {
      return &manifest->segments[index];
    }
  }
  return NULL;
}

static int
lc_pouch_query_index_manifest_read(lc_pouch *pouch, const char *path,
                                   lc_pouch_query_index_manifest *manifest,
                                   lc_error *error) {
  char *bytes;
  char *cursor;
  char *line;
  size_t length;
  unsigned long version;
  int present;
  int rc;

  if (pouch == NULL || path == NULL || manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest read requires pouch, "
                        "path, and manifest",
                        NULL, NULL, NULL);
  }
  memset(manifest, 0, sizeof(*manifest));
  bytes = NULL;
  length = 0U;
  present = 0;
  rc = lc_pouch_query_index_read_file_bytes(&pouch->allocator, path, &bytes,
                                            &length, &present, error);
  if (rc != LC_OK || !present) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    manifest->present = present;
    return rc;
  }
  manifest->present = 1;
  manifest->intentionally_invalidated =
      length == sizeof("invalid\n") - 1U &&
      memcmp(bytes, "invalid\n", sizeof("invalid\n") - 1U) == 0;
  cursor = bytes;
  line = lc_pouch_query_index_manifest_next_line(&cursor);
  if (line == NULL ||
      strcmp(line, "format=" LC_POUCH_QUERY_INDEX_MANIFEST_FORMAT) != 0) {
    goto done;
  }
  line = lc_pouch_query_index_manifest_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_query_index_parse_header_line(line, "version", &version) ||
      version != LC_POUCH_QUERY_INDEX_MANIFEST_VERSION) {
    goto done;
  }
  line = lc_pouch_query_index_manifest_next_line(&cursor);
  if (line == NULL || !lc_pouch_query_index_parse_header_u64(
                          line, "state_index_seq", &manifest->index_seq)) {
    goto done;
  }
  while ((line = lc_pouch_query_index_manifest_next_line(&cursor)) != NULL) {
    char *id;
    lc_pouch_generation base_index_seq;
    lc_pouch_generation index_seq;
    unsigned long row_count;
    unsigned long row_hash;
    unsigned long delete_count;
    unsigned long delete_hash;
    unsigned long term_index_complete;
    unsigned long presence_index_complete;

    if (line[0] == '\0') {
      continue;
    }
    if (strncmp(line, "artifact ", 9U) == 0) {
      lc_pouch_query_index_manifest_segment *segment;
      unsigned long artifact_index;
      uint64_t size;
      unsigned long mtime;
      unsigned long mtime_nsec;
      unsigned long ctime;
      unsigned long ctime_nsec;
      unsigned long inode;

      char *artifact_cursor;

      artifact_cursor = line + 9U;
      id = lc_pouch_query_index_manifest_next_token(&artifact_cursor);
      if (id == NULL || strlen(id) >= 64U ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &artifact_index) ||
          !lc_pouch_query_index_manifest_parse_u64(&artifact_cursor, &size) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &mtime) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &mtime_nsec) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &ctime) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &ctime_nsec) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &inode) ||
          lc_pouch_query_index_manifest_next_token(&artifact_cursor) != NULL ||
          artifact_index >= LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT) {
        goto done;
      }
      segment = lc_pouch_query_index_manifest_find_segment(manifest, id);
      if (segment == NULL) {
        goto done;
      }
      segment->artifact_signatures[artifact_index].size = size;
      segment->artifact_signatures[artifact_index].mtime = mtime;
      segment->artifact_signatures[artifact_index].mtime_nsec = mtime_nsec;
      segment->artifact_signatures[artifact_index].ctime = ctime;
      segment->artifact_signatures[artifact_index].ctime_nsec = ctime_nsec;
      segment->artifact_signatures[artifact_index].inode = inode;
      segment->artifact_signatures[artifact_index].present = 1;
      continue;
    }
    if (strncmp(line, "segment ", 8U) != 0) {
      goto done;
    }
    {
      char *segment_cursor;

      segment_cursor = line + 8U;
      id = lc_pouch_query_index_manifest_next_token(&segment_cursor);
      if (id == NULL || strlen(id) >= 64U ||
          !lc_pouch_query_index_manifest_parse_u64(&segment_cursor,
                                                   &base_index_seq) ||
          !lc_pouch_query_index_manifest_parse_u64(&segment_cursor,
                                                   &index_seq) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &row_count) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &row_hash) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &delete_count) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &delete_hash) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &term_index_complete) ||
          !lc_pouch_query_index_manifest_parse_ulong(
              &segment_cursor, &presence_index_complete) ||
          term_index_complete > 1UL || presence_index_complete > 1UL ||
          lc_pouch_query_index_manifest_next_token(&segment_cursor) != NULL) {
        goto done;
      }
    }
    rc = lc_pouch_query_index_manifest_reserve(
        &pouch->allocator, manifest, manifest->segment_count + 1U, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return rc;
    }
    manifest->segments[manifest->segment_count].id =
        lc_strdup_with_allocator(&pouch->allocator, id);
    if (manifest->segments[manifest->segment_count].id == NULL) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index manifest "
                          "segment",
                          NULL, NULL, NULL);
    }
    manifest->segments[manifest->segment_count].base_index_seq = base_index_seq;
    manifest->segments[manifest->segment_count].index_seq = index_seq;
    manifest->segments[manifest->segment_count].row_count = row_count;
    manifest->segments[manifest->segment_count].row_hash = row_hash;
    manifest->segments[manifest->segment_count].delete_count = delete_count;
    manifest->segments[manifest->segment_count].delete_hash = delete_hash;
    manifest->segments[manifest->segment_count].term_index_complete =
        term_index_complete != 0UL;
    manifest->segments[manifest->segment_count].presence_index_complete =
        presence_index_complete != 0UL;
    ++manifest->segment_count;
  }
  manifest->valid = lc_pouch_query_index_manifest_coherent(manifest);

done:
  lc_free_with_allocator(&pouch->allocator, bytes);
  return LC_OK;
}

static void lc_pouch_query_index_unlink_artifact(lc_pouch *pouch,
                                                 const char *path) {
  char *descriptor_path;

  if (path == NULL) {
    return;
  }
  lc_pouch_query_index_packed_cache_forget(pouch, path);
  (void)unlink(path);
  descriptor_path = lc_pouch_query_index_crypto_descriptor_path(
      &pouch->allocator, path, NULL);
  if (descriptor_path != NULL) {
    (void)unlink(descriptor_path);
    lc_free_with_allocator(&pouch->allocator, descriptor_path);
  }
}

static int lc_pouch_query_index_manifest_has_segment(
    const lc_pouch_query_index_manifest *manifest, const char *segment_id) {
  size_t index;

  if (manifest == NULL || segment_id == NULL || segment_id[0] == '\0') {
    return 0;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    if (manifest->segments[index].id != NULL &&
        strcmp(manifest->segments[index].id, segment_id) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_artifact_segment_id(const char *leaf,
                                                    char *segment_id,
                                                    size_t segment_id_size) {
  size_t index;
  size_t leaf_length;

  if (leaf == NULL || segment_id == NULL || segment_id_size < 21U ||
      strncmp(leaf, "query.", 6U) != 0) {
    return 0;
  }
  leaf_length = strlen(leaf);
  if (leaf_length <= 26U) {
    return 0;
  }
  for (index = 0U; index < 20U; ++index) {
    char ch;

    ch = leaf[6U + index];
    if (ch < '0' || ch > '9') {
      return 0;
    }
    segment_id[index] = ch;
  }
  if (leaf[26U] != '.') {
    return 0;
  }
  segment_id[20U] = '\0';
  return 1;
}

static int lc_pouch_query_index_unlink_unreferenced_artifacts(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_query_index_manifest *keep_manifest, lc_error *error) {
  char *namespace_path;
  char *index_path;
  DIR *dir;
  struct dirent *entry;
  int rc;

  if (pouch == NULL || ns == NULL || keep_manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index orphan cleanup requires pouch, "
                        "namespace, and manifest",
                        NULL, NULL, NULL);
  }
  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path, ns);
  if (namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index path", NULL, NULL,
                        NULL);
  }
  dir = opendir(index_path);
  if (dir == NULL) {
    rc = errno == ENOENT ? LC_OK
                         : lc_error_set(error, LC_ERR_TRANSPORT, errno,
                                        "failed to open pouch query-index "
                                        "directory for cleanup",
                                        index_path, NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, index_path);
    return rc;
  }
  rc = LC_OK;
  while ((entry = readdir(dir)) != NULL) {
    char segment_id[21];
    char *path;
    int unlink_entry;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        strcmp(entry->d_name, LC_POUCH_QUERY_INDEX_MANIFEST_LEAF) == 0) {
      continue;
    }
    unlink_entry = 0;
    if (lc_pouch_query_index_artifact_segment_id(entry->d_name, segment_id,
                                                 sizeof(segment_id))) {
      unlink_entry =
          !lc_pouch_query_index_manifest_has_segment(keep_manifest, segment_id);
    } else if (strcmp(entry->d_name, LC_POUCH_QUERY_INDEX_LEAF) == 0 ||
               strncmp(entry->d_name, LC_POUCH_QUERY_INDEX_LEAF ".",
                       strlen(LC_POUCH_QUERY_INDEX_LEAF) + 1U) == 0) {
      unlink_entry = 1;
    }
    if (!unlink_entry) {
      continue;
    }
    path = lc_pouch_path_join(&pouch->allocator, index_path, entry->d_name);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index cleanup path",
                        NULL, NULL, NULL);
      break;
    }
    lc_pouch_query_index_unlink_artifact(pouch, path);
    lc_free_with_allocator(&pouch->allocator, path);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, errno,
                      "failed to close pouch query-index cleanup directory",
                      index_path, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, index_path);
  return rc;
}

static void lc_pouch_query_index_manifest_unlink_segments(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_query_index_manifest *manifest,
    const lc_pouch_query_index_manifest *keep_manifest) {
  size_t segment_index;

  if (pouch == NULL || ns == NULL || manifest == NULL) {
    return;
  }
  for (segment_index = 0U; segment_index < manifest->segment_count;
       ++segment_index) {
    char *packed_path;
    lc_error ignored;
    int keep;

    keep = lc_pouch_query_index_manifest_has_segment(
        keep_manifest, manifest->segments[segment_index].id);
    if (keep) {
      continue;
    }
    packed_path = NULL;
    lc_error_init(&ignored);
    packed_path = lc_pouch_query_index_segment_artifact_path(
        pouch, ns, manifest->segments[segment_index].id,
        LC_POUCH_QUERY_INDEX_PACKED_LEAF, &ignored);
    if (packed_path != NULL) {
      lc_pouch_query_index_unlink_artifact(pouch, packed_path);
    }
    lc_error_cleanup(&ignored);
    lc_free_with_allocator(&pouch->allocator, packed_path);
  }
}

static int lc_pouch_query_index_manifest_write(
    lc_pouch *pouch, const char *path,
    const lc_pouch_query_index_manifest *manifest, lc_error *error) {
  lc_pouch_query_index_text text;
  size_t index;
  int rc;

  if (pouch == NULL || path == NULL || manifest == NULL || !manifest->valid) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest write requires valid "
                        "manifest",
                        NULL, NULL, NULL);
  }
  memset(&text, 0, sizeof(text));
  text.allocator = &pouch->allocator;
  rc = lc_pouch_query_index_text_append_cstr(
      &text, "format=" LC_POUCH_QUERY_INDEX_MANIFEST_FORMAT "\n", NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &text, "version", LC_POUCH_QUERY_INDEX_MANIFEST_VERSION, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_u64_line(&text, "state_index_seq",
                                                   manifest->index_seq, error);
  }
  for (index = 0U; rc == LC_OK && index < manifest->segment_count; ++index) {
    char line[192];
    char base_index_seq_text[32];
    char index_seq_text[32];
    size_t artifact_index;
    int written;

    if (lc_u64_format_base10((lc_u64)manifest->segments[index].base_index_seq,
                             base_index_seq_text,
                             sizeof(base_index_seq_text)) < 0 ||
        lc_u64_format_base10((lc_u64)manifest->segments[index].index_seq,
                             index_seq_text, sizeof(index_seq_text)) < 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest segment exceeds local "
                        "limit",
                        NULL, NULL, NULL);
      break;
    }
    written = snprintf(
        line, sizeof(line), "segment %s %s %s %lu %lu %lu %lu %d %d\n",
        manifest->segments[index].id, base_index_seq_text, index_seq_text,
        manifest->segments[index].row_count, manifest->segments[index].row_hash,
        manifest->segments[index].delete_count,
        manifest->segments[index].delete_hash,
        manifest->segments[index].term_index_complete ? 1 : 0,
        manifest->segments[index].presence_index_complete ? 1 : 0);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest segment exceeds local "
                        "limit",
                        NULL, NULL, NULL);
    } else {
      rc = lc_pouch_query_index_text_append(&text, line, (size_t)written, NULL,
                                            error);
    }
    for (artifact_index = 0U;
         rc == LC_OK &&
         artifact_index < LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT;
         ++artifact_index) {
      const lc_pouch_query_index_manifest_artifact_signature *signature;
      char artifact_line[256];
      char size_text[32];

      signature =
          &manifest->segments[index].artifact_signatures[artifact_index];
      if (!signature->present) {
        continue;
      }
      if (lc_u64_format_base10((lc_u64)signature->size, size_text,
                               sizeof(size_text)) < 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index manifest artifact exceeds local "
                          "limit",
                          NULL, NULL, NULL);
        break;
      }
      written =
          snprintf(artifact_line, sizeof(artifact_line),
                   "artifact %s %lu %s %lu %lu %lu %lu %lu\n",
                   manifest->segments[index].id, (unsigned long)artifact_index,
                   size_text, signature->mtime, signature->mtime_nsec,
                   signature->ctime, signature->ctime_nsec, signature->inode);
      if (written < 0 || (size_t)written >= sizeof(artifact_line)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index manifest artifact exceeds local "
                          "limit",
                          NULL, NULL, NULL);
      } else {
        rc = lc_pouch_query_index_text_append(&text, artifact_line,
                                              (size_t)written, NULL, error);
      }
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_write_text_file_relaxed(
        path, text.bytes != NULL ? text.bytes : "", error);
  }
  lc_free_with_allocator(&pouch->allocator, text.bytes);
  return rc;
}

static void lc_pouch_query_index_key_hex_set_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_key_hex_set *set) {
  size_t index;

  if (set == NULL) {
    return;
  }
  for (index = 0U; index < set->count; ++index) {
    lc_free_with_allocator(allocator, set->items[index]);
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_key_hex_set_find(
    const lc_pouch_query_index_key_hex_set *set, const char *key_hex,
    size_t *position_out) {
  size_t low;
  size_t high;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (set == NULL || key_hex == NULL) {
    return 0;
  }
  low = 0U;
  high = set->count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(key_hex, set->items[mid]);
    if (cmp == 0) {
      if (position_out != NULL) {
        *position_out = mid;
      }
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return 0;
}

static int
lc_pouch_query_index_key_hex_set_add(const lc_allocator *allocator,
                                     lc_pouch_query_index_key_hex_set *set,
                                     const char *key_hex, lc_error *error) {
  char **next;
  size_t position;
  size_t next_capacity;

  if (set == NULL || key_hex == NULL || key_hex[0] == '\0') {
    return LC_OK;
  }
  if (lc_pouch_query_index_key_hex_set_find(set, key_hex, &position)) {
    return LC_OK;
  }
  if (set->count == set->capacity) {
    next_capacity = set->capacity == 0U ? 32U : set->capacity * 2U;
    next = (char **)lc_alloc_with_allocator(allocator,
                                            next_capacity * sizeof(next[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index key set", NULL,
                          NULL, NULL);
    }
    if (set->items != NULL) {
      memcpy(next, set->items, set->count * sizeof(next[0]));
      lc_free_with_allocator(allocator, set->items);
    }
    set->items = next;
    set->capacity = next_capacity;
  }
  if (position < set->count) {
    memmove(&set->items[position + 1U], &set->items[position],
            (set->count - position) * sizeof(set->items[0]));
  }
  set->items[position] = lc_strdup_with_allocator(allocator, key_hex);
  if (set->items[position] == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index key", NULL, NULL,
                        NULL);
  }
  ++set->count;
  return LC_OK;
}

static void
lc_pouch_query_index_key_hex_set_remove(const lc_allocator *allocator,
                                        lc_pouch_query_index_key_hex_set *set,
                                        const char *key_hex) {
  size_t position;

  if (set == NULL || key_hex == NULL ||
      !lc_pouch_query_index_key_hex_set_find(set, key_hex, &position)) {
    return;
  }
  lc_free_with_allocator(allocator, set->items[position]);
  if (position + 1U < set->count) {
    memmove(&set->items[position], &set->items[position + 1U],
            (set->count - position - 1U) * sizeof(set->items[0]));
  }
  --set->count;
}

static int lc_pouch_query_index_encode_delete_keys(
    lc_pouch_query_index_incremental_change *changes, size_t change_count,
    lc_pouch_query_index_text *out, const lc_allocator *allocator,
    unsigned long *delete_count, unsigned long *delete_hash, lc_error *error) {
  size_t index;
  int rc;

  if (out == NULL || delete_count == NULL || delete_hash == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index delete encode requires outputs",
                        NULL, NULL, NULL);
  }
  *delete_count = 0UL;
  *delete_hash = lc_pouch_query_index_hash_init();
  memset(out, 0, sizeof(*out));
  out->allocator = allocator;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < change_count; ++index) {
    if (changes[index].found || changes[index].key_hex == NULL) {
      continue;
    }
    rc = lc_pouch_query_index_text_append_cstr(out, changes[index].key_hex,
                                               NULL, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(out, "\n", NULL, error);
    }
    if (rc == LC_OK) {
      ++*delete_count;
    }
  }
  if (rc == LC_OK && out->bytes != NULL) {
    lc_pouch_query_index_hash_bytes(delete_hash, out->bytes, out->length);
  }
  return rc;
}

static int lc_pouch_query_index_encode_delete_set(
    const lc_pouch_query_index_key_hex_set *set, lc_pouch_query_index_text *out,
    const lc_allocator *allocator, unsigned long *delete_count,
    unsigned long *delete_hash, lc_error *error) {
  size_t index;
  int rc;

  if (out == NULL || delete_count == NULL || delete_hash == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index delete encode requires outputs",
                        NULL, NULL, NULL);
  }
  *delete_count = 0UL;
  *delete_hash = lc_pouch_query_index_hash_init();
  memset(out, 0, sizeof(*out));
  out->allocator = allocator;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && set != NULL && index < set->count; ++index) {
    rc = lc_pouch_query_index_text_append_cstr(out, set->items[index], NULL,
                                               error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(out, "\n", NULL, error);
    }
    if (rc == LC_OK) {
      ++*delete_count;
    }
  }
  if (rc == LC_OK && out->bytes != NULL) {
    lc_pouch_query_index_hash_bytes(delete_hash, out->bytes, out->length);
  }
  return rc;
}

static void
lc_pouch_query_index_summary_remove_key(lc_pouch_query_index_summary *summary,
                                        const char *key, const char *key_hex) {
  size_t read_index;
  size_t write_index;

  if (summary == NULL || key == NULL || key_hex == NULL) {
    return;
  }
  write_index = 0U;
  for (read_index = 0U; read_index < summary->count; ++read_index) {
    if (strcmp(summary->rows[read_index].key, key) == 0) {
      lc_pouch_query_index_summary_row_cleanup(summary,
                                               &summary->rows[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->rows[write_index] = summary->rows[read_index];
      memset(&summary->rows[read_index], 0, sizeof(summary->rows[read_index]));
    }
    ++write_index;
  }
  summary->count = write_index;
  write_index = 0U;
  for (read_index = 0U; read_index < summary->term_count; ++read_index) {
    if (strcmp(summary->terms[read_index].key_hex, key_hex) == 0) {
      lc_pouch_query_index_summary_term_cleanup(summary,
                                                &summary->terms[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->terms[write_index] = summary->terms[read_index];
      memset(&summary->terms[read_index], 0,
             sizeof(summary->terms[read_index]));
    }
    ++write_index;
  }
  summary->term_count = write_index;
  write_index = 0U;
  for (read_index = 0U; read_index < summary->presence_count; ++read_index) {
    if (strcmp(summary->presences[read_index].key_hex, key_hex) == 0) {
      lc_pouch_query_index_summary_presence_cleanup(
          summary, &summary->presences[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->presences[write_index] = summary->presences[read_index];
      memset(&summary->presences[read_index], 0,
             sizeof(summary->presences[read_index]));
    }
    ++write_index;
  }
  summary->presence_count = write_index;
}

static int lc_pouch_query_index_summary_merge_move(
    lc_pouch_query_index_summary *destination,
    lc_pouch_query_index_summary *source, lc_error *error) {
  size_t index;
  int rc;

  if (destination == NULL || source == NULL ||
      destination->allocator != source->allocator) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index pending summary is incompatible",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_summary_reserve(
      destination, destination->count + source->count, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_term_reserve(
        destination, destination->term_count + source->term_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_reserve(
        destination, destination->presence_count + source->presence_count,
        error);
  }
  if (rc != LC_OK) {
    return rc;
  }
  for (index = 0U; index < source->count; ++index) {
    destination->rows[destination->count++] = source->rows[index];
    memset(&source->rows[index], 0, sizeof(source->rows[index]));
  }
  for (index = 0U; index < source->term_count; ++index) {
    destination->terms[destination->term_count++] = source->terms[index];
    memset(&source->terms[index], 0, sizeof(source->terms[index]));
  }
  for (index = 0U; index < source->presence_count; ++index) {
    destination->presences[destination->presence_count++] =
        source->presences[index];
    memset(&source->presences[index], 0, sizeof(source->presences[index]));
  }
  source->count = 0U;
  source->term_count = 0U;
  source->presence_count = 0U;
  return LC_OK;
}

/* Pending summaries retain metadata and derived terms, never document bodies.
 * Their retention budget must therefore follow the retained term cardinality,
 * not the aggregate size of source values that have already been discarded. */
static int lc_pouch_query_index_summary_within_pending_limit(
    const lc_pouch_query_index_summary *summary) {
  return summary != NULL &&
         summary->term_count <= LC_POUCH_QUERY_INDEX_PENDING_TERMS_MAX;
}

static void lc_pouch_query_index_pending_entry_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_pending_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->ns);
  lc_pouch_query_index_memtable_cleanup(allocator, entry->memtable);
  lc_free_with_allocator(allocator, entry->memtable);
  lc_pouch_query_index_summary_cleanup(&entry->summary);
  lc_pouch_query_index_key_hex_set_cleanup(allocator, &entry->deletes);
  lc_pouch_query_index_key_hex_set_cleanup(allocator, &entry->incomplete_keys);
  memset(entry, 0, sizeof(*entry));
}

static lc_pouch_query_index_pending_entry *lc_pouch_query_index_pending_find(
    lc_pouch *pouch, const char *ns,
    lc_pouch_query_index_pending_entry **previous) {
  lc_pouch_query_index_pending_entry *entry;
  lc_pouch_query_index_pending_entry *prior;

  if (previous != NULL) {
    *previous = NULL;
  }
  if (pouch == NULL || ns == NULL) {
    return NULL;
  }
  prior = NULL;
  for (entry = pouch->query_pending_index; entry != NULL; entry = entry->next) {
    if (entry->ns != NULL && strcmp(entry->ns, ns) == 0) {
      if (previous != NULL) {
        *previous = prior;
      }
      return entry;
    }
    prior = entry;
  }
  return NULL;
}

static void lc_pouch_query_index_pending_remove_locked(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *entry,
    lc_pouch_query_index_pending_entry *previous) {
  if (pouch == NULL || entry == NULL) {
    return;
  }
  if (previous != NULL) {
    previous->next = entry->next;
  } else {
    pouch->query_pending_index = entry->next;
  }
  lc_pouch_query_index_pending_entry_cleanup(&pouch->allocator, entry);
  lc_free_with_allocator(&pouch->allocator, entry);
}

static lc_pouch_query_index_pending_entry *
lc_pouch_query_index_pending_create(lc_pouch *pouch, const char *ns,
                                    lc_pouch_generation base_index_seq,
                                    int covers_namespace) {
  lc_pouch_query_index_pending_entry *entry;

  entry = (lc_pouch_query_index_pending_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return NULL;
  }
  entry->ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (entry->ns == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    return NULL;
  }
  entry->base_index_seq = base_index_seq;
  entry->last_index_seq = base_index_seq;
  entry->covers_namespace = covers_namespace != 0 ? 1 : 0;
  entry->summary.allocator = &pouch->allocator;
  entry->summary.pouch = pouch;
  entry->summary.ns = entry->ns;
  entry->summary.term_index_complete = 1;
  entry->summary.presence_index_complete = 1;
  entry->next = pouch->query_pending_index;
  pouch->query_pending_index = entry;
  return entry;
}

/* Keep an overflowed capture as a lightweight marker. Removing it would let a
 * later suffix look complete relative to the same manifest generation. Active
 * keys are operation lifecycle state, so an incomplete derived capture must
 * retain them until its lease or transaction reaches completion. */
static void lc_pouch_query_index_pending_invalidate_locked(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *pending,
    lc_pouch_generation index_seq) {
  if (pouch == NULL || pending == NULL) {
    return;
  }
  lc_pouch_query_index_summary_cleanup(&pending->summary);
  lc_pouch_query_index_key_hex_set_cleanup(&pouch->allocator,
                                           &pending->deletes);
  lc_pouch_query_index_memtable_cleanup(&pouch->allocator, pending->memtable);
  lc_free_with_allocator(&pouch->allocator, pending->memtable);
  pending->memtable = NULL;
  pending->covers_namespace = 0;
  pending->incomplete = 1;
  if (index_seq > pending->last_index_seq) {
    pending->last_index_seq = index_seq;
  }
}

static void lc_pouch_query_index_pending_add_incomplete_key_locked(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *pending,
    const char *key) {
  char *key_hex;
  lc_error ignored;

  if (pouch == NULL || pending == NULL || key == NULL || key[0] == '\0') {
    return;
  }
  key_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, key);
  if (key_hex == NULL) {
    return;
  }
  lc_error_init(&ignored);
  (void)lc_pouch_query_index_key_hex_set_add(
      &pouch->allocator, &pending->incomplete_keys, key_hex, &ignored);
  lc_error_cleanup(&ignored);
  lc_free_with_allocator(&pouch->allocator, key_hex);
}

static lc_pouch_unix_seconds lc_pouch_query_index_now_unix(void) {
  time_t now;

  now = time(NULL);
  return now > 0 ? (lc_pouch_unix_seconds)now : 0L;
}

static int lc_pouch_query_index_active_operation_is_expired(
    const lc_pouch_query_index_active_operation *operation,
    lc_pouch_unix_seconds now_unix) {
  return operation != NULL && operation->expires_at_unix > 0L &&
         now_unix > 0L && operation->expires_at_unix <= now_unix;
}

static void
lc_pouch_query_index_active_operations_prune_locked(lc_pouch *pouch) {
  lc_pouch_query_index_active_operation **link;
  lc_pouch_unix_seconds now_unix;

  if (pouch == NULL) {
    return;
  }
  now_unix = lc_pouch_query_index_now_unix();
  if (now_unix <= 0L) {
    return;
  }
  link = &pouch->query_active_operations;
  while (*link != NULL) {
    lc_pouch_query_index_active_operation *operation;

    operation = *link;
    if (!lc_pouch_query_index_active_operation_is_expired(operation,
                                                          now_unix)) {
      link = &operation->next;
      continue;
    }
    *link = operation->next;
    lc_free_with_allocator(&pouch->allocator, operation->ns);
    lc_free_with_allocator(&pouch->allocator, operation->key);
    lc_free_with_allocator(&pouch->allocator, operation->operation_id);
    lc_free_with_allocator(&pouch->allocator, operation);
  }
}

static int lc_pouch_query_index_active_operation_add_locked(
    lc_pouch *pouch, const char *ns, const char *key, const char *operation_id,
    lc_pouch_unix_seconds expires_at_unix, int *started_out, lc_error *error) {
  lc_pouch_query_index_active_operation *operation;

  if (started_out != NULL) {
    *started_out = 0;
  }
  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || operation_id == NULL || operation_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index operation guard requires key and "
                        "operation id",
                        NULL, NULL, "pouch");
  }
  lc_pouch_query_index_active_operations_prune_locked(pouch);
  for (operation = pouch->query_active_operations; operation != NULL;
       operation = operation->next) {
    if (strcmp(operation->ns, ns) == 0 && strcmp(operation->key, key) == 0 &&
        strcmp(operation->operation_id, operation_id) == 0) {
      operation->expires_at_unix = expires_at_unix;
      return LC_OK;
    }
  }
  operation = (lc_pouch_query_index_active_operation *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*operation));
  if (operation == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index operation guard",
                        NULL, NULL, "pouch");
  }
  operation->operation_id =
      lc_strdup_with_allocator(&pouch->allocator, operation_id);
  operation->ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  operation->key = lc_strdup_with_allocator(&pouch->allocator, key);
  if (operation->operation_id == NULL || operation->ns == NULL ||
      operation->key == NULL) {
    lc_free_with_allocator(&pouch->allocator, operation->ns);
    lc_free_with_allocator(&pouch->allocator, operation->operation_id);
    lc_free_with_allocator(&pouch->allocator, operation->key);
    lc_free_with_allocator(&pouch->allocator, operation);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index operation id",
                        NULL, NULL, "pouch");
  }
  operation->expires_at_unix = expires_at_unix;
  operation->next = pouch->query_active_operations;
  pouch->query_active_operations = operation;
  if (started_out != NULL) {
    *started_out = 1;
  }
  return LC_OK;
}

static int lc_pouch_query_index_active_operation_remove_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    const char *operation_id) {
  lc_pouch_query_index_active_operation **link;

  if (pouch == NULL || ns == NULL || key == NULL || key[0] == '\0' ||
      operation_id == NULL || operation_id[0] == '\0') {
    return 0;
  }
  lc_pouch_query_index_active_operations_prune_locked(pouch);
  link = &pouch->query_active_operations;
  while (*link != NULL) {
    lc_pouch_query_index_active_operation *operation;

    operation = *link;
    if (strcmp(operation->ns, ns) != 0 || strcmp(operation->key, key) != 0 ||
        strcmp(operation->operation_id, operation_id) != 0) {
      link = &operation->next;
      continue;
    }
    *link = operation->next;
    lc_free_with_allocator(&pouch->allocator, operation->ns);
    lc_free_with_allocator(&pouch->allocator, operation->key);
    lc_free_with_allocator(&pouch->allocator, operation->operation_id);
    lc_free_with_allocator(&pouch->allocator, operation);
    return 1;
  }
  return 0;
}

static int lc_pouch_query_index_active_operation_refresh_locked(
    lc_pouch *pouch, const char *ns, const char *key, const char *operation_id,
    lc_pouch_unix_seconds expires_at_unix) {
  lc_pouch_query_index_active_operation *operation;

  if (pouch == NULL || ns == NULL || key == NULL || operation_id == NULL ||
      operation_id[0] == '\0') {
    return 0;
  }
  lc_pouch_query_index_active_operations_prune_locked(pouch);
  for (operation = pouch->query_active_operations; operation != NULL;
       operation = operation->next) {
    if (strcmp(operation->ns, ns) == 0 && strcmp(operation->key, key) == 0 &&
        strcmp(operation->operation_id, operation_id) == 0) {
      operation->expires_at_unix = expires_at_unix;
      return 1;
    }
  }
  return 0;
}

static int
lc_pouch_query_index_pending_has_active_operations_locked(lc_pouch *pouch,
                                                          const char *ns) {
  lc_pouch_query_index_active_operation *operation;

  if (pouch == NULL || ns == NULL) {
    return 0;
  }
  lc_pouch_query_index_active_operations_prune_locked(pouch);
  for (operation = pouch->query_active_operations; operation != NULL;
       operation = operation->next) {
    if (strcmp(operation->ns, ns) == 0) {
      return 1;
    }
  }
  return 0;
}

/* A threshold publication must not fall back to a live-state rebuild while a
 * lease or transaction is still staging a logical value.  Pending batches are
 * an optimization and may legitimately be absent for private staging writes,
 * so the publication boundary is the operation guard itself. */
static int
lc_pouch_query_index_namespace_has_active_operations(lc_pouch *pouch,
                                                     const char *ns) {
  int active;

  if (pouch == NULL || ns == NULL || !pouch->indexer_mutex_initialized) {
    return 0;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  active = lc_pouch_query_index_pending_has_active_operations_locked(pouch, ns);
  pthread_mutex_unlock(&pouch->indexer_mutex);
  return active;
}

static void lc_pouch_query_index_pending_mark_incomplete(
    lc_pouch *pouch, const char *ns, lc_pouch_generation index_seq,
    const char *key, int operation_active) {
  lc_pouch_query_index_pending_entry *pending;
  lc_pouch_query_index_pending_entry *previous;
  lc_pouch_generation base_index_seq = 0UL;
  lc_error error;
  int rc;

  if (pouch == NULL || !pouch->indexer_mutex_initialized || ns == NULL) {
    return;
  }
  (void)operation_active;
  base_index_seq = 0UL;
  lc_error_init(&error);
  rc = lc_pouch_query_index_manifest_seq(pouch, ns, &base_index_seq, &error);
  lc_error_cleanup(&error);
  if (rc != LC_OK) {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  if (pending == NULL) {
    pending = lc_pouch_query_index_pending_create(pouch, ns, base_index_seq, 0);
  }
  if (pending != NULL) {
    lc_pouch_query_index_pending_invalidate_locked(pouch, pending, index_seq);
    lc_pouch_query_index_pending_add_incomplete_key_locked(pouch, pending, key);
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
}

int lc_pouch_query_index_operation_begin(lc_pouch *pouch, const char *ns,
                                         const char *key,
                                         const char *operation_id,
                                         lc_pouch_unix_seconds expires_at_unix,
                                         int *started_out, lc_error *error) {
  int rc;

  if (started_out != NULL) {
    *started_out = 0;
  }
  if (pouch == NULL || ns == NULL || ns[0] == '\0' || key == NULL ||
      key[0] == '\0' || !pouch->query_indexing_enabled ||
      !lc_pouch_single_writer_enabled(pouch) ||
      lc_pouch_query_index_key_is_staged(key)) {
    return LC_OK;
  }
  if (operation_id == NULL || operation_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active query-index operation requires an id",
                        NULL, NULL, "pouch");
  }
  if (!pouch->indexer_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active query-index operation requires an "
                        "indexer",
                        NULL, NULL, "pouch");
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  rc = lc_pouch_query_index_active_operation_add_locked(
      pouch, ns, key, operation_id, expires_at_unix, started_out, error);
  pthread_mutex_unlock(&pouch->indexer_mutex);
  return rc;
}

void lc_pouch_query_index_operation_cancel(lc_pouch *pouch, const char *ns,
                                           const char *key,
                                           const char *operation_id) {
  if (pouch == NULL || ns == NULL || key == NULL || operation_id == NULL ||
      !pouch->indexer_mutex_initialized) {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  (void)lc_pouch_query_index_active_operation_remove_locked(pouch, ns, key,
                                                            operation_id);
  pthread_mutex_unlock(&pouch->indexer_mutex);
}

void lc_pouch_query_index_operation_refresh(
    lc_pouch *pouch, const char *ns, const char *key, const char *operation_id,
    lc_pouch_unix_seconds expires_at_unix) {
  if (pouch == NULL || ns == NULL || key == NULL || operation_id == NULL ||
      !pouch->indexer_mutex_initialized) {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  (void)lc_pouch_query_index_active_operation_refresh_locked(
      pouch, ns, key, operation_id, expires_at_unix);
  pthread_cond_signal(&pouch->indexer_cond);
  pthread_mutex_unlock(&pouch->indexer_mutex);
}

lc_pouch_unix_seconds
lc_pouch_query_index_next_operation_expiry_locked(lc_pouch *pouch,
                                                  const char *ns) {
  lc_pouch_query_index_active_operation *operation;
  lc_pouch_unix_seconds earliest;

  if (pouch == NULL || ns == NULL) {
    return 0L;
  }
  earliest = 0L;
  lc_pouch_query_index_active_operations_prune_locked(pouch);
  for (operation = pouch->query_active_operations; operation != NULL;
       operation = operation->next) {
    if (strcmp(operation->ns, ns) == 0 && operation->expires_at_unix > 0L &&
        (earliest == 0L || operation->expires_at_unix < earliest)) {
      earliest = operation->expires_at_unix;
    }
  }
  return earliest;
}

static int
lc_pouch_query_index_pending_seed(lc_pouch *pouch, const char *ns,
                                  size_t captured_row_count,
                                  lc_pouch_generation *base_index_seq,
                                  int *covers_namespace, lc_error *error) {
  size_t visible_count;
  int rc;

  if (pouch == NULL || ns == NULL || base_index_seq == NULL ||
      covers_namespace == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index pending seed requires pouch, "
                        "namespace, and outputs",
                        NULL, NULL, NULL);
  }
  *base_index_seq = 0UL;
  *covers_namespace = 0;
  rc = lc_pouch_query_index_manifest_seq(pouch, ns, base_index_seq, error);
  if (rc != LC_OK) {
    return rc;
  }
  /* A nonzero manifest sequence starts an incremental segment.  The pending
   * capture is still valid; it simply covers the durable tail rather than the
   * whole namespace.  Treating this as a zero base forces every later flush
   * to discard its body-free memtable and replay the payloads it just indexed.
   */
  if (*base_index_seq != 0UL) {
    return LC_OK;
  }
  visible_count = 0U;
  rc = lc_pouch_state_visible_count(pouch, ns, &visible_count, error);
  if (rc == LC_OK && visible_count == captured_row_count) {
    *covers_namespace = 1;
  }
  return rc;
}

static int lc_pouch_query_index_pending_prepare_write(
    lc_pouch *pouch, const char *ns, const char *key, const char *content_type,
    lc_source *body, const lc_pouch_state_write_result *result,
    lc_pouch_query_index_summary *summary, int *deferred_out, lc_error *error) {
  lc_pouch_state_visit_entry entry;
  lc_pouch_query_index_row *row;
  int rc;

  if (pouch == NULL || ns == NULL || key == NULL || result == NULL ||
      result->index_seq == 0UL || summary == NULL || deferred_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index pending write is not replayable",
                        NULL, NULL, NULL);
  }
  /* A staged promotion has already finalized a durable payload span, but
   * deliberately has no caller-owned source to replay here. Treat that like a
   * bounded large-body capture: the foreground flush extracts it from state
   * after the transaction decision rather than invalidating the whole pending
   * batch and falling back to the background worker. */
  *deferred_out =
      body == NULL || body->reset == NULL ||
      result->bytes > LC_POUCH_QUERY_INDEX_FOREGROUND_CAPTURE_BYTES_MAX;
  if (!*deferred_out &&
      result->bytes > LC_POUCH_QUERY_INDEX_PENDING_SOURCE_BYTES_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index pending write is not replayable",
                        NULL, NULL, NULL);
  }
  memset(summary, 0, sizeof(*summary));
  summary->allocator = &pouch->allocator;
  summary->pouch = pouch;
  summary->ns = ns;
  summary->term_index_complete = 1;
  summary->presence_index_complete = 1;
  memset(&entry, 0, sizeof(entry));
  entry.key = key;
  entry.content_type = content_type;
  entry.etag = result->etag;
  entry.version = result->version;
  entry.bytes = result->bytes;
  entry.cipher_bytes = result->cipher_bytes;
  entry.descriptor = result->descriptor;
  entry.updated_at_unix = result->updated_at_unix;
  entry.has_query_hidden = result->has_query_hidden;
  entry.query_hidden = result->query_hidden;
  entry.has_payload = 1;
  rc = lc_pouch_query_index_summary_visit(&entry, summary, error);
  if (rc != LC_OK || summary->count != 1U) {
    return rc != LC_OK
               ? rc
               : lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch query-index pending row is missing", NULL,
                              NULL, NULL);
  }
  row = &summary->rows[0];
  if (*deferred_out) {
    row->needs_extraction = 1;
    return LC_OK;
  }
  rc = lc_pouch_query_index_extract_terms_from_body(summary, key, row, body,
                                                    error);
  if (rc == LC_OK &&
      (!summary->term_index_complete || !summary->presence_index_complete)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index pending body is incomplete", NULL,
                      NULL, NULL);
  }
  return rc;
}

static void lc_pouch_query_index_pending_note_write(
    lc_pouch *pouch, const char *ns, const char *key, const char *content_type,
    lc_source *body, const lc_pouch_state_write_result *result,
    int operation_active) {
  lc_pouch_query_index_summary update;
  lc_pouch_query_index_pending_entry *pending;
  lc_pouch_query_index_pending_entry *previous;
  lc_pouch_query_index_row *previous_row;
  lc_pouch_generation base_index_seq;
  char *key_hex;
  lc_error error;
  int covers_namespace = 0;
  int deferred;
  int need_seed;
  int rc;

  if (pouch == NULL || !lc_pouch_single_writer_enabled(pouch) ||
      !pouch->indexer_mutex_initialized || ns == NULL || key == NULL ||
      result == NULL || result->index_seq == 0UL) {
    return;
  }
  (void)operation_active;
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  if (pending != NULL && pending->incomplete) {
    if (result->index_seq > pending->last_index_seq) {
      pending->last_index_seq = result->index_seq;
    }
    pthread_mutex_unlock(&pouch->indexer_mutex);
    lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                 key, operation_active);
    return;
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  memset(&update, 0, sizeof(update));
  deferred = 0;
  lc_error_init(&error);
  rc = lc_pouch_query_index_pending_prepare_write(
      pouch, ns, key, content_type, body, result, &update, &deferred, &error);
  if (rc != LC_OK) {
    lc_pouch_query_index_summary_cleanup(&update);
    lc_error_cleanup(&error);
    lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                 key, operation_active);
    return;
  }
  key_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, key);
  if (key_hex == NULL) {
    lc_pouch_query_index_summary_cleanup(&update);
    lc_error_cleanup(&error);
    lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                 key, operation_active);
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  need_seed = pending == NULL;
  pthread_mutex_unlock(&pouch->indexer_mutex);
  if (need_seed) {
    base_index_seq = 0UL;
    covers_namespace = 0;
    rc = lc_pouch_query_index_pending_seed(
        pouch, ns, update.count, &base_index_seq, &covers_namespace, &error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, key_hex);
      lc_pouch_query_index_summary_cleanup(&update);
      lc_error_cleanup(&error);
      lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                   key, operation_active);
      return;
    }
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  if (pending == NULL && !need_seed) {
    pthread_mutex_unlock(&pouch->indexer_mutex);
    base_index_seq = 0UL;
    covers_namespace = 0;
    rc = lc_pouch_query_index_pending_seed(
        pouch, ns, update.count, &base_index_seq, &covers_namespace, &error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, key_hex);
      lc_pouch_query_index_summary_cleanup(&update);
      lc_error_cleanup(&error);
      lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                   key, operation_active);
      return;
    }
    pthread_mutex_lock(&pouch->indexer_mutex);
    previous = NULL;
    pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  }
  if (pending == NULL) {
    pending = lc_pouch_query_index_pending_create(pouch, ns, base_index_seq,
                                                  covers_namespace);
  }
  if (pending == NULL || result->index_seq <= pending->last_index_seq) {
    if (pending != NULL) {
      lc_pouch_query_index_pending_invalidate_locked(pouch, pending,
                                                     result->index_seq);
    }
    pthread_mutex_unlock(&pouch->indexer_mutex);
    if (pending == NULL) {
      lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                   key, operation_active);
    }
    lc_free_with_allocator(&pouch->allocator, key_hex);
    lc_pouch_query_index_summary_cleanup(&update);
    lc_error_cleanup(&error);
    return;
  }
  previous_row = lc_pouch_query_index_summary_find_row(&pending->summary, key);
  if (deferred && previous_row != NULL && !previous_row->needs_extraction) {
    /* Replacing a captured row without its new derived terms invalidates the
     * resident projection. A newly deferred row has no memtable entry yet, so
     * it can remain row-local until extraction or a bounded replacement. */
    lc_pouch_query_index_memtable_cleanup(&pouch->allocator, pending->memtable);
    lc_free_with_allocator(&pouch->allocator, pending->memtable);
    pending->memtable = NULL;
    lc_pouch_query_index_summary_mark_rows_deferred(&pending->summary);
  }
  rc = deferred ? LC_OK
                : lc_pouch_query_index_memtable_note_write(pending, &update,
                                                           key_hex, &error);
  if (rc == LC_OK) {
    lc_pouch_query_index_summary_remove_key(&pending->summary, key, key_hex);
    lc_pouch_query_index_key_hex_set_remove(&pouch->allocator,
                                            &pending->deletes, key_hex);
    rc = lc_pouch_query_index_summary_merge_move(&pending->summary, &update,
                                                 &error);
  }
  if (rc == LC_OK) {
    pending->last_index_seq = result->index_seq;
    if (!lc_pouch_query_index_summary_within_pending_limit(&pending->summary)) {
      rc = LC_ERR_NOMEM;
    }
  }
  if (rc != LC_OK) {
    lc_pouch_query_index_pending_invalidate_locked(pouch, pending,
                                                   result->index_seq);
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  lc_free_with_allocator(&pouch->allocator, key_hex);
  lc_pouch_query_index_summary_cleanup(&update);
  lc_error_cleanup(&error);
}

static void lc_pouch_query_index_pending_note_delete(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_state_write_result *result, int operation_active) {
  lc_pouch_query_index_pending_entry *pending;
  lc_pouch_query_index_pending_entry *previous;
  lc_pouch_query_index_row *previous_row;
  lc_pouch_generation base_index_seq;
  char *key_hex;
  lc_error error;
  int covers_namespace;
  int need_seed;
  int rc;

  if (pouch == NULL || !lc_pouch_single_writer_enabled(pouch) ||
      !pouch->indexer_mutex_initialized || ns == NULL || key == NULL ||
      result == NULL || result->index_seq == 0UL) {
    return;
  }
  (void)operation_active;
  key_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, key);
  if (key_hex == NULL) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                 key, operation_active);
    return;
  }
  lc_error_init(&error);
  /* A concurrent flush may consume the pending capture between either lookup
   * and creation. The fresh-capture defaults are conservative until seeding
   * establishes a durable projection watermark. */
  base_index_seq = 0UL;
  covers_namespace = 0;
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  if (pending != NULL && pending->incomplete) {
    if (result->index_seq > pending->last_index_seq) {
      pending->last_index_seq = result->index_seq;
    }
    pthread_mutex_unlock(&pouch->indexer_mutex);
    lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                 key, operation_active);
    lc_free_with_allocator(&pouch->allocator, key_hex);
    lc_error_cleanup(&error);
    return;
  }
  need_seed = pending == NULL;
  pthread_mutex_unlock(&pouch->indexer_mutex);
  if (need_seed) {
    base_index_seq = 0UL;
    covers_namespace = 0;
    rc = lc_pouch_query_index_pending_seed(pouch, ns, 0U, &base_index_seq,
                                           &covers_namespace, &error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, key_hex);
      lc_error_cleanup(&error);
      lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                   key, operation_active);
      return;
    }
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  if (pending == NULL && !need_seed) {
    pthread_mutex_unlock(&pouch->indexer_mutex);
    base_index_seq = 0UL;
    covers_namespace = 0;
    rc = lc_pouch_query_index_pending_seed(pouch, ns, 0U, &base_index_seq,
                                           &covers_namespace, &error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, key_hex);
      lc_error_cleanup(&error);
      lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                   key, operation_active);
      return;
    }
    pthread_mutex_lock(&pouch->indexer_mutex);
    previous = NULL;
    pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  }
  if (pending == NULL) {
    pending = lc_pouch_query_index_pending_create(pouch, ns, base_index_seq,
                                                  covers_namespace);
  }
  if (pending == NULL || result->index_seq <= pending->last_index_seq) {
    if (pending != NULL) {
      lc_pouch_query_index_pending_invalidate_locked(pouch, pending,
                                                     result->index_seq);
    }
    pthread_mutex_unlock(&pouch->indexer_mutex);
    if (pending == NULL) {
      lc_pouch_query_index_pending_mark_incomplete(pouch, ns, result->index_seq,
                                                   key, operation_active);
    }
    lc_free_with_allocator(&pouch->allocator, key_hex);
    lc_error_cleanup(&error);
    return;
  }
  previous_row = lc_pouch_query_index_summary_find_row(&pending->summary, key);
  rc =
      pending->memtable == NULL || previous_row == NULL ||
              previous_row->needs_extraction
          ? LC_OK
          : lc_pouch_query_index_memtable_note_delete(pending, key_hex, &error);
  if (rc == LC_OK) {
    lc_pouch_query_index_summary_remove_key(&pending->summary, key, key_hex);
    rc = lc_pouch_query_index_key_hex_set_add(
        &pouch->allocator, &pending->deletes, key_hex, &error);
  }
  if (rc == LC_OK) {
    pending->last_index_seq = result->index_seq;
  } else {
    lc_pouch_query_index_pending_invalidate_locked(pouch, pending,
                                                   result->index_seq);
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  lc_free_with_allocator(&pouch->allocator, key_hex);
  lc_error_cleanup(&error);
}

static int lc_pouch_query_index_pending_take(
    lc_pouch *pouch, const char *ns, lc_pouch_generation base_index_seq,
    lc_pouch_generation state_index_seq, int require_namespace_coverage,
    int require_completed_operations, int *deferred_for_active_operation,
    lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_key_hex_set *deletes,
    lc_pouch_generation *last_index_seq,
    lc_pouch_query_index_memtable **memtable_out) {
  lc_pouch_query_index_pending_entry *pending;
  lc_pouch_query_index_pending_entry *previous;

  if (pouch == NULL || !lc_pouch_single_writer_enabled(pouch) ||
      !pouch->indexer_mutex_initialized || ns == NULL || summary == NULL ||
      deletes == NULL || last_index_seq == NULL) {
    return 0;
  }
  if (memtable_out != NULL) {
    *memtable_out = NULL;
  }
  if (deferred_for_active_operation != NULL) {
    *deferred_for_active_operation = 0;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  /* Completion was observed before foreground publication began, but an
   * independent key can become active while this thread obtains its current
   * state sequence. Recheck under the same mutex that records active keys so
   * the foreground path never consumes a provisional version. */
  if (pending != NULL && require_completed_operations &&
      lc_pouch_query_index_pending_has_active_operations_locked(pouch, ns)) {
    if (deferred_for_active_operation != NULL) {
      *deferred_for_active_operation = 1;
    }
    pthread_mutex_unlock(&pouch->indexer_mutex);
    return 0;
  }
  if (pending == NULL || pending->base_index_seq != base_index_seq ||
      pending->last_index_seq > state_index_seq || pending->incomplete ||
      (require_namespace_coverage && !pending->covers_namespace)) {
    pthread_mutex_unlock(&pouch->indexer_mutex);
    return 0;
  }
  *summary = pending->summary;
  *deletes = pending->deletes;
  *last_index_seq = pending->last_index_seq;
  if (memtable_out != NULL) {
    *memtable_out = pending->memtable;
  }
  memset(&pending->summary, 0, sizeof(pending->summary));
  memset(&pending->deletes, 0, sizeof(pending->deletes));
  pending->memtable = NULL;
  lc_pouch_query_index_pending_remove_locked(pouch, pending, previous);
  pthread_mutex_unlock(&pouch->indexer_mutex);
  return 1;
}

int lc_pouch_query_index_pending_operation_complete_locked(
    lc_pouch *pouch, const char *ns, const char *key,
    const char *operation_id) {
  if (pouch == NULL || ns == NULL || key == NULL || key[0] == '\0' ||
      operation_id == NULL || operation_id[0] == '\0') {
    return 0;
  }
  return lc_pouch_query_index_active_operation_remove_locked(pouch, ns, key,
                                                             operation_id);
}

int lc_pouch_query_index_pending_document_limit_reached_locked(
    lc_pouch *pouch, const char *ns, uint64_t document_limit) {
  lc_pouch_query_index_pending_entry *pending;

  if (pouch == NULL || ns == NULL || document_limit == 0U) {
    return 0;
  }
  pending = lc_pouch_query_index_pending_find(pouch, ns, NULL);
  return pending != NULL &&
         !lc_pouch_query_index_pending_has_active_operations_locked(pouch,
                                                                    ns) &&
         ((uint64_t)pending->summary.count +
                  (uint64_t)pending->incomplete_keys.count >=
              document_limit ||
          (uint64_t)pending->deletes.count >=
              document_limit - ((uint64_t)pending->summary.count +
                                (uint64_t)pending->incomplete_keys.count));
}

static int lc_pouch_query_index_pending_apply_tail_deletes(
    const lc_allocator *allocator,
    const lc_pouch_query_index_incremental_context *incremental,
    lc_pouch_query_index_key_hex_set *deletes, lc_error *error) {
  size_t index;
  int rc;

  if (incremental == NULL || deletes == NULL) {
    return LC_OK;
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < incremental->change_count; ++index) {
    const lc_pouch_query_index_incremental_change *change;

    change = &incremental->changes[index];
    if (change->key_hex == NULL) {
      continue;
    }
    if (change->found) {
      lc_pouch_query_index_key_hex_set_remove(allocator, deletes,
                                              change->key_hex);
    } else {
      rc = lc_pouch_query_index_key_hex_set_add(allocator, deletes,
                                                change->key_hex, error);
    }
  }
  return rc;
}

static void lc_pouch_query_index_pending_discard_published(
    lc_pouch *pouch, const char *ns, lc_pouch_generation state_index_seq) {
  lc_pouch_query_index_pending_entry *pending;
  lc_pouch_query_index_pending_entry *previous;

  if (pouch == NULL || !pouch->indexer_mutex_initialized || ns == NULL) {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  pending = lc_pouch_query_index_pending_find(pouch, ns, &previous);
  if (pending != NULL) {
    if (pending->last_index_seq <= state_index_seq) {
      lc_pouch_query_index_pending_remove_locked(pouch, pending, previous);
    } else if (pending->base_index_seq < state_index_seq) {
      /* A background flush may publish while a writer is still extending the
       * capture that began at the previous manifest. That capture overlaps
       * the published range, so its rows cannot safely become an incremental
       * segment. Retain an explicit tail-replay marker instead: the next
       * flush replays only records after this durable publication point. */
      lc_pouch_query_index_pending_invalidate_locked(pouch, pending,
                                                     pending->last_index_seq);
      pending->base_index_seq = state_index_seq;
      pending->covers_namespace = 0;
    }
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
}

static int lc_pouch_query_index_load_delete_keys(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_query_index_key_hex_set *set, unsigned long *delete_count,
    unsigned long *delete_hash, lc_error *error) {
  char *bytes;
  char *cursor;
  char *line;
  size_t length;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || path == NULL || set == NULL || delete_count == NULL ||
      delete_hash == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index delete load requires pouch, path, "
                        "set, count, and hash",
                        NULL, NULL, NULL);
  }
  *delete_count = 0UL;
  *delete_hash = lc_pouch_query_index_hash_init();
  bytes = NULL;
  length = 0U;
  present = 0;
  valid = 1;
  rc = lc_pouch_query_index_read_packed_component_bytes(
      pouch, ns, path, LC_POUCH_QUERY_INDEX_PACKED_DELETE, &bytes, &length,
      &present, &valid, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return rc;
  }
  if (!present) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return LC_OK;
  }
  if (!valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index delete artifact is not readable",
                        NULL, NULL, "pouch");
  }
  lc_pouch_query_index_hash_bytes(delete_hash, bytes, length);
  cursor = bytes;
  while ((line = lc_pouch_query_index_manifest_next_line(&cursor)) != NULL) {
    if (line[0] == '\0') {
      continue;
    }
    if (!lc_pouch_query_index_hex_token_valid(line)) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index delete key is invalid", NULL, NULL,
                          "pouch");
    }
    rc = lc_pouch_query_index_key_hex_set_add(&pouch->allocator, set, line,
                                              error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return rc;
    }
    ++*delete_count;
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return LC_OK;
}

static int lc_pouch_query_index_doc_table_generation_load_artifact(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *present, int *valid, lc_error *error) {
  char *bytes;
  size_t length;
  int artifact_valid;
  int rc;

  if (pouch == NULL || table == NULL || present == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc-table generation load requires inputs", NULL,
                        NULL, NULL);
  }
  memset(table, 0, sizeof(*table));
  *present = 0;
  *valid = 0;
  bytes = NULL;
  length = 0U;
  artifact_valid = 0;
  rc = lc_pouch_query_index_read_packed_component_bytes(
      pouch, ns, path, LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE, &bytes, &length,
      present, &artifact_valid, error);
  if (rc == LC_OK && *present && artifact_valid) {
    rc = lc_pouch_index_doc_table_generation_load_bytes(
        &pouch->allocator, &bytes, length, expected_index_seq,
        expected_row_count, expected_row_hash, table, valid, error);
  } else {
    *valid = artifact_valid;
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_parse_header_line(const char *line,
                                                  const char *name,
                                                  unsigned long *out) {
  const char *value;
  char *end;
  unsigned long parsed;
  size_t name_len;

  if (line == NULL || name == NULL || out == NULL) {
    return 0;
  }
  name_len = strlen(name);
  if (strncmp(line, name, name_len) != 0 || line[name_len] != '=') {
    return 0;
  }
  value = line + name_len + 1U;
  if (*value == '\0' || *value == '\n') {
    return 0;
  }
  errno = 0;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || (*end != '\n' && *end != '\0')) {
    return 0;
  }
  *out = parsed;
  return 1;
}

static int lc_pouch_query_index_parse_header_u64(const char *line,
                                                 const char *name,
                                                 uint64_t *out) {
  const char *value;
  size_t name_len;
  size_t value_len;
  lc_u64 parsed;

  if (line == NULL || name == NULL || out == NULL) {
    return 0;
  }
  name_len = strlen(name);
  if (strncmp(line, name, name_len) != 0 || line[name_len] != '=') {
    return 0;
  }
  value = line + name_len + 1U;
  if (*value == '\0' || *value == '\n') {
    return 0;
  }
  value_len = strcspn(value, "\n");
  if (value_len == 0U ||
      (value[value_len] != '\0' && value[value_len] != '\n') ||
      !lc_parse_u64_base10_range_checked(value, value_len, &parsed)) {
    return 0;
  }
  *out = (uint64_t)parsed;
  return 1;
}

static int lc_pouch_query_index_read_rows(
    FILE *fp, unsigned long row_count, unsigned long row_hash,
    lc_pouch_query_index_row_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "row ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_rows;
  int got_line;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_rows = 0UL;
  rc = LC_OK;
  while (actual_rows < row_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      reader->row_index = actual_rows;
      rc = lc_pouch_query_index_parse_and_visit_row(line.bytes, reader, error);
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_rows;
  }
  if (rc == LC_OK && (actual_rows != row_count || computed_hash != row_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_terms(
    FILE *fp, unsigned long term_count, unsigned long term_hash,
    lc_pouch_query_index_term_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "term ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_terms;
  int got_line;
  int allow_sorted_stop;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_terms = 0UL;
  allow_sorted_stop = reader != NULL && reader->visit != NULL;
  if (allow_sorted_stop) {
    reader->stop = 0;
  }
  rc = LC_OK;
  while (actual_terms < term_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_term(line.bytes, reader, error);
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        reader->stop = 1;
      }
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_terms;
    if (allow_sorted_stop && reader->stop) {
      break;
    }
  }
  if (rc == LC_OK && allow_sorted_stop && reader->stop) {
    *valid = 1;
  } else if (rc == LC_OK &&
             (actual_terms != term_count || computed_hash != term_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int
lc_pouch_query_index_read_terms_slice(FILE *fp, unsigned long term_count,
                                      lc_pouch_query_index_term_reader *reader,
                                      int *valid, lc_error *error) {
  static const char prefix[] = "term ";
  lc_pouch_query_index_text line;
  unsigned long actual_terms;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term slice read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  actual_terms = 0UL;
  rc = LC_OK;
  *valid = 0;
  if (reader != NULL) {
    reader->stop = 0;
  }
  while (actual_terms < term_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL, NULL,
                        NULL);
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_term(line.bytes, reader, error);
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        reader->stop = 1;
      }
      if (rc != LC_OK) {
        break;
      }
    }
    ++actual_terms;
    if (reader != NULL && reader->stop) {
      break;
    }
  }
  if (rc == LC_OK &&
      (actual_terms == term_count || (reader != NULL && reader->stop))) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_seek_term_slice(FILE *fp,
                                                uint64_t term_section_start,
                                                uint64_t first_byte,
                                                const lc_allocator *allocator,
                                                int *valid, lc_error *error) {
  uint64_t offset;
  off_t target;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term seek requires valid output",
                        NULL, NULL, NULL);
  }
  *valid = 0;
  if (fp == NULL || first_byte > LC_U64_MAX - term_section_start) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term byte range is invalid", NULL,
                        NULL, NULL);
  }
  offset = term_section_start + first_byte;
  target = (off_t)offset;
  if (target < 0 || (uint64_t)target != offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term byte range exceeds platform "
                        "seek limit",
                        NULL, NULL, NULL);
  }
  if (fseeko(fp, target, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seek pouch query-index term slice",
                        strerror(errno), NULL, NULL);
  }
  (void)allocator;
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_read_term_fields(
    FILE *fp, unsigned long term_field_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_field **out_fields,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_field *fields;
  unsigned long actual_fields;
  unsigned long previous_end;
  uint64_t previous_byte_end;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  if (out_fields != NULL) {
    *out_fields = NULL;
  }
  if (out_count != NULL) {
    *out_count = 0U;
  }
  *valid = 0;
  fields = NULL;
  if (term_field_count > 0UL && out_fields != NULL) {
    fields = (lc_pouch_index_term_field *)lc_alloc_with_allocator(
        allocator, (size_t)term_field_count * sizeof(*fields));
    if (fields == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index term fields",
                          NULL, NULL, NULL);
    }
    memset(fields, 0, (size_t)term_field_count * sizeof(*fields));
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual_fields = 0UL;
  previous_end = 0UL;
  previous_byte_end = 0U;
  rc = LC_OK;
  while (actual_fields < term_field_count) {
    lc_pouch_index_term_field parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_index_term_field_parse_line(line.bytes, &parsed, allocator,
                                              error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.byte_count == 0U ||
        parsed.first_line < previous_end ||
        parsed.first_byte < previous_byte_end ||
        parsed.byte_count > LC_U64_MAX - parsed.first_byte ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_fields > 0UL && fields != NULL &&
         strcmp(fields[actual_fields - 1UL].field_hex, parsed.field_hex) >=
             0)) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field range is invalid", NULL,
                        NULL, NULL);
      break;
    }
    if (fields != NULL) {
      fields[actual_fields] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    previous_byte_end = parsed.first_byte + parsed.byte_count;
    ++actual_fields;
  }
  if (rc == LC_OK && actual_fields == term_field_count) {
    *valid = 1;
    if (out_fields != NULL) {
      *out_fields = fields;
      fields = NULL;
    }
    if (out_count != NULL) {
      *out_count = (size_t)actual_fields;
    }
  }
  lc_pouch_index_term_fields_cleanup(allocator, fields,
                                     (size_t)term_field_count);
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_term_values(
    FILE *fp, unsigned long term_value_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_value **out_values,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_value *values;
  unsigned long actual_values;
  unsigned long previous_end;
  uint64_t previous_byte_end;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  if (out_values != NULL) {
    *out_values = NULL;
  }
  if (out_count != NULL) {
    *out_count = 0U;
  }
  *valid = 0;
  values = NULL;
  if (term_value_count > 0UL && out_values != NULL) {
    values = (lc_pouch_index_term_value *)lc_alloc_with_allocator(
        allocator, (size_t)term_value_count * sizeof(*values));
    if (values == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index term values",
                          NULL, NULL, NULL);
    }
    memset(values, 0, (size_t)term_value_count * sizeof(*values));
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual_values = 0UL;
  previous_end = 0UL;
  previous_byte_end = 0U;
  rc = LC_OK;
  while (actual_values < term_value_count) {
    lc_pouch_index_term_value parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_index_term_value_parse_line(line.bytes, &parsed, allocator,
                                              error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.byte_count == 0U ||
        parsed.first_line < previous_end ||
        parsed.first_byte < previous_byte_end ||
        parsed.byte_count > LC_U64_MAX - parsed.first_byte ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_values > 0UL && values != NULL &&
         (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) > 0 ||
          (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) ==
               0 &&
           strcmp(values[actual_values - 1UL].value_hex, parsed.value_hex) >=
               0)))) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value range is invalid", NULL,
                        NULL, NULL);
      break;
    }
    if (values != NULL) {
      values[actual_values] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    previous_byte_end = parsed.first_byte + parsed.byte_count;
    ++actual_values;
  }
  if (rc == LC_OK && actual_values == term_value_count) {
    *valid = 1;
    if (out_values != NULL) {
      *out_values = values;
      values = NULL;
    }
    if (out_count != NULL) {
      *out_count = (size_t)actual_values;
    }
  }
  lc_pouch_index_term_values_cleanup(allocator, values,
                                     (size_t)term_value_count);
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_term_reader_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const lc_pouch_query_index_term_reader *reader, unsigned long term_count,
    uint64_t term_byte_count, unsigned long *first_line,
    unsigned long *line_count, uint64_t *first_byte, uint64_t *byte_count) {
  if (reader == NULL || first_line == NULL || line_count == NULL) {
    return 0;
  }
  if (lc_pouch_query_index_field_hex_is_any_text(reader->field_hex)) {
    *first_line = 0UL;
    *line_count = term_count;
    if (first_byte != NULL) {
      *first_byte = 0U;
    }
    if (byte_count != NULL) {
      *byte_count = term_byte_count;
    }
    return 1;
  }
  return lc_pouch_index_term_fields_select_range(
      fields, field_count, reader->field_hex, NULL, 0U, term_count,
      term_byte_count, first_line, line_count, first_byte, byte_count);
}

static int lc_pouch_query_index_read_presences(
    FILE *fp, unsigned long presence_count, unsigned long presence_hash,
    lc_pouch_query_index_presence_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "present ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_presences;
  int got_line;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_presences = 0UL;
  rc = LC_OK;
  while (actual_presences < presence_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_presence(line.bytes, reader,
                                                         error);
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_presences;
  }
  if (rc == LC_OK &&
      (actual_presences != presence_count || computed_hash != presence_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static const lc_allocator *lc_pouch_query_index_reader_allocator(
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader) {
  if (reader != NULL) {
    return reader->allocator;
  }
  if (term_reader != NULL) {
    return term_reader->allocator;
  }
  if (presence_reader != NULL) {
    return presence_reader->allocator;
  }
  return NULL;
}

static int lc_pouch_query_index_skip_lines(FILE *fp, unsigned long count,
                                           const lc_allocator *allocator,
                                           int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  unsigned long actual;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index skip requires valid output", NULL,
                        NULL, NULL);
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual = 0UL;
  rc = LC_OK;
  *valid = 0;
  while (actual < count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    ++actual;
  }
  if (rc == LC_OK && actual == count) {
    *valid = 1;
  }
  lc_free_with_allocator(allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_with_reader_fp(
    FILE *fp, lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  char format[64];
  char line[256];
  unsigned long version;
  lc_pouch_generation parsed_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_index_complete;
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long presence_index_complete;
  uint64_t first_byte;
  uint64_t byte_count;
  uint64_t term_byte_count;
  lc_pouch_index_term_field *term_fields;
  lc_pouch_index_term_value *term_values;
  size_t term_field_table_count;
  size_t term_value_table_count;
  uint64_t term_section_start;
  int matched;
  int row_valid;
  int term_valid;
  int term_field_valid;
  int term_value_valid;
  int presence_valid;
  int full_read;
  int want_rows;
  int want_terms;
  int want_presences;
  int rc;

  if (fp == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index read requires a stream and output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(format, 0, sizeof(format));
  version = 0UL;
  parsed_seq = 0UL;
  row_count = 0UL;
  row_hash = 0UL;
  term_count = 0UL;
  term_hash = 0UL;
  term_index_complete = 0UL;
  term_field_count = 0UL;
  term_value_count = 0UL;
  presence_count = 0UL;
  presence_hash = 0UL;
  presence_index_complete = 0UL;
  first_byte = 0U;
  byte_count = 0U;
  term_byte_count = 0U;
  term_fields = NULL;
  term_values = NULL;
  term_field_table_count = 0U;
  term_value_table_count = 0U;
  term_section_start = 0U;
  matched = 0;
  if (fgets(line, sizeof(line), fp) != NULL &&
      sscanf(line, "format=%63s\n", format) == 1) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "version", &version)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_u64(line, "state_index_seq",
                                            &parsed_seq)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "row_count", &row_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "summary_hash", &row_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_count", &term_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_hash", &term_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_index_complete",
                                             &term_index_complete) &&
      term_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_count",
                                             &presence_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_hash",
                                             &presence_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_index_complete",
                                             &presence_index_complete) &&
      presence_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_field_count",
                                             &term_field_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_value_count",
                                             &term_value_count)) {
    ++matched;
  }
  out->present = 1;
  if (matched == 13 && strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    out->term_count = term_count;
    out->term_hash = term_hash;
    out->term_field_count = term_field_count;
    out->term_value_count = term_value_count;
    out->presence_count = presence_count;
    out->presence_hash = presence_hash;
    out->term_index_complete = term_index_complete != 0UL;
    out->presence_index_complete = presence_index_complete != 0UL;
    full_read =
        reader == NULL && term_reader == NULL && presence_reader == NULL;
    want_rows = full_read || reader != NULL;
    want_terms = full_read || term_reader != NULL;
    want_presences = full_read || presence_reader != NULL;
    row_valid = !want_rows;
    term_valid = !want_terms;
    term_field_valid = 0;
    term_value_valid = 0;
    presence_valid = !want_presences;
    rc = lc_pouch_query_index_read_term_fields(
        fp, term_field_count, term_count,
        lc_pouch_query_index_reader_allocator(reader, term_reader,
                                              presence_reader),
        want_terms ? &term_fields : NULL,
        want_terms ? &term_field_table_count : NULL, &term_field_valid, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_read_term_values(
          fp, term_value_count, term_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          want_terms ? &term_values : NULL,
          want_terms ? &term_value_table_count : NULL, &term_value_valid,
          error);
    }
    if (rc == LC_OK) {
      off_t term_position;

      term_position = ftello(fp);
      if (term_position < 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to locate pouch query-index term section",
                          strerror(errno), NULL, NULL);
      } else {
        term_section_start = (uint64_t)term_position;
      }
    }
    if (rc == LC_OK && term_field_table_count > 0U) {
      lc_pouch_index_term_field *last_field;

      last_field = &term_fields[term_field_table_count - 1U];
      if (last_field->byte_count > LC_U64_MAX - last_field->first_byte) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term byte range is invalid", NULL,
                          NULL, NULL);
      } else {
        term_byte_count = last_field->first_byte + last_field->byte_count;
      }
    }
    if (rc == LC_OK && want_terms) {
      unsigned long first_line;
      unsigned long line_count;

      first_line = 0UL;
      line_count = 0UL;
      first_byte = 0U;
      byte_count = 0U;
      if (rc == LC_OK && term_reader != NULL &&
          lc_pouch_query_index_term_reader_range(
              term_fields, term_field_table_count, term_reader, term_count,
              term_byte_count, &first_line, &line_count, &first_byte,
              &byte_count)) {
        rc = lc_pouch_query_index_seek_term_slice(
            fp, term_section_start, first_byte,
            lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                  presence_reader),
            &term_valid, error);
        if (rc == LC_OK && term_valid) {
          rc = lc_pouch_query_index_read_terms_slice(
              fp, line_count, term_reader, &term_valid, error);
        }
        if (rc == LC_OK && (want_rows || want_presences) && term_valid) {
          rc = lc_pouch_query_index_seek_term_slice(
              fp, term_section_start, term_byte_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (rc == LC_OK && term_reader != NULL && want_rows) {
        rc = lc_pouch_query_index_seek_term_slice(
            fp, term_section_start, 0U,
            lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                  presence_reader),
            &term_valid, error);
        if (rc == LC_OK && term_valid) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (term_reader != NULL) {
        term_valid = 1;
        if (want_rows || want_presences) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else {
        rc = lc_pouch_query_index_read_terms(fp, term_count, term_hash,
                                             term_reader, &term_valid, error);
      }
    } else if (rc == LC_OK && (want_rows || want_presences)) {
      rc = lc_pouch_query_index_seek_term_slice(
          fp, term_section_start, 0U,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &term_valid, error);
    }
    if (rc == LC_OK && !want_terms && (want_rows || want_presences)) {
      rc = lc_pouch_query_index_skip_lines(
          fp, term_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &term_valid, error);
    }
    if (want_rows) {
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_read_rows(fp, row_count, row_hash, reader,
                                            &row_valid, error);
      }
    } else if (rc == LC_OK && want_presences) {
      rc = lc_pouch_query_index_skip_lines(
          fp, row_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &row_valid, error);
    }
    if (rc == LC_OK && want_presences) {
      rc = lc_pouch_query_index_read_presences(fp, presence_count,
                                               presence_hash, presence_reader,
                                               &presence_valid, error);
    }
    if (rc == LC_OK && full_read) {
      int got_extra;
      lc_pouch_query_index_text extra;

      memset(&extra, 0, sizeof(extra));
      extra.allocator = lc_pouch_query_index_reader_allocator(
          reader, term_reader, presence_reader);
      rc = lc_pouch_query_index_read_line(fp, &extra, &got_extra, error);
      if (rc == LC_OK && got_extra) {
        presence_valid = 0;
      }
      lc_free_with_allocator(extra.allocator, extra.bytes);
    }
    if (rc == LC_OK) {
      out->valid = row_valid && term_valid && term_field_valid &&
                   term_value_valid && presence_valid;
    }
  } else {
    rc = LC_OK;
  }
  lc_pouch_index_term_fields_cleanup(lc_pouch_query_index_reader_allocator(
                                         reader, term_reader, presence_reader),
                                     term_fields, term_field_table_count);
  lc_pouch_index_term_values_cleanup(lc_pouch_query_index_reader_allocator(
                                         reader, term_reader, presence_reader),
                                     term_values, term_value_table_count);
  return rc;
}

static int lc_pouch_query_index_read_with_reader_file(
    const char *path, lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  FILE *fp;
  int rc;

  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      memset(out, 0, sizeof(*out));
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index header artifact",
                        strerror(errno), NULL, NULL);
  }
  rc = lc_pouch_query_index_read_with_reader_fp(fp, out, reader, term_reader,
                                                presence_reader, error);
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index header artifact",
                        strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_query_index_read_with_reader(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  char *bytes;
  size_t length;
  FILE *fp;
  int present;
  int valid;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_query_index_read_with_reader_file(
        path, out, reader, term_reader, presence_reader, error);
  }
  bytes = NULL;
  length = 0U;
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_read_encrypted_artifact_bytes(
      pouch, ns, path, 1, &bytes, &length, &present, &valid, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  out->present = present;
  if (!present || !valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return LC_OK;
  }
  fp = fmemopen(bytes, length, "rb");
  if (fp == NULL) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to open encrypted pouch query-index memory",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_read_with_reader_fp(fp, out, reader, term_reader,
                                                presence_reader, error);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close encrypted pouch query-index memory",
                      strerror(errno), NULL, NULL);
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_exact_generation_flush_posting(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    unsigned long term_id, const lc_pouch_index_docid_set *docids,
    lc_error *error) {
  if (term_id == 0UL || docids == NULL || docids->count == 0U) {
    return LC_OK;
  }
  return lc_pouch_index_term_posting_table_append_sorted_unique_trusted(
      allocator, &generation->postings, term_id, docids->items, docids->count,
      error);
}

static int lc_pouch_query_index_docid_compare(const void *left,
                                              const void *right) {
  const unsigned long *a;
  const unsigned long *b;

  a = (const unsigned long *)left;
  b = (const unsigned long *)right;
  if (*a < *b) {
    return -1;
  }
  if (*a > *b) {
    return 1;
  }
  return 0;
}

static void lc_pouch_query_index_exact_generation_docids_normalize(
    lc_pouch_query_index_exact_generation_docids *item) {
  size_t read_index;
  size_t write_index;

  if (item == NULL || item->docids.count <= 1U) {
    return;
  }
  if (item->docids_need_sort) {
    qsort(item->docids.items, item->docids.count, sizeof(item->docids.items[0]),
          lc_pouch_query_index_docid_compare);
  }
  write_index = 1U;
  for (read_index = 1U; read_index < item->docids.count; ++read_index) {
    if (item->docids.items[read_index] ==
        item->docids.items[write_index - 1U]) {
      continue;
    }
    if (write_index != read_index) {
      item->docids.items[write_index] = item->docids.items[read_index];
    }
    ++write_index;
  }
  item->docids.count = write_index;
  item->docids_need_sort = 0;
}

static void lc_pouch_query_index_exact_generation_accumulator_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator) {
  size_t index;

  if (accumulator == NULL) {
    return;
  }
  for (index = 0U; index < accumulator->count; ++index) {
    if (!accumulator->items[index].using_inline_docids) {
      lc_pouch_index_docid_set_cleanup(allocator,
                                       &accumulator->items[index].docids);
    } else {
      memset(&accumulator->items[index].docids, 0,
             sizeof(accumulator->items[index].docids));
      accumulator->items[index].using_inline_docids = 0;
    }
  }
  lc_free_with_allocator(allocator, accumulator->items);
  lc_free_with_allocator(allocator, accumulator->term_positions);
  memset(accumulator, 0, sizeof(*accumulator));
}

static void lc_pouch_query_index_exact_generation_accumulator_rebind_inline(
    lc_pouch_query_index_exact_generation_accumulator *accumulator) {
  size_t index;

  if (accumulator == NULL) {
    return;
  }
  for (index = 0U; index < accumulator->count; ++index) {
    if (accumulator->items[index].using_inline_docids) {
      accumulator->items[index].docids.items =
          accumulator->items[index].inline_docids;
      accumulator->items[index].docids.capacity =
          LC_POUCH_QUERY_INDEX_INLINE_DOCIDS;
    }
  }
}

static int lc_pouch_query_index_exact_generation_docids_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_docids *item, size_t needed,
    lc_error *error) {
  unsigned long *next_items;
  size_t next_capacity;

  if (item == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation docID reserve requires item",
                        NULL, NULL, NULL);
  }
  if (item->docids.items == NULL) {
    item->docids.items = item->inline_docids;
    item->docids.capacity = LC_POUCH_QUERY_INDEX_INLINE_DOCIDS;
    item->using_inline_docids = 1;
  }
  if (needed <= item->docids.capacity) {
    return LC_OK;
  }
  next_capacity = item->docids.capacity == 0U ? 8U : item->docids.capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch exact generation docID set exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (unsigned long *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact generation docID set",
                        NULL, NULL, NULL);
  }
  if (item->docids.count > 0U) {
    memcpy(next_items, item->docids.items,
           item->docids.count * sizeof(*next_items));
  }
  if (!item->using_inline_docids) {
    lc_free_with_allocator(allocator, item->docids.items);
  }
  item->docids.items = next_items;
  item->docids.capacity = next_capacity;
  item->using_inline_docids = 0;
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_docids_append_unique(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_docids *item, unsigned long doc_id,
    lc_error *error) {
  int rc;

  if (item == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation docID append requires item",
                        NULL, NULL, NULL);
  }
  if (item->docids.count > 0U) {
    if (doc_id == item->docids.items[item->docids.count - 1U]) {
      return LC_OK;
    }
    if (doc_id < item->docids.items[item->docids.count - 1U]) {
      item->docids_need_sort = 1;
    }
  }
  rc = lc_pouch_query_index_exact_generation_docids_reserve(
      allocator, item, item->docids.count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  item->docids.items[item->docids.count] = doc_id;
  ++item->docids.count;
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_accumulator_position_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, lc_error *error) {
  size_t *next_positions;
  size_t next_capacity;

  if (accumulator == NULL || term_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator position cache "
                        "requires accumulator and term id",
                        NULL, NULL, NULL);
  }
  if (term_id < (unsigned long)accumulator->term_position_capacity) {
    return LC_OK;
  }
  next_capacity = accumulator->term_position_capacity == 0U
                      ? 256U
                      : accumulator->term_position_capacity;
  while (term_id >= (unsigned long)next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch exact generation accumulator position cache "
                          "exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_positions = (size_t *)lc_calloc_with_allocator(allocator, next_capacity,
                                                      sizeof(*next_positions));
  if (next_positions == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact generation "
                        "accumulator position cache",
                        NULL, NULL, NULL);
  }
  if (accumulator->term_positions != NULL) {
    memcpy(next_positions, accumulator->term_positions,
           accumulator->term_position_capacity * sizeof(*next_positions));
    lc_free_with_allocator(allocator, accumulator->term_positions);
  }
  accumulator->term_positions = next_positions;
  accumulator->term_position_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_accumulator_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    size_t needed, lc_error *error) {
  lc_pouch_query_index_exact_generation_docids *next_items;
  size_t next_capacity;

  if (accumulator == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator requires "
                        "accumulator",
                        NULL, NULL, NULL);
  }
  if (needed <= accumulator->capacity) {
    return LC_OK;
  }
  next_capacity = accumulator->capacity == 0U ? 16U : accumulator->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch exact generation accumulator exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_exact_generation_docids *)lc_alloc_with_allocator(
          allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact generation "
                        "accumulator",
                        NULL, NULL, NULL);
  }
  if (accumulator->items != NULL) {
    memcpy(next_items, accumulator->items,
           accumulator->count * sizeof(next_items[0]));
    lc_free_with_allocator(allocator, accumulator->items);
  }
  memset(next_items + accumulator->count, 0,
         (next_capacity - accumulator->count) * sizeof(next_items[0]));
  accumulator->items = next_items;
  accumulator->capacity = next_capacity;
  lc_pouch_query_index_exact_generation_accumulator_rebind_inline(accumulator);
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_accumulator_find(
    const lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, size_t *position_out) {
  size_t low;
  size_t high;
  size_t mid;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (accumulator == NULL || term_id == 0UL) {
    return 0;
  }
  low = 0U;
  high = accumulator->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    if (accumulator->items[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < accumulator->count && accumulator->items[low].term_id == term_id;
}

static int lc_pouch_query_index_exact_generation_accumulator_append(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, unsigned long doc_id, lc_error *error) {
  size_t position;
  int rc;

  if (term_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator requires term id",
                        NULL, NULL, "pouch");
  }
  position = 0U;
  if (term_id < (unsigned long)accumulator->term_position_capacity &&
      accumulator->term_positions[term_id] > 0U &&
      accumulator->term_positions[term_id] - 1U < accumulator->count &&
      accumulator->items[accumulator->term_positions[term_id] - 1U].term_id ==
          term_id) {
    position = accumulator->term_positions[term_id] - 1U;
  } else if (!lc_pouch_query_index_exact_generation_accumulator_find(
                 accumulator, term_id, &position)) {
    rc = lc_pouch_query_index_exact_generation_accumulator_reserve(
        allocator, accumulator, accumulator->count + 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (position < accumulator->count) {
      memmove(&accumulator->items[position + 1U], &accumulator->items[position],
              (accumulator->count - position) * sizeof(accumulator->items[0]));
      lc_pouch_query_index_exact_generation_accumulator_rebind_inline(
          accumulator);
      memset(&accumulator->items[position], 0,
             sizeof(accumulator->items[position]));
    }
    accumulator->items[position].term_id = term_id;
    ++accumulator->count;
  }
  rc = lc_pouch_query_index_exact_generation_accumulator_position_reserve(
      allocator, accumulator, term_id, error);
  if (rc != LC_OK) {
    return rc;
  }
  accumulator->term_positions[term_id] = position + 1U;
  return lc_pouch_query_index_exact_generation_docids_append_unique(
      allocator, &accumulator->items[position], doc_id, error);
}

static void lc_pouch_query_index_exact_generation_accumulator_remove(
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, unsigned long doc_id) {
  lc_pouch_query_index_exact_generation_docids *item;
  size_t position;
  size_t read_index;
  size_t write_index;

  if (accumulator == NULL || term_id == 0UL) {
    return;
  }
  position = 0U;
  if (term_id < (unsigned long)accumulator->term_position_capacity &&
      accumulator->term_positions[term_id] > 0U &&
      accumulator->term_positions[term_id] - 1U < accumulator->count &&
      accumulator->items[accumulator->term_positions[term_id] - 1U].term_id ==
          term_id) {
    position = accumulator->term_positions[term_id] - 1U;
  } else if (!lc_pouch_query_index_exact_generation_accumulator_find(
                 accumulator, term_id, &position)) {
    return;
  }
  item = &accumulator->items[position];
  write_index = 0U;
  for (read_index = 0U; read_index < item->docids.count; ++read_index) {
    if (item->docids.items[read_index] == doc_id) {
      continue;
    }
    if (write_index != read_index) {
      item->docids.items[write_index] = item->docids.items[read_index];
    }
    ++write_index;
  }
  item->docids.count = write_index;
}

static void lc_pouch_query_index_trigram_value_set_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_value_set *set) {
  if (set == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_trigram_value_set_reserve(
    const lc_allocator *allocator, lc_pouch_query_index_trigram_value_set *set,
    size_t needed, lc_error *error) {
  unsigned long *next_items;
  size_t next_capacity;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram set reserve requires set", NULL, NULL,
                        NULL);
  }
  if (needed <= set->capacity) {
    return LC_OK;
  }
  next_capacity = set->capacity == 0U ? 64U : set->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch trigram set exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  if (next_capacity > ((size_t)-1 / sizeof(*next_items))) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch trigram set exceeds local limit", NULL, NULL,
                        NULL);
  }
  next_items = (unsigned long *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram set", NULL, NULL,
                        NULL);
  }
  if (set->items != NULL) {
    memcpy(next_items, set->items, set->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, set->items);
  }
  memset(next_items + set->count, 0,
         (next_capacity - set->count) * sizeof(*next_items));
  set->items = next_items;
  set->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_trigram_value_compare(const void *left,
                                                      const void *right) {
  unsigned long left_value;
  unsigned long right_value;

  left_value = *(const unsigned long *)left;
  right_value = *(const unsigned long *)right;
  if (left_value < right_value) {
    return -1;
  }
  if (left_value > right_value) {
    return 1;
  }
  return 0;
}

/* Trigram extraction can visit every byte of a large string. Append first,
 * then canonicalize once: maintaining sorted order at each insertion makes
 * high-entropy values quadratic in the number of distinct trigrams. */
static void lc_pouch_query_index_trigram_value_set_sort_unique(
    lc_pouch_query_index_trigram_value_set *set) {
  size_t read_index;
  size_t write_index;

  if (set == NULL || set->count < 2U) {
    return;
  }
  qsort(set->items, set->count, sizeof(set->items[0]),
        lc_pouch_query_index_trigram_value_compare);
  write_index = 1U;
  for (read_index = 1U; read_index < set->count; ++read_index) {
    if (set->items[read_index] != set->items[write_index - 1U]) {
      set->items[write_index++] = set->items[read_index];
    }
  }
  set->count = write_index;
}

static int lc_pouch_query_index_trigram_value_set_add(
    const lc_allocator *allocator, lc_pouch_query_index_trigram_value_set *set,
    unsigned long key, lc_error *error) {
  int rc;

  if (key > 0xffffffUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram set requires a 24-bit trigram key", NULL,
                        NULL, "pouch");
  }
  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram set add requires set", NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_trigram_value_set_reserve(allocator, set,
                                                      set->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count] = key;
  ++set->count;
  return LC_OK;
}

static unsigned long
lc_pouch_query_index_trigram_field_hash(const char *field_hex) {
  unsigned long hash;

  hash = lc_pouch_query_index_hash_init();
  if (field_hex != NULL) {
    lc_pouch_query_index_hash_bytes(&hash, field_hex, strlen(field_hex));
  }
  return hash;
}

static void lc_pouch_query_index_field_cache_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_field_cache *cache) {
  size_t index;

  if (cache == NULL) {
    return;
  }
  for (index = 0U; index < cache->capacity; ++index) {
    if (cache->items[index].used) {
      lc_free_with_allocator(allocator, cache->items[index].field_hex);
    }
  }
  lc_free_with_allocator(allocator, cache->items);
  memset(cache, 0, sizeof(*cache));
}

static int lc_pouch_query_index_field_cache_reserve(
    const lc_allocator *allocator, lc_pouch_query_index_field_cache *cache,
    size_t needed, lc_error *error) {
  lc_pouch_query_index_field_cache_entry *next_items;
  size_t next_capacity;
  size_t index;

  if (cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch field cache reserve requires cache", NULL, NULL,
                        NULL);
  }
  if (needed * 2U <= cache->capacity) {
    return LC_OK;
  }
  next_capacity = cache->capacity == 0U ? 128U : cache->capacity;
  while (needed * 2U > next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch field cache exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_field_cache_entry *)lc_calloc_with_allocator(
          allocator, next_capacity, sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch field cache", NULL, NULL,
                        NULL);
  }
  if (cache->items != NULL) {
    for (index = 0U; index < cache->capacity; ++index) {
      size_t slot;

      if (!cache->items[index].used) {
        continue;
      }
      slot = (size_t)(cache->items[index].field_hash %
                      (unsigned long)next_capacity);
      while (next_items[slot].used) {
        slot = (slot + 1U) % next_capacity;
      }
      next_items[slot] = cache->items[index];
    }
    lc_free_with_allocator(allocator, cache->items);
  }
  cache->items = next_items;
  cache->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_field_cache_find_or_add(
    const lc_allocator *allocator, lc_pouch_query_index_field_cache *cache,
    const char *field_hex, unsigned long *field_id_out,
    unsigned long *field_hash_out, const char **canonical_field_hex_out,
    lc_error *error) {
  char *field_hex_copy;
  unsigned long field_hash;
  size_t slot;
  int rc;

  if (field_id_out != NULL) {
    *field_id_out = 0UL;
  }
  if (field_hash_out != NULL) {
    *field_hash_out = 0UL;
  }
  if (canonical_field_hex_out != NULL) {
    *canonical_field_hex_out = NULL;
  }
  if (cache == NULL || field_hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch field cache requires cache and field", NULL,
                        NULL, NULL);
  }
  field_hash = lc_pouch_query_index_trigram_field_hash(field_hex);
  rc = lc_pouch_query_index_field_cache_reserve(allocator, cache,
                                                cache->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  slot = (size_t)(field_hash % (unsigned long)cache->capacity);
  while (cache->items[slot].used) {
    if (cache->items[slot].field_hash == field_hash &&
        strcmp(cache->items[slot].field_hex, field_hex) == 0) {
      if (field_id_out != NULL) {
        *field_id_out = cache->items[slot].field_id;
      }
      if (field_hash_out != NULL) {
        *field_hash_out = field_hash;
      }
      if (canonical_field_hex_out != NULL) {
        *canonical_field_hex_out = cache->items[slot].field_hex;
      }
      return LC_OK;
    }
    slot = (slot + 1U) % cache->capacity;
  }
  if (cache->next_field_id == 0UL) {
    cache->next_field_id = 1UL;
  }
  field_hex_copy = lc_strdup_with_allocator(allocator, field_hex);
  if (field_hex_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch field cache key", NULL, NULL,
                        NULL);
  }
  cache->items[slot].field_hex = field_hex_copy;
  cache->items[slot].field_hash = field_hash;
  cache->items[slot].field_id = cache->next_field_id++;
  cache->items[slot].used = 1;
  ++cache->count;
  if (field_id_out != NULL) {
    *field_id_out = cache->items[slot].field_id;
  }
  if (field_hash_out != NULL) {
    *field_hash_out = field_hash;
  }
  if (canonical_field_hex_out != NULL) {
    *canonical_field_hex_out = cache->items[slot].field_hex;
  }
  return LC_OK;
}

static unsigned long lc_pouch_query_index_term_cache_hash(const char *field_hex,
                                                          const char *value_hex,
                                                          char value_type) {
  unsigned long hash;

  hash = lc_pouch_query_index_hash_init();
  if (field_hex != NULL) {
    lc_pouch_query_index_hash_bytes(&hash, field_hex, strlen(field_hex));
  }
  lc_pouch_query_index_hash_bytes(&hash, &value_type, 1U);
  if (value_hex != NULL) {
    lc_pouch_query_index_hash_bytes(&hash, value_hex, strlen(value_hex));
  }
  return hash != 0UL ? hash : 1UL;
}

static void lc_pouch_query_index_term_cache_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_term_cache *cache) {
  if (cache == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, cache->items);
  memset(cache, 0, sizeof(*cache));
}

static int
lc_pouch_query_index_term_cache_reserve(const lc_allocator *allocator,
                                        lc_pouch_query_index_term_cache *cache,
                                        size_t needed, lc_error *error) {
  lc_pouch_query_index_term_cache_entry *next_items;
  size_t next_capacity;
  size_t index;

  if (cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch term cache reserve requires cache", NULL, NULL,
                        NULL);
  }
  if (needed * 2U <= cache->capacity) {
    return LC_OK;
  }
  next_capacity = cache->capacity == 0U ? 256U : cache->capacity;
  while (needed * 2U > next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch term cache exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_term_cache_entry *)lc_calloc_with_allocator(
          allocator, next_capacity, sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch term cache", NULL, NULL,
                        NULL);
  }
  if (cache->items != NULL) {
    for (index = 0U; index < cache->capacity; ++index) {
      size_t slot;

      if (!cache->items[index].used) {
        continue;
      }
      slot = (size_t)(cache->items[index].hash % (unsigned long)next_capacity);
      while (next_items[slot].used) {
        slot = (slot + 1U) % next_capacity;
      }
      next_items[slot] = cache->items[index];
    }
    lc_free_with_allocator(allocator, cache->items);
  }
  cache->items = next_items;
  cache->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_term_cache_find(
    const lc_pouch_query_index_term_cache *cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long hash,
    unsigned long *term_id_out) {
  size_t slot;
  size_t visited;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (cache == NULL || cache->items == NULL || cache->capacity == 0U ||
      field_hex == NULL || value_hex == NULL) {
    return 0;
  }
  slot = (size_t)(hash % (unsigned long)cache->capacity);
  visited = 0U;
  while (visited < cache->capacity && cache->items[slot].used) {
    if (cache->items[slot].hash == hash &&
        cache->items[slot].value_type == value_type &&
        strcmp(cache->items[slot].field_hex, field_hex) == 0 &&
        strcmp(cache->items[slot].value_hex, value_hex) == 0) {
      if (term_id_out != NULL) {
        *term_id_out = cache->items[slot].term_id;
      }
      return 1;
    }
    slot = (slot + 1U) % cache->capacity;
    ++visited;
  }
  return 0;
}

static int lc_pouch_query_index_term_cache_put(
    const lc_allocator *allocator, lc_pouch_query_index_term_cache *cache,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long hash, unsigned long term_id, lc_error *error) {
  size_t slot;
  int rc;

  rc = lc_pouch_query_index_term_cache_reserve(allocator, cache,
                                               cache->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  slot = (size_t)(hash % (unsigned long)cache->capacity);
  while (cache->items[slot].used) {
    if (cache->items[slot].hash == hash &&
        cache->items[slot].value_type == value_type &&
        strcmp(cache->items[slot].field_hex, field_hex) == 0 &&
        strcmp(cache->items[slot].value_hex, value_hex) == 0) {
      cache->items[slot].term_id = term_id;
      return LC_OK;
    }
    slot = (slot + 1U) % cache->capacity;
  }
  cache->items[slot].field_hex = field_hex;
  cache->items[slot].value_hex = value_hex;
  cache->items[slot].value_type = value_type;
  cache->items[slot].hash = hash;
  cache->items[slot].term_id = term_id;
  cache->items[slot].used = 1;
  ++cache->count;
  return LC_OK;
}

static int lc_pouch_query_index_term_find_or_add_cached(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_term_cache *cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long *term_id_out,
    lc_error *error) {
  unsigned long hash;
  unsigned long term_id;
  int rc;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  hash = lc_pouch_query_index_term_cache_hash(field_hex, value_hex, value_type);
  if (lc_pouch_query_index_term_cache_find(cache, field_hex, value_hex,
                                           value_type, hash, &term_id)) {
    if (term_id_out != NULL) {
      *term_id_out = term_id;
    }
    return LC_OK;
  }
  term_id = 0UL;
  rc = lc_pouch_index_term_table_append_trusted(allocator, &generation->terms,
                                                field_hex, value_hex,
                                                value_type, &term_id, error);
  if (rc == LC_OK) {
    const lc_pouch_index_term_entry *entry;

    entry = NULL;
    if (term_id > 0UL && term_id <= (unsigned long)generation->terms.count) {
      entry = &generation->terms.items[term_id - 1UL];
    }
    if (entry == NULL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch term cache append returned invalid term id",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_query_index_term_cache_put(allocator, cache, entry->field_hex,
                                             entry->value_hex, value_type, hash,
                                             term_id, error);
  }
  if (rc == LC_OK && term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return rc;
}

static void lc_pouch_query_index_trigram_term_cache_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_term_cache *cache) {
  if (cache == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, cache->items);
  memset(cache, 0, sizeof(*cache));
}

/* Table capacities are powers of two. Raw ASCII trigram bits cluster badly
 * under a low-bit modulo (especially for high-cardinality text), so mix the
 * field/trigram pair before deriving a slot. This is only an in-memory
 * builder hash; it has no persisted-format or query-order effect. */
static unsigned long
lc_pouch_query_index_trigram_term_cache_hash(unsigned long field_hash,
                                             unsigned long trigram_key) {
  unsigned long hash;

  hash = (field_hash ^ (trigram_key * 0x9e3779b1UL)) & 0xffffffffUL;
  hash ^= hash >> 16U;
  hash *= 0x7feb352dUL;
  hash &= 0xffffffffUL;
  hash ^= hash >> 15U;
  hash *= 0x846ca68bUL;
  hash &= 0xffffffffUL;
  hash ^= hash >> 16U;
  return hash;
}

static int lc_pouch_query_index_trigram_term_cache_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_term_cache *cache, size_t needed,
    lc_error *error) {
  lc_pouch_query_index_trigram_term_cache_entry *next_items;
  size_t next_capacity;
  size_t index;

  if (cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram term cache reserve requires cache", NULL,
                        NULL, NULL);
  }
  if (needed * 2U <= cache->capacity) {
    return LC_OK;
  }
  next_capacity = cache->capacity == 0U ? 4096U : cache->capacity;
  while (needed * 2U > next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch trigram term cache exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_trigram_term_cache_entry *)lc_calloc_with_allocator(
          allocator, next_capacity, sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram term cache", NULL,
                        NULL, NULL);
  }
  if (cache->items != NULL) {
    for (index = 0U; index < cache->capacity; ++index) {
      size_t slot;

      if (!cache->items[index].used) {
        continue;
      }
      slot = (size_t)(lc_pouch_query_index_trigram_term_cache_hash(
                          cache->items[index].field_hash,
                          cache->items[index].trigram_key) %
                      (unsigned long)next_capacity);
      while (next_items[slot].used) {
        slot = (slot + 1U) % next_capacity;
      }
      next_items[slot] = cache->items[index];
    }
    lc_free_with_allocator(allocator, cache->items);
  }
  cache->items = next_items;
  cache->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_trigram_term_cache_find(
    const lc_pouch_query_index_trigram_term_cache *cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long *term_id_out) {
  size_t slot;
  size_t visited;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (cache == NULL || cache->items == NULL || cache->capacity == 0U ||
      field_hex == NULL) {
    return 0;
  }
  slot = (size_t)(lc_pouch_query_index_trigram_term_cache_hash(field_hash,
                                                               trigram_key) %
                  (unsigned long)cache->capacity);
  visited = 0U;
  while (visited < cache->capacity && cache->items[slot].used) {
    if ((field_id != 0UL
             ? cache->items[slot].field_id == field_id
             : cache->items[slot].field_hash == field_hash &&
                   strcmp(cache->items[slot].field_hex, field_hex) == 0) &&
        cache->items[slot].trigram_key == trigram_key &&
        cache->items[slot].field_hex != NULL) {
      if (term_id_out != NULL) {
        *term_id_out = cache->items[slot].term_id;
      }
      return 1;
    }
    slot = (slot + 1U) % cache->capacity;
    ++visited;
  }
  return 0;
}

static int lc_pouch_query_index_trigram_term_cache_put(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_term_cache *cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long term_id, lc_error *error) {
  size_t slot;
  int rc;

  rc = lc_pouch_query_index_trigram_term_cache_reserve(
      allocator, cache, cache->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  slot = (size_t)(lc_pouch_query_index_trigram_term_cache_hash(field_hash,
                                                               trigram_key) %
                  (unsigned long)cache->capacity);
  while (cache->items[slot].used) {
    if ((field_id != 0UL
             ? cache->items[slot].field_id == field_id
             : cache->items[slot].field_hash == field_hash &&
                   strcmp(cache->items[slot].field_hex, field_hex) == 0) &&
        cache->items[slot].trigram_key == trigram_key &&
        cache->items[slot].field_hex != NULL) {
      cache->items[slot].term_id = term_id;
      return LC_OK;
    }
    slot = (slot + 1U) % cache->capacity;
  }
  cache->items[slot].field_hex = field_hex;
  cache->items[slot].field_id = field_id;
  cache->items[slot].field_hash = field_hash;
  cache->items[slot].trigram_key = trigram_key;
  cache->items[slot].term_id = term_id;
  cache->items[slot].used = 1;
  ++cache->count;
  return LC_OK;
}

static int lc_pouch_query_index_trigram_term_find_or_add(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_trigram_term_cache *cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long *term_id_out, lc_error *error) {
  unsigned long term_id;
  int rc;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (lc_pouch_query_index_trigram_term_cache_find(
          cache, field_hex, field_id, field_hash, trigram_key, &term_id)) {
    if (term_id_out != NULL) {
      *term_id_out = term_id;
    }
    return LC_OK;
  }
  rc = lc_pouch_index_term_table_append_trigram_trusted(
      allocator, &generation->terms, field_hex, trigram_key, &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_trigram_term_cache_put(
        allocator, cache, field_hex, field_id, field_hash, trigram_key, term_id,
        error);
  }
  if (rc == LC_OK && term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return rc;
}

static int lc_pouch_query_index_trigram_generation_append_docid_for_field(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long doc_id, lc_error *error) {
  unsigned long term_id;
  int rc;

  if (field_hex == NULL) {
    return LC_OK;
  }
  term_id = 0UL;
  rc = lc_pouch_query_index_trigram_term_find_or_add(
      allocator, generation, term_cache, field_hex, field_id, field_hash,
      trigram_key, &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_append(
        allocator, accumulator, term_id, doc_id, error);
  }
  return rc;
}

static int lc_pouch_query_index_trigram_generation_append_docid(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    unsigned long trigram_key, unsigned long doc_id, lc_error *error) {
  const char *canonical_field_hex;
  unsigned long field_hash;
  unsigned long field_id;
  int rc;

  if (field_hex == NULL) {
    return LC_OK;
  }
  canonical_field_hex = NULL;
  field_hash = 0UL;
  field_id = 0UL;
  rc = lc_pouch_query_index_field_cache_find_or_add(
      allocator, field_cache, field_hex, &field_id, &field_hash,
      &canonical_field_hex, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_query_index_trigram_generation_append_docid_for_field(
      allocator, generation, accumulator, term_cache, canonical_field_hex,
      field_id, field_hash, trigram_key, doc_id, error);
}

static void lc_pouch_query_index_trigram_generation_remove_docid_for_field(
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long doc_id) {
  unsigned long term_id;

  if (accumulator == NULL || term_cache == NULL || field_hex == NULL) {
    return;
  }
  term_id = 0UL;
  if (lc_pouch_query_index_trigram_term_cache_find(
          term_cache, field_hex, field_id, field_hash, trigram_key, &term_id)) {
    lc_pouch_query_index_exact_generation_accumulator_remove(accumulator,
                                                             term_id, doc_id);
  }
}

static uint64_t lc_pouch_query_index_text_trigram_bloom_add(uint64_t bloom,
                                                            unsigned long key) {
  unsigned int first;
  unsigned int second;

  first = (unsigned int)(key & 63UL);
  second = (unsigned int)((key >> 6U) & 63UL);
  return bloom | ((uint64_t)1U << first) | ((uint64_t)1U << second);
}

/* Case-insensitive text matching folds ASCII bytes only, so this compact
 * raw-byte summary can reject a complete short value without false negatives.
 */
static uint64_t
lc_pouch_query_index_text_trigram_bloom_hex(const char *value_hex) {
  size_t length;
  size_t index;
  uint64_t bloom;

  if (value_hex == NULL) {
    return 0U;
  }
  length = strlen(value_hex);
  if ((length & 1U) != 0U || length < 6U) {
    return 0U;
  }
  bloom = 0U;
  for (index = 0U; index + 5U < length; index += 2U) {
    int high;
    int low;
    unsigned char bytes[3];
    size_t byte_index;
    unsigned long key;

    for (byte_index = 0U; byte_index < 3U; ++byte_index) {
      high = lc_pouch_query_index_hex_value(
          (unsigned char)value_hex[index + (byte_index * 2U)]);
      low = lc_pouch_query_index_hex_value(
          (unsigned char)value_hex[index + (byte_index * 2U) + 1U]);
      if (high < 0 || low < 0) {
        return 0U;
      }
      bytes[byte_index] = (unsigned char)((high << 4) | low);
      if (bytes[byte_index] >= 'A' && bytes[byte_index] <= 'Z') {
        bytes[byte_index] = (unsigned char)(bytes[byte_index] - 'A' + 'a');
      }
    }
    key = ((unsigned long)bytes[0] << 16U) | ((unsigned long)bytes[1] << 8U) |
          (unsigned long)bytes[2];
    bloom = lc_pouch_query_index_text_trigram_bloom_add(bloom, key);
  }
  return bloom;
}

static int lc_pouch_query_index_text_generation_add_term(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long doc_id,
    lc_error *error) {
  unsigned long term_id;
  int rc;

  term_id = 0UL;
  rc = lc_pouch_query_index_term_find_or_add_cached(
      allocator, generation, term_cache, field_hex, value_hex, value_type,
      &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_append(
        allocator, accumulator, term_id, doc_id, error);
  }
  if (rc == LC_OK &&
      (value_type == 's' ||
       value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE) &&
      term_id > 0UL && term_id <= generation->terms.count &&
      generation->terms.items[term_id - 1U].term_id == term_id) {
    generation->terms.items[term_id - 1U].text_trigram_bloom =
        lc_pouch_query_index_text_trigram_bloom_hex(value_hex);
    generation->terms.items[term_id - 1U].has_text_trigram_bloom = 1;
  }
  return rc;
}

static int lc_pouch_query_index_text_token_byte(unsigned char value) {
  return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
         (value >= '0' && value <= '9');
}

/* Add a small, durable all-text token directory. A token posting proves a
 * positive ASCII case-insensitive contains match, but omitted tokens must
 * never affect correctness: callers fall back to the raw text verifier when
 * this directory cannot cover every trigram candidate. */
static int lc_pouch_query_index_text_generation_add_all_text_tokens(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *value_hex,
    unsigned long doc_id, lc_error *error) {
  char token[LC_POUCH_QUERY_INDEX_TEXT_TOKEN_BYTES_MAX + 1U];
  size_t value_hex_length;
  size_t index;
  size_t token_count;
  size_t token_length;
  int token_overlong;
  int rc;

  if (allocator == NULL || generation == NULL || accumulator == NULL ||
      term_cache == NULL || value_hex == NULL || strcmp(value_hex, "-") == 0) {
    return LC_OK;
  }
  value_hex_length = strlen(value_hex);
  if ((value_hex_length & 1U) != 0U ||
      (value_hex_length / 2U) >
          LC_POUCH_QUERY_INDEX_TEXT_TOKEN_SOURCE_BYTES_MAX) {
    return LC_OK;
  }
  token_count = 0U;
  token_length = 0U;
  token_overlong = 0;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index <= value_hex_length; index += 2U) {
    unsigned char value;
    int word;

    value = 0U;
    word = 0;
    if (index < value_hex_length) {
      int high;
      int low;

      high = lc_pouch_query_index_hex_value((unsigned char)value_hex[index]);
      low =
          lc_pouch_query_index_hex_value((unsigned char)value_hex[index + 1U]);
      if (high < 0 || low < 0) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch text token source has invalid hex", NULL,
                            NULL, "pouch");
      }
      value = (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
      word = lc_pouch_query_index_text_token_byte(value);
    }
    if (word) {
      if (!token_overlong) {
        if (token_length < LC_POUCH_QUERY_INDEX_TEXT_TOKEN_BYTES_MAX) {
          token[token_length++] = (char)lc_pouch_query_index_ascii_lower(value);
        } else {
          token_overlong = 1;
        }
      }
      continue;
    }
    if (token_length > 0U && !token_overlong) {
      char *token_hex;

      token[token_length] = '\0';
      token_hex =
          lc_pouch_query_index_hex_encode_bytes(allocator, token, token_length);
      if (token_hex == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch text token", NULL, NULL,
                          NULL);
      } else {
        rc = lc_pouch_query_index_text_generation_add_term(
            allocator, generation, accumulator, term_cache,
            LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, token_hex,
            LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE, doc_id, error);
      }
      lc_free_with_allocator(allocator, token_hex);
      ++token_count;
      if (token_count == LC_POUCH_QUERY_INDEX_TEXT_TOKEN_COUNT_MAX) {
        break;
      }
    }
    token_length = 0U;
    token_overlong = 0;
  }
  return rc;
}

static int lc_pouch_query_index_all_text_token_hex_from_needle(
    const lc_allocator *allocator, const char *needle, char **token_hex,
    int *eligible, lc_error *error) {
  size_t length;
  size_t index;

  if (token_hex == NULL || eligible == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch text token lookup requires outputs", NULL, NULL,
                        NULL);
  }
  *token_hex = NULL;
  *eligible = 0;
  if (needle == NULL || needle[0] == '\0') {
    return LC_OK;
  }
  length = strlen(needle);
  if (length > LC_POUCH_QUERY_INDEX_TEXT_TOKEN_BYTES_MAX) {
    return LC_OK;
  }
  for (index = 0U; index < length; ++index) {
    if (!lc_pouch_query_index_text_token_byte((unsigned char)needle[index])) {
      return LC_OK;
    }
  }
  *token_hex = lc_pouch_query_index_hex_encode_folded_ascii_bytes(
      allocator, needle, length);
  if (*token_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch text lookup token", NULL,
                        NULL, NULL);
  }
  *eligible = 1;
  return LC_OK;
}

static int lc_pouch_query_index_text_generation_add_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, unsigned long doc_id, lc_error *error) {
  char *prefix_hex;
  size_t prefix_hex_len;
  int rc;

  if (value_hex == NULL) {
    return LC_OK;
  }
  if (!lc_pouch_query_index_exact_string_is_long_hex(value_hex)) {
    rc = lc_pouch_query_index_text_generation_add_term(
        allocator, generation, accumulator, term_cache, field_hex, value_hex,
        's', doc_id, error);
    return rc;
  }
  prefix_hex_len = LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES * 2U;
  prefix_hex = (char *)lc_alloc_with_allocator(allocator, prefix_hex_len + 1U);
  if (prefix_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch text prefix term", NULL, NULL,
                        NULL);
  }
  memcpy(prefix_hex, value_hex, prefix_hex_len);
  prefix_hex[prefix_hex_len] = '\0';
  rc = lc_pouch_query_index_text_generation_add_term(
      allocator, generation, accumulator, term_cache, field_hex, prefix_hex,
      LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id, error);
  lc_free_with_allocator(allocator, prefix_hex);
  return rc;
}

static int lc_pouch_query_index_text_generation_add_raw_long_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value, size_t value_len, unsigned long doc_id,
    lc_error *error) {
  char *prefix_hex;
  size_t prefix_len;
  int rc;

  if (value == NULL ||
      value_len <= LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES) {
    return LC_OK;
  }
  prefix_len = LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
  prefix_hex =
      lc_pouch_query_index_hex_encode_bytes(allocator, value, prefix_len);
  if (prefix_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch raw text prefix term", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_text_generation_add_term(
      allocator, generation, accumulator, term_cache, field_hex, prefix_hex,
      LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id, error);
  lc_free_with_allocator(allocator, prefix_hex);
  return rc;
}

static char *lc_pouch_query_index_temporal_instant_value_hex(
    const lc_allocator *allocator, const lc_pouch_index_instant *instant,
    lc_error *error) {
  char value[96];
  int written;

  if (instant == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch temporal generation value requires instant", NULL, NULL,
                 NULL);
    return NULL;
  }
  written = snprintf(value, sizeof(value), "%.17g", instant->seconds);
  if (written < 0 || (size_t)written >= sizeof(value)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch temporal generation value exceeds local limit", NULL,
                 NULL, "pouch");
    return NULL;
  }
  return lc_pouch_query_index_hex_encode(allocator, value);
}

static int lc_pouch_query_index_term_precompute_derived(
    lc_pouch_query_index_summary *summary, lc_pouch_query_index_term *term,
    const char *value, size_t value_len, lc_error *error) {
  lc_pouch_query_index_trigram_value_set trigram_set;
  lc_pouch_index_instant instant;
  unsigned char *seen;
  size_t index;
  int rc;

  if (summary == NULL || term == NULL || value == NULL) {
    return LC_OK;
  }
  memset(&trigram_set, 0, sizeof(trigram_set));
  seen = NULL;
  rc = LC_OK;
  if (value_len > LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES) {
    term->text_prefix_hex = lc_pouch_query_index_hex_encode_bytes(
        summary->allocator, value,
        LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES);
    if (term->text_prefix_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to precompute pouch text prefix", NULL, NULL,
                        NULL);
      goto cleanup;
    }
  }
  if (value_len > LC_POUCH_QUERY_INDEX_TRIGRAM_INDEXED_BYTES_MAX) {
    term->trigram_keys_truncated = 1;
  } else if (value_len >= LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD) {
    seen = (unsigned char *)lc_calloc_with_allocator(
        summary->allocator, LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES, 1U);
    if (seen == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch precomputed trigram seen set",
                        NULL, NULL, NULL);
      goto cleanup;
    }
  }
  for (index = 0U;
       rc == LC_OK && !term->trigram_keys_truncated && index + 2U < value_len;
       ++index) {
    unsigned char bytes[3];
    unsigned char folded_bytes[3];
    size_t byte_index;
    unsigned long folded_key;

    for (byte_index = 0U; byte_index < 3U; ++byte_index) {
      unsigned char byte;

      byte = (unsigned char)value[index + byte_index];
      bytes[byte_index] = byte;
      folded_bytes[byte_index] =
          byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
    }
    folded_key = ((unsigned long)folded_bytes[0] << 16U) |
                 ((unsigned long)folded_bytes[1] << 8U) |
                 (unsigned long)folded_bytes[2];
    if (lc_pouch_query_index_trigram_seen_mark(seen, folded_key)) {
      rc = lc_pouch_query_index_trigram_value_set_add(
          summary->allocator, &trigram_set, folded_key, error);
    }
    if (lc_pouch_query_index_trigram_repeated_space(bytes[0], bytes[1],
                                                    bytes[2])) {
      size_t run_end;

      run_end = index + 3U;
      while (run_end < value_len && (unsigned char)value[run_end] == bytes[0]) {
        ++run_end;
      }
      if (run_end > index + 3U) {
        index = run_end >= 3U ? run_end - 3U : run_end;
      }
    }
  }
  if (rc == LC_OK && seen == NULL) {
    lc_pouch_query_index_trigram_value_set_sort_unique(&trigram_set);
  }
  if (rc == LC_OK && lc_pouch_index_parse_lql_datetime(value, &instant)) {
    term->temporal_value_hex = lc_pouch_query_index_temporal_instant_value_hex(
        summary->allocator, &instant, error);
    if (term->temporal_value_hex == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK && trigram_set.count > 0U) {
    term->trigram_keys = trigram_set.items;
    term->trigram_key_count = trigram_set.count;
    term->trigram_key_capacity = trigram_set.capacity;
    trigram_set.items = NULL;
    trigram_set.count = 0U;
    trigram_set.capacity = 0U;
  }
  if (rc == LC_OK) {
    term->derived_terms_ready = 1;
    term->trigram_keys_sorted = seen == NULL ? 1 : 0;
  }

cleanup:
  lc_free_with_allocator(summary->allocator, seen);
  lc_pouch_query_index_trigram_value_set_cleanup(summary->allocator,
                                                 &trigram_set);
  return rc;
}

static int lc_pouch_query_index_exact_generation_accumulator_flush(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  size_t index;
  size_t posting_count;
  int rc;

  if (accumulator == NULL || generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation flush requires accumulator "
                        "and generation",
                        NULL, NULL, NULL);
  }
  posting_count = 0U;
  for (index = 0U; index < accumulator->count; ++index) {
    lc_pouch_query_index_exact_generation_docids_normalize(
        &accumulator->items[index]);
    if (accumulator->items[index].docids.count > 0U) {
      ++posting_count;
    }
  }
  if (posting_count > (size_t)-1 - generation->postings.count) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index posting count exceeds local limit", NULL,
                        NULL, "pouch");
  }
  /* The immutable batch size is known. Reserve once before appending instead
   * of repeatedly moving and rebinding the same posting entries. */
  rc = lc_pouch_index_term_posting_table_reserve(
      allocator, &generation->postings,
      generation->postings.count + posting_count, error);
  for (index = 0U; rc == LC_OK && index < accumulator->count; ++index) {
    rc = lc_pouch_query_index_exact_generation_flush_posting(
        allocator, generation, accumulator->items[index].term_id,
        &accumulator->items[index].docids, error);
  }
  return rc;
}

static void lc_pouch_query_index_trigram_pair_radix_pass(
    const lc_pouch_query_index_trigram_pair *source,
    lc_pouch_query_index_trigram_pair *destination, size_t count,
    unsigned int shift, int document_id) {
  size_t offsets[256];
  size_t index;
  size_t offset;

  memset(offsets, 0, sizeof(offsets));
  for (index = 0U; index < count; ++index) {
    unsigned long value;

    value = document_id != 0 ? source[index].doc_id : source[index].key;
    ++offsets[(size_t)((value >> shift) & 0xffUL)];
  }
  offset = 0U;
  for (index = 0U; index < sizeof(offsets) / sizeof(offsets[0]); ++index) {
    size_t next_offset;

    next_offset = offset + offsets[index];
    offsets[index] = offset;
    offset = next_offset;
  }
  for (index = 0U; index < count; ++index) {
    unsigned long value;
    size_t bucket;

    value = document_id != 0 ? source[index].doc_id : source[index].key;
    bucket = (size_t)((value >> shift) & 0xffUL);
    destination[offsets[bucket]++] = source[index];
  }
}

/* Stable doc-id passes followed by stable raw-trigram passes leave a field's
 * pairs in immutable term order with sorted posting doc ids. */
static lc_pouch_query_index_trigram_pair *
lc_pouch_query_index_trigram_pairs_sort(
    lc_pouch_query_index_trigram_pair *pairs,
    lc_pouch_query_index_trigram_pair *scratch, size_t count,
    unsigned long max_doc_id) {
  lc_pouch_query_index_trigram_pair *source;
  lc_pouch_query_index_trigram_pair *destination;
  unsigned long remaining;
  unsigned int shift;

  source = pairs;
  destination = scratch;
  remaining = max_doc_id;
  shift = 0U;
  do {
    lc_pouch_query_index_trigram_pair_radix_pass(source, destination, count,
                                                 shift, 1);
    source = destination;
    destination = source == pairs ? scratch : pairs;
    remaining >>= 8U;
    shift += 8U;
  } while (remaining != 0UL);
  for (shift = 0U; shift < 24U; shift += 8U) {
    lc_pouch_query_index_trigram_pair_radix_pass(source, destination, count,
                                                 shift, 0);
    source = destination;
    destination = source == pairs ? scratch : pairs;
  }
  return source;
}

static int lc_pouch_query_index_field_hex_ref_compare(const void *left,
                                                      const void *right) {
  const char *const *a;
  const char *const *b;

  a = (const char *const *)left;
  b = (const char *const *)right;
  return strcmp(*a, *b);
}

static void lc_pouch_query_index_term_trigram_keys_sort_unique(
    lc_pouch_query_index_term *term) {
  size_t read_index;
  size_t write_index;

  if (term == NULL || term->trigram_key_count < 2U) {
    if (term != NULL) {
      term->trigram_keys_sorted = 1;
    }
    return;
  }
  qsort(term->trigram_keys, term->trigram_key_count,
        sizeof(term->trigram_keys[0]),
        lc_pouch_query_index_trigram_value_compare);
  write_index = 1U;
  for (read_index = 1U; read_index < term->trigram_key_count; ++read_index) {
    if (term->trigram_keys[read_index] !=
        term->trigram_keys[write_index - 1U]) {
      term->trigram_keys[write_index++] = term->trigram_keys[read_index];
    }
  }
  term->trigram_key_count = write_index;
  term->trigram_keys_sorted = 1;
}

static int
lc_pouch_query_index_term_add_trigram_fallback(const lc_allocator *allocator,
                                               lc_pouch_query_index_term *term,
                                               lc_error *error) {
  unsigned long *next_keys;
  size_t next_capacity;

  if (allocator == NULL || term == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram fallback requires allocator and term",
                        NULL, NULL, NULL);
  }
  if (!term->trigram_keys_truncated) {
    return LC_OK;
  }
  if (term->trigram_key_count == term->trigram_key_capacity) {
    if (term->trigram_key_capacity == (size_t)-1) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch trigram fallback exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity =
        term->trigram_key_capacity == 0U ? 1U : term->trigram_key_capacity;
    while (next_capacity <= term->trigram_key_count) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        next_capacity = term->trigram_key_count + 1U;
        break;
      }
      next_capacity *= 2U;
    }
    if (next_capacity > (size_t)-1 / sizeof(*next_keys)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch trigram fallback exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_keys = (unsigned long *)lc_alloc_with_allocator(
        allocator, next_capacity * sizeof(*next_keys));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch trigram fallback", NULL,
                          NULL, NULL);
    }
    if (term->trigram_key_count > 0U) {
      memcpy(next_keys, term->trigram_keys,
             term->trigram_key_count * sizeof(*next_keys));
    }
    lc_free_with_allocator(allocator, term->trigram_keys);
    term->trigram_keys = next_keys;
    term->trigram_key_capacity = next_capacity;
  }
  term->trigram_keys[term->trigram_key_count++] =
      LC_POUCH_QUERY_INDEX_TRIGRAM_FALLBACK_KEY;
  term->trigram_keys_truncated = 0;
  term->trigram_keys_sorted = 1;
  return LC_OK;
}

/* The normal builder is intentionally retained for summaries that need raw
 * parsing. Single-writer summaries carry sorted derived keys, so merge those
 * sources directly into lexical term and posting order. `/...` remains a
 * logical selector: readers union concrete fields instead of duplicating
 * every posting into a synthetic all-text field. */
static int lc_pouch_query_index_trigram_generation_build_sorted(
    const lc_allocator *allocator, lc_pouch_query_index_summary *summary,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  const char **fields;
  lc_pouch_query_index_trigram_pair *pairs;
  lc_pouch_query_index_trigram_pair *scratch;
  lc_pouch_query_index_trigram_pair *sorted_pairs;
  unsigned long *doc_ids;
  size_t field_count;
  size_t field_index;
  size_t pair_count;
  size_t pair_index;
  size_t source_count;
  size_t source_index;
  size_t write_index;
  size_t doc_id_count;
  unsigned long max_doc_id;
  unsigned long term_id;
  int rc;

  if (allocator == NULL || summary == NULL || generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch sorted trigram build requires allocator, "
                        "summary, and generation",
                        NULL, NULL, NULL);
  }
  fields = NULL;
  pairs = NULL;
  scratch = NULL;
  sorted_pairs = NULL;
  doc_ids = NULL;
  field_count = 0U;
  source_count = 0U;
  for (source_index = 0U; source_index < summary->term_count; ++source_index) {
    lc_pouch_query_index_term *term;

    term = &summary->terms[source_index];
    if (term->value_type != 's') {
      continue;
    }
    if (!term->derived_terms_ready) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch sorted trigram build requires derived terms",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_query_index_term_add_trigram_fallback(allocator, term, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (!term->trigram_keys_sorted) {
      lc_pouch_query_index_term_trigram_keys_sort_unique(term);
    }
    if (term->trigram_key_count > 0U && source_count == (size_t)-1) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch sorted trigram build exceeds local limit",
                          NULL, NULL, "pouch");
    }
    if (term->trigram_key_count > 0U) {
      ++source_count;
      ++field_count;
    }
  }
  if (source_count == 0U) {
    generation->terms_sorted = 1;
    return LC_OK;
  }
  if (source_count == (size_t)-1) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch sorted trigram build exceeds local limit", NULL,
                        NULL, NULL);
  }
  if (source_count > (size_t)-1 / sizeof(*doc_ids) ||
      field_count > (size_t)-1 / sizeof(*fields)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch sorted trigram build exceeds local limit", NULL,
                        NULL, "pouch");
  }
  fields = (const char **)lc_alloc_with_allocator(
      allocator, field_count * sizeof(*fields));
  doc_ids = (unsigned long *)lc_alloc_with_allocator(
      allocator, source_count * sizeof(*doc_ids));
  if (fields == NULL || doc_ids == NULL) {
    lc_free_with_allocator(allocator, fields);
    lc_free_with_allocator(allocator, doc_ids);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch sorted trigrams", NULL, NULL,
                        NULL);
  }
  field_index = 0U;
  for (source_index = 0U; source_index < summary->term_count; ++source_index) {
    const lc_pouch_query_index_term *term;

    term = &summary->terms[source_index];
    if (term->value_type == 's' && term->trigram_key_count > 0U) {
      fields[field_index++] = term->field_hex;
    }
  }
  qsort(fields, field_count, sizeof(fields[0]),
        lc_pouch_query_index_field_hex_ref_compare);
  write_index = 0U;
  for (field_index = 0U; field_index < field_count; ++field_index) {
    if (write_index == 0U ||
        strcmp(fields[write_index - 1U], fields[field_index]) != 0) {
      fields[write_index++] = fields[field_index];
    }
  }
  field_count = write_index;
  rc = LC_OK;
  /* The immutable format is field-major. Group each field's normalized
   * trigrams in linear radix passes instead of comparison-heavy heap merges. */
  for (field_index = 0U; rc == LC_OK && field_index < field_count;
       ++field_index) {
    pair_count = 0U;
    max_doc_id = 0UL;
    for (source_index = 0U; source_index < summary->term_count;
         ++source_index) {
      const lc_pouch_query_index_term *term;

      term = &summary->terms[source_index];
      if (term->value_type != 's' || term->trigram_key_count == 0U ||
          strcmp(term->field_hex, fields[field_index]) != 0) {
        continue;
      }
      if (term->trigram_key_count > (size_t)-1 - pair_count) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch sorted trigram build exceeds local limit",
                          NULL, NULL, "pouch");
        break;
      }
      pair_count += term->trigram_key_count;
      if (term->doc_id > max_doc_id) {
        max_doc_id = term->doc_id;
      }
    }
    if (rc != LC_OK || pair_count == 0U) {
      continue;
    }
    if (pair_count > (size_t)-1 / sizeof(*pairs)) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch sorted trigram build exceeds local limit", NULL,
                        NULL, "pouch");
      break;
    }
    pairs = (lc_pouch_query_index_trigram_pair *)lc_alloc_with_allocator(
        allocator, pair_count * sizeof(*pairs));
    scratch = (lc_pouch_query_index_trigram_pair *)lc_alloc_with_allocator(
        allocator, pair_count * sizeof(*scratch));
    if (pairs == NULL || scratch == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch sorted trigrams", NULL, NULL,
                        NULL);
      break;
    }
    pair_index = 0U;
    for (source_index = 0U; source_index < summary->term_count;
         ++source_index) {
      const lc_pouch_query_index_term *term;
      size_t key_index;

      term = &summary->terms[source_index];
      if (term->value_type != 's' || term->trigram_key_count == 0U ||
          strcmp(term->field_hex, fields[field_index]) != 0) {
        continue;
      }
      for (key_index = 0U; key_index < term->trigram_key_count; ++key_index) {
        pairs[pair_index].key = term->trigram_keys[key_index];
        pairs[pair_index].doc_id = term->doc_id;
        ++pair_index;
      }
    }
    sorted_pairs = lc_pouch_query_index_trigram_pairs_sort(
        pairs, scratch, pair_count, max_doc_id);
    pair_index = 0U;
    while (rc == LC_OK && pair_index < pair_count) {
      unsigned long trigram_key;

      trigram_key = sorted_pairs[pair_index].key;
      doc_id_count = 0U;
      while (pair_index < pair_count &&
             sorted_pairs[pair_index].key == trigram_key) {
        if (doc_id_count == 0U ||
            doc_ids[doc_id_count - 1U] != sorted_pairs[pair_index].doc_id) {
          doc_ids[doc_id_count++] = sorted_pairs[pair_index].doc_id;
        }
        ++pair_index;
      }
      term_id = 0UL;
      rc = lc_pouch_index_term_table_append_trigram_trusted(
          allocator, &generation->terms, fields[field_index], trigram_key,
          &term_id, error);
      if (rc == LC_OK) {
        rc = lc_pouch_index_term_posting_table_append_sorted_unique_trusted(
            allocator, &generation->postings, term_id, doc_ids, doc_id_count,
            error);
      }
    }
    lc_free_with_allocator(allocator, scratch);
    lc_free_with_allocator(allocator, pairs);
    scratch = NULL;
    pairs = NULL;
  }
  if (rc == LC_OK) {
    /* The sort order is the encoder's lexical order for fixed-width hex. */
    generation->terms_sorted = 1;
  }
  lc_free_with_allocator(allocator, scratch);
  lc_free_with_allocator(allocator, pairs);
  lc_free_with_allocator(allocator, doc_ids);
  lc_free_with_allocator(allocator, fields);
  return rc;
}

static int lc_pouch_query_index_build_text(
    lc_pouch_generation index_seq, lc_pouch_query_index_summary *summary,
    unsigned long *row_hash_out, unsigned long *term_count_out,
    unsigned long *term_hash_out, unsigned long *term_field_count_out,
    unsigned long *term_value_count_out, unsigned long *presence_count_out,
    unsigned long *presence_hash_out, char **doc_table_generation_out,
    size_t *doc_table_generation_length_out, char **exact_term_generation_out,
    size_t *exact_term_generation_length_out,
    char **presence_term_generation_out,
    size_t *presence_term_generation_length_out,
    char **range_term_generation_out, size_t *range_term_generation_length_out,
    char **text_term_generation_out, size_t *text_term_generation_length_out,
    char **trigram_term_generation_out,
    size_t *trigram_term_generation_length_out,
    char **temporal_term_generation_out,
    size_t *temporal_term_generation_length_out, lc_error *error) {
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation exact_generation;
  lc_pouch_index_term_generation presence_generation;
  lc_pouch_index_term_generation range_generation;
  lc_pouch_index_term_generation text_generation;
  lc_pouch_index_term_generation trigram_generation;
  lc_pouch_index_term_generation temporal_generation;
  lc_pouch_query_index_exact_generation_accumulator exact_accumulator;
  lc_pouch_query_index_exact_generation_accumulator presence_accumulator;
  lc_pouch_query_index_exact_generation_accumulator range_accumulator;
  lc_pouch_query_index_exact_generation_accumulator text_accumulator;
  lc_pouch_query_index_exact_generation_accumulator trigram_accumulator;
  lc_pouch_query_index_exact_generation_accumulator temporal_accumulator;
  lc_pouch_query_index_field_cache trigram_field_cache;
  lc_pouch_query_index_trigram_term_cache trigram_term_cache;
  lc_pouch_query_index_term_cache exact_term_cache;
  lc_pouch_query_index_term_cache presence_term_cache;
  lc_pouch_query_index_term_cache range_term_cache;
  lc_pouch_query_index_term_cache text_term_cache;
  lc_pouch_query_index_term_cache temporal_term_cache;
  char line[256];
  unsigned long row_hash;
  unsigned long term_hash;
  unsigned long term_line_count;
  unsigned long term_field_line_count;
  unsigned long term_value_line_count;
  unsigned long presence_hash;
  unsigned long presence_line_count;
  unsigned long doc_id;
  const char *last_term_key_hex;
  const char *last_presence_key_hex;
  size_t index;
  int found;
  int written;
  int use_sorted_trigrams;
  int rc;

  if (summary == NULL || row_hash_out == NULL || term_count_out == NULL ||
      term_hash_out == NULL || term_field_count_out == NULL ||
      term_value_count_out == NULL || presence_count_out == NULL ||
      presence_hash_out == NULL || doc_table_generation_out == NULL ||
      doc_table_generation_length_out == NULL ||
      exact_term_generation_out == NULL ||
      exact_term_generation_length_out == NULL ||
      presence_term_generation_out == NULL ||
      presence_term_generation_length_out == NULL ||
      range_term_generation_out == NULL ||
      range_term_generation_length_out == NULL ||
      text_term_generation_out == NULL ||
      text_term_generation_length_out == NULL ||
      trigram_term_generation_out == NULL ||
      trigram_term_generation_length_out == NULL ||
      temporal_term_generation_out == NULL ||
      temporal_term_generation_length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index build requires summary outputs",
                        NULL, NULL, NULL);
  }
  *doc_table_generation_out = NULL;
  *doc_table_generation_length_out = 0U;
  *exact_term_generation_out = NULL;
  *exact_term_generation_length_out = 0U;
  *presence_term_generation_out = NULL;
  *presence_term_generation_length_out = 0U;
  *range_term_generation_out = NULL;
  *range_term_generation_length_out = 0U;
  *text_term_generation_out = NULL;
  *text_term_generation_length_out = 0U;
  *trigram_term_generation_out = NULL;
  *trigram_term_generation_length_out = 0U;
  *temporal_term_generation_out = NULL;
  *temporal_term_generation_length_out = 0U;
  *row_hash_out = lc_pouch_query_index_hash_init();
  *term_count_out = 0UL;
  *term_hash_out = lc_pouch_query_index_hash_init();
  *term_field_count_out = 0UL;
  *term_value_count_out = 0UL;
  *presence_count_out = 0UL;
  *presence_hash_out = lc_pouch_query_index_hash_init();
  if (summary->count > 1U) {
    qsort(summary->rows, summary->count, sizeof(summary->rows[0]),
          lc_pouch_query_index_row_compare);
  }
  memset(&doc_table, 0, sizeof(doc_table));
  memset(&exact_generation, 0, sizeof(exact_generation));
  memset(&presence_generation, 0, sizeof(presence_generation));
  memset(&range_generation, 0, sizeof(range_generation));
  memset(&text_generation, 0, sizeof(text_generation));
  memset(&trigram_generation, 0, sizeof(trigram_generation));
  memset(&temporal_generation, 0, sizeof(temporal_generation));
  memset(&exact_accumulator, 0, sizeof(exact_accumulator));
  memset(&presence_accumulator, 0, sizeof(presence_accumulator));
  memset(&range_accumulator, 0, sizeof(range_accumulator));
  memset(&text_accumulator, 0, sizeof(text_accumulator));
  memset(&trigram_accumulator, 0, sizeof(trigram_accumulator));
  memset(&temporal_accumulator, 0, sizeof(temporal_accumulator));
  memset(&trigram_field_cache, 0, sizeof(trigram_field_cache));
  memset(&trigram_term_cache, 0, sizeof(trigram_term_cache));
  memset(&exact_term_cache, 0, sizeof(exact_term_cache));
  memset(&presence_term_cache, 0, sizeof(presence_term_cache));
  memset(&range_term_cache, 0, sizeof(range_term_cache));
  memset(&text_term_cache, 0, sizeof(text_term_cache));
  memset(&temporal_term_cache, 0, sizeof(temporal_term_cache));
  row_hash = lc_pouch_query_index_hash_init();
  term_hash = lc_pouch_query_index_hash_init();
  term_line_count = 0UL;
  term_field_line_count = 0UL;
  term_value_line_count = 0UL;
  last_term_key_hex = NULL;
  last_presence_key_hex = NULL;
  presence_hash = lc_pouch_query_index_hash_init();
  presence_line_count = 0UL;
  use_sorted_trigrams = 1;
  for (index = 0U; index < summary->term_count; ++index) {
    if (summary->terms[index].value_type == 's' &&
        !summary->terms[index].derived_terms_ready) {
      use_sorted_trigrams = 0;
      break;
    }
  }
  rc = LC_OK;
  exact_generation.ns =
      lc_strdup_with_allocator(summary->allocator, summary->ns);
  if (exact_generation.ns == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch exact generation namespace",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    presence_generation.ns =
        lc_strdup_with_allocator(summary->allocator, summary->ns);
    if (presence_generation.ns == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch presence generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    range_generation.ns =
        lc_strdup_with_allocator(summary->allocator, summary->ns);
    if (range_generation.ns == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch range generation namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    text_generation.ns =
        lc_strdup_with_allocator(summary->allocator, summary->ns);
    if (text_generation.ns == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch text generation namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    temporal_generation.ns =
        lc_strdup_with_allocator(summary->allocator, summary->ns);
    if (temporal_generation.ns == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch temporal generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    trigram_generation.ns =
        lc_strdup_with_allocator(summary->allocator, summary->ns);
    if (trigram_generation.ns == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->count; ++index) {
    lc_pouch_query_index_row *row;
    char version_text[32];
    char bytes_text[32];

    row = &summary->rows[index];
    if (lc_u64_format_base10((lc_u64)row->version, version_text,
                             sizeof(version_text)) < 0 ||
        lc_u64_format_base10((lc_u64)row->bytes, bytes_text,
                             sizeof(bytes_text)) < 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row exceeds local limit", NULL, NULL,
                        NULL);
      break;
    }
    written = snprintf(line, sizeof(line), "row %s %s %d %d ", version_text,
                       bytes_text, row->has_query_hidden ? 1 : 0,
                       row->query_hidden ? 1 : 0);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row exceeds local limit", NULL, NULL,
                        NULL);
      break;
    }
    rc = lc_pouch_query_index_text_append(NULL, line, (size_t)written,
                                          &row_hash, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->key_hex, &row_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->content_type_hex,
                                                 &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->etag_hex, &row_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_doc_table_append_sorted_unique(
          &doc_table, row->key_hex, row->version, row->bytes,
          row->has_query_hidden, row->query_hidden, &doc_id, summary->allocator,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, "\n", &row_hash, error);
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->term_count; ++index) {
    lc_pouch_query_index_term *term;

    term = &summary->terms[index];
    if (index > 0U &&
        lc_pouch_query_index_term_equal(&summary->terms[index - 1U], term)) {
      continue;
    }
    if (last_term_key_hex != NULL &&
        strcmp(last_term_key_hex, term->key_hex) == 0) {
      term->doc_id = doc_id;
    } else {
      rc = lc_pouch_index_doc_table_find_key_hex(&doc_table, term->key_hex,
                                                 &term->doc_id, &found, error);
      if (rc != LC_OK) {
        break;
      }
      if (!found) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term references missing doc table "
                          "entry",
                          NULL, NULL, NULL);
        break;
      }
      last_term_key_hex = term->key_hex;
      doc_id = term->doc_id;
    }
    if (rc == LC_OK) {
      char *allocated_exact_value_hex;
      const char *exact_value_hex;
      const char *indexed_exact_value_hex;
      unsigned long exact_term_id;
      char indexed_exact_value_type;

      allocated_exact_value_hex = NULL;
      exact_value_hex = NULL;
      indexed_exact_value_hex = NULL;
      exact_term_id = 0UL;
      indexed_exact_value_type = term->value_type;
      if (term->value_type == 's' &&
          (term->long_value != NULL || term->text_prefix_hex != NULL)) {
        exact_value_hex = term->value_hex;
        indexed_exact_value_hex = term->value_hex;
        indexed_exact_value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
      } else if (term->value_type == 'n') {
        if (term->canonical_value_hex != NULL) {
          exact_value_hex = term->canonical_value_hex;
          indexed_exact_value_hex = term->canonical_value_hex;
        } else {
          rc = lc_pouch_query_index_exact_generation_value_hex(
              summary->allocator, term->value_hex, term->value_type,
              &allocated_exact_value_hex, error);
          exact_value_hex = allocated_exact_value_hex;
          indexed_exact_value_hex = allocated_exact_value_hex;
        }
      } else {
        exact_value_hex = term->value_hex;
        indexed_exact_value_hex = term->value_hex;
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_term_find_or_add_cached(
            summary->allocator, &exact_generation, &exact_term_cache,
            term->field_hex, indexed_exact_value_hex, indexed_exact_value_type,
            &exact_term_id, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            summary->allocator, &exact_accumulator, exact_term_id, term->doc_id,
            error);
      }
      if (rc == LC_OK && term->value_type == 'n') {
        unsigned long range_term_id;

        range_term_id = 0UL;
        rc = lc_pouch_query_index_term_find_or_add_cached(
            summary->allocator, &range_generation, &range_term_cache,
            term->field_hex, exact_value_hex, term->value_type, &range_term_id,
            error);
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_exact_generation_accumulator_append(
              summary->allocator, &range_accumulator, range_term_id,
              term->doc_id, error);
        }
      }
      if (rc == LC_OK && term->value_type == 's') {
        char *date_text;
        const char *token_source_hex;
        const char *trigram_field_hex;
        lc_pouch_index_instant ignored_instant;
        unsigned long trigram_field_hash;
        unsigned long trigram_field_id;

        date_text = NULL;
        token_source_hex = NULL;
        trigram_field_hex = NULL;
        trigram_field_hash = 0UL;
        trigram_field_id = 0UL;
        if (term->derived_terms_ready) {
          if (term->text_prefix_hex != NULL) {
            rc = lc_pouch_query_index_text_generation_add_term(
                summary->allocator, &text_generation, &text_accumulator,
                &text_term_cache, term->field_hex, term->text_prefix_hex,
                LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, term->doc_id, error);
          } else {
            rc = lc_pouch_query_index_text_generation_add_value(
                summary->allocator, &text_generation, &text_accumulator,
                &text_term_cache, term->field_hex, exact_value_hex,
                term->doc_id, error);
          }
          if (rc == LC_OK && !use_sorted_trigrams &&
              term->trigram_key_count > 0U) {
            rc = lc_pouch_query_index_field_cache_find_or_add(
                summary->allocator, &trigram_field_cache, term->field_hex,
                &trigram_field_id, &trigram_field_hash, &trigram_field_hex,
                error);
          }
          if (rc == LC_OK && !use_sorted_trigrams) {
            size_t derived_index;

            for (derived_index = 0U;
                 rc == LC_OK && derived_index < term->trigram_key_count;
                 ++derived_index) {
              rc =
                  lc_pouch_query_index_trigram_generation_append_docid_for_field(
                      summary->allocator, &trigram_generation,
                      &trigram_accumulator, &trigram_term_cache,
                      trigram_field_hex, trigram_field_id, trigram_field_hash,
                      term->trigram_keys[derived_index], term->doc_id, error);
            }
          }
          if (rc == LC_OK && term->temporal_value_hex != NULL) {
            unsigned long temporal_term_id;

            temporal_term_id = 0UL;
            rc = lc_pouch_query_index_term_find_or_add_cached(
                summary->allocator, &temporal_generation, &temporal_term_cache,
                term->field_hex, term->temporal_value_hex, 'n',
                &temporal_term_id, error);
            if (rc == LC_OK) {
              rc = lc_pouch_query_index_exact_generation_accumulator_append(
                  summary->allocator, &temporal_accumulator, temporal_term_id,
                  term->doc_id, error);
            }
          }
        } else if (term->long_value != NULL) {
          rc = lc_pouch_query_index_text_generation_add_raw_long_value(
              summary->allocator, &text_generation, &text_accumulator,
              &text_term_cache, term->field_hex, term->long_value,
              term->long_value_len, term->doc_id, error);
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_trigram_generation_add_raw_value(
                summary->allocator, &trigram_generation, &trigram_accumulator,
                &trigram_field_cache, &trigram_term_cache, term->field_hex,
                term->long_value, term->long_value_len, term->doc_id, error);
          }
        } else {
          rc = lc_pouch_query_index_text_generation_add_value(
              summary->allocator, &text_generation, &text_accumulator,
              &text_term_cache, term->field_hex, exact_value_hex, term->doc_id,
              error);
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_trigram_generation_add_value(
                summary->allocator, &trigram_generation, &trigram_accumulator,
                &trigram_field_cache, &trigram_term_cache, term->field_hex,
                exact_value_hex, term->doc_id, error);
          }
        }
        if (rc == LC_OK) {
          token_source_hex = term->text_prefix_hex != NULL
                                 ? term->text_prefix_hex
                                 : exact_value_hex;
          if (token_source_hex != NULL &&
              !lc_pouch_query_index_exact_string_is_long_hex(
                  token_source_hex)) {
            rc = lc_pouch_query_index_text_generation_add_all_text_tokens(
                summary->allocator, &text_generation, &text_accumulator,
                &text_term_cache, token_source_hex, term->doc_id, error);
          }
        }
        if (rc == LC_OK && !term->derived_terms_ready &&
            exact_value_hex != NULL &&
            lc_pouch_query_index_hex_string_may_be_datetime(exact_value_hex)) {
          date_text = lc_pouch_query_index_hex_decode(summary->allocator,
                                                      exact_value_hex, error);
          if (date_text == NULL) {
            rc = error != NULL && error->code != LC_OK ? error->code
                                                       : LC_ERR_NOMEM;
          }
        }
        if (rc == LC_OK && !term->derived_terms_ready &&
            lc_pouch_index_parse_lql_datetime(date_text, &ignored_instant)) {
          char *temporal_value_hex;
          unsigned long temporal_term_id;

          temporal_value_hex = lc_pouch_query_index_temporal_instant_value_hex(
              summary->allocator, &ignored_instant, error);
          if (temporal_value_hex == NULL) {
            rc = error != NULL && error->code != LC_OK ? error->code
                                                       : LC_ERR_NOMEM;
          }
          temporal_term_id = 0UL;
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_term_find_or_add_cached(
                summary->allocator, &temporal_generation, &temporal_term_cache,
                term->field_hex, temporal_value_hex, 'n', &temporal_term_id,
                error);
          }
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_exact_generation_accumulator_append(
                summary->allocator, &temporal_accumulator, temporal_term_id,
                term->doc_id, error);
          }
          lc_free_with_allocator(summary->allocator, temporal_value_hex);
        }
        lc_free_with_allocator(summary->allocator, date_text);
      }
      lc_free_with_allocator(summary->allocator, allocated_exact_value_hex);
    }
    if (rc != LC_OK) {
      break;
    }
    if (rc == LC_OK) {
      ++term_line_count;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &exact_accumulator, &exact_generation, error);
  }
  for (index = 0U; rc == LC_OK && index < summary->presence_count; ++index) {
    lc_pouch_query_index_presence *presence;

    presence = &summary->presences[index];
    if (index > 0U && lc_pouch_query_index_presence_equal(
                          &summary->presences[index - 1U], presence)) {
      continue;
    }
    if (last_presence_key_hex == NULL ||
        strcmp(last_presence_key_hex, presence->key_hex) != 0) {
      rc = lc_pouch_index_doc_table_find_key_hex(&doc_table, presence->key_hex,
                                                 &doc_id, &found, error);
      if (rc != LC_OK) {
        break;
      }
      if (!found) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index presence references missing doc "
                          "table entry",
                          NULL, NULL, NULL);
        break;
      }
      last_presence_key_hex = presence->key_hex;
    }
    if (rc == LC_OK) {
      unsigned long presence_term_id;

      presence_term_id = 0UL;
      rc = lc_pouch_query_index_term_find_or_add_cached(
          summary->allocator, &presence_generation, &presence_term_cache,
          presence->field_hex, "-", 'z', &presence_term_id, error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            summary->allocator, &presence_accumulator, presence_term_id, doc_id,
            error);
      }
    }
    if (rc != LC_OK) {
      break;
    }
    if (rc == LC_OK) {
      ++presence_line_count;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &presence_accumulator, &presence_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &range_accumulator, &range_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &text_accumulator, &text_generation, error);
  }
  if (rc == LC_OK && use_sorted_trigrams) {
    rc = lc_pouch_query_index_trigram_generation_build_sorted(
        summary->allocator, summary, &trigram_generation, error);
  }
  if (rc == LC_OK && !use_sorted_trigrams) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &trigram_accumulator, &trigram_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &temporal_accumulator, &temporal_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_doc_table_generation_encode(
        &doc_table, index_seq, row_hash, summary->allocator,
        doc_table_generation_out, doc_table_generation_length_out, error);
  }
  if (rc == LC_OK) {
    exact_generation.index_seq = index_seq;
    exact_generation.row_count = (unsigned long)summary->count;
    exact_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &exact_generation, summary->allocator, exact_term_generation_out,
        exact_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    presence_generation.index_seq = index_seq;
    presence_generation.row_count = (unsigned long)summary->count;
    presence_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &presence_generation, summary->allocator, presence_term_generation_out,
        presence_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    range_generation.index_seq = index_seq;
    range_generation.row_count = (unsigned long)summary->count;
    range_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &range_generation, summary->allocator, range_term_generation_out,
        range_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    text_generation.index_seq = index_seq;
    text_generation.row_count = (unsigned long)summary->count;
    text_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &text_generation, summary->allocator, text_term_generation_out,
        text_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    trigram_generation.index_seq = index_seq;
    trigram_generation.row_count = (unsigned long)summary->count;
    trigram_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &trigram_generation, summary->allocator, trigram_term_generation_out,
        trigram_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    temporal_generation.index_seq = index_seq;
    temporal_generation.row_count = (unsigned long)summary->count;
    temporal_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &temporal_generation, summary->allocator, temporal_term_generation_out,
        temporal_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    *row_hash_out = row_hash;
    *term_count_out = term_line_count;
    *term_hash_out = term_hash;
    *term_field_count_out = term_field_line_count;
    *term_value_count_out = term_value_line_count;
    *presence_count_out = presence_line_count;
    *presence_hash_out = presence_hash;
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(summary->allocator, *doc_table_generation_out);
    *doc_table_generation_out = NULL;
    *doc_table_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *exact_term_generation_out);
    *exact_term_generation_out = NULL;
    *exact_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *presence_term_generation_out);
    *presence_term_generation_out = NULL;
    *presence_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *range_term_generation_out);
    *range_term_generation_out = NULL;
    *range_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *text_term_generation_out);
    *text_term_generation_out = NULL;
    *text_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *trigram_term_generation_out);
    *trigram_term_generation_out = NULL;
    *trigram_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *temporal_term_generation_out);
    *temporal_term_generation_out = NULL;
    *temporal_term_generation_length_out = 0U;
  }
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &exact_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &presence_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &range_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &text_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &trigram_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &temporal_accumulator);
  lc_pouch_query_index_trigram_term_cache_cleanup(summary->allocator,
                                                  &trigram_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &temporal_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator, &text_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &range_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &presence_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &exact_term_cache);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &exact_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &presence_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &range_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &text_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &trigram_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &temporal_generation);
  lc_pouch_query_index_field_cache_cleanup(summary->allocator,
                                           &trigram_field_cache);
  lc_pouch_index_doc_table_cleanup(summary->allocator, &doc_table);
  return rc;
}

static int lc_pouch_query_index_memtable_generation_init(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    const char *ns, lc_error *error) {
  generation->ns = lc_strdup_with_allocator(allocator, ns);
  if (generation->ns == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index memtable namespace",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

static lc_pouch_query_index_memtable *
lc_pouch_query_index_memtable_new(const lc_allocator *allocator, const char *ns,
                                  lc_error *error) {
  lc_pouch_query_index_memtable *memtable;
  int rc;

  memtable = (lc_pouch_query_index_memtable *)lc_calloc_with_allocator(
      allocator, 1U, sizeof(*memtable));
  if (memtable == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch index memtable", NULL, NULL,
                 "pouch");
    return NULL;
  }
  rc = lc_pouch_query_index_memtable_generation_init(
      allocator, &memtable->exact_generation, ns, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_generation_init(
        allocator, &memtable->presence_generation, ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_generation_init(
        allocator, &memtable->range_generation, ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_generation_init(
        allocator, &memtable->text_generation, ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_generation_init(
        allocator, &memtable->trigram_generation, ns, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_generation_init(
        allocator, &memtable->temporal_generation, ns, error);
  }
  if (rc != LC_OK) {
    lc_pouch_query_index_memtable_cleanup(allocator, memtable);
    lc_free_with_allocator(allocator, memtable);
    return NULL;
  }
  return memtable;
}

static void
lc_pouch_query_index_memtable_cleanup(const lc_allocator *allocator,
                                      lc_pouch_query_index_memtable *memtable) {
  if (memtable == NULL) {
    return;
  }
  lc_pouch_index_doc_table_cleanup(allocator, &memtable->doc_table);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &memtable->exact_generation);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &memtable->presence_generation);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &memtable->range_generation);
  lc_pouch_index_term_generation_cleanup(allocator, &memtable->text_generation);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &memtable->trigram_generation);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &memtable->temporal_generation);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &memtable->exact_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &memtable->presence_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &memtable->range_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &memtable->text_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &memtable->trigram_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &memtable->temporal_accumulator);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &memtable->exact_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &memtable->presence_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &memtable->range_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &memtable->text_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &memtable->temporal_term_cache);
  lc_pouch_query_index_field_cache_cleanup(allocator,
                                           &memtable->trigram_field_cache);
  lc_pouch_query_index_trigram_term_cache_cleanup(
      allocator, &memtable->trigram_term_cache);
  memset(memtable, 0, sizeof(*memtable));
}

static void lc_pouch_query_index_memtable_remove_posting(
    lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long doc_id) {
  unsigned long hash;
  unsigned long term_id;

  if (generation == NULL || accumulator == NULL || cache == NULL ||
      field_hex == NULL || value_hex == NULL) {
    return;
  }
  hash = lc_pouch_query_index_term_cache_hash(field_hex, value_hex, value_type);
  term_id = 0UL;
  if (lc_pouch_query_index_term_cache_find(cache, field_hex, value_hex,
                                           value_type, hash, &term_id)) {
    lc_pouch_query_index_exact_generation_accumulator_remove(accumulator,
                                                             term_id, doc_id);
  }
  (void)generation;
}

static int lc_pouch_query_index_memtable_add_term(
    const lc_allocator *allocator, lc_pouch_query_index_memtable *memtable,
    lc_pouch_query_index_term *term, unsigned long doc_id, lc_error *error) {
  char *allocated_exact_value_hex;
  const char *trigram_field_hex;
  const char *exact_value_hex;
  const char *indexed_exact_value_hex;
  unsigned long trigram_field_hash;
  unsigned long trigram_field_id;
  char indexed_exact_value_type;
  size_t index;
  int rc;

  if (allocator == NULL || memtable == NULL || term == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable add requires a term", NULL, NULL,
                        "pouch");
  }
  allocated_exact_value_hex = NULL;
  trigram_field_hex = NULL;
  exact_value_hex = NULL;
  indexed_exact_value_hex = NULL;
  trigram_field_hash = 0UL;
  trigram_field_id = 0UL;
  indexed_exact_value_type = term->value_type;
  rc = LC_OK;
  if (term->value_type == 's' &&
      (term->long_value != NULL || term->text_prefix_hex != NULL)) {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
    indexed_exact_value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
  } else if (term->value_type == 'n') {
    if (term->canonical_value_hex != NULL) {
      exact_value_hex = term->canonical_value_hex;
      indexed_exact_value_hex = term->canonical_value_hex;
    } else {
      rc = lc_pouch_query_index_exact_generation_value_hex(
          allocator, term->value_hex, term->value_type,
          &allocated_exact_value_hex, error);
      exact_value_hex = allocated_exact_value_hex;
      indexed_exact_value_hex = allocated_exact_value_hex;
    }
  } else {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
  }
  if (rc == LC_OK) {
    unsigned long term_id;

    term_id = 0UL;
    rc = lc_pouch_query_index_term_find_or_add_cached(
        allocator, &memtable->exact_generation, &memtable->exact_term_cache,
        term->field_hex, indexed_exact_value_hex, indexed_exact_value_type,
        &term_id, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_exact_generation_accumulator_append(
          allocator, &memtable->exact_accumulator, term_id, doc_id, error);
    }
  }
  if (rc == LC_OK && term->value_type == 'n') {
    unsigned long term_id;

    term_id = 0UL;
    rc = lc_pouch_query_index_term_find_or_add_cached(
        allocator, &memtable->range_generation, &memtable->range_term_cache,
        term->field_hex, exact_value_hex, term->value_type, &term_id, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_exact_generation_accumulator_append(
          allocator, &memtable->range_accumulator, term_id, doc_id, error);
    }
  }
  if (rc == LC_OK && term->value_type == 's') {
    if (!term->derived_terms_ready) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable requires derived string terms",
                        NULL, NULL, "pouch");
    } else if (term->text_prefix_hex != NULL) {
      rc = lc_pouch_query_index_text_generation_add_term(
          allocator, &memtable->text_generation, &memtable->text_accumulator,
          &memtable->text_term_cache, term->field_hex, term->text_prefix_hex,
          LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id, error);
    } else {
      rc = lc_pouch_query_index_text_generation_add_value(
          allocator, &memtable->text_generation, &memtable->text_accumulator,
          &memtable->text_term_cache, term->field_hex, exact_value_hex, doc_id,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_term_add_trigram_fallback(allocator, term,
                                                          error);
    }
    if (rc == LC_OK && term->trigram_key_count > 0U) {
      rc = lc_pouch_query_index_field_cache_find_or_add(
          allocator, &memtable->trigram_field_cache, term->field_hex,
          &trigram_field_id, &trigram_field_hash, &trigram_field_hex, error);
    }
    for (index = 0U; rc == LC_OK && index < term->trigram_key_count; ++index) {
      rc = lc_pouch_query_index_trigram_generation_append_docid_for_field(
          allocator, &memtable->trigram_generation,
          &memtable->trigram_accumulator, &memtable->trigram_term_cache,
          trigram_field_hex, trigram_field_id, trigram_field_hash,
          term->trigram_keys[index], doc_id, error);
    }
    if (rc == LC_OK && term->temporal_value_hex != NULL) {
      unsigned long term_id;

      term_id = 0UL;
      rc = lc_pouch_query_index_term_find_or_add_cached(
          allocator, &memtable->temporal_generation,
          &memtable->temporal_term_cache, term->field_hex,
          term->temporal_value_hex, 'n', &term_id, error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            allocator, &memtable->temporal_accumulator, term_id, doc_id, error);
      }
    }
  }
  lc_free_with_allocator(allocator, allocated_exact_value_hex);
  return rc;
}

static int lc_pouch_query_index_memtable_remove_term(
    const lc_allocator *allocator, lc_pouch_query_index_memtable *memtable,
    lc_pouch_query_index_term *term, unsigned long doc_id, lc_error *error) {
  char *allocated_exact_value_hex;
  const char *trigram_field_hex;
  const char *exact_value_hex;
  const char *indexed_exact_value_hex;
  unsigned long trigram_field_hash;
  unsigned long trigram_field_id;
  char indexed_exact_value_type;
  size_t index;
  int rc;

  if (allocator == NULL || memtable == NULL || term == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable remove requires a term", NULL,
                        NULL, "pouch");
  }
  allocated_exact_value_hex = NULL;
  trigram_field_hex = NULL;
  exact_value_hex = NULL;
  indexed_exact_value_hex = NULL;
  trigram_field_hash = 0UL;
  trigram_field_id = 0UL;
  indexed_exact_value_type = term->value_type;
  rc = LC_OK;
  if (term->value_type == 's' &&
      (term->long_value != NULL || term->text_prefix_hex != NULL)) {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
    indexed_exact_value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
  } else if (term->value_type == 'n') {
    if (term->canonical_value_hex != NULL) {
      exact_value_hex = term->canonical_value_hex;
      indexed_exact_value_hex = term->canonical_value_hex;
    } else {
      rc = lc_pouch_query_index_exact_generation_value_hex(
          allocator, term->value_hex, term->value_type,
          &allocated_exact_value_hex, error);
      exact_value_hex = allocated_exact_value_hex;
      indexed_exact_value_hex = allocated_exact_value_hex;
    }
  } else {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_memtable_remove_posting(
        &memtable->exact_generation, &memtable->exact_accumulator,
        &memtable->exact_term_cache, term->field_hex, indexed_exact_value_hex,
        indexed_exact_value_type, doc_id);
    if (term->value_type == 'n') {
      lc_pouch_query_index_memtable_remove_posting(
          &memtable->range_generation, &memtable->range_accumulator,
          &memtable->range_term_cache, term->field_hex, exact_value_hex,
          term->value_type, doc_id);
    }
    if (term->value_type == 's' && term->derived_terms_ready) {
      if (term->text_prefix_hex != NULL) {
        lc_pouch_query_index_memtable_remove_posting(
            &memtable->text_generation, &memtable->text_accumulator,
            &memtable->text_term_cache, term->field_hex, term->text_prefix_hex,
            LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id);
      } else {
        lc_pouch_query_index_memtable_remove_posting(
            &memtable->text_generation, &memtable->text_accumulator,
            &memtable->text_term_cache, term->field_hex, exact_value_hex, 's',
            doc_id);
      }
      rc = lc_pouch_query_index_term_add_trigram_fallback(allocator, term,
                                                          error);
      if (rc == LC_OK && term->trigram_key_count > 0U) {
        rc = lc_pouch_query_index_field_cache_find_or_add(
            allocator, &memtable->trigram_field_cache, term->field_hex,
            &trigram_field_id, &trigram_field_hash, &trigram_field_hex, error);
      }
      for (index = 0U; rc == LC_OK && index < term->trigram_key_count;
           ++index) {
        lc_pouch_query_index_trigram_generation_remove_docid_for_field(
            &memtable->trigram_accumulator, &memtable->trigram_term_cache,
            trigram_field_hex, trigram_field_id, trigram_field_hash,
            term->trigram_keys[index], doc_id);
      }
      if (rc == LC_OK && term->temporal_value_hex != NULL) {
        lc_pouch_query_index_memtable_remove_posting(
            &memtable->temporal_generation, &memtable->temporal_accumulator,
            &memtable->temporal_term_cache, term->field_hex,
            term->temporal_value_hex, 'n', doc_id);
      }
    }
  }
  lc_free_with_allocator(allocator, allocated_exact_value_hex);
  return rc;
}

static int lc_pouch_query_index_memtable_apply_terms(
    const lc_allocator *allocator, lc_pouch_query_index_memtable *memtable,
    lc_pouch_query_index_summary *summary, const char *key_hex,
    unsigned long doc_id, int add, lc_error *error) {
  size_t index;
  int rc;

  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < summary->term_count; ++index) {
    lc_pouch_query_index_term *term;

    term = &summary->terms[index];
    if (strcmp(term->key_hex, key_hex) != 0) {
      continue;
    }
    rc = add ? lc_pouch_query_index_memtable_add_term(allocator, memtable, term,
                                                      doc_id, error)
             : lc_pouch_query_index_memtable_remove_term(allocator, memtable,
                                                         term, doc_id, error);
  }
  return rc;
}

static int lc_pouch_query_index_memtable_apply_presences(
    const lc_allocator *allocator, lc_pouch_query_index_memtable *memtable,
    const lc_pouch_query_index_summary *summary, const char *key_hex,
    unsigned long doc_id, int add, lc_error *error) {
  size_t index;
  int rc;

  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < summary->presence_count; ++index) {
    const lc_pouch_query_index_presence *presence;
    unsigned long term_id;

    presence = &summary->presences[index];
    if (strcmp(presence->key_hex, key_hex) != 0) {
      continue;
    }
    if (!add) {
      lc_pouch_query_index_memtable_remove_posting(
          &memtable->presence_generation, &memtable->presence_accumulator,
          &memtable->presence_term_cache, presence->field_hex, "-", 'z',
          doc_id);
      continue;
    }
    term_id = 0UL;
    rc = lc_pouch_query_index_term_find_or_add_cached(
        allocator, &memtable->presence_generation,
        &memtable->presence_term_cache, presence->field_hex, "-", 'z', &term_id,
        error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_exact_generation_accumulator_append(
          allocator, &memtable->presence_accumulator, term_id, doc_id, error);
    }
  }
  return rc;
}

static int lc_pouch_query_index_memtable_append_summary(
    const lc_allocator *allocator, lc_pouch_query_index_memtable *memtable,
    lc_pouch_query_index_summary *summary, lc_error *error) {
  lc_pouch_query_index_row *row;
  unsigned long doc_id;
  int rc;

  if (summary == NULL || summary->count != 1U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable requires one pending row", NULL,
                        NULL, "pouch");
  }
  row = &summary->rows[0];
  doc_id = 0UL;
  rc = lc_pouch_index_doc_table_append_owned(
      &memtable->doc_table, row->key_hex, row->version, row->bytes,
      row->has_query_hidden, row->query_hidden, &doc_id, allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_apply_terms(
        allocator, memtable, summary, row->key_hex, doc_id, 1, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_apply_presences(
        allocator, memtable, summary, row->key_hex, doc_id, 1, error);
  }
  return rc;
}

static int lc_pouch_query_index_memtable_add_deferred_rows(
    const lc_allocator *allocator, const char *ns,
    lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_memtable **memtable_io, lc_error *error) {
  size_t index;
  int rc;

  if (summary == NULL || memtable_io == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index deferred rows require summary and "
                        "memtable",
                        NULL, NULL, "pouch");
  }
  if (*memtable_io == NULL) {
    *memtable_io = lc_pouch_query_index_memtable_new(allocator, ns, error);
    if (*memtable_io == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < summary->count; ++index) {
    lc_pouch_query_index_summary one_row;

    if (!summary->rows[index].needs_extraction) {
      continue;
    }
    memset(&one_row, 0, sizeof(one_row));
    one_row.allocator = summary->allocator;
    one_row.rows = &summary->rows[index];
    one_row.count = 1U;
    one_row.terms = summary->terms;
    one_row.term_count = summary->term_count;
    one_row.presences = summary->presences;
    one_row.presence_count = summary->presence_count;
    rc = lc_pouch_query_index_memtable_append_summary(allocator, *memtable_io,
                                                      &one_row, error);
    if (rc == LC_OK) {
      summary->rows[index].needs_extraction = 0;
    }
  }
  return rc;
}

static int lc_pouch_query_index_memtable_replace_summary(
    const lc_allocator *allocator, lc_pouch_query_index_memtable *memtable,
    lc_pouch_query_index_summary *old_summary,
    lc_pouch_query_index_summary *new_summary, const char *key_hex,
    lc_error *error) {
  lc_pouch_index_doc *doc;
  lc_pouch_query_index_row *row;
  unsigned long doc_id;
  int found;
  int rc;

  if (new_summary == NULL || new_summary->count != 1U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable replacement requires one row",
                        NULL, NULL, "pouch");
  }
  doc_id = 0UL;
  found = 0;
  rc = lc_pouch_index_doc_table_find_key_hex(&memtable->doc_table, key_hex,
                                             &doc_id, &found, error);
  if (rc != LC_OK || !found ||
      doc_id >= (unsigned long)memtable->doc_table.count) {
    return rc != LC_OK
               ? rc
               : lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch index memtable replacement is missing "
                              "its document",
                              NULL, NULL, "pouch");
  }
  rc = lc_pouch_query_index_memtable_apply_terms(
      allocator, memtable, old_summary, key_hex, doc_id, 0, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_apply_presences(
        allocator, memtable, old_summary, key_hex, doc_id, 0, error);
  }
  if (rc == LC_OK) {
    row = &new_summary->rows[0];
    doc = &memtable->doc_table.items[doc_id];
    doc->version = row->version;
    doc->bytes = row->bytes;
    doc->has_query_hidden = row->has_query_hidden ? 1 : 0;
    doc->query_hidden = row->query_hidden ? 1 : 0;
    rc = lc_pouch_query_index_memtable_apply_terms(
        allocator, memtable, new_summary, row->key_hex, doc_id, 1, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_apply_presences(
        allocator, memtable, new_summary, new_summary->rows[0].key_hex, doc_id,
        1, error);
  }
  return rc;
}

static int lc_pouch_query_index_memtable_note_write(
    lc_pouch_query_index_pending_entry *pending,
    lc_pouch_query_index_summary *update, const char *key_hex,
    lc_error *error) {
  lc_pouch_query_index_row *previous_row;

  if (pending == NULL || update == NULL || key_hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable write requires pending data",
                        NULL, NULL, "pouch");
  }
  if (pending->memtable == NULL) {
    pending->memtable = lc_pouch_query_index_memtable_new(
        pending->summary.allocator, pending->ns, error);
    if (pending->memtable == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  previous_row = lc_pouch_query_index_summary_find_row(&pending->summary,
                                                       update->rows[0].key);
  if (previous_row == NULL || previous_row->needs_extraction) {
    return lc_pouch_query_index_memtable_append_summary(
        pending->summary.allocator, pending->memtable, update, error);
  }
  (void)previous_row;
  return lc_pouch_query_index_memtable_replace_summary(
      pending->summary.allocator, pending->memtable, &pending->summary, update,
      key_hex, error);
}

static int lc_pouch_query_index_memtable_note_delete(
    lc_pouch_query_index_pending_entry *pending, const char *key_hex,
    lc_error *error) {
  lc_pouch_query_index_row *row;

  if (pending == NULL || key_hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable delete requires pending data",
                        NULL, NULL, "pouch");
  }
  row = NULL;
  for (row = pending->summary.rows;
       row != NULL && row < pending->summary.rows + pending->summary.count;
       ++row) {
    if (strcmp(row->key_hex, key_hex) == 0) {
      break;
    }
  }
  if (row == NULL || row >= pending->summary.rows + pending->summary.count) {
    return LC_OK;
  }
  /* Removing a local doc changes every later docID. Leave this uncommon
   * shape to the durable-replay builder rather than weakening posting IDs. */
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index memtable delete requires rebuild", NULL,
                      NULL, "pouch");
}

static int lc_pouch_query_index_memtable_tail_compatible(
    lc_pouch_query_index_memtable *memtable,
    const lc_pouch_query_index_summary *summary,
    const lc_pouch_query_index_incremental_context *incremental) {
  size_t index;

  if (memtable == NULL || summary == NULL || incremental == NULL ||
      memtable->doc_table.count != summary->count) {
    return 0;
  }
  for (index = 0U; index < incremental->change_count; ++index) {
    const lc_pouch_query_index_incremental_change *change;

    change = &incremental->changes[index];
    if (!change->found || !change->preserves_cached_body) {
      return 0;
    }
  }
  for (index = 0U; index < summary->count; ++index) {
    const lc_pouch_query_index_row *row;
    lc_pouch_index_doc *doc;
    unsigned long doc_id;
    int found;
    lc_error ignored;

    row = &summary->rows[index];
    doc_id = 0UL;
    found = 0;
    lc_error_init(&ignored);
    if (lc_pouch_index_doc_table_find_key_hex(&memtable->doc_table,
                                              row->key_hex, &doc_id, &found,
                                              &ignored) != LC_OK ||
        !found || doc_id >= (unsigned long)memtable->doc_table.count) {
      lc_error_cleanup(&ignored);
      return 0;
    }
    lc_error_cleanup(&ignored);
    doc = &memtable->doc_table.items[doc_id];
    doc->version = row->version;
    doc->bytes = row->bytes;
    doc->has_query_hidden = row->has_query_hidden ? 1 : 0;
    doc->query_hidden = row->query_hidden ? 1 : 0;
  }
  return 1;
}

static int
lc_pouch_query_index_memtable_hash_rows(lc_pouch_query_index_summary *summary,
                                        unsigned long *row_hash_out,
                                        lc_error *error) {
  char line[256];
  unsigned long row_hash;
  size_t index;
  int rc;

  if (summary == NULL || row_hash_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable row hash requires outputs", NULL,
                        NULL, "pouch");
  }
  if (summary->count > 1U) {
    qsort(summary->rows, summary->count, sizeof(summary->rows[0]),
          lc_pouch_query_index_row_compare);
  }
  row_hash = lc_pouch_query_index_hash_init();
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < summary->count; ++index) {
    const lc_pouch_query_index_row *row;
    char version_text[32];
    char bytes_text[32];
    int written;

    row = &summary->rows[index];
    if (lc_u64_format_base10((lc_u64)row->version, version_text,
                             sizeof(version_text)) < 0 ||
        lc_u64_format_base10((lc_u64)row->bytes, bytes_text,
                             sizeof(bytes_text)) < 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index memtable row exceeds local limit", NULL,
                          NULL, "pouch");
    }
    written = snprintf(line, sizeof(line), "row %s %s %d %d ", version_text,
                       bytes_text, row->has_query_hidden ? 1 : 0,
                       row->query_hidden ? 1 : 0);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index memtable row exceeds local limit", NULL,
                          NULL, "pouch");
    }
    rc = lc_pouch_query_index_text_append(NULL, line, (size_t)written,
                                          &row_hash, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->key_hex, &row_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->content_type_hex,
                                                 &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->etag_hex, &row_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, "\n", &row_hash, error);
    }
  }
  if (rc == LC_OK) {
    *row_hash_out = row_hash;
  }
  return rc;
}

static int lc_pouch_query_index_memtable_encode_generation(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_generation index_seq, unsigned long row_count,
    unsigned long row_hash, char **out, size_t *out_length, lc_error *error) {
  generation->index_seq = index_seq;
  generation->row_count = row_count;
  generation->row_hash = row_hash;
  return lc_pouch_index_term_generation_encode(generation, allocator, out,
                                               out_length, error);
}

static int
lc_pouch_query_index_memtable_trigram_term_compare(const void *left,
                                                   const void *right) {
  const lc_pouch_index_term_entry *a;
  const lc_pouch_index_term_entry *b;
  int field_compare;

  a = (const lc_pouch_index_term_entry *)left;
  b = (const lc_pouch_index_term_entry *)right;
  field_compare = strcmp(a->field_hex, b->field_hex);
  if (field_compare != 0) {
    return field_compare;
  }
  if (a->trigram_key < b->trigram_key) {
    return -1;
  }
  if (a->trigram_key > b->trigram_key) {
    return 1;
  }
  if (a->term_id < b->term_id) {
    return -1;
  }
  if (a->term_id > b->term_id) {
    return 1;
  }
  return 0;
}

static int lc_pouch_query_index_memtable_encode(
    lc_pouch_query_index_memtable *memtable, lc_pouch_generation index_seq,
    lc_pouch_query_index_summary *summary, unsigned long *row_hash_out,
    unsigned long *term_count_out, unsigned long *term_hash_out,
    unsigned long *term_field_count_out, unsigned long *term_value_count_out,
    unsigned long *presence_count_out, unsigned long *presence_hash_out,
    char **doc_table_generation_out, size_t *doc_table_generation_length_out,
    char **exact_term_generation_out, size_t *exact_term_generation_length_out,
    char **presence_term_generation_out,
    size_t *presence_term_generation_length_out,
    char **range_term_generation_out, size_t *range_term_generation_length_out,
    char **text_term_generation_out, size_t *text_term_generation_length_out,
    char **trigram_term_generation_out,
    size_t *trigram_term_generation_length_out,
    char **temporal_term_generation_out,
    size_t *temporal_term_generation_length_out, lc_error *error) {
  unsigned long row_hash;
  unsigned long row_count;
  int rc;

  if (memtable == NULL || summary == NULL || row_hash_out == NULL ||
      term_count_out == NULL || term_hash_out == NULL ||
      term_field_count_out == NULL || term_value_count_out == NULL ||
      presence_count_out == NULL || presence_hash_out == NULL ||
      doc_table_generation_out == NULL ||
      doc_table_generation_length_out == NULL ||
      exact_term_generation_out == NULL ||
      exact_term_generation_length_out == NULL ||
      presence_term_generation_out == NULL ||
      presence_term_generation_length_out == NULL ||
      range_term_generation_out == NULL ||
      range_term_generation_length_out == NULL ||
      text_term_generation_out == NULL ||
      text_term_generation_length_out == NULL ||
      trigram_term_generation_out == NULL ||
      trigram_term_generation_length_out == NULL ||
      temporal_term_generation_out == NULL ||
      temporal_term_generation_length_out == NULL ||
      memtable->doc_table.count != summary->count ||
      summary->term_count > (size_t)ULONG_MAX ||
      summary->presence_count > (size_t)ULONG_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index memtable encode requires a complete "
                        "projection",
                        NULL, NULL, "pouch");
  }
  *doc_table_generation_out = NULL;
  *doc_table_generation_length_out = 0U;
  *exact_term_generation_out = NULL;
  *exact_term_generation_length_out = 0U;
  *presence_term_generation_out = NULL;
  *presence_term_generation_length_out = 0U;
  *range_term_generation_out = NULL;
  *range_term_generation_length_out = 0U;
  *text_term_generation_out = NULL;
  *text_term_generation_length_out = 0U;
  *trigram_term_generation_out = NULL;
  *trigram_term_generation_length_out = 0U;
  *temporal_term_generation_out = NULL;
  *temporal_term_generation_length_out = 0U;
  row_hash = lc_pouch_query_index_hash_init();
  row_count = (unsigned long)summary->count;
  rc = lc_pouch_query_index_memtable_hash_rows(summary, &row_hash, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &memtable->exact_accumulator,
        &memtable->exact_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &memtable->presence_accumulator,
        &memtable->presence_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &memtable->range_accumulator,
        &memtable->range_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &memtable->text_accumulator,
        &memtable->text_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &memtable->trigram_accumulator,
        &memtable->trigram_generation, error);
  }
  if (rc == LC_OK) {
    lc_pouch_index_term_generation_sort_terms(&memtable->exact_generation);
    lc_pouch_index_term_generation_sort_terms(&memtable->presence_generation);
    lc_pouch_index_term_generation_sort_terms(&memtable->range_generation);
    lc_pouch_index_term_generation_sort_terms(&memtable->text_generation);
    if (memtable->trigram_generation.terms.count > 1U) {
      qsort(memtable->trigram_generation.terms.items,
            memtable->trigram_generation.terms.count,
            sizeof(memtable->trigram_generation.terms.items[0]),
            lc_pouch_query_index_memtable_trigram_term_compare);
    }
    /* The packed generation encoder requires lexical field/trigram order. */
    memtable->trigram_generation.terms_sorted = 1;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &memtable->temporal_accumulator,
        &memtable->temporal_generation, error);
  }
  if (rc == LC_OK) {
    lc_pouch_index_term_generation_sort_terms(&memtable->temporal_generation);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_doc_table_generation_encode(
        &memtable->doc_table, index_seq, row_hash, summary->allocator,
        doc_table_generation_out, doc_table_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_encode_generation(
        summary->allocator, &memtable->exact_generation, index_seq, row_count,
        row_hash, exact_term_generation_out, exact_term_generation_length_out,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_encode_generation(
        summary->allocator, &memtable->presence_generation, index_seq,
        row_count, row_hash, presence_term_generation_out,
        presence_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_encode_generation(
        summary->allocator, &memtable->range_generation, index_seq, row_count,
        row_hash, range_term_generation_out, range_term_generation_length_out,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_encode_generation(
        summary->allocator, &memtable->text_generation, index_seq, row_count,
        row_hash, text_term_generation_out, text_term_generation_length_out,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_encode_generation(
        summary->allocator, &memtable->trigram_generation, index_seq, row_count,
        row_hash, trigram_term_generation_out,
        trigram_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_memtable_encode_generation(
        summary->allocator, &memtable->temporal_generation, index_seq,
        row_count, row_hash, temporal_term_generation_out,
        temporal_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    *row_hash_out = row_hash;
    *term_count_out = (unsigned long)summary->term_count;
    *term_hash_out = lc_pouch_query_index_hash_init();
    *term_field_count_out = 0UL;
    *term_value_count_out = 0UL;
    *presence_count_out = (unsigned long)summary->presence_count;
    *presence_hash_out = lc_pouch_query_index_hash_init();
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(summary->allocator, *doc_table_generation_out);
    *doc_table_generation_out = NULL;
    *doc_table_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *exact_term_generation_out);
    *exact_term_generation_out = NULL;
    *exact_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *presence_term_generation_out);
    *presence_term_generation_out = NULL;
    *presence_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *range_term_generation_out);
    *range_term_generation_out = NULL;
    *range_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *text_term_generation_out);
    *text_term_generation_out = NULL;
    *text_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *trigram_term_generation_out);
    *trigram_term_generation_out = NULL;
    *trigram_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *temporal_term_generation_out);
    *temporal_term_generation_out = NULL;
    *temporal_term_generation_length_out = 0U;
  }
  return rc;
}

static int lc_pouch_query_index_segmented_paths(
    lc_pouch *pouch, const char *ns, const char *segment_id, char **header_path,
    char **doc_table_path, char **exact_term_path, char **presence_term_path,
    char **range_term_path, char **text_term_path, char **trigram_term_path,
    char **temporal_term_path, char **delete_path, lc_error *error) {
  if (header_path == NULL || doc_table_path == NULL ||
      exact_term_path == NULL || presence_term_path == NULL ||
      range_term_path == NULL || text_term_path == NULL ||
      trigram_term_path == NULL || temporal_term_path == NULL ||
      delete_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment paths require outputs", NULL,
                        NULL, NULL);
  }
  *header_path = lc_pouch_query_index_segment_artifact_path(
      pouch, ns, segment_id, LC_POUCH_QUERY_INDEX_LEAF, error);
  if (*header_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  *doc_table_path = lc_pouch_query_index_segment_artifact_path(
      pouch, ns, segment_id, LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF, error);
  *exact_term_path = *doc_table_path != NULL
                         ? lc_pouch_query_index_segment_artifact_path(
                               pouch, ns, segment_id,
                               LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF, error)
                         : NULL;
  *presence_term_path =
      *exact_term_path != NULL
          ? lc_pouch_query_index_segment_artifact_path(
                pouch, ns, segment_id, LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF,
                error)
          : NULL;
  *range_term_path = *presence_term_path != NULL
                         ? lc_pouch_query_index_segment_artifact_path(
                               pouch, ns, segment_id,
                               LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF, error)
                         : NULL;
  *text_term_path = *range_term_path != NULL
                        ? lc_pouch_query_index_segment_artifact_path(
                              pouch, ns, segment_id,
                              LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF, error)
                        : NULL;
  *trigram_term_path = *text_term_path != NULL
                           ? lc_pouch_query_index_segment_artifact_path(
                                 pouch, ns, segment_id,
                                 LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF, error)
                           : NULL;
  *temporal_term_path =
      *trigram_term_path != NULL
          ? lc_pouch_query_index_segment_artifact_path(
                pouch, ns, segment_id, LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF,
                error)
          : NULL;
  *delete_path =
      *temporal_term_path != NULL
          ? lc_pouch_query_index_segment_artifact_path(
                pouch, ns, segment_id, LC_POUCH_QUERY_INDEX_DELETE_LEAF, error)
          : NULL;
  if (*doc_table_path == NULL || *exact_term_path == NULL ||
      *presence_term_path == NULL || *range_term_path == NULL ||
      *text_term_path == NULL || *trigram_term_path == NULL ||
      *temporal_term_path == NULL || *delete_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  return LC_OK;
}

static void lc_pouch_query_index_segmented_paths_cleanup(
    lc_pouch *pouch, char **header_path, char **doc_table_path,
    char **exact_term_path, char **presence_term_path, char **range_term_path,
    char **text_term_path, char **trigram_term_path, char **temporal_term_path,
    char **delete_path) {
  if (pouch == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator,
                         delete_path != NULL ? *delete_path : NULL);
  lc_free_with_allocator(&pouch->allocator, temporal_term_path != NULL
                                                ? *temporal_term_path
                                                : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         trigram_term_path != NULL ? *trigram_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         text_term_path != NULL ? *text_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         range_term_path != NULL ? *range_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator, presence_term_path != NULL
                                                ? *presence_term_path
                                                : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         exact_term_path != NULL ? *exact_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         doc_table_path != NULL ? *doc_table_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         header_path != NULL ? *header_path : NULL);
}

static int lc_pouch_query_index_manifest_segments_validate(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_query_index_manifest *manifest, int *valid_out,
    lc_error *error) {
  size_t segment_index;
  int rc;

  if (pouch == NULL || ns == NULL || manifest == NULL || valid_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest validation requires "
                        "pouch, namespace, manifest, and output",
                        NULL, NULL, NULL);
  }
  *valid_out = 1;
  rc = LC_OK;
  for (segment_index = 0U;
       rc == LC_OK && segment_index < manifest->segment_count;
       ++segment_index) {
    const lc_pouch_query_index_manifest_segment *segment;
    char *packed_path;

    segment = &manifest->segments[segment_index];
    packed_path = lc_pouch_query_index_segment_artifact_path(
        pouch, ns, segment->id, LC_POUCH_QUERY_INDEX_PACKED_LEAF, error);
    if (packed_path == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else if (!segment->term_index_complete ||
               !segment->presence_index_complete ||
               !lc_pouch_query_index_manifest_segment_artifacts_match(
                   segment, packed_path, NULL)) {
      *valid_out = 0;
    }
    lc_free_with_allocator(&pouch->allocator, packed_path);
  }
  return rc;
}

static int lc_pouch_query_index_flush_segmented(
    lc_pouch *pouch, const char *ns, lc_pouch_generation state_index_seq,
    int validate_existing_segments, int require_completed_operations,
    lc_pouch_query_index_flush_result *out, lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  lc_pouch_query_index_manifest retired_manifest;
  lc_pouch_query_index_summary summary;
  lc_pouch_query_index_extract_batch extract_batch;
  lc_pouch_query_index_incremental_context incremental;
  lc_pouch_query_index_text deletes;
  lc_pouch_query_index_key_hex_set pending_deletes;
  const char **extract_keys;
  size_t *extract_row_indices;
  char segment_id[32];
  char *manifest_path;
  char *doc_table_generation;
  char *exact_term_generation;
  char *presence_term_generation;
  char *range_term_generation;
  char *text_term_generation;
  char *trigram_term_generation;
  char *temporal_term_generation;
  lc_pouch_query_index_memtable *memtable;
  char *packed_path;
  char *packed_generation;
  size_t doc_table_generation_length;
  size_t exact_term_generation_length;
  size_t presence_term_generation_length;
  size_t range_term_generation_length;
  size_t text_term_generation_length;
  size_t trigram_term_generation_length;
  size_t temporal_term_generation_length;
  size_t packed_generation_length;
  size_t extract_count;
  size_t extract_index;
  unsigned long segment_row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long delete_count;
  unsigned long delete_hash;
  lc_pouch_generation pending_last_index_seq;
  lc_pouch_generation segment_base_index_seq;
  lc_pouch_generation trusted_manifest_seq;
  int manifest_segments_valid;
  int manifest_from_trust;
  int manifest_trusted;
  int manifest_current;
  int full_rebuild;
  int cleanup_unreferenced;
  int packed_path_unpublished;
  int pending_taken;
  int pending_deferred_for_active_operation;
  int memtable_usable;
  int query_flush_locked;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_flush requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("index_seq", state_index_seq);
    fields[2] = lc_log_bool_field("validate", validate_existing_segments);
    lc_log_trace(pouch->logger, "index.flush.start", fields, 3U);
  }
  memset(out, 0, sizeof(*out));
  memset(&manifest, 0, sizeof(manifest));
  memset(&retired_manifest, 0, sizeof(retired_manifest));
  memset(&summary, 0, sizeof(summary));
  memset(&extract_batch, 0, sizeof(extract_batch));
  memset(&incremental, 0, sizeof(incremental));
  memset(&deletes, 0, sizeof(deletes));
  memset(&pending_deletes, 0, sizeof(pending_deletes));
  extract_keys = NULL;
  extract_row_indices = NULL;
  manifest_path = NULL;
  doc_table_generation = NULL;
  exact_term_generation = NULL;
  presence_term_generation = NULL;
  range_term_generation = NULL;
  text_term_generation = NULL;
  trigram_term_generation = NULL;
  temporal_term_generation = NULL;
  memtable = NULL;
  packed_path = NULL;
  packed_generation = NULL;
  doc_table_generation_length = 0U;
  exact_term_generation_length = 0U;
  presence_term_generation_length = 0U;
  range_term_generation_length = 0U;
  text_term_generation_length = 0U;
  trigram_term_generation_length = 0U;
  temporal_term_generation_length = 0U;
  packed_generation_length = 0U;
  segment_row_count = 0UL;
  row_hash = lc_pouch_query_index_hash_init();
  delete_hash = lc_pouch_query_index_hash_init();
  pending_last_index_seq = 0UL;
  segment_base_index_seq = 0UL;
  trusted_manifest_seq = 0UL;
  full_rebuild = 0;
  manifest_from_trust = 0;
  cleanup_unreferenced = 0;
  packed_path_unpublished = 0;
  pending_taken = 0;
  pending_deferred_for_active_operation = 0;
  memtable_usable = 0;
  query_flush_locked = 0;
  rc = LC_OK;
  summary.allocator = &pouch->allocator;
  summary.pouch = pouch;
  summary.ns = ns;
  summary.term_index_complete = 1;
  summary.presence_index_complete = 1;
  deletes.allocator = &pouch->allocator;

  rc = lc_pouch_query_index_flush_lock(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  query_flush_locked = 1;

  manifest_path = lc_pouch_query_index_manifest_path(pouch, ns, error);
  if (manifest_path == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  if (!validate_existing_segments && lc_pouch_query_index_manifest_trust_seq(
                                         pouch, ns, &trusted_manifest_seq)) {
    if (lc_pouch_query_index_manifest_trust_snapshot(
            pouch, ns, trusted_manifest_seq, &manifest)) {
      manifest_from_trust = 1;
    }
  }
  if (!manifest_from_trust) {
    rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                            error);
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  manifest_segments_valid = 0;
  manifest_trusted = 0;
  manifest_current = 0;
  if (manifest.present && manifest.valid) {
    manifest_trusted =
        manifest_from_trust || lc_pouch_query_index_manifest_trust_valid(
                                   pouch, ns, manifest.index_seq);
    if (!validate_existing_segments &&
        (manifest_trusted || manifest.index_seq <= state_index_seq)) {
      manifest_segments_valid = 1;
    } else {
      rc = lc_pouch_query_index_manifest_segments_validate(
          pouch, ns, &manifest, &manifest_segments_valid, error);
      if (rc != LC_OK) {
        goto cleanup;
      }
    }
    if (manifest_segments_valid && manifest.index_seq == state_index_seq &&
        manifest.segment_count < LC_POUCH_QUERY_INDEX_MAX_SEGMENTS) {
      manifest_current = 1;
    }
  }
  full_rebuild = !manifest_segments_valid || !manifest.present ||
                 !manifest.valid || manifest.index_seq > state_index_seq ||
                 manifest.segment_count >= LC_POUCH_QUERY_INDEX_MAX_SEGMENTS;
  /* The new index generation is unreachable until this flush installs its
   * manifest. Direct encrypted output is safe for that case: a crash can
   * leave only an unreferenced artifact, which the next publish replaces or
   * reclaims. Rebuilds that may replace a current generation keep rename
   * publication so concurrent readers never observe an in-place rewrite. */
  packed_path_unpublished = !full_rebuild && manifest.present &&
                            manifest.valid &&
                            manifest.index_seq < state_index_seq;
  /* The first manifest cannot reference stale artifacts. Do not turn its
   * foreground publication into a directory sweep; a later validated repair
   * or rebuild from an existing manifest reclaims interrupted bootstrap
   * artifacts. */
  cleanup_unreferenced =
      validate_existing_segments || (full_rebuild && manifest.present);
  if (manifest_current) {
    out->index_seq = state_index_seq;
    goto cleanup;
  }
  if (require_completed_operations &&
      lc_pouch_query_index_namespace_has_active_operations(pouch, ns)) {
    /* Do not substitute a live-state rebuild for a deferred foreground
     * projection: it could publish a namespace while another key remains
     * provisional. */
    goto cleanup;
  }
  pending_taken = lc_pouch_query_index_pending_take(
      pouch, ns, manifest.index_seq, state_index_seq, full_rebuild,
      require_completed_operations, &pending_deferred_for_active_operation,
      &summary, &pending_deletes, &pending_last_index_seq, &memtable);
  if (pending_deferred_for_active_operation) {
    /* A threshold flush is an operation-completion boundary, not a request to
     * rebuild from the live tail. Leave the pending batch intact for the
     * final active key or the ordinary background policy. */
    goto cleanup;
  }
  if (pending_taken) {
    summary.pouch = pouch;
    summary.ns = ns;
    if (full_rebuild) {
      retired_manifest = manifest;
      memset(&manifest, 0, sizeof(manifest));
      segment_base_index_seq = 0UL;
    } else {
      segment_base_index_seq = manifest.index_seq;
    }
    if (pending_last_index_seq < state_index_seq) {
      incremental.summary = &summary;
      rc = lc_pouch_state_visit_since(pouch, ns, pending_last_index_seq,
                                      lc_pouch_query_index_incremental_visit,
                                      &incremental, error);
      if (rc == LC_OK) {
        rc =
            lc_pouch_query_index_incremental_apply_changes(&incremental, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_pending_apply_tail_deletes(
            &pouch->allocator, &incremental, &pending_deletes, error);
      }
      if (rc == LC_OK && memtable != NULL &&
          !lc_pouch_query_index_summary_has_deferred_rows(&summary) &&
          lc_pouch_query_index_memtable_tail_compatible(memtable, &summary,
                                                        &incremental)) {
        memtable_usable = 1;
      } else if (rc == LC_OK) {
        lc_pouch_query_index_memtable_cleanup(&pouch->allocator, memtable);
        lc_free_with_allocator(&pouch->allocator, memtable);
        memtable = NULL;
        lc_pouch_query_index_summary_mark_rows_deferred(&summary);
      }
    } else if (memtable != NULL &&
               !lc_pouch_query_index_summary_has_deferred_rows(&summary)) {
      memtable_usable = 1;
    }
  } else if (full_rebuild) {
    retired_manifest = manifest;
    memset(&manifest, 0, sizeof(manifest));
    segment_base_index_seq = 0UL;
    rc = lc_pouch_state_visit(pouch, ns, lc_pouch_query_index_summary_visit,
                              &summary, error);
  } else {
    segment_base_index_seq = manifest.index_seq;
    incremental.summary = &summary;
    rc = lc_pouch_state_visit_since(pouch, ns, manifest.index_seq,
                                    lc_pouch_query_index_incremental_visit,
                                    &incremental, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_incremental_apply_changes(&incremental, error);
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  /* The exclusive writer's pending memtable is populated while the update
   * source is still live.  Once it covers this exact durable tail, reopening
   * every payload here only repeats parsing and defeats the point of a
   * Lucene-style mutable index.  Fallback and shared-root paths retain the
   * durable extraction pass; memtable_usable is deliberately set only after
   * the tail has been checked for body-changing records. */
  if (!memtable_usable && summary.count > 0U) {
    extract_count = 0U;
    for (extract_index = 0U; extract_index < summary.count; ++extract_index) {
      if (memtable == NULL || summary.rows[extract_index].needs_extraction) {
        ++extract_count;
      }
    }
    if (extract_count == 0U) {
      goto extracted;
    }
    extract_keys = (const char **)lc_calloc_with_allocator(
        &pouch->allocator, extract_count, sizeof(*extract_keys));
    extract_row_indices = (size_t *)lc_calloc_with_allocator(
        &pouch->allocator, extract_count, sizeof(*extract_row_indices));
    if (extract_keys == NULL || extract_row_indices == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index extraction keys",
                        NULL, NULL, NULL);
      goto cleanup;
    }
    extract_count = 0U;
    for (extract_index = 0U; extract_index < summary.count; ++extract_index) {
      if (memtable != NULL && !summary.rows[extract_index].needs_extraction) {
        continue;
      }
      extract_keys[extract_count] = summary.rows[extract_index].key;
      extract_row_indices[extract_count] = extract_index;
      ++extract_count;
    }
    extract_batch.summary = &summary;
    extract_batch.row_indices = extract_row_indices;
    extract_batch.index = 0U;
    if (extract_count > 0U) {
      rc = lc_pouch_state_read_many_cached(
          pouch, ns, extract_keys, extract_count,
          lc_pouch_query_index_extract_batch_visit, &extract_batch, error);
    }
  }
extracted:
  if (rc == LC_OK && pending_taken && !memtable_usable &&
      lc_pouch_query_index_summary_has_deferred_rows(&summary)) {
    rc = lc_pouch_query_index_memtable_add_deferred_rows(
        &pouch->allocator, ns, &summary, &memtable, error);
    if (rc == LC_OK) {
      memtable_usable = 1;
    }
  }
  if (rc == LC_OK && pending_taken) {
    rc = lc_pouch_query_index_encode_delete_set(
        &pending_deletes, &deletes, &pouch->allocator, &delete_count,
        &delete_hash, error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_query_index_encode_delete_keys(
        incremental.changes, incremental.change_count, &deletes,
        &pouch->allocator, &delete_count, &delete_hash, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (memtable_usable) {
    rc = lc_pouch_query_index_memtable_encode(
        memtable, state_index_seq, &summary, &row_hash, &term_count, &term_hash,
        &term_field_count, &term_value_count, &presence_count, &presence_hash,
        &doc_table_generation, &doc_table_generation_length,
        &exact_term_generation, &exact_term_generation_length,
        &presence_term_generation, &presence_term_generation_length,
        &range_term_generation, &range_term_generation_length,
        &text_term_generation, &text_term_generation_length,
        &trigram_term_generation, &trigram_term_generation_length,
        &temporal_term_generation, &temporal_term_generation_length, error);
  } else {
    rc = lc_pouch_query_index_build_text(
        state_index_seq, &summary, &row_hash, &term_count, &term_hash,
        &term_field_count, &term_value_count, &presence_count, &presence_hash,
        &doc_table_generation, &doc_table_generation_length,
        &exact_term_generation, &exact_term_generation_length,
        &presence_term_generation, &presence_term_generation_length,
        &range_term_generation, &range_term_generation_length,
        &text_term_generation, &text_term_generation_length,
        &trigram_term_generation, &trigram_term_generation_length,
        &temporal_term_generation, &temporal_term_generation_length, error);
  }
  segment_row_count = (unsigned long)summary.count;
  (void)term_count;
  (void)term_hash;
  (void)term_field_count;
  (void)term_value_count;
  (void)presence_count;
  (void)presence_hash;
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_query_index_segment_id(state_index_seq, segment_id,
                                       sizeof(segment_id), error);
  if (rc == LC_OK) {
    packed_path = lc_pouch_query_index_segment_artifact_path(
        pouch, ns, segment_id, LC_POUCH_QUERY_INDEX_PACKED_LEAF, error);
    if (packed_path == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pack_components(
        &pouch->allocator,
        doc_table_generation != NULL ? doc_table_generation : "",
        doc_table_generation_length,
        exact_term_generation != NULL ? exact_term_generation : "",
        exact_term_generation_length,
        presence_term_generation != NULL ? presence_term_generation : "",
        presence_term_generation_length,
        range_term_generation != NULL ? range_term_generation : "",
        range_term_generation_length,
        text_term_generation != NULL ? text_term_generation : "",
        text_term_generation_length,
        trigram_term_generation != NULL ? trigram_term_generation : "",
        trigram_term_generation_length,
        temporal_term_generation != NULL ? temporal_term_generation : "",
        temporal_term_generation_length,
        deletes.bytes != NULL ? deletes.bytes : "", deletes.length,
        &packed_generation, &packed_generation_length, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, ns, packed_path,
        packed_generation != NULL ? packed_generation : "",
        packed_generation_length, packed_path_unpublished, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_manifest_add_front(
        &pouch->allocator, &manifest, segment_id, segment_base_index_seq,
        state_index_seq, segment_row_count, row_hash, delete_count, delete_hash,
        summary.term_index_complete, summary.presence_index_complete, error);
  }
  if (rc == LC_OK && manifest.segment_count > 0U) {
    lc_pouch_query_index_manifest_segment_capture_artifacts(
        &manifest.segments[0], packed_path);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_manifest_write(pouch, manifest_path, &manifest,
                                             error);
  }
  if (rc == LC_OK && retired_manifest.segment_count > 0U) {
    lc_pouch_query_index_manifest_unlink_segments(pouch, ns, &retired_manifest,
                                                  &manifest);
    cleanup_unreferenced = 1;
  }
  if (rc == LC_OK && cleanup_unreferenced) {
    rc = lc_pouch_query_index_unlink_unreferenced_artifacts(pouch, ns,
                                                            &manifest, error);
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_manifest_trust_remember(pouch, ns, state_index_seq,
                                                 &manifest);
    lc_pouch_query_index_pending_discard_published(pouch, ns, state_index_seq);
    out->index_seq = state_index_seq;
    out->repaired = 1;
  }

cleanup:
  if (rc == LC_OK) {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("index_seq", out->index_seq);
    fields[2] = lc_log_bool_field("repaired", out->repaired);
    fields[3] = lc_log_bool_field("full_rebuild", full_rebuild);
    fields[4] = lc_log_u64_field("records", segment_row_count);
    lc_log_debug(pouch->logger, "index.flush", fields, 5U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_u64_field("index_seq", state_index_seq);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "index.flush.error", fields, 4U);
  }
  lc_free_with_allocator(&pouch->allocator, packed_generation);
  lc_pouch_query_index_memtable_cleanup(&pouch->allocator, memtable);
  lc_free_with_allocator(&pouch->allocator, memtable);
  lc_free_with_allocator(&pouch->allocator, temporal_term_generation);
  lc_free_with_allocator(&pouch->allocator, trigram_term_generation);
  lc_free_with_allocator(&pouch->allocator, text_term_generation);
  lc_free_with_allocator(&pouch->allocator, range_term_generation);
  lc_free_with_allocator(&pouch->allocator, presence_term_generation);
  lc_free_with_allocator(&pouch->allocator, exact_term_generation);
  lc_free_with_allocator(&pouch->allocator, doc_table_generation);
  lc_free_with_allocator(&pouch->allocator, deletes.bytes);
  lc_free_with_allocator(&pouch->allocator, extract_row_indices);
  lc_free_with_allocator(&pouch->allocator, extract_keys);
  lc_pouch_query_index_key_hex_set_cleanup(&pouch->allocator, &pending_deletes);
  lc_pouch_query_index_incremental_context_cleanup(&incremental);
  lc_pouch_query_index_summary_cleanup(&summary);
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &retired_manifest);
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, packed_path);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (query_flush_locked) {
    lc_pouch_query_index_flush_unlock(pouch);
  }
  return rc;
}

typedef struct lc_pouch_query_index_flush_context {
  lc_pouch *pouch;
  const char *ns;
  int validate_existing_segments;
  int require_completed_operations;
  lc_pouch_query_index_flush_result *out;
} lc_pouch_query_index_flush_context;

static int lc_pouch_query_index_flush_current_locked(void *context,
                                                     lc_error *error) {
  lc_pouch_query_index_flush_context *flush;
  lc_pouch_generation state_index_seq;
  int rc;

  flush = (lc_pouch_query_index_flush_context *)context;
  state_index_seq = 0UL;
  rc = lc_pouch_state_query_index_seq(flush->pouch, flush->ns, &state_index_seq,
                                      error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_query_index_flush_segmented(
      flush->pouch, flush->ns, state_index_seq,
      flush->validate_existing_segments, flush->require_completed_operations,
      flush->out, error);
}

/* Shared roots must serialize the state snapshot, derived artifacts, and
 * manifest publication under the namespace's cross-process write authority.
 * Exclusive roots retain their lower-overhead per-handle artifact mutex. */
static int lc_pouch_query_index_flush_coordinated(
    lc_pouch *pouch, const char *ns,
    lc_pouch_generation requested_state_index_seq,
    int validate_existing_segments, int require_completed_operations,
    lc_pouch_query_index_flush_result *out, lc_error *error) {
  lc_pouch_query_index_flush_context flush;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_pouch_query_index_flush_segmented(
        pouch, ns, requested_state_index_seq, validate_existing_segments,
        require_completed_operations, out, error);
  }
  memset(&flush, 0, sizeof(flush));
  flush.pouch = pouch;
  flush.ns = ns;
  flush.validate_existing_segments = validate_existing_segments;
  flush.require_completed_operations = require_completed_operations;
  flush.out = out;
  if (!lc_pouch_single_writer_enabled(pouch)) {
    return lc_pouch_state_with_namespace_lock(
        pouch, ns, lc_pouch_query_index_flush_current_locked, &flush, error);
  }
  return lc_pouch_query_index_flush_current_locked(&flush, error);
}

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *ns,
                               lc_pouch_generation state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error) {
  return lc_pouch_query_index_flush_coordinated(pouch, ns, state_index_seq, 0,
                                                0, out, error);
}

int lc_pouch_query_index_flush_threshold(lc_pouch *pouch, const char *ns,
                                         lc_pouch_generation state_index_seq,
                                         lc_pouch_query_index_flush_result *out,
                                         lc_error *error) {
  return lc_pouch_query_index_flush_coordinated(pouch, ns, state_index_seq, 0,
                                                1, out, error);
}

int lc_pouch_query_index_flush_validated(lc_pouch *pouch, const char *ns,
                                         lc_pouch_generation state_index_seq,
                                         lc_pouch_query_index_flush_result *out,
                                         lc_error *error) {
  return lc_pouch_query_index_flush_coordinated(pouch, ns, state_index_seq, 1,
                                                0, out, error);
}

int lc_pouch_query_index_manifest_seq(lc_pouch *pouch, const char *ns,
                                      lc_pouch_generation *index_seq,
                                      lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  char *manifest_path;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest sequence requires pouch, "
                        "namespace, and output",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  if (lc_pouch_query_index_manifest_trust_seq(pouch, ns, index_seq)) {
    return LC_OK;
  }
  memset(&manifest, 0, sizeof(manifest));
  manifest_path = lc_pouch_query_index_manifest_path(pouch, ns, error);
  if (manifest_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc == LC_OK && manifest.present) {
    if (!manifest.valid) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest is invalid", NULL, NULL,
                        "pouch");
    } else {
      *index_seq = manifest.index_seq;
      lc_pouch_query_index_manifest_trust_remember(
          pouch, ns, manifest.index_seq, &manifest);
    }
  }
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_query_index_ensure_current(lc_pouch *pouch, const char *ns,
                                        lc_pouch_generation state_index_seq,
                                        int validate_current,
                                        lc_pouch_query_index_flush_result *out,
                                        lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  char *manifest_path;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_ensure_current requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (!validate_current && lc_pouch_query_index_manifest_trust_snapshot(
                               pouch, ns, state_index_seq, NULL)) {
    out->index_seq = state_index_seq;
    return LC_OK;
  }
  memset(&manifest, 0, sizeof(manifest));
  manifest_path = lc_pouch_query_index_manifest_path(pouch, ns, error);
  if (manifest_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (manifest.present && manifest.valid &&
      manifest.index_seq == state_index_seq) {
    int segments_valid;

    if (!validate_current) {
      out->index_seq = state_index_seq;
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
      return LC_OK;
    }
    segments_valid = 0;
    rc = lc_pouch_query_index_manifest_segments_validate(
        pouch, ns, &manifest, &segments_valid, error);
    if (rc != LC_OK) {
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
    if (segments_valid) {
      out->index_seq = state_index_seq;
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
      return LC_OK;
    }
  }
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  if (validate_current) {
    return lc_pouch_query_index_flush_validated(pouch, ns, state_index_seq, out,
                                                error);
  }
  return lc_pouch_query_index_flush(pouch, ns, state_index_seq, out, error);
}

int lc_pouch_query_index_visit(lc_pouch *pouch, const char *ns,
                               lc_pouch_query_index_row_visit_fn visit,
                               void *context, lc_pouch_generation *index_seq,
                               lc_error *error) {
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_doc_table_cache_borrow_list doc_table_cache_borrows;
  lc_pouch_query_index_row_view row_view;
  size_t row_index;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_visit requires pouch, "
                        "namespace, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  memset(&rows, 0, sizeof(rows));
  memset(&doc_table_cache_borrows, 0, sizeof(doc_table_cache_borrows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, ns, LC_POUCH_QUERY_INDEX_SEGMENTED_ALL, NULL, 0U, NULL, NULL, NULL,
      0, 0, 0, NULL, NULL, &rows, &doc_table_cache_borrows, NULL, index_seq,
      NULL, error);
  if (rc == LC_OK) {
    for (row_index = 0U; rc == LC_OK && row_index < rows.count; ++row_index) {
      lc_pouch_index_result_row *row;

      row = &rows.items[row_index];
      memset(&row_view, 0, sizeof(row_view));
      row_view.key = row->key;
      row_view.key_hex = row->key_hex;
      row_view.doc_id = row->doc_id;
      row_view.version = row->version;
      row_view.bytes = row->bytes;
      row_view.has_query_hidden = row->has_query_hidden;
      row_view.query_hidden = row->query_hidden;
      rc = visit(&row_view, context, error);
    }
    if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
      rc = LC_OK;
    }
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
      pouch, &doc_table_cache_borrows);
  return rc;
}

static int lc_pouch_query_index_visit_term_match(
    lc_pouch *pouch, const char *ns, const char *field, const char *value,
    char value_type, int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_query_index_date_bounds *date_bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result header_artifact;
  lc_pouch_query_index_term_reader reader;
  lc_pouch_index_term_key *exact_terms;
  char *header_path;
  char *field_hex;
  char *value_hex;
  size_t exact_term_count;
  int exact_scalar;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' ||
      (value == NULL && range_bounds == NULL && date_bounds == NULL) ||
      visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term lookup requires pouch, "
                        "namespace, field, value, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  header_path = lc_pouch_query_index_path(pouch, ns, error);
  if (header_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  exact_terms = NULL;
  exact_term_count = 0U;
  field_hex = NULL;
  value_hex = NULL;
  exact_scalar = !prefix_match && !contains_match && !ignore_case &&
                 range_bounds == NULL && date_bounds == NULL;
  if (exact_scalar && (value_type != 's' && value_type != 'n' &&
                       value_type != 'b' && value_type != 'z')) {
    lc_free_with_allocator(&pouch->allocator, header_path);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar lookup requires a JSON "
                        "scalar value type",
                        NULL, NULL, NULL);
  }
  if (exact_scalar) {
    char value_types[1];

    value_types[0] = value_type;
    rc = lc_pouch_query_index_build_exact_terms_for_field(
        field, &value, value_types, 1U, &exact_terms, &exact_term_count,
        &pouch->allocator, error);
  } else {
    field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
    value_hex = range_bounds == NULL && date_bounds == NULL
                    ? lc_pouch_query_index_hex_encode(&pouch->allocator, value)
                    : lc_strdup_with_allocator(&pouch->allocator, "");
    rc = field_hex != NULL && value_hex != NULL ? LC_OK : LC_ERR_NOMEM;
  }
  if (rc != LC_OK) {
    if (rc == LC_ERR_NOMEM) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch query-index scalar lookup", NULL,
                   NULL, NULL);
    }
    lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    lc_free_with_allocator(&pouch->allocator, value_hex);
    lc_free_with_allocator(&pouch->allocator, header_path);
    return rc;
  }
  if (exact_scalar) {
    rc = lc_pouch_query_index_visit_exact_generation(pouch, ns, exact_terms,
                                                     exact_term_count, visit,
                                                     context, index_seq, error);
    lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
    lc_free_with_allocator(&pouch->allocator, header_path);
    return rc;
  }
  memset(&reader, 0, sizeof(reader));
  reader.allocator = &pouch->allocator;
  reader.field_hex = field_hex;
  reader.value_hex = value_hex;
  reader.value_text = value;
  reader.prefix_match = prefix_match;
  reader.contains_match = contains_match;
  reader.ignore_case = ignore_case;
  reader.string_values_only = prefix_match || contains_match;
  if (range_bounds != NULL) {
    reader.range_match = 1;
    reader.range_bounds = *range_bounds;
  }
  if (date_bounds != NULL) {
    reader.date_match = 1;
    reader.date_bounds = *date_bounds;
    rc = lc_pouch_index_parse_date_bounds(date_bounds,
                                          &reader.parsed_date_bounds, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
      lc_free_with_allocator(&pouch->allocator, field_hex);
      lc_free_with_allocator(&pouch->allocator, value_hex);
      lc_free_with_allocator(&pouch->allocator, header_path);
      return rc;
    }
  }
  reader.visit = visit;
  reader.context = context;
  rc = lc_pouch_query_index_read_with_reader(
      pouch, ns, header_path, &header_artifact, NULL, &reader, NULL, error);
  if (rc == LC_OK && (!header_artifact.present || !header_artifact.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index header artifact is not readable", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && !header_artifact.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    *index_seq = header_artifact.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, value_hex);
  lc_free_with_allocator(&pouch->allocator, header_path);
  return rc;
}

int lc_pouch_query_index_visit_scalar(
    lc_pouch *pouch, const char *ns, const char *field, const char *value,
    char value_type, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  return lc_pouch_query_index_visit_term_match(
      pouch, ns, field, value, value_type, 0, 0, 0, NULL, NULL, visit, context,
      index_seq, error);
}

int lc_pouch_query_index_visit_scalar_any(
    lc_pouch *pouch, const char *ns, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' || values == NULL || value_types == NULL ||
      value_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(pouch, ns, field, values[0],
                                             value_types[0], visit, context,
                                             index_seq, error);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_query_index_build_exact_terms_for_field(
      field, values, value_types, value_count, &exact_terms, &exact_term_count,
      &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(pouch, ns, exact_terms,
                                                     exact_term_count, visit,
                                                     context, index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || terms == NULL ||
      term_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term-set lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, ns, terms[0].field, terms[0].value, terms[0].value_type, visit,
        context, index_seq, error);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_query_index_build_exact_terms(terms, term_count, &exact_terms,
                                              &exact_term_count,
                                              &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(pouch, ns, exact_terms,
                                                     exact_term_count, visit,
                                                     context, index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

static int
lc_pouch_query_index_rows_emit(const lc_pouch_index_result_row_list *rows,
                               lc_pouch_query_index_key_visit_fn visit,
                               void *context, lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  size_t index;
  int rc;

  if (rows == NULL || visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row emit requires rows and visitor",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < rows->count; ++index) {
    const lc_pouch_index_result_row *row;

    row = &rows->items[index];
    memset(&key_view, 0, sizeof(key_view));
    key_view.key = row->key;
    key_view.key_hex = row->key_hex;
    key_view.doc_id = row->doc_id;
    key_view.version = row->version;
    key_view.bytes = row->bytes;
    key_view.has_query_hidden = row->has_query_hidden;
    key_view.query_hidden = row->query_hidden;
    key_view.value_index = row->value_index;
    rc = visit(&key_view, context, error);
  }
  if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
    rc = LC_OK;
  }
  return rc;
}

static void lc_pouch_query_index_generation_cache_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_generation_cache_entry *entry) {
  size_t index;

  if (entry == NULL) {
    return;
  }
  if (entry->has_direct_generation && !entry->fields_borrow_direct_generation) {
    for (index = 0U; index < entry->field_count; ++index) {
      lc_free_with_allocator(allocator, (void *)entry->fields[index].field_hex);
    }
  }
  lc_free_with_allocator(allocator, entry->path);
  lc_free_with_allocator(allocator, entry->bytes);
  lc_free_with_allocator(allocator, entry->terms);
  lc_free_with_allocator(allocator, entry->fields);
  lc_free_with_allocator(allocator, entry->postings);
  lc_free_with_allocator(allocator, entry->direct_text_prefix_indices);
  lc_pouch_index_term_generation_cleanup(allocator, &entry->direct_generation);
  memset(entry, 0, sizeof(*entry));
}

static void lc_pouch_query_index_doc_table_cache_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_doc_table_cache_entry *entry) {
  size_t index;

  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->path);
  for (index = 0U; index < entry->decoded_key_count; ++index) {
    lc_free_with_allocator(allocator, entry->decoded_keys[index]);
  }
  lc_free_with_allocator(allocator, entry->decoded_keys);
  lc_pouch_index_doc_table_cleanup(allocator, &entry->table);
  memset(entry, 0, sizeof(*entry));
}

static int lc_pouch_query_index_doc_table_cache_lock(lc_pouch *pouch,
                                                     lc_error *error) {
  int pthread_rc;

  if (pouch == NULL || !pouch->query_doc_table_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc-table cache requires an open pouch", NULL,
                        NULL, "pouch");
  }
  pthread_rc = pthread_mutex_lock(&pouch->query_doc_table_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch doc-table cache",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void lc_pouch_query_index_doc_table_cache_unlock(lc_pouch *pouch) {
  if (pouch != NULL && pouch->query_doc_table_cache_mutex_initialized) {
    (void)pthread_mutex_unlock(&pouch->query_doc_table_cache_mutex);
  }
}

/* Requires query_doc_table_cache_mutex. */
static void lc_pouch_query_index_doc_table_cache_retire(
    lc_pouch *pouch, lc_pouch_query_index_doc_table_cache_entry *entry) {
  if (entry == NULL) {
    return;
  }
  entry->next = NULL;
  entry->retired = 1;
  if (entry->borrow_count == 0U) {
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       entry);
    lc_free_with_allocator(&pouch->allocator, entry);
  }
}

/* Requires query_doc_table_cache_mutex. */
static void lc_pouch_query_index_doc_table_cache_trim(lc_pouch *pouch) {
  size_t capacity;

  capacity = LC_POUCH_QUERY_INDEX_DOC_TABLE_CACHE_MAX;
#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_doc_table_cache_capacity > 0U) {
    capacity = lc_pouch_test_doc_table_cache_capacity;
  }
#endif
  while (pouch->query_doc_table_cache_count > capacity) {
    lc_pouch_query_index_doc_table_cache_entry *previous;
    lc_pouch_query_index_doc_table_cache_entry *entry;
    lc_pouch_query_index_doc_table_cache_entry *victim;
    lc_pouch_query_index_doc_table_cache_entry *victim_previous;

    previous = NULL;
    entry = pouch->query_doc_table_cache;
    victim = NULL;
    victim_previous = NULL;
    while (entry != NULL) {
      if (entry->borrow_count == 0U) {
        victim = entry;
        victim_previous = previous;
      }
      previous = entry;
      entry = entry->next;
    }
    if (victim == NULL) {
      /* Every resident entry is in use. Keep the bounded cache temporarily
       * over capacity; the next lookup or adoption will trim it. */
      break;
    }
    if (victim_previous != NULL) {
      victim_previous->next = victim->next;
    } else {
      pouch->query_doc_table_cache = victim->next;
    }
    --pouch->query_doc_table_cache_count;
    lc_pouch_query_index_doc_table_cache_retire(pouch, victim);
  }
}

static void lc_pouch_query_index_doc_table_cache_release(
    lc_pouch *pouch, lc_pouch_query_index_doc_table_cache_entry **entry_ptr) {
  lc_pouch_query_index_doc_table_cache_entry *entry;

  if (entry_ptr == NULL || *entry_ptr == NULL) {
    return;
  }
  entry = *entry_ptr;
  *entry_ptr = NULL;
  if (pouch == NULL || !pouch->query_doc_table_cache_mutex_initialized ||
      pthread_mutex_lock(&pouch->query_doc_table_cache_mutex) != 0) {
    /* Failing to acquire the ownership mutex during teardown must not free a
     * potentially live cache entry. */
    return;
  }
  if (entry->borrow_count > 0U) {
    --entry->borrow_count;
  }
  if (entry->retired && entry->borrow_count == 0U) {
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       entry);
    lc_free_with_allocator(&pouch->allocator, entry);
  } else {
    lc_pouch_query_index_doc_table_cache_trim(pouch);
  }
  lc_pouch_query_index_doc_table_cache_unlock(pouch);
}

static int lc_pouch_query_index_doc_table_cache_borrow_list_take(
    lc_pouch *pouch, lc_pouch_query_index_doc_table_cache_borrow_list *list,
    lc_pouch_query_index_doc_table_cache_entry *entry, lc_error *error) {
  lc_pouch_query_index_doc_table_cache_entry **items;
  size_t capacity;

  if (entry == NULL) {
    return LC_OK;
  }
  if (pouch == NULL || list == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc-table cache borrow list requires inputs",
                        NULL, NULL, "pouch");
  }
  if (list->count == list->capacity) {
    if (list->capacity == 0U) {
      capacity = 4U;
    } else {
      if (list->capacity > (size_t)-1 / 2U) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch doc-table cache borrow list exceeds local "
                            "limit",
                            NULL, NULL, "pouch");
      }
      capacity = list->capacity * 2U;
    }
    if (capacity > (size_t)-1 / sizeof(*items)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch doc-table cache borrow list exceeds local "
                          "limit",
                          NULL, NULL, "pouch");
    }
    items =
        (lc_pouch_query_index_doc_table_cache_entry **)lc_alloc_with_allocator(
            &pouch->allocator, capacity * sizeof(*items));
    if (items == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain pouch doc-table cache borrow", NULL,
                          NULL, "pouch");
    }
    if (list->items != NULL) {
      memcpy(items, list->items, list->count * sizeof(*items));
      lc_free_with_allocator(&pouch->allocator, list->items);
    }
    list->items = items;
    list->capacity = capacity;
  }
  list->items[list->count++] = entry;
  return LC_OK;
}

static void lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
    lc_pouch *pouch, lc_pouch_query_index_doc_table_cache_borrow_list *list) {
  size_t index;

  if (list == NULL) {
    return;
  }
  for (index = 0U; index < list->count; ++index) {
    lc_pouch_query_index_doc_table_cache_release(pouch, &list->items[index]);
  }
  if (pouch != NULL) {
    lc_free_with_allocator(&pouch->allocator, list->items);
  }
  memset(list, 0, sizeof(*list));
}

static int lc_pouch_query_index_artifact_signature(
    const char *path, lc_pouch_query_index_file_signature *out) {
  struct stat st;

  if (path == NULL || out == NULL) {
    return 0;
  }
  memset(out, 0, sizeof(*out));
  if (stat(path, &st) != 0) {
    return 0;
  }
  if (st.st_size < 0) {
    return 0;
  }
  out->size = (uint64_t)st.st_size;
#if defined(__APPLE__)
  out->mtime = (unsigned long)st.st_mtimespec.tv_sec;
  out->mtime_nsec = (unsigned long)st.st_mtimespec.tv_nsec;
  out->ctime = (unsigned long)st.st_ctimespec.tv_sec;
  out->ctime_nsec = (unsigned long)st.st_ctimespec.tv_nsec;
#elif defined(__linux__)
  out->mtime = (unsigned long)st.st_mtim.tv_sec;
  out->mtime_nsec = (unsigned long)st.st_mtim.tv_nsec;
  out->ctime = (unsigned long)st.st_ctim.tv_sec;
  out->ctime_nsec = (unsigned long)st.st_ctim.tv_nsec;
#else
  out->mtime = (unsigned long)st.st_mtime;
  out->mtime_nsec = 0UL;
  out->ctime = (unsigned long)st.st_ctime;
  out->ctime_nsec = 0UL;
#endif
  out->inode = (unsigned long)st.st_ino;
  out->present = 1;
  return 1;
}

static void lc_pouch_query_index_manifest_signature_from_file(
    lc_pouch_query_index_manifest_artifact_signature *out,
    const lc_pouch_query_index_file_signature *signature) {
  if (out == NULL) {
    return;
  }
  memset(out, 0, sizeof(*out));
  if (signature == NULL || !signature->present) {
    return;
  }
  out->size = signature->size;
  out->mtime = signature->mtime;
  out->mtime_nsec = signature->mtime_nsec;
  out->ctime = signature->ctime;
  out->ctime_nsec = signature->ctime_nsec;
  out->inode = signature->inode;
  out->present = 1;
}

static void lc_pouch_query_index_manifest_segment_capture_artifacts(
    lc_pouch_query_index_manifest_segment *segment, const char *packed_path) {
  lc_pouch_query_index_file_signature packed_signature;

  if (segment == NULL) {
    return;
  }
  memset(segment->artifact_signatures, 0, sizeof(segment->artifact_signatures));
  if (packed_path != NULL &&
      lc_pouch_query_index_artifact_signature(packed_path, &packed_signature)) {
    lc_pouch_query_index_manifest_signature_from_file(
        &segment->artifact_signatures
             [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PACKED],
        &packed_signature);
  }
}

static int lc_pouch_query_index_manifest_segment_artifacts_match(
    const lc_pouch_query_index_manifest_segment *segment,
    const char *packed_path,
    lc_pouch_query_index_file_signature *actual_signatures) {
  lc_pouch_query_index_file_signature packed_signature;
  const lc_pouch_query_index_manifest_artifact_signature *expected;

  if (segment == NULL || packed_path == NULL) {
    return 0;
  }
  if (!lc_pouch_query_index_artifact_signature(packed_path,
                                               &packed_signature)) {
    return 0;
  }
  expected =
      &segment
           ->artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PACKED];
  if (!expected->present || expected->size != packed_signature.size ||
      expected->mtime != packed_signature.mtime ||
      expected->mtime_nsec != packed_signature.mtime_nsec ||
      expected->ctime != packed_signature.ctime ||
      expected->ctime_nsec != packed_signature.ctime_nsec ||
      expected->inode != packed_signature.inode) {
    return 0;
  }
  if (actual_signatures != NULL) {
    actual_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PACKED] =
        packed_signature;
  }
  return 1;
}

static void lc_pouch_query_index_manifest_trust_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_manifest_trust_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->ns);
  lc_pouch_query_index_manifest_cleanup(allocator, &entry->manifest);
  memset(entry, 0, sizeof(*entry));
}

static int
lc_pouch_query_index_manifest_trust_valid(const lc_pouch *pouch, const char *ns,
                                          lc_pouch_generation index_seq) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  uint64_t writer_mode_epoch;
  int valid;

  if (pouch == NULL || !pouch->indexer_mutex_initialized ||
      !lc_pouch_single_writer_snapshot((lc_pouch *)pouch, &writer_mode_epoch) ||
      ns == NULL || ns[0] == '\0') {
    return 0;
  }
  pthread_mutex_lock(&((lc_pouch *)pouch)->indexer_mutex);
  valid = 0;
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->writer_mode_epoch == writer_mode_epoch &&
        entry->index_seq == index_seq && entry->ns != NULL &&
        strcmp(entry->ns, ns) == 0) {
      valid = 1;
      break;
    }
    entry = entry->next;
  }
  pthread_mutex_unlock(&((lc_pouch *)pouch)->indexer_mutex);
  return valid;
}

static int
lc_pouch_query_index_manifest_trust_seq(const lc_pouch *pouch, const char *ns,
                                        lc_pouch_generation *index_seq) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  uint64_t writer_mode_epoch;
  int found;

  if (index_seq == NULL || pouch == NULL || !pouch->indexer_mutex_initialized ||
      !lc_pouch_single_writer_snapshot((lc_pouch *)pouch, &writer_mode_epoch) ||
      ns == NULL || ns[0] == '\0') {
    return 0;
  }
  pthread_mutex_lock(&((lc_pouch *)pouch)->indexer_mutex);
  found = 0;
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->manifest_cached &&
        entry->writer_mode_epoch == writer_mode_epoch && entry->ns != NULL &&
        strcmp(entry->ns, ns) == 0) {
      *index_seq = entry->index_seq;
      found = 1;
      break;
    }
    entry = entry->next;
  }
  pthread_mutex_unlock(&((lc_pouch *)pouch)->indexer_mutex);
  return found;
}

static void lc_pouch_query_index_manifest_trust_remember(
    lc_pouch *pouch, const char *ns, lc_pouch_generation index_seq,
    const lc_pouch_query_index_manifest *manifest) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  uint64_t writer_mode_epoch;

  if (pouch == NULL || !pouch->indexer_mutex_initialized ||
      !lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch) ||
      ns == NULL || ns[0] == '\0') {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->ns != NULL && strcmp(entry->ns, ns) == 0) {
      entry->index_seq = index_seq;
      entry->writer_mode_epoch = writer_mode_epoch;
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator,
                                            &entry->manifest);
      entry->manifest_cached = 0;
      if (manifest != NULL && manifest->present && manifest->valid &&
          manifest->index_seq == index_seq &&
          lc_pouch_query_index_manifest_clone(
              &pouch->allocator, &entry->manifest, manifest) == LC_OK) {
        entry->manifest_cached = 1;
      }
      pthread_mutex_unlock(&pouch->indexer_mutex);
      return;
    }
    entry = entry->next;
  }
  entry = (lc_pouch_query_index_manifest_trust_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    pthread_mutex_unlock(&pouch->indexer_mutex);
    return;
  }
  entry->ns = lc_strdup_with_allocator(&pouch->allocator, ns);
  if (entry->ns == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    pthread_mutex_unlock(&pouch->indexer_mutex);
    return;
  }
  entry->index_seq = index_seq;
  entry->writer_mode_epoch = writer_mode_epoch;
  if (manifest != NULL && manifest->present && manifest->valid &&
      manifest->index_seq == index_seq &&
      lc_pouch_query_index_manifest_clone(&pouch->allocator, &entry->manifest,
                                          manifest) == LC_OK) {
    entry->manifest_cached = 1;
  }
  entry->next = pouch->query_manifest_trust;
  pouch->query_manifest_trust = entry;
  pthread_mutex_unlock(&pouch->indexer_mutex);
}

static int lc_pouch_query_index_manifest_trust_snapshot(
    lc_pouch *pouch, const char *ns, lc_pouch_generation index_seq,
    lc_pouch_query_index_manifest *out) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  uint64_t writer_mode_epoch;
  int found;

  if (pouch == NULL || !pouch->indexer_mutex_initialized ||
      !lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch) ||
      ns == NULL || ns[0] == '\0') {
    return 0;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  found = 0;
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->manifest_cached &&
        entry->writer_mode_epoch == writer_mode_epoch &&
        entry->index_seq == index_seq &&
        entry->manifest.index_seq == index_seq && entry->ns != NULL &&
        strcmp(entry->ns, ns) == 0) {
      found = 1;
      if (out != NULL &&
          lc_pouch_query_index_manifest_clone(&pouch->allocator, out,
                                              &entry->manifest) != LC_OK) {
        lc_pouch_query_index_manifest_cleanup(&pouch->allocator, out);
        found = 0;
      }
      break;
    }
    entry = entry->next;
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  return found;
}

static void lc_pouch_query_index_manifest_trust_forget(lc_pouch *pouch,
                                                       const char *ns) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  lc_pouch_query_index_manifest_trust_entry *previous;

  if (pouch == NULL || !pouch->indexer_mutex_initialized || ns == NULL ||
      ns[0] == '\0') {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  previous = NULL;
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->ns != NULL && strcmp(entry->ns, ns) == 0) {
      if (previous != NULL) {
        previous->next = entry->next;
      } else {
        pouch->query_manifest_trust = entry->next;
      }
      lc_pouch_query_index_manifest_trust_entry_cleanup(&pouch->allocator,
                                                        entry);
      lc_free_with_allocator(&pouch->allocator, entry);
      break;
    }
    previous = entry;
    entry = entry->next;
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
}

int lc_pouch_query_index_invalidate_namespace(lc_pouch *pouch, const char *ns,
                                              lc_error *error) {
  char *manifest_path;
  int manifest_present;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index invalidation requires pouch and "
                        "namespace",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_flush_lock(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_query_index_manifest_trust_forget(pouch, ns);
  manifest_path = lc_pouch_query_index_manifest_path(pouch, ns, error);
  manifest_present = 0;
  if (manifest_path == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  } else if (access(manifest_path, F_OK) == 0) {
    manifest_present = 1;
    rc = LC_OK;
  } else if (errno == ENOENT) {
    rc = LC_OK;
  } else {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, errno,
                      "failed to inspect pouch query-index manifest",
                      strerror(errno), NULL, "pouch");
  }
  if (rc == LC_OK && manifest_present) {
    /* Compaction may now discard mutations absent from this derived view.
     * Publish invalidation durably before that irreversible boundary. */
    rc = lc_pouch_path_write_text_file(manifest_path, "invalid\n", error);
  }
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  lc_pouch_query_index_flush_unlock(pouch);
  return rc;
}

void lc_pouch_query_index_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_query_index_generation_cache_entry *entry;
  lc_pouch_query_index_doc_table_cache_entry *doc_entry;
  lc_pouch_query_index_packed_cache_entry *packed_entry;
  lc_pouch_query_index_manifest_trust_entry *trust_entry;
  lc_pouch_query_index_pending_entry *pending_entry;

  if (pouch == NULL) {
    return;
  }
  entry = pouch->query_generation_cache;
  while (entry != NULL) {
    lc_pouch_query_index_generation_cache_entry *next;

    next = entry->next;
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    entry = next;
  }
  pouch->query_generation_cache = NULL;
  pouch->query_generation_cache_count = 0U;
  if (pouch->query_doc_table_cache_mutex_initialized &&
      pthread_mutex_lock(&pouch->query_doc_table_cache_mutex) == 0) {
    doc_entry = pouch->query_doc_table_cache;
    pouch->query_doc_table_cache = NULL;
    pouch->query_doc_table_cache_count = 0U;
    while (doc_entry != NULL) {
      lc_pouch_query_index_doc_table_cache_entry *next;

      next = doc_entry->next;
      lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                         doc_entry);
      lc_free_with_allocator(&pouch->allocator, doc_entry);
      doc_entry = next;
    }
    (void)pthread_mutex_unlock(&pouch->query_doc_table_cache_mutex);
  }
  packed_entry = pouch->query_packed_cache;
  while (packed_entry != NULL) {
    lc_pouch_query_index_packed_cache_entry *next;

    next = packed_entry->next;
    lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator,
                                                    packed_entry);
    lc_free_with_allocator(&pouch->allocator, packed_entry);
    packed_entry = next;
  }
  pouch->query_packed_cache = NULL;
  pouch->query_packed_cache_count = 0U;
  trust_entry = pouch->query_manifest_trust;
  while (trust_entry != NULL) {
    lc_pouch_query_index_manifest_trust_entry *next;

    next = trust_entry->next;
    lc_pouch_query_index_manifest_trust_entry_cleanup(&pouch->allocator,
                                                      trust_entry);
    lc_free_with_allocator(&pouch->allocator, trust_entry);
    trust_entry = next;
  }
  pouch->query_manifest_trust = NULL;
  pending_entry = pouch->query_pending_index;
  while (pending_entry != NULL) {
    lc_pouch_query_index_pending_entry *next;

    next = pending_entry->next;
    lc_pouch_query_index_pending_entry_cleanup(&pouch->allocator,
                                               pending_entry);
    lc_free_with_allocator(&pouch->allocator, pending_entry);
    pending_entry = next;
  }
  pouch->query_pending_index = NULL;
  while (pouch->query_active_operations != NULL) {
    lc_pouch_query_index_active_operation *operation;

    operation = pouch->query_active_operations;
    pouch->query_active_operations = operation->next;
    lc_free_with_allocator(&pouch->allocator, operation->ns);
    lc_free_with_allocator(&pouch->allocator, operation->key);
    lc_free_with_allocator(&pouch->allocator, operation->operation_id);
    lc_free_with_allocator(&pouch->allocator, operation);
  }
}

static int lc_pouch_query_index_generation_cache_term_compare_parts(
    const char *left_field_hex, const char *left_value_hex,
    char left_value_type, const char *right_field_hex,
    const char *right_value_hex, char right_value_type) {
  int cmp;

  cmp = strcmp(left_field_hex, right_field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(left_value_hex, right_value_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (left_value_type < right_value_type) {
    return -1;
  }
  if (left_value_type > right_value_type) {
    return 1;
  }
  return 0;
}

static size_t lc_pouch_query_index_generation_cache_term_lower_bound(
    const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *value_hex, char value_type) {
  size_t low;
  size_t high;

  low = 0U;
  high = entry != NULL ? entry->term_count : 0U;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_query_index_generation_cache_term_compare_parts(
        entry->terms[mid].field_hex, entry->terms[mid].value_hex,
        entry->terms[mid].value_type, field_hex, value_hex, value_type);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

static int lc_pouch_query_index_generation_cache_find_term(
    const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *value_hex, char value_type,
    const lc_pouch_query_index_generation_cache_term **term) {
  size_t position;

  if (term != NULL) {
    *term = NULL;
  }
  if (entry == NULL || field_hex == NULL || value_hex == NULL) {
    return 0;
  }
  position = lc_pouch_query_index_generation_cache_term_lower_bound(
      entry, field_hex, value_hex, value_type);
  if (position >= entry->term_count ||
      lc_pouch_query_index_generation_cache_term_compare_parts(
          entry->terms[position].field_hex, entry->terms[position].value_hex,
          entry->terms[position].value_type, field_hex, value_hex,
          value_type) != 0) {
    return 0;
  }
  if (term != NULL) {
    *term = &entry->terms[position];
  }
  return 1;
}

static size_t lc_pouch_query_index_generation_cache_field_lower_bound(
    const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex) {
  size_t low;
  size_t high;

  low = 0U;
  high = entry != NULL ? entry->term_count : 0U;
  while (low < high) {
    size_t mid;

    mid = low + ((high - low) / 2U);
    if (strcmp(entry->terms[mid].field_hex, field_hex) < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

static int lc_pouch_query_index_generation_cache_has_field(
    const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex) {
  size_t index;
  size_t position;

  if (entry == NULL || field_hex == NULL) {
    return 0;
  }
  if (entry->fields != NULL) {
    for (index = 0U; index < entry->field_count; ++index) {
      if (entry->fields[index].field_hex != NULL &&
          strcmp(entry->fields[index].field_hex, field_hex) == 0) {
        return 1;
      }
    }
  }
  position =
      lc_pouch_query_index_generation_cache_field_lower_bound(entry, field_hex);
  return position < entry->term_count &&
         strcmp(entry->terms[position].field_hex, field_hex) == 0;
}

static int lc_pouch_query_index_generation_cache_find_posting(
    const lc_pouch_query_index_generation_cache_entry *entry,
    unsigned long term_id,
    const lc_pouch_query_index_generation_cache_posting **posting) {
  size_t low;
  size_t high;

  if (posting != NULL) {
    *posting = NULL;
  }
  if (entry == NULL || term_id == 0UL) {
    return 0;
  }
  low = 0U;
  high = entry->posting_count;
  while (low < high) {
    size_t mid;

    mid = low + ((high - low) / 2U);
    if (entry->postings[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (low >= entry->posting_count || entry->postings[low].term_id != term_id) {
    return 0;
  }
  if (posting != NULL) {
    *posting = &entry->postings[low];
  }
  return 1;
}

typedef struct lc_pouch_query_index_binary_cursor {
  const unsigned char *bytes;
  size_t length;
  size_t offset;
} lc_pouch_query_index_binary_cursor;

static int
lc_pouch_query_index_binary_read(lc_pouch_query_index_binary_cursor *cursor,
                                 size_t length, const unsigned char **out) {
  if (out != NULL) {
    *out = NULL;
  }
  if (cursor == NULL || (length > 0U && out == NULL)) {
    return 0;
  }
  if (length > cursor->length || cursor->offset > cursor->length - length) {
    return 0;
  }
  if (out != NULL) {
    *out = cursor->bytes + cursor->offset;
  }
  cursor->offset += length;
  return 1;
}

static int
lc_pouch_query_index_binary_u8(lc_pouch_query_index_binary_cursor *cursor,
                               unsigned char *out) {
  const unsigned char *bytes;

  if (out == NULL || !lc_pouch_query_index_binary_read(cursor, 1U, &bytes)) {
    return 0;
  }
  *out = bytes[0];
  return 1;
}

static int
lc_pouch_query_index_binary_u64(lc_pouch_query_index_binary_cursor *cursor,
                                uint64_t *out) {
  const unsigned char *bytes;
  uint64_t value;
  size_t index;

  if (out == NULL || !lc_pouch_query_index_binary_read(cursor, 8U, &bytes)) {
    return 0;
  }
  value = (uint64_t)0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)bytes[index]) << (index * 8U);
  }
  *out = value;
  return 1;
}

static int
lc_pouch_query_index_binary_ulong(lc_pouch_query_index_binary_cursor *cursor,
                                  unsigned long *out) {
  uint64_t value;

  if (out == NULL || !lc_pouch_query_index_binary_u64(cursor, &value) ||
      value > (uint64_t)ULONG_MAX) {
    return 0;
  }
  *out = (unsigned long)value;
  return 1;
}

static int
lc_pouch_query_index_binary_string(lc_pouch_query_index_binary_cursor *cursor,
                                   char **out) {
  const unsigned char *bytes;
  uint64_t length64;
  size_t length;

  if (out == NULL) {
    return 0;
  }
  *out = NULL;
  if (!lc_pouch_query_index_binary_u64(cursor, &length64) ||
      length64 > (uint64_t)((size_t)-1)) {
    return 0;
  }
  length = (size_t)length64;
  if (length > cursor->length || cursor->offset > cursor->length - length ||
      cursor->offset + length >= cursor->length) {
    return 0;
  }
  bytes = cursor->bytes + cursor->offset;
  if (memchr(bytes, '\0', length) != NULL ||
      cursor->bytes[cursor->offset + length] != (unsigned char)'\0') {
    return 0;
  }
  cursor->offset += length + 1U;
  *out = (char *)bytes;
  return 1;
}

static void lc_pouch_query_index_trigram_hex(unsigned long key, char out[7]) {
  static const char hex[] = "0123456789abcdef";

  out[0] = hex[(key >> 20U) & 0x0fUL];
  out[1] = hex[(key >> 16U) & 0x0fUL];
  out[2] = hex[(key >> 12U) & 0x0fUL];
  out[3] = hex[(key >> 8U) & 0x0fUL];
  out[4] = hex[(key >> 4U) & 0x0fUL];
  out[5] = hex[key & 0x0fUL];
  out[6] = '\0';
}

static int lc_pouch_query_index_generation_cache_parse(
    const lc_allocator *allocator,
    lc_pouch_query_index_generation_cache_entry *entry, lc_error *error) {
  lc_pouch_query_index_binary_cursor cursor;
  const unsigned char *magic;
  uint64_t version;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long field_count;
  unsigned long term_count;
  unsigned long posting_count;
  unsigned long index;
  char **field_dictionary;
  unsigned char *term_id_seen;
  int rc;

  if (entry == NULL || entry->bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation cache parse requires bytes", NULL,
                        NULL, NULL);
  }
  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = (const unsigned char *)entry->bytes;
  cursor.length = entry->length;
  field_dictionary = NULL;
  field_count = 0UL;
  term_id_seen = NULL;
  rc = LC_OK;
  magic = NULL;
  if (!lc_pouch_query_index_binary_read(
          &cursor, LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN, &magic) ||
      memcmp(magic, LC_POUCH_INDEX_TERM_GENERATION_MAGIC,
             LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN) != 0 ||
      !lc_pouch_query_index_binary_u64(&cursor, &version) ||
      (version != LC_POUCH_INDEX_TERM_GENERATION_VERSION_V1 &&
       version != LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) ||
      !lc_pouch_query_index_binary_u64(&cursor, &index_seq) ||
      !lc_pouch_query_index_binary_ulong(&cursor, &row_count) ||
      !lc_pouch_query_index_binary_ulong(&cursor, &row_hash)) {
    return LC_ERR_INVALID;
  }
  if (index_seq != entry->index_seq || row_count != entry->row_count ||
      row_hash != entry->row_hash) {
    return LC_ERR_INVALID;
  }
  {
    char *ns;

    ns = NULL;
    if (!lc_pouch_query_index_binary_string(&cursor, &ns) || ns[0] == '\0') {
      return LC_ERR_INVALID;
    }
  }
  if (version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) {
    if (!lc_pouch_query_index_binary_ulong(&cursor, &field_count) ||
        field_count >
            (unsigned long)((size_t)-1 / sizeof(field_dictionary[0]))) {
      return LC_ERR_INVALID;
    }
    if (field_count > 0UL) {
      field_dictionary = (char **)lc_alloc_with_allocator(
          allocator, (size_t)field_count * sizeof(field_dictionary[0]));
      if (field_dictionary == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch generation field "
                            "dictionary",
                            NULL, NULL, NULL);
      }
      memset(field_dictionary, 0,
             (size_t)field_count * sizeof(field_dictionary[0]));
    }
    for (index = 0UL; index < field_count; ++index) {
      if (!lc_pouch_query_index_binary_string(&cursor,
                                              &field_dictionary[index]) ||
          field_dictionary[index][0] == '\0' ||
          !lc_pouch_query_index_hex_token_valid(field_dictionary[index]) ||
          (index > 0UL && strcmp(field_dictionary[index - 1UL],
                                 field_dictionary[index]) >= 0)) {
        rc = LC_ERR_INVALID;
        goto cleanup;
      }
    }
  }
  if (!lc_pouch_query_index_binary_ulong(&cursor, &term_count) ||
      !lc_pouch_query_index_binary_ulong(&cursor, &posting_count)) {
    rc = LC_ERR_INVALID;
    goto cleanup;
  }
  if (version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 &&
      ((term_count == 0UL && field_count != 0UL) || field_count > term_count)) {
    rc = LC_ERR_INVALID;
    goto cleanup;
  }
  if (term_count > (unsigned long)((size_t)-1 / sizeof(entry->terms[0])) ||
      posting_count >
          (unsigned long)((size_t)-1 / sizeof(entry->postings[0]))) {
    rc = LC_ERR_NOMEM;
    goto cleanup;
  }
  if (term_count > 0UL) {
    entry->terms =
        (lc_pouch_query_index_generation_cache_term *)lc_alloc_with_allocator(
            allocator, (size_t)term_count * sizeof(entry->terms[0]));
    if (entry->terms == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation term cache", NULL,
                        NULL, NULL);
      goto cleanup;
    }
    memset(entry->terms, 0, (size_t)term_count * sizeof(entry->terms[0]));
    entry->fields =
        (lc_pouch_query_index_generation_cache_field *)lc_alloc_with_allocator(
            allocator, (size_t)term_count * sizeof(entry->fields[0]));
    if (entry->fields == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation field cache", NULL,
                        NULL, NULL);
      goto cleanup;
    }
    memset(entry->fields, 0, (size_t)term_count * sizeof(entry->fields[0]));
  }
  if (posting_count > 0UL) {
    entry->postings = (lc_pouch_query_index_generation_cache_posting *)
        lc_alloc_with_allocator(allocator, (size_t)posting_count *
                                               sizeof(entry->postings[0]));
    if (entry->postings == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation posting cache",
                        NULL, NULL, NULL);
      goto cleanup;
    }
    memset(entry->postings, 0,
           (size_t)posting_count * sizeof(entry->postings[0]));
  }
  entry->term_count = (size_t)term_count;
  entry->posting_count = (size_t)posting_count;
  if (term_count > 0UL) {
    term_id_seen = (unsigned char *)lc_calloc_with_allocator(
        allocator, (size_t)term_count + 1U, sizeof(term_id_seen[0]));
    if (term_id_seen == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation term id set", NULL,
                        NULL, NULL);
      goto cleanup;
    }
  }
  for (index = 0UL; index < term_count; ++index) {
    char *field_hex;
    char *value_hex;
    const unsigned char *trigram_bytes;
    unsigned long term_id;
    unsigned long field_id;
    unsigned char value_type;
    unsigned char value_encoding;

    field_hex = NULL;
    value_hex = NULL;
    trigram_bytes = NULL;
    field_id = 0UL;
    value_encoding = LC_POUCH_INDEX_TERM_VALUE_STRING;
    if (!lc_pouch_query_index_binary_ulong(&cursor, &term_id) ||
        !lc_pouch_query_index_binary_u8(&cursor, &value_type)) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    if (version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V1 &&
        (!lc_pouch_query_index_binary_string(&cursor, &field_hex) ||
         !lc_pouch_query_index_binary_string(&cursor, &value_hex))) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    if (version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2 &&
        (!lc_pouch_query_index_binary_ulong(&cursor, &field_id) ||
         !lc_pouch_query_index_binary_u8(&cursor, &value_encoding) ||
         field_id == 0UL || field_id > field_count ||
         (value_encoding != LC_POUCH_INDEX_TERM_VALUE_STRING &&
          value_encoding != LC_POUCH_INDEX_TERM_VALUE_TRIGRAM))) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    if (version == LC_POUCH_INDEX_TERM_GENERATION_VERSION_V2) {
      field_hex = field_dictionary[field_id - 1UL];
      if (value_encoding == LC_POUCH_INDEX_TERM_VALUE_STRING) {
        if (!lc_pouch_query_index_binary_string(&cursor, &value_hex)) {
          rc = LC_ERR_INVALID;
          goto cleanup;
        }
      } else {
        if (!lc_pouch_query_index_binary_read(&cursor, 3U, &trigram_bytes)) {
          rc = LC_ERR_INVALID;
          goto cleanup;
        }
        lc_pouch_query_index_trigram_hex(
            ((unsigned long)trigram_bytes[0] << 16U) |
                ((unsigned long)trigram_bytes[1] << 8U) |
                (unsigned long)trigram_bytes[2],
            entry->terms[index].value_hex_inline);
        value_hex = entry->terms[index].value_hex_inline;
      }
    }
    if (term_id == 0UL ||
        (value_encoding == LC_POUCH_INDEX_TERM_VALUE_TRIGRAM &&
         value_type != (unsigned char)'s') ||
        (value_type != (unsigned char)'s' && value_type != (unsigned char)'n' &&
         value_type != (unsigned char)'b' && value_type != (unsigned char)'z' &&
         value_type != (unsigned char)LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE &&
         value_type != (unsigned char)LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE &&
         value_type != (unsigned char)LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE) ||
        !lc_pouch_query_index_hex_token_valid(field_hex) ||
        !lc_pouch_query_index_hex_token_valid(value_hex)) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    if (term_id > term_count || term_id_seen[term_id] != 0U) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    term_id_seen[term_id] = 1U;
    if (index > 0UL && lc_pouch_query_index_generation_cache_term_compare_parts(
                           entry->terms[index - 1UL].field_hex,
                           entry->terms[index - 1UL].value_hex,
                           entry->terms[index - 1UL].value_type, field_hex,
                           value_hex, (char)value_type) >= 0) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    entry->terms[index].field_hex = field_hex;
    entry->terms[index].value_hex = value_hex;
    entry->terms[index].value_type = (char)value_type;
    entry->terms[index].term_id = term_id;
    entry->terms[index].ordinal = index;
    if (index == 0UL ||
        strcmp(entry->terms[index - 1UL].field_hex, field_hex) != 0) {
      entry->fields[entry->field_count].field_hex = field_hex;
      ++entry->field_count;
    }
  }
  for (index = 0UL; index < posting_count; ++index) {
    const unsigned char *payload;
    unsigned long term_id;
    unsigned long count;
    unsigned long max_doc_id;
    unsigned long payload_length;
    unsigned char kind;

    payload = NULL;
    if (!lc_pouch_query_index_binary_ulong(&cursor, &term_id) ||
        !lc_pouch_query_index_binary_u8(&cursor, &kind) ||
        !lc_pouch_query_index_binary_ulong(&cursor, &count) ||
        !lc_pouch_query_index_binary_ulong(&cursor, &max_doc_id) ||
        !lc_pouch_query_index_binary_ulong(&cursor, &payload_length) ||
        !lc_pouch_query_index_binary_read(&cursor, (size_t)payload_length,
                                          &payload) ||
        term_id == 0UL ||
        (kind != (unsigned char)'s' && kind != (unsigned char)'d') ||
        (index > 0UL && entry->postings[index - 1UL].term_id >= term_id) ||
        term_id > term_count || term_id_seen[term_id] == 0U ||
        (count == 0UL && payload_length != 0UL) ||
        (count > 0UL && payload_length == 0UL)) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    entry->postings[index].term_id = term_id;
    entry->postings[index].kind = (char)kind;
    entry->postings[index].count = count;
    entry->postings[index].max_doc_id = max_doc_id;
    entry->postings[index].payload_length = payload_length;
    entry->postings[index].payload = payload;
  }
  if (cursor.offset != cursor.length) {
    rc = LC_ERR_INVALID;
    goto cleanup;
  }
cleanup:
  lc_free_with_allocator(allocator, term_id_seen);
  lc_free_with_allocator(allocator, field_dictionary);
  return rc;
}

static int lc_pouch_query_index_generation_cache_get(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash,
    lc_pouch_query_index_generation_cache_entry **out, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  lc_pouch_query_index_generation_cache_entry *previous;
  char *bytes;
  size_t length;
  size_t component;
  int artifact_valid;
  int rc;

  if (pouch == NULL || path == NULL || out == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation cache lookup requires inputs", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  *present = 0;
  *valid = 0;
  previous = NULL;
  entry = pouch->query_generation_cache;
  while (entry != NULL) {
    if (entry->index_seq == expected_index_seq &&
        entry->row_count == expected_row_count &&
        entry->row_hash == expected_row_hash &&
        strcmp(entry->path, path) == 0) {
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_generation_cache;
        pouch->query_generation_cache = entry;
      }
      *present = 1;
      *valid = 1;
      *out = entry;
      return LC_OK;
    }
    previous = entry;
    entry = entry->next;
  }

  bytes = NULL;
  length = 0U;
  artifact_valid = 1;
  component = lc_pouch_query_index_packed_component_for_path(path);
  if (component == LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT ||
      component == LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE ||
      component == LC_POUCH_QUERY_INDEX_PACKED_DELETE) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index generation cache path has "
                        "unexpected component",
                        NULL, NULL, "pouch");
  }
  artifact_valid = 0;
  rc = lc_pouch_query_index_read_packed_component_bytes(
      pouch, ns, path, component, &bytes, &length, present, &artifact_valid,
      error);
  if (rc != LC_OK || !*present || !artifact_valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    *valid = artifact_valid;
    return rc;
  }

  entry =
      (lc_pouch_query_index_generation_cache_entry *)lc_alloc_with_allocator(
          &pouch->allocator, sizeof(*entry));
  if (entry == NULL) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation cache entry", NULL,
                        NULL, NULL);
  }
  memset(entry, 0, sizeof(*entry));
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  entry->bytes = bytes;
  entry->length = length;
  entry->index_seq = expected_index_seq;
  entry->row_count = expected_row_count;
  entry->row_hash = expected_row_hash;
  if (entry->path == NULL) {
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation cache path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_generation_cache_parse(&pouch->allocator, entry,
                                                   error);
  if (rc != LC_OK) {
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    *valid = 0;
    return rc == LC_ERR_INVALID ? LC_OK : rc;
  }
  entry->next = pouch->query_generation_cache;
  pouch->query_generation_cache = entry;
  ++pouch->query_generation_cache_count;
  while (pouch->query_generation_cache_count > 512U) {
    lc_pouch_query_index_generation_cache_entry *victim_prev;
    lc_pouch_query_index_generation_cache_entry *victim;

    victim_prev = NULL;
    victim = pouch->query_generation_cache;
    while (victim != NULL && victim->next != NULL) {
      victim_prev = victim;
      victim = victim->next;
    }
    if (victim == NULL || victim_prev == NULL) {
      break;
    }
    victim_prev->next = NULL;
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        victim);
    lc_free_with_allocator(&pouch->allocator, victim);
    --pouch->query_generation_cache_count;
  }
  *valid = 1;
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_doc_table_cache_get(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash,
    lc_pouch_query_index_doc_table_cache_entry **out, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_doc_table_cache_entry *entry;
  lc_pouch_query_index_doc_table_cache_entry *previous;
  int rc;

  if (pouch == NULL || path == NULL || out == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc-table cache lookup requires inputs", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  *present = 0;
  *valid = 0;
  rc = lc_pouch_query_index_doc_table_cache_lock(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  previous = NULL;
  entry = pouch->query_doc_table_cache;
  while (entry != NULL) {
    if (entry->index_seq == expected_index_seq &&
        entry->row_count == expected_row_count &&
        entry->row_hash == expected_row_hash &&
        strcmp(entry->path, path) == 0) {
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_doc_table_cache;
        pouch->query_doc_table_cache = entry;
      }
      ++entry->borrow_count;
      *present = 1;
      *valid = 1;
      *out = entry;
      lc_pouch_query_index_doc_table_cache_unlock(pouch);
      return LC_OK;
    }
    previous = entry;
    entry = entry->next;
  }

  entry = (lc_pouch_query_index_doc_table_cache_entry *)lc_alloc_with_allocator(
      &pouch->allocator, sizeof(*entry));
  if (entry == NULL) {
    lc_pouch_query_index_doc_table_cache_unlock(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch doc-table cache entry", NULL,
                        NULL, NULL);
  }
  memset(entry, 0, sizeof(*entry));
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  entry->index_seq = expected_index_seq;
  entry->row_count = expected_row_count;
  entry->row_hash = expected_row_hash;
  if (entry->path == NULL) {
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    lc_pouch_query_index_doc_table_cache_unlock(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch doc-table cache path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_doc_table_generation_load_artifact(
      pouch, ns, path, expected_index_seq, expected_row_count,
      expected_row_hash, &entry->table, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    lc_pouch_query_index_doc_table_cache_unlock(pouch);
    return rc;
  }
  if (entry->table.count > 0U) {
    entry->decoded_keys = (char **)lc_alloc_with_allocator(
        &pouch->allocator, entry->table.count * sizeof(entry->decoded_keys[0]));
    if (entry->decoded_keys == NULL) {
      lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                         entry);
      lc_free_with_allocator(&pouch->allocator, entry);
      lc_pouch_query_index_doc_table_cache_unlock(pouch);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch doc-table decoded keys",
                          NULL, NULL, NULL);
    }
    memset(entry->decoded_keys, 0,
           entry->table.count * sizeof(entry->decoded_keys[0]));
    entry->decoded_key_count = entry->table.count;
  }
  entry->next = pouch->query_doc_table_cache;
  pouch->query_doc_table_cache = entry;
  ++pouch->query_doc_table_cache_count;
  entry->borrow_count = 1U;
  lc_pouch_query_index_doc_table_cache_trim(pouch);
  *out = entry;
  lc_pouch_query_index_doc_table_cache_unlock(pouch);
  return LC_OK;
}

static int lc_pouch_query_index_warm_generation_artifact(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_generation index_seq, unsigned long row_count,
    unsigned long row_hash, lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  int present;
  int valid;
  int rc;

  entry = NULL;
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_generation_cache_get(pouch, ns, path, index_seq,
                                                 row_count, row_hash, &entry,
                                                 &present, &valid, error);
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index generation artifact is not readable",
                      NULL, NULL, "pouch");
  }
  return rc;
}

int lc_pouch_query_index_warm_namespace(lc_pouch *pouch, const char *ns,
                                        lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  char *manifest_path;
  size_t segment_index;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index warm requires pouch and namespace",
                        NULL, NULL, NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  manifest_path = lc_pouch_query_index_manifest_path(pouch, ns, error);
  if (manifest_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!manifest.present) {
    goto cleanup;
  }
  if (manifest.intentionally_invalidated) {
    pslog_field fields[2];

    /* The writer deliberately retired this derived view before compaction.
     * Leave it invalid so the next query/flush rebuilds canonical state. */
    fields[0] = lc_log_str_field("ns", ns);
    fields[1] = lc_log_str_field("reason", "intentional-invalidation");
    lc_log_debug(pouch->logger, "cache.load.index.invalidated", fields, 2U);
    goto cleanup;
  }
  if (!manifest.valid) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index manifest is not readable", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  for (segment_index = 0U;
       rc == LC_OK && segment_index < manifest.segment_count; ++segment_index) {
    const lc_pouch_query_index_manifest_segment *segment;
    lc_pouch_query_index_doc_table_cache_entry *doc_entry;
    char *header_path;
    char *doc_table_path;
    char *exact_term_path;
    char *presence_term_path;
    char *range_term_path;
    char *text_term_path;
    char *trigram_term_path;
    char *temporal_term_path;
    char *delete_path;
    lc_pouch_query_index_key_hex_set deletes;
    unsigned long delete_count;
    unsigned long delete_hash;
    int present;
    int valid;

    segment = &manifest.segments[segment_index];
    doc_entry = NULL;
    header_path = NULL;
    doc_table_path = NULL;
    exact_term_path = NULL;
    presence_term_path = NULL;
    range_term_path = NULL;
    text_term_path = NULL;
    trigram_term_path = NULL;
    temporal_term_path = NULL;
    delete_path = NULL;
    memset(&deletes, 0, sizeof(deletes));
    delete_count = 0UL;
    delete_hash = lc_pouch_query_index_hash_init();
    rc = lc_pouch_query_index_segmented_paths(
        pouch, ns, segment->id, &header_path, &doc_table_path, &exact_term_path,
        &presence_term_path, &range_term_path, &text_term_path,
        &trigram_term_path, &temporal_term_path, &delete_path, error);
    if (rc == LC_OK &&
        (!segment->term_index_complete || !segment->presence_index_complete)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment completeness metadata is "
                        "not readable",
                        NULL, NULL, "pouch");
    }
    present = 0;
    valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_doc_table_cache_get(
          pouch, ns, doc_table_path, segment->index_seq, segment->row_count,
          segment->row_hash, &doc_entry, &present, &valid, error);
    }
    if (rc == LC_OK && (!present || !valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment doc table is not readable",
                        NULL, NULL, "pouch");
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, ns, exact_term_path, segment->index_seq, segment->row_count,
          segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, ns, presence_term_path, segment->index_seq, segment->row_count,
          segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, ns, range_term_path, segment->index_seq, segment->row_count,
          segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, ns, text_term_path, segment->index_seq, segment->row_count,
          segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, ns, trigram_term_path, segment->index_seq, segment->row_count,
          segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, ns, temporal_term_path, segment->index_seq, segment->row_count,
          segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_load_delete_keys(
          pouch, ns, delete_path, &deletes, &delete_count, &delete_hash, error);
    }
    if (rc == LC_OK && (delete_count != segment->delete_count ||
                        delete_hash != segment->delete_hash)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment deletes do not match the "
                        "manifest",
                        NULL, NULL, "pouch");
    }
    lc_pouch_query_index_doc_table_cache_release(pouch, &doc_entry);
    lc_pouch_query_index_key_hex_set_cleanup(&pouch->allocator, &deletes);
    lc_pouch_query_index_segmented_paths_cleanup(
        pouch, &header_path, &doc_table_path, &exact_term_path,
        &presence_term_path, &range_term_path, &text_term_path,
        &trigram_term_path, &temporal_term_path, &delete_path);
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_manifest_trust_remember(pouch, ns, manifest.index_seq,
                                                 &manifest);
  }

cleanup:
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

static int lc_pouch_query_index_generation_cache_append_posting(
    const lc_allocator *allocator,
    const lc_pouch_query_index_generation_cache_entry *entry,
    unsigned long term_id, size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  const lc_pouch_query_index_generation_cache_posting *posting;

  if (!lc_pouch_query_index_generation_cache_find_posting(entry, term_id,
                                                          &posting)) {
    return LC_OK;
  }
  return lc_pouch_query_index_append_generation_posting(
      allocator, posting->kind, posting->count, posting->max_doc_id,
      posting->payload_length, posting->payload, value_index,
      docid_filter_sorted, docids, error);
}

static int lc_pouch_query_index_collect_all_text_token_docids(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, const char *token_hex,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  const lc_pouch_query_index_generation_cache_term *term;
  int rc;

  if (pouch == NULL || ns == NULL || path == NULL || token_hex == NULL ||
      docids == NULL || present == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch all-text token lookup requires inputs", NULL,
                        NULL, "pouch");
  }
  *present = 0;
  *valid = 0;
  entry = NULL;
  rc = lc_pouch_query_index_generation_cache_get(
      pouch, ns, path, expected_index_seq, expected_row_count,
      expected_row_hash, &entry, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    return rc;
  }
  term = NULL;
  if (lc_pouch_query_index_generation_cache_find_term(
          entry, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, token_hex,
          LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE, &term)) {
    rc = lc_pouch_query_index_generation_cache_append_posting(
        &pouch->allocator, entry, term->term_id, 0U, docid_filter_sorted,
        docids, error);
  }
  return rc;
}

static int lc_pouch_query_index_direct_trigram_key(const char *hex,
                                                   unsigned long *out) {
  int first;
  int second;
  size_t index;
  unsigned long key;

  if (hex == NULL || out == NULL || strlen(hex) != 6U) {
    return 0;
  }
  key = 0UL;
  for (index = 0U; index < 6U; index += 2U) {
    first = lc_pouch_query_index_hex_value((unsigned char)hex[index]);
    second = lc_pouch_query_index_hex_value((unsigned char)hex[index + 1U]);
    if (first < 0 || second < 0) {
      return 0;
    }
    key = (key << 8U) | (unsigned long)((first << 4) | second);
  }
  *out = key;
  return 1;
}

static int lc_pouch_query_index_direct_find_trigram(
    const lc_pouch_index_term_generation *generation, const char *field_hex,
    unsigned long trigram_key, unsigned long *term_id_out) {
  size_t first;
  size_t last;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (generation == NULL || field_hex == NULL || term_id_out == NULL) {
    return 0;
  }
  first = 0U;
  last = generation->terms.count;
  while (first < last) {
    size_t middle;
    const lc_pouch_index_term_entry *term;
    int field_compare;

    middle = first + ((last - first) / 2U);
    term = &generation->terms.items[middle];
    field_compare = strcmp(term->field_hex, field_hex);
    if (field_compare < 0 ||
        (field_compare == 0 && term->trigram_key < trigram_key)) {
      first = middle + 1U;
    } else {
      last = middle;
    }
  }
  if (first >= generation->terms.count ||
      strcmp(generation->terms.items[first].field_hex, field_hex) != 0 ||
      generation->terms.items[first].trigram_key != trigram_key) {
    return 0;
  }
  *term_id_out = generation->terms.items[first].term_id;
  return 1;
}

static int lc_pouch_query_index_direct_append_posting(
    const lc_allocator *allocator,
    const lc_pouch_index_term_generation *generation, unsigned long term_id,
    size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_query_index_direct_posting_append append;

  if (allocator == NULL || generation == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct posting append requires inputs", NULL,
                        NULL, "pouch");
  }
  memset(&append, 0, sizeof(append));
  append.allocator = allocator;
  append.filter = docid_filter_sorted;
  append.docids = docids;
  append.value_index = value_index;
  return lc_pouch_index_term_posting_table_visit(
      &generation->postings, term_id,
      lc_pouch_query_index_direct_posting_append_docid, &append, error);
}

static void lc_pouch_query_index_docid_bitmap_cleanup(
    lc_pouch_query_index_docid_bitmap *bitmap) {
  if (bitmap == NULL) {
    return;
  }
  lc_free_with_allocator(bitmap->allocator, bitmap->bits);
  memset(bitmap, 0, sizeof(*bitmap));
}

static int lc_pouch_query_index_docid_bitmap_init(
    const lc_allocator *allocator, unsigned long doc_count,
    lc_pouch_query_index_docid_bitmap *bitmap, lc_error *error) {
  size_t length;

  if (allocator == NULL || bitmap == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query bitmap requires allocator and output",
                        NULL, NULL, NULL);
  }
  memset(bitmap, 0, sizeof(*bitmap));
  if (doc_count == 0UL) {
    bitmap->allocator = allocator;
    return LC_OK;
  }
  if (doc_count > (unsigned long)((size_t)-1 - ((size_t)CHAR_BIT - 1U))) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query bitmap exceeds local limit", NULL, NULL,
                        "pouch");
  }
  length = ((size_t)doc_count + (size_t)CHAR_BIT - 1U) / (size_t)CHAR_BIT;
  bitmap->bits = (unsigned char *)lc_calloc_with_allocator(
      allocator, length, sizeof(*bitmap->bits));
  if (bitmap->bits == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query bitmap", NULL, NULL,
                        NULL);
  }
  bitmap->allocator = allocator;
  bitmap->length = length;
  bitmap->doc_count = doc_count;
  return LC_OK;
}

static int lc_pouch_query_index_docid_bitmap_mark(void *context,
                                                  unsigned long doc_id,
                                                  lc_error *error) {
  lc_pouch_query_index_docid_bitmap *bitmap;
  size_t byte_index;

  bitmap = (lc_pouch_query_index_docid_bitmap *)context;
  if (bitmap == NULL || (bitmap->length > 0U && bitmap->bits == NULL) ||
      doc_id >= bitmap->doc_count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query bitmap received invalid docID", NULL, NULL,
                        "pouch");
  }
  byte_index = (size_t)(doc_id / (unsigned long)CHAR_BIT);
  bitmap->bits[byte_index] =
      (unsigned char)(bitmap->bits[byte_index] |
                      (unsigned char)(1U << (doc_id % CHAR_BIT)));
  return LC_OK;
}

static int lc_pouch_query_index_docid_bitmap_has(
    const lc_pouch_query_index_docid_bitmap *bitmap, unsigned long doc_id) {
  size_t byte_index;

  if (bitmap == NULL || (bitmap->length > 0U && bitmap->bits == NULL) ||
      doc_id >= bitmap->doc_count) {
    return 0;
  }
  byte_index = (size_t)(doc_id / (unsigned long)CHAR_BIT);
  return (bitmap->bits[byte_index] &
          (unsigned char)(1U << (doc_id % CHAR_BIT))) != 0U;
}

/* Mark one durable posting directly into a request-local bitmap. This keeps
 * `/...` as a logical union of concrete fields without materializing and
 * sorting a temporary result list for every field. */
static int lc_pouch_query_index_generation_cache_mark_posting(
    const lc_pouch_query_index_generation_cache_entry *entry,
    unsigned long term_id, lc_pouch_query_index_docid_bitmap *bitmap,
    lc_error *error) {
  const lc_pouch_query_index_generation_cache_posting *posting;
  size_t index;
  size_t offset;
  size_t seen;
  unsigned long delta;
  unsigned long doc_id;
  int rc;

  if (entry == NULL || bitmap == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting bitmap requires generation and bitmap",
                        NULL, NULL, NULL);
  }
  if (!lc_pouch_query_index_generation_cache_find_posting(entry, term_id,
                                                          &posting)) {
    return LC_OK;
  }
  if ((posting->payload == NULL && posting->payload_length > 0UL) ||
      (posting->count == 0UL && posting->payload_length != 0UL) ||
      (posting->count > 0UL && posting->payload_length == 0UL) ||
      posting->payload_length > (unsigned long)((size_t)-1)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting count and payload mismatch", NULL, NULL,
                        "pouch");
  }
  rc = LC_OK;
  if (posting->kind == 's') {
    doc_id = 0UL;
    delta = 0UL;
    offset = 0U;
    for (index = 0U; rc == LC_OK && index < (size_t)posting->count; ++index) {
      rc = lc_pouch_query_index_read_sparse_posting_varint(
          posting->payload, (size_t)posting->payload_length, &offset, &delta,
          error);
      if (rc != LC_OK) {
        break;
      }
      if (index > 0U && delta > ULONG_MAX - doc_id) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch posting docID delta overflows", NULL, NULL,
                          "pouch");
        break;
      }
      doc_id = index == 0U ? delta : doc_id + delta;
      rc = lc_pouch_query_index_docid_bitmap_mark(bitmap, doc_id, error);
    }
    if (rc == LC_OK && offset != (size_t)posting->payload_length) {
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch posting has trailing bytes", NULL, NULL, "pouch");
    }
    if (rc == LC_OK && posting->count > 0UL && doc_id != posting->max_doc_id) {
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch posting max docID mismatch", NULL, NULL, "pouch");
    }
  } else if (posting->kind == 'd') {
    seen = 0U;
    for (index = 0U; rc == LC_OK && index < (size_t)posting->payload_length;
         ++index) {
      unsigned int bit_index;

      for (bit_index = 0U; bit_index < (unsigned int)CHAR_BIT; ++bit_index) {
        if ((posting->payload[index] & (unsigned char)(1U << bit_index)) ==
            0U) {
          continue;
        }
        doc_id = ((unsigned long)index * (unsigned long)CHAR_BIT) +
                 (unsigned long)bit_index;
        if (doc_id > posting->max_doc_id) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch posting dense bit exceeds max docID", NULL,
                            NULL, "pouch");
          break;
        }
        rc = lc_pouch_query_index_docid_bitmap_mark(bitmap, doc_id, error);
        if (rc != LC_OK) {
          break;
        }
        ++seen;
      }
    }
    if (rc == LC_OK && seen != (size_t)posting->count) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting count mismatch", NULL, NULL, "pouch");
    }
  } else {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch posting has invalid kind", NULL, NULL, "pouch");
  }
  return rc;
}

static int lc_pouch_query_index_docid_bitmap_covers_list(
    const lc_pouch_query_index_docid_bitmap *bitmap,
    const lc_pouch_index_result_docid_list *docids) {
  size_t index;

  if (bitmap == NULL || docids == NULL) {
    return 0;
  }
  for (index = 0U; index < docids->count; ++index) {
    if (!lc_pouch_query_index_docid_bitmap_has(bitmap,
                                               docids->items[index].doc_id)) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_docid_bitmap_append(
    const lc_allocator *allocator,
    const lc_pouch_query_index_docid_bitmap *bitmap,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  unsigned long doc_id;
  int rc;

  if (bitmap == NULL || docids == NULL ||
      (bitmap->length > 0U && bitmap->bits == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query bitmap append requires inputs", NULL, NULL,
                        NULL);
  }
  rc = LC_OK;
  for (doc_id = 0UL; rc == LC_OK && doc_id < bitmap->doc_count; ++doc_id) {
    size_t byte_index;

    byte_index = (size_t)(doc_id / (unsigned long)CHAR_BIT);
    if ((bitmap->bits[byte_index] &
         (unsigned char)(1U << (doc_id % CHAR_BIT))) != 0U) {
      rc = lc_pouch_index_result_docid_list_add(allocator, docids, doc_id, 0U,
                                                error);
    }
  }
  return rc;
}

static int lc_pouch_query_index_direct_mark_posting(
    const lc_pouch_index_term_generation *generation, unsigned long term_id,
    lc_pouch_query_index_docid_bitmap *bitmap, lc_error *error) {
  int dense_applied;

  if (generation == NULL || bitmap == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct bitmap posting requires inputs", NULL,
                        NULL, NULL);
  }
  dense_applied = 0;
  if (bitmap->length > 0U) {
    int rc;

    rc = lc_pouch_index_term_posting_table_or_dense_bits(
        &generation->postings, term_id, bitmap->bits, bitmap->length,
        &dense_applied, error);
    if (rc != LC_OK || dense_applied) {
      return rc;
    }
  }
  return lc_pouch_index_term_posting_table_visit(
      &generation->postings, term_id, lc_pouch_query_index_docid_bitmap_mark,
      bitmap, error);
}

static int lc_pouch_query_index_direct_collect_trigram_docids(
    lc_pouch *pouch, const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *needle_hex,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  const lc_pouch_index_term_generation *generation;
  unsigned long trigram_key;
  size_t index;
  int rc;

  if (pouch == NULL || entry == NULL || field_hex == NULL ||
      needle_hex == NULL || docids == NULL ||
      !lc_pouch_query_index_direct_trigram_key(needle_hex, &trigram_key)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct trigram lookup requires valid inputs",
                        NULL, NULL, "pouch");
  }
  generation = &entry->direct_generation;
  rc = LC_OK;
  if (!lc_pouch_query_index_field_hex_is_any_text(field_hex)) {
    unsigned long term_id;

    term_id = 0UL;
    if (lc_pouch_query_index_direct_find_trigram(generation, field_hex,
                                                 trigram_key, &term_id)) {
      rc = lc_pouch_query_index_direct_append_posting(
          &pouch->allocator, generation, term_id, 0U, NULL, docids, error);
    }
    return rc;
  }
  if (lc_pouch_query_index_generation_cache_has_field(entry, field_hex)) {
    unsigned long term_id;

    term_id = 0UL;
    if (lc_pouch_query_index_direct_find_trigram(generation, field_hex,
                                                 trigram_key, &term_id)) {
      rc = lc_pouch_query_index_direct_append_posting(
          &pouch->allocator, generation, term_id, 0U, NULL, docids, error);
    }
    return rc;
  }
  {
    lc_pouch_query_index_docid_bitmap bitmap;

    memset(&bitmap, 0, sizeof(bitmap));
    rc = lc_pouch_query_index_docid_bitmap_init(
        &pouch->allocator, generation->row_count, &bitmap, error);
    for (index = 0U; rc == LC_OK && index < entry->field_count; ++index) {
      const char *candidate_field;
      unsigned long term_id;

      candidate_field = entry->fields[index].field_hex;
      if (lc_pouch_query_index_field_hex_is_any_text(candidate_field)) {
        continue;
      }
      term_id = 0UL;
      if (lc_pouch_query_index_direct_find_trigram(generation, candidate_field,
                                                   trigram_key, &term_id)) {
        rc = lc_pouch_query_index_direct_mark_posting(generation, term_id,
                                                      &bitmap, error);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_docid_bitmap_append(&pouch->allocator, &bitmap,
                                                    docids, error);
    }
    lc_pouch_query_index_docid_bitmap_cleanup(&bitmap);
  }
  return rc;
}

static int lc_pouch_query_index_read_sparse_posting_varint(
    const unsigned char *bytes, size_t length, size_t *offset,
    unsigned long *out, lc_error *error) {
  unsigned long value;
  unsigned int shift;

  if ((bytes == NULL && length > 0U) || offset == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting varint read requires inputs", NULL, NULL,
                        NULL);
  }
  value = 0UL;
  shift = 0U;
  while (*offset < length) {
    unsigned char byte;

    byte = bytes[*offset];
    ++*offset;
    if (shift >= (unsigned int)(sizeof(unsigned long) * CHAR_BIT)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch posting varint is too large", NULL, NULL,
                          "pouch");
    }
    value |= ((unsigned long)(byte & 0x7FU)) << shift;
    if ((byte & 0x80U) == 0U) {
      *out = value;
      return LC_OK;
    }
    shift += 7U;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch posting varint is truncated", NULL, NULL, "pouch");
}

static int lc_pouch_query_index_docid_list_contains_sorted(
    const lc_pouch_index_result_docid_list *list, unsigned long doc_id) {
  size_t left;
  size_t right;

  if (list == NULL) {
    return 1;
  }
  left = 0U;
  right = list->count;
  while (left < right) {
    size_t middle;
    unsigned long candidate;

    middle = left + ((right - left) / 2U);
    candidate = list->items[middle].doc_id;
    if (candidate == doc_id) {
      return 1;
    }
    if (candidate < doc_id) {
      left = middle + 1U;
    } else {
      right = middle;
    }
  }
  return 0;
}

static int lc_pouch_query_index_docid_list_covers_generation(
    const lc_pouch_index_result_docid_list *list,
    unsigned long generation_row_count) {
  size_t index;

  if (list == NULL || list->count != (size_t)generation_row_count) {
    return 0;
  }
  for (index = 0U; index < list->count; ++index) {
    if (list->items[index].doc_id != (unsigned long)index) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_direct_posting_append_docid(
    void *context, unsigned long doc_id, lc_error *error) {
  lc_pouch_query_index_direct_posting_append *append;

  append = (lc_pouch_query_index_direct_posting_append *)context;
  if (append == NULL || append->docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct posting append requires context", NULL,
                        NULL, "pouch");
  }
  if (!lc_pouch_query_index_docid_list_contains_sorted(append->filter,
                                                       doc_id)) {
    return LC_OK;
  }
  return lc_pouch_index_result_docid_list_add(
      append->allocator, append->docids, doc_id, append->value_index, error);
}

/* The direct full-text path sees the same docID through every matching
 * indexed value. Keep the first value-index selected by term order, which is
 * the same value that the later docID sort/compact phase would retain. */
static int lc_pouch_query_index_direct_unique_posting_append_docid(
    void *context, unsigned long doc_id, lc_error *error) {
  lc_pouch_query_index_direct_unique_posting_append *append;
  int rc;

  append = (lc_pouch_query_index_direct_unique_posting_append *)context;
  if (append == NULL || append->seen == NULL || append->docids == NULL ||
      append->pending == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct unique posting requires context", NULL,
                        NULL, "pouch");
  }
  if (!lc_pouch_query_index_docid_list_contains_sorted(append->filter,
                                                       doc_id) ||
      lc_pouch_query_index_docid_bitmap_has(append->seen, doc_id)) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_docid_bitmap_mark(append->seen, doc_id, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_result_docid_list_add(append->allocator, append->docids,
                                            doc_id, append->value_index, error);
  if (rc == LC_OK && *append->pending > 0U) {
    --*append->pending;
  }
  return rc;
}

static int lc_pouch_query_index_direct_append_unique_posting(
    const lc_allocator *allocator,
    const lc_pouch_index_term_generation *generation, unsigned long term_id,
    size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_query_index_docid_bitmap *seen,
    lc_pouch_index_result_docid_list *docids, size_t *pending,
    lc_error *error) {
  lc_pouch_query_index_direct_unique_posting_append append;

  if (allocator == NULL || generation == NULL || seen == NULL ||
      docids == NULL || pending == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct unique posting requires inputs", NULL,
                        NULL, "pouch");
  }
  memset(&append, 0, sizeof(append));
  append.allocator = allocator;
  append.filter = docid_filter_sorted;
  append.seen = seen;
  append.docids = docids;
  append.value_index = value_index;
  append.pending = pending;
  return lc_pouch_index_term_posting_table_visit(
      &generation->postings, term_id,
      lc_pouch_query_index_direct_unique_posting_append_docid, &append, error);
}

static int lc_pouch_query_index_docid_list_intersect_sorted(
    const lc_allocator *allocator, const lc_pouch_index_result_docid_list *left,
    const lc_pouch_index_result_docid_list *right,
    lc_pouch_index_result_docid_list *out, lc_error *error) {
  size_t left_index;
  size_t right_index;
  int rc;

  if (left == NULL || right == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch docID intersection requires inputs", NULL, NULL,
                        NULL);
  }
  left_index = 0U;
  right_index = 0U;
  rc = LC_OK;
  while (rc == LC_OK && left_index < left->count &&
         right_index < right->count) {
    unsigned long left_doc_id;
    unsigned long right_doc_id;

    left_doc_id = left->items[left_index].doc_id;
    right_doc_id = right->items[right_index].doc_id;
    if (left_doc_id == right_doc_id) {
      rc = lc_pouch_index_result_docid_list_add(
          allocator, out, left_doc_id, left->items[left_index].value_index,
          error);
      ++left_index;
      ++right_index;
    } else if (left_doc_id < right_doc_id) {
      ++left_index;
    } else {
      ++right_index;
    }
  }
  return rc;
}

static int lc_pouch_query_index_docid_list_is_subset_sorted(
    const lc_pouch_index_result_docid_list *subset,
    const lc_pouch_index_result_docid_list *superset) {
  size_t subset_index;
  size_t superset_index;

  if (subset == NULL || superset == NULL) {
    return 0;
  }
  subset_index = 0U;
  superset_index = 0U;
  while (subset_index < subset->count && superset_index < superset->count) {
    unsigned long subset_doc_id;
    unsigned long superset_doc_id;

    subset_doc_id = subset->items[subset_index].doc_id;
    superset_doc_id = superset->items[superset_index].doc_id;
    if (subset_doc_id == superset_doc_id) {
      ++subset_index;
      ++superset_index;
    } else if (subset_doc_id > superset_doc_id) {
      ++superset_index;
    } else {
      return 0;
    }
  }
  return subset_index == subset->count;
}

static int lc_pouch_query_index_collect_trigram_docids(
    lc_pouch *pouch, const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *needle_hex,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  size_t index;
  int rc;

  if (pouch == NULL || entry == NULL || field_hex == NULL ||
      needle_hex == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram collection requires inputs", NULL, NULL,
                        NULL);
  }
  if (entry->has_direct_generation) {
    return lc_pouch_query_index_direct_collect_trigram_docids(
        pouch, entry, field_hex, needle_hex, docids, error);
  }
  rc = LC_OK;
  if (!lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
      lc_pouch_query_index_generation_cache_has_field(entry, field_hex)) {
    const lc_pouch_query_index_generation_cache_term *term;

    if (lc_pouch_query_index_generation_cache_find_term(
            entry, field_hex, needle_hex, 's', &term)) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, NULL, docids, error);
    }
    return rc;
  }

  for (index = 0U; rc == LC_OK && index < entry->field_count; ++index) {
    const lc_pouch_query_index_generation_cache_field *field;
    const lc_pouch_query_index_generation_cache_term *term;

    field = &entry->fields[index];
    if (lc_pouch_query_index_field_hex_is_any_text(field->field_hex)) {
      continue;
    }
    if (lc_pouch_query_index_generation_cache_find_term(
            entry, field->field_hex, needle_hex, 's', &term)) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, NULL, docids, error);
    }
  }
  return rc;
}

/* For an all-text direct generation, each trigram's postings are the union
 * across indexed fields. Intersect those unions as bitmaps and materialize
 * candidate docIDs once. This preserves the fallback posting, which is not
 * constrained by trigrams because it represents values that were not fully
 * indexed. */
static int lc_pouch_query_index_direct_collect_all_text_trigram_intersection(
    lc_pouch *pouch, const lc_pouch_query_index_generation_cache_entry *entry,
    const char trigrams[][7], size_t trigram_count,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  const lc_pouch_index_term_generation *generation;
  lc_pouch_query_index_docid_bitmap intersection;
  lc_pouch_query_index_docid_bitmap matches;
  size_t trigram_index;
  size_t field_index;
  size_t byte_index;
  unsigned long trigram_key;
  int rc;

  if (pouch == NULL || entry == NULL || trigrams == NULL ||
      trigram_count == 0U || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct all-text trigram intersection requires "
                        "inputs",
                        NULL, NULL, NULL);
  }
  generation = &entry->direct_generation;
  memset(&intersection, 0, sizeof(intersection));
  memset(&matches, 0, sizeof(matches));
  rc = lc_pouch_query_index_docid_bitmap_init(
      &pouch->allocator, generation->row_count, &intersection, error);
  if (rc == LC_OK && intersection.length > 0U) {
    memset(intersection.bits, UCHAR_MAX, intersection.length);
  }
  for (trigram_index = 0U; rc == LC_OK && trigram_index < trigram_count;
       ++trigram_index) {
    if (!lc_pouch_query_index_direct_trigram_key(trigrams[trigram_index],
                                                 &trigram_key)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct all-text trigram is invalid", NULL, NULL,
                        "pouch");
      break;
    }
    rc = lc_pouch_query_index_docid_bitmap_init(
        &pouch->allocator, generation->row_count, &matches, error);
    for (field_index = 0U; rc == LC_OK && field_index < entry->field_count;
         ++field_index) {
      const lc_pouch_query_index_generation_cache_field *field;
      unsigned long term_id;

      field = &entry->fields[field_index];
      if (lc_pouch_query_index_field_hex_is_any_text(field->field_hex)) {
        continue;
      }
      term_id = 0UL;
      if (lc_pouch_query_index_direct_find_trigram(generation, field->field_hex,
                                                   trigram_key, &term_id)) {
        rc = lc_pouch_query_index_direct_mark_posting(generation, term_id,
                                                      &matches, error);
      }
    }
    for (byte_index = 0U; rc == LC_OK && byte_index < intersection.length;
         ++byte_index) {
      intersection.bits[byte_index] =
          (unsigned char)(intersection.bits[byte_index] &
                          matches.bits[byte_index]);
    }
    lc_pouch_query_index_docid_bitmap_cleanup(&matches);
  }
  if (rc == LC_OK) {
    if (!lc_pouch_query_index_direct_trigram_key(
            LC_POUCH_QUERY_INDEX_TRIGRAM_FALLBACK_HEX, &trigram_key)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct all-text fallback trigram is invalid",
                        NULL, NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_docid_bitmap_init(
        &pouch->allocator, generation->row_count, &matches, error);
  }
  for (field_index = 0U; rc == LC_OK && field_index < entry->field_count;
       ++field_index) {
    const lc_pouch_query_index_generation_cache_field *field;
    unsigned long term_id;

    field = &entry->fields[field_index];
    if (lc_pouch_query_index_field_hex_is_any_text(field->field_hex)) {
      continue;
    }
    term_id = 0UL;
    if (lc_pouch_query_index_direct_find_trigram(generation, field->field_hex,
                                                 trigram_key, &term_id)) {
      rc = lc_pouch_query_index_direct_mark_posting(generation, term_id,
                                                    &matches, error);
    }
  }
  for (byte_index = 0U; rc == LC_OK && byte_index < intersection.length;
       ++byte_index) {
    intersection.bits[byte_index] =
        (unsigned char)(intersection.bits[byte_index] |
                        matches.bits[byte_index]);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_docid_bitmap_append(&pouch->allocator,
                                                  &intersection, docids, error);
  }
  lc_pouch_query_index_docid_bitmap_cleanup(&matches);
  lc_pouch_query_index_docid_bitmap_cleanup(&intersection);
  return rc;
}

/* `/...` has no persisted synthetic field. Fold each durable field posting
 * into a bounded request-local bitmap, intersect the requested trigrams, and
 * then include the fallback postings for truncated values. */
static int lc_pouch_query_index_collect_all_text_trigram_intersection(
    lc_pouch *pouch, const lc_pouch_query_index_generation_cache_entry *entry,
    const char trigrams[][7], size_t trigram_count,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_query_index_docid_bitmap intersection;
  lc_pouch_query_index_docid_bitmap matches;
  size_t byte_index;
  size_t field_index;
  size_t trigram_index;
  int rc;

  if (pouch == NULL || entry == NULL || trigrams == NULL ||
      trigram_count == 0U || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch all-text trigram intersection requires inputs",
                        NULL, NULL, "pouch");
  }
  memset(&intersection, 0, sizeof(intersection));
  memset(&matches, 0, sizeof(matches));
  rc = lc_pouch_query_index_docid_bitmap_init(
      &pouch->allocator, entry->row_count, &intersection, error);
  if (rc == LC_OK && intersection.length > 0U) {
    memset(intersection.bits, UCHAR_MAX, intersection.length);
  }
  for (trigram_index = 0U; rc == LC_OK && trigram_index < trigram_count;
       ++trigram_index) {
    rc = lc_pouch_query_index_docid_bitmap_init(
        &pouch->allocator, entry->row_count, &matches, error);
    for (field_index = 0U; rc == LC_OK && field_index < entry->field_count;
         ++field_index) {
      const lc_pouch_query_index_generation_cache_field *field;
      const lc_pouch_query_index_generation_cache_term *term;

      field = &entry->fields[field_index];
      if (lc_pouch_query_index_field_hex_is_any_text(field->field_hex)) {
        continue;
      }
      term = NULL;
      if (lc_pouch_query_index_generation_cache_find_term(
              entry, field->field_hex, trigrams[trigram_index], 's', &term)) {
        rc = lc_pouch_query_index_generation_cache_mark_posting(
            entry, term->term_id, &matches, error);
      }
    }
    for (byte_index = 0U; rc == LC_OK && byte_index < intersection.length;
         ++byte_index) {
      intersection.bits[byte_index] =
          (unsigned char)(intersection.bits[byte_index] &
                          matches.bits[byte_index]);
    }
    lc_pouch_query_index_docid_bitmap_cleanup(&matches);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_docid_bitmap_init(
        &pouch->allocator, entry->row_count, &matches, error);
  }
  for (field_index = 0U; rc == LC_OK && field_index < entry->field_count;
       ++field_index) {
    const lc_pouch_query_index_generation_cache_field *field;
    const lc_pouch_query_index_generation_cache_term *term;

    field = &entry->fields[field_index];
    if (lc_pouch_query_index_field_hex_is_any_text(field->field_hex)) {
      continue;
    }
    term = NULL;
    if (lc_pouch_query_index_generation_cache_find_term(
            entry, field->field_hex, LC_POUCH_QUERY_INDEX_TRIGRAM_FALLBACK_HEX,
            's', &term)) {
      rc = lc_pouch_query_index_generation_cache_mark_posting(
          entry, term->term_id, &matches, error);
    }
  }
  for (byte_index = 0U; rc == LC_OK && byte_index < intersection.length;
       ++byte_index) {
    intersection.bits[byte_index] =
        (unsigned char)(intersection.bits[byte_index] |
                        matches.bits[byte_index]);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_docid_bitmap_append(&pouch->allocator,
                                                  &intersection, docids, error);
  }
  lc_pouch_query_index_docid_bitmap_cleanup(&matches);
  lc_pouch_query_index_docid_bitmap_cleanup(&intersection);
  return rc;
}

static int lc_pouch_query_index_collect_trigram_intersection_docids(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, const char *field_hex,
    const char trigrams[][7], size_t trigram_count,
    lc_pouch_index_result_docid_list *docids, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  lc_pouch_index_result_docid_list intersection;
  size_t trigram_index;
  int has_intersection;
  int rc;

  if (pouch == NULL || field_hex == NULL || trigrams == NULL ||
      trigram_count == 0U || docids == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram intersection requires inputs", NULL,
                        NULL, NULL);
  }
  *present = 0;
  *valid = 0;
  entry = NULL;
  rc = lc_pouch_query_index_generation_cache_get(
      pouch, ns, path, expected_index_seq, expected_row_count,
      expected_row_hash, &entry, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    return rc;
  }
  if (lc_pouch_query_index_field_hex_is_any_text(field_hex)) {
    if (entry->has_direct_generation) {
      return lc_pouch_query_index_direct_collect_all_text_trigram_intersection(
          pouch, entry, trigrams, trigram_count, docids, error);
    }
    return lc_pouch_query_index_collect_all_text_trigram_intersection(
        pouch, entry, trigrams, trigram_count, docids, error);
  }

  memset(&intersection, 0, sizeof(intersection));
  has_intersection = 0;
  for (trigram_index = 0U; rc == LC_OK && trigram_index < trigram_count;
       ++trigram_index) {
    lc_pouch_index_result_docid_list postings;

    memset(&postings, 0, sizeof(postings));
    rc = lc_pouch_query_index_collect_trigram_docids(
        pouch, entry, field_hex, trigrams[trigram_index], &postings, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_result_docid_list_sort_compact(&pouch->allocator,
                                                         &postings, error);
    }
    if (rc == LC_OK && !has_intersection) {
      intersection = postings;
      memset(&postings, 0, sizeof(postings));
      has_intersection = 1;
    } else if (rc == LC_OK) {
      lc_pouch_index_result_docid_list next;

      memset(&next, 0, sizeof(next));
      rc = lc_pouch_query_index_docid_list_intersect_sorted(
          &pouch->allocator, &intersection, &postings, &next, error);
      lc_pouch_index_result_docid_list_cleanup(&pouch->allocator,
                                               &intersection);
      intersection = next;
    }
    lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &postings);
    if (rc == LC_OK && has_intersection && intersection.count == 0U) {
      break;
    }
  }
  if (rc == LC_OK) {
    lc_pouch_index_result_docid_list fallback;
    size_t fallback_index;

    memset(&fallback, 0, sizeof(fallback));
    rc = lc_pouch_query_index_collect_trigram_docids(
        pouch, entry, field_hex, LC_POUCH_QUERY_INDEX_TRIGRAM_FALLBACK_HEX,
        &fallback, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_result_docid_list_sort_compact(&pouch->allocator,
                                                         &fallback, error);
    }
    for (fallback_index = 0U; rc == LC_OK && fallback_index < fallback.count;
         ++fallback_index) {
      rc = lc_pouch_index_result_docid_list_add(
          &pouch->allocator, &intersection,
          fallback.items[fallback_index].doc_id,
          fallback.items[fallback_index].value_index, error);
    }
    lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &fallback);
    if (rc == LC_OK && intersection.count > 0U) {
      rc = lc_pouch_index_result_docid_list_sort_compact(&pouch->allocator,
                                                         &intersection, error);
    }
  }
  if (rc == LC_OK && has_intersection) {
    *docids = intersection;
    memset(&intersection, 0, sizeof(intersection));
  }
  lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &intersection);
  return rc;
}

static int lc_pouch_query_index_append_generation_posting(
    const lc_allocator *allocator, char kind, unsigned long count,
    unsigned long max_doc_id, unsigned long payload_length,
    const unsigned char *payload_bytes, size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  size_t index;
  size_t offset;
  size_t seen;
  unsigned long doc_id;
  unsigned long delta;
  int rc;

  (void)allocator;
  if ((payload_bytes == NULL && payload_length > 0UL) ||
      (count == 0UL && payload_length != 0UL) ||
      (count > 0UL && payload_length == 0UL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting count and payload mismatch", NULL, NULL,
                        "pouch");
  }
  if (payload_length > (unsigned long)((size_t)-1)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting payload exceeds local limit", NULL, NULL,
                        "pouch");
  }
  rc = LC_OK;
  if (kind == 's') {
    doc_id = 0UL;
    delta = 0UL;
    offset = 0U;
    for (index = 0U; rc == LC_OK && index < (size_t)count; ++index) {
      rc = lc_pouch_query_index_read_sparse_posting_varint(
          payload_bytes, (size_t)payload_length, &offset, &delta, error);
      if (rc != LC_OK) {
        break;
      }
      if (index > 0U && delta > ULONG_MAX - doc_id) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch posting docID delta overflows", NULL, NULL,
                          "pouch");
        break;
      }
      doc_id = index == 0U ? delta : doc_id + delta;
      if (lc_pouch_query_index_docid_list_contains_sorted(docid_filter_sorted,
                                                          doc_id)) {
        rc = lc_pouch_index_result_docid_list_add(allocator, docids, doc_id,
                                                  value_index, error);
      }
    }
    if (rc == LC_OK && offset != (size_t)payload_length) {
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch posting has trailing bytes", NULL, NULL, "pouch");
    }
    if (rc == LC_OK && count > 0UL && doc_id != max_doc_id) {
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch posting max docID mismatch", NULL, NULL, "pouch");
    }
  } else if (kind == 'd') {
    seen = 0U;
    for (index = 0U; rc == LC_OK && index < (size_t)payload_length; ++index) {
      unsigned int bit_index;

      for (bit_index = 0U; bit_index < (unsigned int)CHAR_BIT; ++bit_index) {
        if ((payload_bytes[index] & (unsigned char)(1U << bit_index)) == 0U) {
          continue;
        }
        doc_id = ((unsigned long)index * (unsigned long)CHAR_BIT) +
                 (unsigned long)bit_index;
        if (doc_id > max_doc_id) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch posting dense bit exceeds max docID", NULL,
                            NULL, "pouch");
          break;
        }
        if (lc_pouch_query_index_docid_list_contains_sorted(docid_filter_sorted,
                                                            doc_id)) {
          rc = lc_pouch_index_result_docid_list_add(allocator, docids, doc_id,
                                                    value_index, error);
          if (rc != LC_OK) {
            break;
          }
        }
        ++seen;
      }
    }
    if (rc == LC_OK && seen != (size_t)count) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting count mismatch", NULL, NULL, "pouch");
    }
  } else {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch posting has invalid kind", NULL, NULL, "pouch");
  }
  return rc;
}

static int lc_pouch_query_index_generation_term_matches(
    const lc_allocator *allocator,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    unsigned long term_ordinal, const char *term_field_hex,
    const char *term_value_hex, char term_value_type, int *matched,
    size_t *value_index, lc_error *error) {
  size_t index;

  *matched = 0;
  *value_index = (size_t)term_ordinal;
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT) {
    for (index = 0U; index < exact_term_count; ++index) {
      if (strcmp(term_field_hex, exact_terms[index].field_hex) == 0 &&
          strcmp(term_value_hex, exact_terms[index].value_hex) == 0 &&
          term_value_type == exact_terms[index].value_type) {
        *matched = 1;
        *value_index = index;
        break;
      }
    }
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE) {
    *matched = strcmp(term_field_hex, field_hex) == 0 &&
               strcmp(term_value_hex, "-") == 0 && term_value_type == 'z';
    *value_index = 0U;
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM) {
    *matched = (lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
                strcmp(term_field_hex, field_hex) == 0) &&
               term_value_type == 's' &&
               strcmp(term_value_hex, needle_hex) == 0;
    *value_index = 0U;
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE ||
             kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL) {
    if (strcmp(term_field_hex, field_hex) == 0 && term_value_type == 'n' &&
        strcmp(term_value_hex, "-") != 0) {
      char *value_text;
      double number;

      value_text =
          lc_pouch_query_index_hex_decode(allocator, term_value_hex, error);
      if (value_text == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      if (lc_pouch_query_index_parse_number_value(value_text, &number)) {
        if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE) {
          *matched =
              lc_pouch_query_index_range_contains_value(range_bounds, number);
        } else {
          lc_pouch_index_instant instant;

          instant.seconds = number;
          *matched =
              lc_pouch_index_date_contains_value(temporal_bounds, &instant);
        }
      }
      lc_free_with_allocator(allocator, value_text);
    }
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT) {
    if ((lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
         strcmp(term_field_hex, field_hex) == 0) &&
        strcmp(term_value_hex, "-") != 0) {
      if (prefix_match) {
        if (term_value_type == 's' ||
            term_value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE) {
          *matched = ignore_case ? lc_pouch_query_index_hex_text_has_prefix(
                                       term_value_hex, needle_text, 1)
                                 : strncmp(term_value_hex, needle_hex,
                                           strlen(needle_hex)) == 0;
        }
      } else if (contains_match) {
        /* A match in the retained prefix is conclusive. A prefix miss remains
         * incomplete and is covered by the trigram candidate path. */
        if (term_value_type == 's' ||
            term_value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE) {
          if (ignore_case && lc_pouch_query_index_hex_contains_aligned(
                                 term_value_hex, needle_hex)) {
            /* Most persisted text is already lower-case. Avoid hex decoding
             * for that common case; mixed-case input uses the complete
             * case-folding verifier below. */
            *matched = 1;
          } else {
            *matched = ignore_case ? lc_pouch_query_index_hex_text_contains(
                                         term_value_hex, needle_text, 1)
                                   : lc_pouch_query_index_hex_contains_aligned(
                                         term_value_hex, needle_hex);
          }
        }
      }
    }
  }
  return LC_OK;
}

/* A retained prefix can conclusively satisfy a contains query. Resolve those
 * terms before scanning complete values: common large-payload documents then
 * avoid walking unrelated text terms once every trigram candidate is known to
 * match. Prefix misses remain in the normal verifier path. */
static int lc_pouch_query_index_direct_collect_text_prefix_docids(
    lc_pouch *pouch, const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int ignore_case,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, int *text_prefix_seen,
    int *all_candidates_matched, lc_error *error) {
  lc_pouch_query_index_docid_bitmap bitmap;
  const lc_pouch_index_term_generation *generation;
  size_t index;
  int any_text_direct;
  int any_text_lookup;
  uint64_t needle_bloom;
  int rc;

  if (pouch == NULL || entry == NULL || field_hex == NULL ||
      needle_hex == NULL || needle_text == NULL ||
      docid_filter_sorted == NULL || docids == NULL ||
      all_candidates_matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct text prefix lookup requires inputs", NULL,
                        NULL, "pouch");
  }
  generation = &entry->direct_generation;
  *all_candidates_matched = 0;
  needle_bloom = ignore_case
                     ? lc_pouch_query_index_text_trigram_bloom_hex(needle_hex)
                     : 0U;
  memset(&bitmap, 0, sizeof(bitmap));
  any_text_lookup = lc_pouch_query_index_field_hex_is_any_text(field_hex);
  any_text_direct =
      any_text_lookup &&
      lc_pouch_query_index_generation_cache_has_field(entry, field_hex);
  rc = lc_pouch_query_index_docid_bitmap_init(
      &pouch->allocator, generation->row_count, &bitmap, error);
  for (index = 0U; rc == LC_OK && index < entry->direct_text_prefix_count;
       ++index) {
    const lc_pouch_index_term_entry *term;
    size_t term_index;
    int matched;

    term_index = entry->direct_text_prefix_indices[index];
    if (term_index >= generation->terms.count) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct text prefix directory is invalid", NULL,
                        NULL, "pouch");
      break;
    }
    term = &generation->terms.items[term_index];
    if (term->field_hex == NULL || term->value_hex == NULL ||
        term->value_is_trigram_key) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct text generation is invalid", NULL, NULL,
                        "pouch");
      break;
    }
    if ((any_text_direct || !any_text_lookup) &&
        strcmp(term->field_hex, field_hex) != 0) {
      continue;
    }
    if (text_prefix_seen != NULL) {
      *text_prefix_seen = 1;
    }
    if (needle_bloom != 0U && term->has_text_trigram_bloom &&
        (term->text_trigram_bloom & needle_bloom) != needle_bloom) {
      continue;
    }
    /* This directory contains only retained text prefixes. Its field has
     * already been selected above, so avoid the generic multi-index matcher
     * for every trigram candidate. Keep the same ASCII-folded verification:
     * the aligned form is a fast positive; the byte-wise verifier covers
     * mixed case. */
    matched = 0;
    if (ignore_case && lc_pouch_query_index_hex_contains_aligned(
                           term->value_hex, needle_hex)) {
      matched = 1;
    } else {
      matched = ignore_case
                    ? lc_pouch_query_index_hex_text_contains(term->value_hex,
                                                             needle_text, 1)
                    : lc_pouch_query_index_hex_contains_aligned(term->value_hex,
                                                                needle_hex);
    }
    if (matched) {
      rc = lc_pouch_query_index_direct_mark_posting(generation, term->term_id,
                                                    &bitmap, error);
      if (rc == LC_OK && lc_pouch_query_index_docid_bitmap_covers_list(
                             &bitmap, docid_filter_sorted)) {
        *all_candidates_matched = 1;
        break;
      }
    }
  }
  if (rc == LC_OK) {
    size_t matched_count;

    matched_count = 0U;
    for (index = 0U; index < docid_filter_sorted->count; ++index) {
      unsigned long doc_id;

      doc_id = docid_filter_sorted->items[index].doc_id;
      if (!lc_pouch_query_index_docid_bitmap_has(&bitmap, doc_id)) {
        continue;
      }
      ++matched_count;
      rc = lc_pouch_index_result_docid_list_add(&pouch->allocator, docids,
                                                doc_id, 0U, error);
      if (rc != LC_OK) {
        break;
      }
    }
    if (rc == LC_OK && matched_count == docid_filter_sorted->count) {
      *all_candidates_matched = 1;
    }
  }
  lc_pouch_query_index_docid_bitmap_cleanup(&bitmap);
  return rc;
}

static int lc_pouch_query_index_direct_collect_text_docids(
    lc_pouch *pouch, const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, int *text_prefix_seen,
    lc_error *error) {
  lc_pouch_query_index_docid_bitmap seen;
  const lc_pouch_index_term_generation *generation;
  size_t index;
  size_t pending;
  uint64_t needle_bloom;
  int any_text_direct;
  int any_text_lookup;
  int all_candidates_matched;
  int dedupe_candidates;
  int prefix_prepass;
  const lc_pouch_index_result_docid_list *posting_filter;
  int rc;

  if (pouch == NULL || entry == NULL || field_hex == NULL ||
      needle_hex == NULL || needle_text == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct text lookup requires inputs", NULL, NULL,
                        "pouch");
  }
  generation = &entry->direct_generation;
  memset(&seen, 0, sizeof(seen));
  pending = 0U;
  needle_bloom = ignore_case && contains_match
                     ? lc_pouch_query_index_text_trigram_bloom_hex(needle_hex)
                     : 0U;
  prefix_prepass = contains_match && docid_filter_sorted != NULL &&
                   entry->direct_text_prefix_count > 0U;
  all_candidates_matched = 0;
  if (prefix_prepass) {
    rc = lc_pouch_query_index_direct_collect_text_prefix_docids(
        pouch, entry, field_hex, needle_hex, needle_text, ignore_case,
        docid_filter_sorted, docids, text_prefix_seen, &all_candidates_matched,
        error);
    if (rc != LC_OK || all_candidates_matched) {
      return rc;
    }
  }
  any_text_lookup = lc_pouch_query_index_field_hex_is_any_text(field_hex);
  any_text_direct =
      any_text_lookup &&
      lc_pouch_query_index_generation_cache_has_field(entry, field_hex);
  posting_filter = lc_pouch_query_index_docid_list_covers_generation(
                       docid_filter_sorted, generation->row_count)
                       ? NULL
                       : docid_filter_sorted;
  dedupe_candidates = docid_filter_sorted != NULL;
  rc = LC_OK;
  if (dedupe_candidates) {
    pending = docid_filter_sorted->count;
    rc = lc_pouch_query_index_docid_bitmap_init(
        &pouch->allocator, generation->row_count, &seen, error);
    for (index = 0U; rc == LC_OK && index < docids->count; ++index) {
      unsigned long doc_id;

      doc_id = docids->items[index].doc_id;
      if (!lc_pouch_query_index_docid_list_contains_sorted(posting_filter,
                                                           doc_id)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch direct text prefix escaped candidates", NULL,
                          NULL, "pouch");
        break;
      }
      if (!lc_pouch_query_index_docid_bitmap_has(&seen, doc_id)) {
        rc = lc_pouch_query_index_docid_bitmap_mark(&seen, doc_id, error);
        if (rc == LC_OK && pending > 0U) {
          --pending;
        }
      }
    }
  }
  for (index = 0U; rc == LC_OK && index < generation->terms.count; ++index) {
    const lc_pouch_index_term_entry *term;
    size_t value_index;
    int matched;

    if (dedupe_candidates && pending == 0U) {
      break;
    }
    term = &generation->terms.items[index];
    if (term->field_hex == NULL || term->value_hex == NULL ||
        term->value_is_trigram_key) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch direct text generation is invalid", NULL, NULL,
                        "pouch");
      break;
    }
    if ((any_text_direct || !any_text_lookup) &&
        strcmp(term->field_hex, field_hex) != 0) {
      continue;
    }
    if (text_prefix_seen != NULL &&
        term->value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE &&
        (any_text_lookup || strcmp(term->field_hex, field_hex) == 0)) {
      *text_prefix_seen = 1;
    }
    if (prefix_prepass &&
        term->value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE) {
      continue;
    }
    if (needle_bloom != 0U && term->value_type == 's' &&
        term->has_text_trigram_bloom &&
        (term->text_trigram_bloom & needle_bloom) != needle_bloom) {
      continue;
    }
    value_index = index;
    matched = 0;
    rc = lc_pouch_query_index_generation_term_matches(
        &pouch->allocator, LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT, NULL, 0U,
        field_hex, needle_hex, needle_text, prefix_match, contains_match,
        ignore_case, NULL, NULL, (unsigned long)index, term->field_hex,
        term->value_hex, term->value_type, &matched, &value_index, error);
    if (rc == LC_OK && matched) {
      if (dedupe_candidates) {
        rc = lc_pouch_query_index_direct_append_unique_posting(
            &pouch->allocator, generation, term->term_id, value_index,
            posting_filter, &seen, docids, &pending, error);
      } else {
        rc = lc_pouch_query_index_direct_append_posting(
            &pouch->allocator, generation, term->term_id, value_index,
            posting_filter, docids, error);
      }
    }
  }
  lc_pouch_query_index_docid_bitmap_cleanup(&seen);
  return rc;
}

static int lc_pouch_query_index_collect_generation_artifact_docids(
    lc_pouch *pouch, const char *ns, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, int *present, int *valid,
    int *text_prefix_seen, lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  size_t index;
  size_t start;
  int any_text_direct;
  int any_text_lookup;
  int rc;

  *present = 0;
  *valid = 0;
  entry = NULL;
  rc = lc_pouch_query_index_generation_cache_get(
      pouch, ns, path, expected_index_seq, expected_row_count,
      expected_row_hash, &entry, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    return rc;
  }
  if (entry->has_direct_generation && entry->direct_generation_is_text) {
    if (kind != LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch direct text cache used for non-text query",
                          NULL, NULL, "pouch");
    }
    return lc_pouch_query_index_direct_collect_text_docids(
        pouch, entry, field_hex, needle_hex, needle_text, prefix_match,
        contains_match, ignore_case, docid_filter_sorted, docids,
        text_prefix_seen, error);
  }

  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT) {
    for (index = 0U; rc == LC_OK && index < exact_term_count; ++index) {
      const lc_pouch_query_index_generation_cache_term *term;

      if (lc_pouch_query_index_generation_cache_find_term(
              entry, exact_terms[index].field_hex, exact_terms[index].value_hex,
              exact_terms[index].value_type, &term)) {
        rc = lc_pouch_query_index_generation_cache_append_posting(
            &pouch->allocator, entry, term->term_id, index, docid_filter_sorted,
            docids, error);
      }
    }
    return rc;
  }

  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE) {
    const lc_pouch_query_index_generation_cache_term *term;

    if (lc_pouch_query_index_generation_cache_find_term(entry, field_hex, "-",
                                                        'z', &term)) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, docid_filter_sorted,
          docids, error);
    }
    return rc;
  }

  any_text_lookup = (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT ||
                     kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM) &&
                    lc_pouch_query_index_field_hex_is_any_text(field_hex);
  any_text_direct = 0;
  if (any_text_lookup) {
    start = 0U;
  } else {
    start = lc_pouch_query_index_generation_cache_field_lower_bound(entry,
                                                                    field_hex);
  }
  for (index = start; rc == LC_OK && index < entry->term_count; ++index) {
    const lc_pouch_query_index_generation_cache_term *term;
    size_t value_index;
    int matched;

    term = &entry->terms[index];
    if ((any_text_direct || !any_text_lookup) &&
        strcmp(term->field_hex, field_hex) != 0) {
      break;
    }
    if (text_prefix_seen != NULL &&
        kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT &&
        term->value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE &&
        (lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
         strcmp(term->field_hex, field_hex) == 0)) {
      *text_prefix_seen = 1;
    }
    value_index = (size_t)term->ordinal;
    matched = 0;
    rc = lc_pouch_query_index_generation_term_matches(
        &pouch->allocator, kind, exact_terms, exact_term_count, field_hex,
        needle_hex, needle_text, prefix_match, contains_match, ignore_case,
        range_bounds, temporal_bounds, term->ordinal, term->field_hex,
        term->value_hex, term->value_type, &matched, &value_index, error);
    if (rc == LC_OK && matched) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, value_index,
          docid_filter_sorted, docids, error);
    }
  }
  return rc;
}

static int lc_pouch_query_index_result_row_list_add_view(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *rows,
    char *key, char *key_hex, int owns_key, int owns_key_hex,
    unsigned long doc_id, lc_pouch_generation version, uint64_t bytes,
    int has_query_hidden, int query_hidden, size_t value_index,
    lc_error *error) {
  lc_pouch_index_result_row *row;
  int rc;

  if (rows == NULL || key == NULL || key[0] == '\0') {
    return LC_OK;
  }
  rc = lc_pouch_query_index_result_rows_reserve(allocator, rows,
                                                rows->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  row = &rows->items[rows->count++];
  memset(row, 0, sizeof(*row));
  row->key = key;
  row->key_hex = key_hex;
  row->doc_id = doc_id;
  row->version = version;
  row->bytes = bytes;
  row->has_query_hidden = has_query_hidden ? 1 : 0;
  row->query_hidden = query_hidden ? 1 : 0;
  row->owns_key = owns_key ? 1 : 0;
  row->owns_key_hex = owns_key_hex ? 1 : 0;
  row->value_index = value_index;
  return LC_OK;
}

/* A document-table entry stays alive for the caller's borrow, but its lazily
 * decoded keys are shared by concurrent readers. The cache ownership mutex
 * covers both the lookup and the one-time publication of each decoded key.
 * The caller holds query_doc_table_cache_mutex for the whole row batch: a
 * result can contain hundreds of keys, and publishing them one at a time
 * would otherwise take the uncontended mutex once per result row. */
static int lc_pouch_query_index_doc_table_cache_decoded_key_get_locked(
    lc_pouch *pouch, lc_pouch_query_index_doc_table_cache_entry *entry,
    unsigned long doc_id, const char *key_hex, char **out, lc_error *error) {
  char *key;
  int rc;

  if (pouch == NULL || entry == NULL || key_hex == NULL || out == NULL ||
      doc_id >= entry->decoded_key_count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc-table decoded key requires cached entry",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  rc = LC_OK;
  key = entry->decoded_keys[doc_id];
  if (key == NULL) {
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_before_doc_table_decoded_key_populate_hook != NULL) {
      rc = lc_pouch_test_before_doc_table_decoded_key_populate_hook(
          lc_pouch_test_before_doc_table_decoded_key_populate_context, error);
    }
#endif
    if (rc == LC_OK) {
      key = lc_pouch_query_index_hex_decode(&pouch->allocator, key_hex, error);
      if (key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      } else {
        entry->decoded_keys[doc_id] = key;
      }
    }
  }
  if (rc == LC_OK) {
    *out = key;
  }
  return rc;
}

static int lc_pouch_query_index_docid_rows_build(
    lc_pouch *pouch, const lc_allocator *allocator,
    lc_pouch_index_result_docid_list *docids,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_query_index_doc_table_cache_entry *doc_cache,
    lc_pouch_index_result_row_list *rows, lc_error *error) {
  const lc_pouch_index_doc *doc;
  char *key;
  size_t index;
  int doc_cache_locked;
  int rc;

  if (docids == NULL || doc_table == NULL || rows == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index docID row build requires docIDs, "
                        "doc table, and rows",
                        NULL, NULL, NULL);
  }
  if (docids->count == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_index_result_docid_list_sort_compact(allocator, docids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (rows->count == 0U) {
    rows->items = (lc_pouch_index_result_row *)lc_alloc_with_allocator(
        allocator, docids->count * sizeof(rows->items[0]));
    if (rows->items == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index result rows",
                          NULL, NULL, NULL);
    }
    memset(rows->items, 0, docids->count * sizeof(rows->items[0]));
    rows->capacity = docids->count;
  }
  doc_cache_locked = 0;
  if (doc_cache != NULL && doc_cache->decoded_key_count > 0U) {
    rc = lc_pouch_query_index_doc_table_cache_lock(pouch, error);
    if (rc != LC_OK) {
      return rc;
    }
    doc_cache_locked = 1;
  }
  for (index = 0U; rc == LC_OK && index < docids->count; ++index) {
    int key_owned;

    doc = NULL;
    rc = lc_pouch_index_doc_table_get(doc_table, docids->items[index].doc_id,
                                      &doc, error);
    if (rc != LC_OK) {
      break;
    }
    key_owned = 0;
    key = NULL;
    if (doc_cache != NULL &&
        docids->items[index].doc_id < doc_cache->decoded_key_count) {
      rc = lc_pouch_query_index_doc_table_cache_decoded_key_get_locked(
          pouch, doc_cache, docids->items[index].doc_id, doc->key_hex, &key,
          error);
    } else {
      key = lc_pouch_query_index_hex_decode(allocator, doc->key_hex, error);
      if (key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      } else {
        key_owned = 1;
      }
    }
    if (rc != LC_OK) {
      break;
    }
    rc = lc_pouch_query_index_result_row_list_add_view(
        allocator, rows, key, (char *)doc->key_hex, key_owned, 0,
        docids->items[index].doc_id, doc->version, doc->bytes,
        doc->has_query_hidden, doc->query_hidden,
        docids->items[index].value_index, error);
    if (rc != LC_OK && key_owned) {
      lc_free_with_allocator(allocator, key);
    }
  }
  if (doc_cache_locked) {
    lc_pouch_query_index_doc_table_cache_unlock(pouch);
  }
  return rc;
}

static int lc_pouch_query_index_field_hex_is_any_text(const char *field_hex) {
  return field_hex != NULL &&
         strcmp(field_hex, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX) == 0;
}

static int lc_pouch_query_index_result_row_compare_key(const void *left,
                                                       const void *right) {
  const lc_pouch_index_result_row *left_row;
  const lc_pouch_index_result_row *right_row;
  int cmp;

  left_row = (const lc_pouch_index_result_row *)left;
  right_row = (const lc_pouch_index_result_row *)right;
  if (left_row->key_hex == NULL && right_row->key_hex == NULL) {
    cmp = 0;
  } else if (left_row->key_hex == NULL) {
    cmp = -1;
  } else if (right_row->key_hex == NULL) {
    cmp = 1;
  } else {
    cmp = strcmp(left_row->key_hex, right_row->key_hex);
  }
  if (cmp != 0) {
    return cmp;
  }
  if (left_row->value_index < right_row->value_index) {
    return -1;
  }
  if (left_row->value_index > right_row->value_index) {
    return 1;
  }
  if (left_row->doc_id < right_row->doc_id) {
    return -1;
  }
  if (left_row->doc_id > right_row->doc_id) {
    return 1;
  }
  return 0;
}

static void
lc_pouch_query_index_result_rows_sort(const lc_allocator *allocator,
                                      lc_pouch_index_result_row_list *rows) {
  size_t index;
  size_t write_index;
  int sorted_unique;

  if (rows == NULL || rows->count <= 1U) {
    return;
  }
  sorted_unique = 1;
  for (index = 1U; index < rows->count; ++index) {
    if (lc_pouch_query_index_result_row_compare_key(&rows->items[index - 1U],
                                                    &rows->items[index]) >= 0) {
      sorted_unique = 0;
      break;
    }
  }
  if (sorted_unique) {
    return;
  }
  qsort(rows->items, rows->count, sizeof(rows->items[0]),
        lc_pouch_query_index_result_row_compare_key);
  write_index = 0U;
  for (index = 0U; index < rows->count; ++index) {
    if (write_index > 0U && rows->items[index].key_hex != NULL &&
        rows->items[write_index - 1U].key_hex != NULL &&
        strcmp(rows->items[index].key_hex,
               rows->items[write_index - 1U].key_hex) == 0) {
      if (rows->items[index].owns_key) {
        lc_free_with_allocator(allocator, rows->items[index].key);
      }
      if (rows->items[index].owns_key_hex) {
        lc_free_with_allocator(allocator, rows->items[index].key_hex);
      }
      memset(&rows->items[index], 0, sizeof(rows->items[index]));
      continue;
    }
    if (write_index != index) {
      rows->items[write_index] = rows->items[index];
      memset(&rows->items[index], 0, sizeof(rows->items[index]));
    }
    ++write_index;
  }
  rows->count = write_index;
}

static int
lc_pouch_query_index_result_rows_reserve(const lc_allocator *allocator,
                                         lc_pouch_index_result_row_list *rows,
                                         size_t needed, lc_error *error) {
  lc_pouch_index_result_row *next_items;
  size_t next_capacity;

  if (rows == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index result row reserve requires rows",
                        NULL, NULL, NULL);
  }
  if (needed <= rows->capacity) {
    return LC_OK;
  }
  next_capacity = rows->capacity == 0U ? 16U : rows->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index result rows exceed local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_result_row *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index result rows",
                        NULL, NULL, NULL);
  }
  if (rows->items != NULL) {
    memcpy(next_items, rows->items, rows->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, rows->items);
  }
  memset(next_items + rows->count, 0,
         (next_capacity - rows->count) * sizeof(*next_items));
  rows->items = next_items;
  rows->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_segmented_rows_move_visible(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *rows,
    lc_pouch_index_result_row_list *segment_rows,
    const lc_pouch_query_index_key_hex_set *hidden, lc_error *error) {
  size_t index;
  int rc;

  if (rows == NULL || segment_rows == NULL || hidden == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented row merge requires output rows, "
                        "segment rows, and hidden set",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  if (segment_rows->count > ((size_t)-1 - rows->count)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch segmented row merge exceeds local limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_result_rows_reserve(
      allocator, rows, rows->count + segment_rows->count, error);
  for (index = 0U; rc == LC_OK && index < segment_rows->count; ++index) {
    lc_pouch_index_result_row *row;

    row = &segment_rows->items[index];
    if (row->key_hex != NULL &&
        lc_pouch_query_index_key_hex_set_find(hidden, row->key_hex, NULL)) {
      continue;
    }
    rows->items[rows->count++] = *row;
    memset(row, 0, sizeof(*row));
  }
  return rc;
}

static int lc_pouch_query_index_segmented_rows_have_visible(
    const lc_pouch_index_result_row_list *segment_rows,
    const lc_pouch_query_index_key_hex_set *hidden) {
  size_t index;

  if (segment_rows == NULL || hidden == NULL) {
    return 0;
  }
  for (index = 0U; index < segment_rows->count; ++index) {
    const lc_pouch_index_result_row *row;

    row = &segment_rows->items[index];
    if (row->key_hex == NULL ||
        !lc_pouch_query_index_key_hex_set_find(hidden, row->key_hex, NULL)) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_segmented_mark_changed(
    const lc_allocator *allocator, const lc_pouch_index_doc_table *doc_table,
    lc_pouch_query_index_key_hex_set *hidden, lc_error *error) {
  size_t index;
  int rc;

  if (doc_table == NULL || hidden == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented changed-key tracking requires doc "
                        "table and hidden set",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < doc_table->count; ++index) {
    rc = lc_pouch_query_index_key_hex_set_add(
        allocator, hidden, doc_table->items[index].key_hex, error);
  }
  return rc;
}

static const char *lc_pouch_query_index_segment_term_leaf(
    lc_pouch_query_index_segmented_collect_kind kind) {
  switch (kind) {
  case LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT:
    return LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE:
    return LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE:
    return LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT:
    return LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM:
    return LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL:
    return LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_SEGMENTED_ALL:
  default:
    return NULL;
  }
}

static int lc_pouch_query_index_segmented_collect(
    lc_pouch *pouch, const char *ns,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    lc_pouch_index_result_row_list *rows,
    lc_pouch_query_index_doc_table_cache_borrow_list *doc_table_cache_borrows,
    const lc_pouch_generation *state_index_seq_snapshot,
    lc_pouch_generation *index_seq, int *text_complete_out, lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  lc_pouch_query_index_key_hex_set hidden;
  lc_pouch_query_index_segmented_collect_kind collect_kind;
  char *contains_token_hex;
  char *manifest_path;
  const char *term_leaf;
  const char *collect_needle_hex;
  lc_pouch_generation state_index_seq;
  char trigram_needles[LC_POUCH_QUERY_INDEX_MAX_CONTAINS_TRIGRAMS][7];
  size_t trigram_count;
  size_t segment_index;
  int contains_token_eligible;
  int trigram_prefilter;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || rows == NULL ||
      doc_table_cache_borrows == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented query collect requires pouch, "
                        "namespace, rows, and index_seq",
                        NULL, NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT &&
      (exact_terms == NULL || exact_term_count == 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented exact query requires terms", NULL,
                        NULL, NULL);
  }
  if ((kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE ||
       kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE ||
       kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT ||
       kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL) &&
      (field_hex == NULL || field_hex[0] == '\0')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented field query requires a field", NULL,
                        NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE && range_bounds == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented range query requires bounds", NULL,
                        NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL &&
      temporal_bounds == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented date query requires parsed bounds",
                        NULL, NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT &&
      (needle_hex == NULL || needle_text == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented text query requires a needle", NULL,
                        NULL, NULL);
  }

  memset(&manifest, 0, sizeof(manifest));
  memset(&hidden, 0, sizeof(hidden));
  contains_token_hex = NULL;
  state_index_seq = 0UL;
  collect_kind = kind;
  collect_needle_hex = needle_hex;
  memset(trigram_needles, 0, sizeof(trigram_needles));
  trigram_count = 0U;
  contains_token_eligible = 0;
  trigram_prefilter = 0;
  rc = LC_OK;
  if (text_complete_out != NULL) {
    *text_complete_out = 1;
  }
  if ((kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM ||
       (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT && field_hex != NULL &&
        lc_pouch_query_index_field_hex_is_any_text(field_hex))) &&
      contains_match && needle_text != NULL && strlen(needle_text) >= 3U) {
    size_t needle_length;
    size_t window_index;

    needle_length = strlen(needle_text);
    for (window_index = 0U;
         window_index + 2U < needle_length &&
         trigram_count < LC_POUCH_QUERY_INDEX_MAX_CONTAINS_TRIGRAMS;
         ++window_index) {
      char candidate[7];
      size_t byte_index;
      size_t existing_index;
      int duplicate;

      for (byte_index = 0U; byte_index < 3U; ++byte_index) {
        unsigned char byte_value;

        byte_value = (unsigned char)needle_text[window_index + byte_index];
        byte_value = lc_pouch_query_index_ascii_lower(byte_value);
        lc_pouch_query_index_hex_encode_byte(candidate + (byte_index * 2U),
                                             byte_value);
      }
      candidate[6] = '\0';
      duplicate = 0;
      for (existing_index = 0U; existing_index < trigram_count;
           ++existing_index) {
        if (strcmp(trigram_needles[existing_index], candidate) == 0) {
          duplicate = 1;
          break;
        }
      }
      if (!duplicate) {
        memcpy(trigram_needles[trigram_count], candidate, sizeof(candidate));
        ++trigram_count;
      }
    }
    if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM) {
      collect_kind = LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM;
      collect_needle_hex = trigram_needles[0];
    } else {
      trigram_prefilter = trigram_count > 0U;
    }
  }
  if (rc == LC_OK && kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT &&
      contains_match && ignore_case &&
      lc_pouch_query_index_field_hex_is_any_text(field_hex)) {
    rc = lc_pouch_query_index_all_text_token_hex_from_needle(
        &pouch->allocator, needle_text, &contains_token_hex,
        &contains_token_eligible, error);
  }
  term_leaf = lc_pouch_query_index_segment_term_leaf(collect_kind);
  if (kind != LC_POUCH_QUERY_INDEX_SEGMENTED_ALL && term_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented query has unsupported term kind", NULL,
                        NULL, NULL);
  }
  *index_seq = 0UL;
  if (lc_pouch_single_writer_enabled(pouch)) {
    if (state_index_seq_snapshot != NULL) {
      state_index_seq = *state_index_seq_snapshot;
    } else {
      rc = lc_pouch_state_query_index_seq(pouch, ns, &state_index_seq, error);
    }
    if (rc == LC_OK) {
      (void)lc_pouch_query_index_manifest_trust_snapshot(
          pouch, ns, state_index_seq, &manifest);
    }
  }
  if (rc == LC_OK && !manifest.present) {
    manifest_path = lc_pouch_query_index_manifest_path(pouch, ns, error);
    if (manifest_path == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                              error);
    }
    lc_free_with_allocator(&pouch->allocator, manifest_path);
    if (rc == LC_OK && (!manifest.present || !manifest.valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest is not readable", NULL,
                        NULL, "pouch");
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  *index_seq = manifest.index_seq;

  for (segment_index = 0U;
       rc == LC_OK && segment_index < manifest.segment_count; ++segment_index) {
    const lc_pouch_query_index_manifest_segment *segment;
    lc_pouch_query_index_doc_table_cache_entry *doc_cache;
    const lc_pouch_index_doc_table *doc_table;
    lc_pouch_index_result_docid_list docids;
    lc_pouch_index_result_row_list segment_rows;
    char *doc_table_path;
    char *term_path;
    char *trigram_term_path;
    char *delete_path;
    unsigned long delete_count;
    unsigned long delete_hash;
    size_t doc_index;
    int present;
    int valid;
    int segment_fully_hidden;
    int text_prefix_seen;

    segment = &manifest.segments[segment_index];
    doc_cache = NULL;
    doc_table = NULL;
    memset(&docids, 0, sizeof(docids));
    memset(&segment_rows, 0, sizeof(segment_rows));
    doc_table_path = NULL;
    term_path = NULL;
    trigram_term_path = NULL;
    delete_path = NULL;
    delete_count = 0UL;
    delete_hash = lc_pouch_query_index_hash_init();
    segment_fully_hidden = 0;
    text_prefix_seen = 0;
    doc_table_path = lc_pouch_query_index_segment_artifact_path(
        pouch, ns, segment->id, LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF, error);
    if (doc_table_path == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (rc == LC_OK && term_leaf != NULL) {
      term_path = lc_pouch_query_index_segment_artifact_path(
          pouch, ns, segment->id, term_leaf, error);
      if (term_path == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    if (rc == LC_OK && trigram_prefilter) {
      trigram_term_path = lc_pouch_query_index_segment_artifact_path(
          pouch, ns, segment->id, LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF,
          error);
      if (trigram_term_path == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    if (rc == LC_OK) {
      delete_path = lc_pouch_query_index_segment_artifact_path(
          pouch, ns, segment->id, LC_POUCH_QUERY_INDEX_DELETE_LEAF, error);
      if (delete_path == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    if (rc == LC_OK) {
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_doc_table_cache_get(
          pouch, ns, doc_table_path, segment->index_seq, segment->row_count,
          segment->row_hash, &doc_cache, &present, &valid, error);
      if (rc == LC_OK && (!present || !valid)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index segment doc table is not "
                          "readable",
                          NULL, NULL, "pouch");
      }
      if (rc == LC_OK) {
        doc_table = &doc_cache->table;
#ifdef LOCKDC_TEST_BUILD
        if (lc_pouch_test_after_doc_table_cache_borrow_hook != NULL) {
          rc = lc_pouch_test_after_doc_table_cache_borrow_hook(
              lc_pouch_test_after_doc_table_cache_borrow_context, error);
        }
#endif
      }
    }
    if (rc == LC_OK) {
      segment_fully_hidden = doc_table->count > 0U;
      for (doc_index = 0U; segment_fully_hidden && doc_index < doc_table->count;
           ++doc_index) {
        if (!lc_pouch_query_index_key_hex_set_find(
                &hidden, doc_table->items[doc_index].key_hex, NULL)) {
          segment_fully_hidden = 0;
        }
      }
    }
    if (rc == LC_OK && !segment_fully_hidden) {
      switch (kind) {
      case LC_POUCH_QUERY_INDEX_SEGMENTED_ALL:
        for (doc_index = 0U; rc == LC_OK && doc_index < doc_table->count;
             ++doc_index) {
          rc = lc_pouch_index_result_docid_list_add(
              &pouch->allocator, &docids, (unsigned long)doc_index, 0U, error);
        }
        break;
      case LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL:
        if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE &&
            !segment->presence_index_complete) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch query-index segment presence postings are "
                            "incomplete",
                            NULL, NULL, "pouch");
          break;
        }
        if (kind != LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE &&
            !segment->term_index_complete) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch query-index segment scalar postings are "
                            "incomplete",
                            NULL, NULL, "pouch");
          break;
        }
        present = 0;
        valid = 0;
        if (trigram_prefilter) {
          lc_pouch_index_result_docid_list trigram_docids;
          lc_pouch_index_result_docid_list exact_docids;
          lc_pouch_index_result_docid_list token_docids;
          int token_covers_candidates;

          memset(&trigram_docids, 0, sizeof(trigram_docids));
          memset(&exact_docids, 0, sizeof(exact_docids));
          memset(&token_docids, 0, sizeof(token_docids));
          token_covers_candidates = 0;
          text_prefix_seen = 0;
          rc = lc_pouch_query_index_collect_trigram_intersection_docids(
              pouch, ns, trigram_term_path, segment->index_seq,
              segment->row_count, segment->row_hash, field_hex,
              (const char (*)[7])trigram_needles, trigram_count,
              &trigram_docids, &present, &valid, error);
          if (rc == LC_OK && (!present || !valid)) {
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch query-index segment generation is not "
                              "readable",
                              NULL, NULL, "pouch");
          }
          if (rc == LC_OK && trigram_docids.count > 0U) {
            rc = lc_pouch_index_result_docid_list_sort_compact(
                &pouch->allocator, &trigram_docids, error);
          }
          if (rc == LC_OK && trigram_docids.count > 0U &&
              contains_token_eligible) {
            rc = lc_pouch_query_index_collect_all_text_token_docids(
                pouch, ns, term_path, segment->index_seq, segment->row_count,
                segment->row_hash, contains_token_hex, &trigram_docids,
                &token_docids, &present, &valid, error);
          }
          if (rc == LC_OK && token_docids.count > 0U) {
            rc = lc_pouch_index_result_docid_list_sort_compact(
                &pouch->allocator, &token_docids, error);
          }
          if (rc == LC_OK && trigram_docids.count > 0U &&
              lc_pouch_query_index_docid_list_is_subset_sorted(&trigram_docids,
                                                               &token_docids)) {
            exact_docids = token_docids;
            memset(&token_docids, 0, sizeof(token_docids));
            token_covers_candidates = 1;
          }
          if (rc == LC_OK && trigram_docids.count > 0U &&
              !token_covers_candidates) {
            rc = lc_pouch_query_index_collect_generation_artifact_docids(
                pouch, ns, term_path, segment->index_seq, segment->row_count,
                segment->row_hash, LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT,
                exact_terms, exact_term_count, field_hex, needle_hex,
                needle_text, prefix_match, contains_match, ignore_case,
                range_bounds, temporal_bounds, &trigram_docids, &exact_docids,
                &present, &valid, &text_prefix_seen, error);
          }
          if (rc == LC_OK && trigram_docids.count > 0U &&
              (!present || !valid)) {
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch query-index segment generation is not "
                              "readable",
                              NULL, NULL, "pouch");
          }
          if (rc == LC_OK && trigram_docids.count > 0U &&
              exact_docids.count > 0U) {
            rc = lc_pouch_index_result_docid_list_sort_compact(
                &pouch->allocator, &exact_docids, error);
          }
          if (rc == LC_OK && text_prefix_seen && text_complete_out != NULL &&
              !lc_pouch_query_index_docid_list_is_subset_sorted(
                  &trigram_docids, &exact_docids)) {
            /* A retained prefix can only be incomplete for candidate
             * documents it did not already verify. Those are the only rows
             * that require the slower body-level fallback. */
            *text_complete_out = 0;
          }
          if (rc == LC_OK && exact_docids.count > 0U) {
            docids = exact_docids;
            memset(&exact_docids, 0, sizeof(exact_docids));
          }
          lc_pouch_index_result_docid_list_cleanup(&pouch->allocator,
                                                   &exact_docids);
          lc_pouch_index_result_docid_list_cleanup(&pouch->allocator,
                                                   &token_docids);
          lc_pouch_index_result_docid_list_cleanup(&pouch->allocator,
                                                   &trigram_docids);
        } else if (collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM &&
                   trigram_count > 0U) {
          rc = lc_pouch_query_index_collect_trigram_intersection_docids(
              pouch, ns, term_path, segment->index_seq, segment->row_count,
              segment->row_hash, field_hex, (const char (*)[7])trigram_needles,
              trigram_count, &docids, &present, &valid, error);
        } else {
          rc = lc_pouch_query_index_collect_generation_artifact_docids(
              pouch, ns, term_path, segment->index_seq, segment->row_count,
              segment->row_hash, collect_kind, exact_terms, exact_term_count,
              field_hex, collect_needle_hex, needle_text, prefix_match,
              contains_match, ignore_case, range_bounds, temporal_bounds, NULL,
              &docids, &present, &valid,
              text_complete_out != NULL ? &text_prefix_seen : NULL, error);
          if (text_prefix_seen && text_complete_out != NULL) {
            *text_complete_out = 0;
          }
        }
        if (rc == LC_OK && (!present || !valid)) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch query-index segment generation is not "
                            "readable",
                            NULL, NULL, "pouch");
        }
        break;
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_docid_rows_build(pouch, &pouch->allocator,
                                                 &docids, doc_table, doc_cache,
                                                 &segment_rows, error);
    }
    if (rc == LC_OK && lc_pouch_query_index_segmented_rows_have_visible(
                           &segment_rows, &hidden)) {
      rc = lc_pouch_query_index_doc_table_cache_borrow_list_take(
          pouch, doc_table_cache_borrows, doc_cache, error);
      if (rc == LC_OK) {
        doc_cache = NULL;
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_segmented_rows_move_visible(
          &pouch->allocator, rows, &segment_rows, &hidden, error);
    }
    if (rc == LC_OK && segment_index + 1U < manifest.segment_count) {
      rc = lc_pouch_query_index_segmented_mark_changed(
          &pouch->allocator, doc_table, &hidden, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_load_delete_keys(
          pouch, ns, delete_path, &hidden, &delete_count, &delete_hash, error);
    }
    if (rc == LC_OK && (delete_count != segment->delete_count ||
                        delete_hash != segment->delete_hash)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment deletes do not match the "
                        "manifest",
                        NULL, NULL, "pouch");
    }
    lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &segment_rows);
    lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &docids);
    lc_pouch_query_index_doc_table_cache_release(pouch, &doc_cache);
#ifdef LOCKDC_TEST_BUILD
    if (rc == LC_OK &&
        lc_pouch_test_after_doc_table_cache_rows_attached_hook != NULL) {
      rc = lc_pouch_test_after_doc_table_cache_rows_attached_hook(
          lc_pouch_test_after_doc_table_cache_rows_attached_context, error);
    }
#endif
    lc_free_with_allocator(&pouch->allocator, delete_path);
    lc_free_with_allocator(&pouch->allocator, trigram_term_path);
    lc_free_with_allocator(&pouch->allocator, term_path);
    lc_free_with_allocator(&pouch->allocator, doc_table_path);
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_result_rows_sort(&pouch->allocator, rows);
  }

cleanup:
  lc_free_with_allocator(&pouch->allocator, contains_token_hex);
  lc_pouch_query_index_key_hex_set_cleanup(&pouch->allocator, &hidden);
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

static int lc_pouch_query_index_visit_text_generation(
    lc_pouch *pouch, const char *ns, const char *field, const char *needle,
    int prefix_match, int contains_match, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    const lc_pouch_generation *state_index_seq_snapshot,
    lc_pouch_generation *index_seq, int *text_complete_out, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_doc_table_cache_borrow_list doc_table_cache_borrows;
  char *field_hex;
  char *needle_hex;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' || needle == NULL || needle[0] == '\0' ||
      visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index text lookup requires pouch, "
                        "namespace, field, needle, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  needle_hex = ignore_case
                   ? lc_pouch_query_index_hex_encode_folded_ascii_bytes(
                         &pouch->allocator, needle, strlen(needle))
                   : lc_pouch_query_index_hex_encode(&pouch->allocator, needle);
  if (field_hex == NULL || needle_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, needle_hex);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index text lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  memset(&doc_table_cache_borrows, 0, sizeof(doc_table_cache_borrows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, ns, LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT, NULL, 0U, field_hex,
      needle_hex, needle, prefix_match, contains_match, ignore_case, NULL, NULL,
      &rows, &doc_table_cache_borrows, state_index_seq_snapshot, index_seq,
      text_complete_out, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
      pouch, &doc_table_cache_borrows);
  lc_free_with_allocator(&pouch->allocator, needle_hex);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

static int lc_pouch_query_index_visit_exact_generation(
    lc_pouch *pouch, const char *ns, const lc_pouch_index_term_key *exact_terms,
    size_t exact_term_count, lc_pouch_query_index_key_visit_fn visit,
    void *context, lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_doc_table_cache_borrow_list doc_table_cache_borrows;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || exact_terms == NULL ||
      exact_term_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation visit requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  memset(&doc_table_cache_borrows, 0, sizeof(doc_table_cache_borrows));
  *index_seq = 0UL;
  rc = lc_pouch_query_index_segmented_collect(
      pouch, ns, LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT, exact_terms,
      exact_term_count, NULL, NULL, NULL, 0, 0, 0, NULL, NULL, &rows,
      &doc_table_cache_borrows, NULL, index_seq, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
      pouch, &doc_table_cache_borrows);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms_docids(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || terms == NULL ||
      term_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term docID lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_query_index_build_exact_terms(terms, term_count, &exact_terms,
                                              &exact_term_count,
                                              &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(pouch, ns, exact_terms,
                                                     exact_term_count, visit,
                                                     context, index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

int lc_pouch_query_index_visit_scalar_any_docids(
    lc_pouch *pouch, const char *ns, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_query_index_scalar_term *terms;
  size_t index;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' || values == NULL || value_types == NULL ||
      value_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  terms = (lc_pouch_query_index_scalar_term *)lc_alloc_with_allocator(
      &pouch->allocator, value_count * sizeof(*terms));
  if (terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index multi-scalar "
                        "docID lookup",
                        NULL, NULL, NULL);
  }
  memset(terms, 0, value_count * sizeof(*terms));
  rc = LC_OK;
  for (index = 0U; index < value_count; ++index) {
    if (values[index] == NULL ||
        (value_types[index] != 's' && value_types[index] != 'n' &&
         value_types[index] != 'b' && value_types[index] != 'z')) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "non-null values and scalar value types",
                        NULL, NULL, NULL);
      break;
    }
    terms[index].field = field;
    terms[index].value = values[index];
    terms[index].value_type = value_types[index];
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_scalar_terms_docids(
        pouch, ns, terms, value_count, visit, context, index_seq, error);
  }
  lc_free_with_allocator(&pouch->allocator, terms);
  return rc;
}

int lc_pouch_query_index_visit_prefix(
    lc_pouch *pouch, const char *ns, const char *field, const char *prefix,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  if (prefix == NULL || prefix[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix lookup requires non-empty "
                        "prefix",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_text_generation(
      pouch, ns, field, prefix, 1, 0, ignore_case, visit, context, NULL,
      index_seq, NULL, error);
}

int lc_pouch_query_index_prefix_candidates_exact(const char *prefix) {
  return prefix == NULL ||
         strlen(prefix) <= LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
}

int lc_pouch_query_index_visit_prefix_candidates(
    lc_pouch *pouch, const char *ns, const char *field, const char *prefix,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  char *candidate_prefix;
  size_t candidate_len;
  int rc;

  if (prefix == NULL || prefix[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix candidate lookup requires "
                        "non-empty prefix",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix candidate lookup requires "
                        "pouch",
                        NULL, NULL, NULL);
  }
  if (lc_pouch_query_index_prefix_candidates_exact(prefix)) {
    return lc_pouch_query_index_visit_prefix(pouch, ns, field, prefix,
                                             ignore_case, visit, context,
                                             index_seq, error);
  }
  candidate_len = LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
  candidate_prefix =
      (char *)lc_alloc_with_allocator(&pouch->allocator, candidate_len + 1U);
  if (candidate_prefix == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prefix candidate", NULL, NULL,
                        NULL);
  }
  memcpy(candidate_prefix, prefix, candidate_len);
  candidate_prefix[candidate_len] = '\0';
  rc = lc_pouch_query_index_visit_text_generation(
      pouch, ns, field, candidate_prefix, 1, 0, ignore_case, visit, context,
      NULL, index_seq, NULL, error);
  lc_free_with_allocator(&pouch->allocator, candidate_prefix);
  return rc;
}

static int lc_pouch_query_index_visit_contains_complete_with_snapshot(
    lc_pouch *pouch, const char *ns, const char *field, const char *needle,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    const lc_pouch_generation *state_index_seq_snapshot,
    lc_pouch_generation *index_seq, int *complete, lc_error *error) {
  if (complete == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains complete lookup requires "
                        "complete output",
                        NULL, NULL, NULL);
  }
  *complete = 0;
  if (needle == NULL || needle[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains complete lookup requires "
                        "non-empty needle",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_text_generation(
      pouch, ns, field, needle, 0, 1, ignore_case, visit, context,
      state_index_seq_snapshot, index_seq, complete, error);
}

int lc_pouch_query_index_visit_contains_complete_at_snapshot(
    lc_pouch *pouch, const char *ns, const char *field, const char *needle,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    const lc_pouch_generation *state_index_seq_snapshot,
    lc_pouch_generation *index_seq, int *complete, lc_error *error) {
  if (state_index_seq_snapshot == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index snapshot lookup requires query "
                        "watermark",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_contains_complete_with_snapshot(
      pouch, ns, field, needle, ignore_case, visit, context,
      state_index_seq_snapshot, index_seq, complete, error);
}

int lc_pouch_query_index_visit_contains_candidates(
    lc_pouch *pouch, const char *ns, const char *field, const char *needle,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_doc_table_cache_borrow_list doc_table_cache_borrows;
  char *field_hex;
  char *needle_hex;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' || needle == NULL || strlen(needle) < 3U ||
      visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains candidate lookup requires "
                        "pouch, namespace, field, 3-byte needle, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  needle_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, needle);
  if (field_hex == NULL || needle_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, needle_hex);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index contains "
                        "candidate lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  memset(&doc_table_cache_borrows, 0, sizeof(doc_table_cache_borrows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, ns, LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM, NULL, 0U, field_hex,
      needle_hex, needle, 0, 1, ignore_case, NULL, NULL, &rows,
      &doc_table_cache_borrows, NULL, index_seq, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
      pouch, &doc_table_cache_borrows);
  lc_free_with_allocator(&pouch->allocator, needle_hex);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

int lc_pouch_query_index_visit_range(
    lc_pouch *pouch, const char *ns, const char *field,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_doc_table_cache_borrow_list doc_table_cache_borrows;
  char *field_hex;
  int rc;

  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range lookup requires numeric "
                        "bounds",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range lookup requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index range lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  memset(&doc_table_cache_borrows, 0, sizeof(doc_table_cache_borrows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, ns, LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE, NULL, 0U, field_hex,
      NULL, NULL, 0, 0, 0, bounds, NULL, &rows, &doc_table_cache_borrows, NULL,
      index_seq, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
      pouch, &doc_table_cache_borrows);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

int lc_pouch_query_index_visit_date(
    lc_pouch *pouch, const char *ns, const char *field,
    const lc_pouch_query_index_date_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_parsed_date_bounds parsed_bounds;
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_doc_table_cache_borrow_list doc_table_cache_borrows;
  char *field_hex;
  int rc;

  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index date lookup requires temporal "
                        "bounds",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index date lookup requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_index_parse_date_bounds(bounds, &parsed_bounds, error);
  if (rc != LC_OK) {
    return rc;
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index date lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  memset(&doc_table_cache_borrows, 0, sizeof(doc_table_cache_borrows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, ns, LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL, NULL, 0U, field_hex,
      NULL, NULL, 0, 0, 0, NULL, &parsed_bounds, &rows,
      &doc_table_cache_borrows, NULL, index_seq, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
      pouch, &doc_table_cache_borrows);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

int lc_pouch_query_index_visit_exists(lc_pouch *pouch, const char *ns,
                                      const char *field,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      lc_pouch_generation *index_seq,
                                      lc_error *error) {
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_doc_table_cache_borrow_list doc_table_cache_borrows;
  char *field_hex;
  int rc;

  if (pouch == NULL || ns == NULL || ns[0] == '\0' || field == NULL ||
      field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_visit_exists requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index exists lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  memset(&doc_table_cache_borrows, 0, sizeof(doc_table_cache_borrows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, ns, LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE, NULL, 0U, field_hex,
      NULL, NULL, 0, 0, 0, NULL, NULL, &rows, &doc_table_cache_borrows, NULL,
      index_seq, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_pouch_query_index_doc_table_cache_borrow_list_cleanup(
      pouch, &doc_table_cache_borrows);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}
