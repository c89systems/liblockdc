#ifndef LC_COLLECTION_INDEX_H
#define LC_COLLECTION_INDEX_H

#include "lc_collection_order.h"
#include "lc_pouch_index.h"

#define LC_COLLECTION_INDEX_MAX_TERMS 4096U
#define LC_COLLECTION_INDEX_TERM_BYTES (1024U * 1024U)
#define LC_COLLECTION_INDEX_MAX_BYTES (2U * 1024U * 1024U)

/* One physical order page's derived LQL terms, using the normal Pouch immutable
 * term/posting representations. DocIDs are local slots, never public positions.
 * This bounds candidate memory independently of collection/result size. */
typedef struct lc_collection_index {
  lc_allocator allocator;
  lc_pouch_index_term_generation exact;
  lc_pouch_index_term_generation presence;
  lc_u64 covered;
  size_t term_bytes;
  unsigned int rows;
  int incomplete;
  int failed;
  int sealed;
  char query_id[LC_XID_STRING_SIZE];
  unsigned char page_proof[LC_COLLECTION_PAGE_PROOF_BYTES];
} lc_collection_index;

/* Ordinal and hash identify the caller's captured canonical metadata page.
 * The collection ID binds its incarnation. The query ID binds multifile
 * recovery history. Container pages include an ordered payload proof.
 * Initialize empty storage, then add rows in physical order. Destroy all
 * initialized instances, including on error. */
int lc_collection_index_init(lc_collection_index *index,
                             const lc_allocator *allocator,
                             const char *collection_id, const char *query_id,
                             lc_u64 ordinal, unsigned long row_hash,
                             lc_error *error);
void lc_collection_index_cleanup(lc_collection_index *index);
/* A NULL source leaves a slot uncovered. Oversized term metadata likewise
 * leaves that row uncovered, without discarding earlier rows or buffering its
 * body. Extraction/I/O/allocation errors invalidate this build; discard it.
 * Completeness is published only after consuming the complete source. */
int lc_collection_index_add(lc_collection_index *index, lc_source *source,
                            lc_error *error);
/* Drops a page-slot mask from bounded term metadata without opening bodies.
 * Removed slots become uncovered, so readers of earlier visibility generations
 * still evaluate their original states instead of trusting a false nonmatch. */
int lc_collection_index_prune(lc_collection_index *index, lc_u64 removed,
                              lc_error *error);
/* possible includes every uncovered row: lag/overflow cannot hide entries.
 * proven contains only exact positive hits; numeric/hash and recursive-presence
 * hits need residual LQL evaluation. Both are fixed page-slot masks, traversed
 * in order by caller. The key has the same spelling as
 * lc_pouch_query_terms_visit() emits. */
int lc_collection_index_lookup(const lc_collection_index *index,
                               const lc_pouch_index_term_key *key, int presence,
                               lc_u64 *possible, lc_u64 *proven,
                               lc_error *error);
/* Broad path candidates for wildcard/array selectors. The key field is the
 * ordinary ancestor prefix through its final '/'. Always residual: matching
 * descendant values do not prove that the complete LQL path matched. */
int lc_collection_index_lookup_prefix(const lc_collection_index *index,
                                      const lc_pouch_index_term_key *key,
                                      int presence, lc_u64 *possible,
                                      lc_u64 *proven, lc_error *error);
/* Bounded metadata encoding, never application payload serialization. Wrap
 * persisted bytes in the existing Pouch index protection policy. Decoding
 * checks page binding, bounds, checksums, and all posting slot ranges. */
int lc_collection_index_encode(lc_collection_index *index, char **bytes,
                               size_t *length, lc_error *error);
int lc_collection_index_decode(lc_collection_index *index,
                               const lc_allocator *allocator,
                               const char *collection_id, const char *query_id,
                               lc_u64 ordinal, unsigned long row_hash,
                               const char *bytes, size_t length,
                               lc_error *error);

#endif
