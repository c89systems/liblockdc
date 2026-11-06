#ifndef LC_POUCH_QUERY_INDEX_H
#define LC_POUCH_QUERY_INDEX_H

#include "lc_pouch.h"
#include "lc_pouch_index.h"

typedef struct lc_pouch_query_index_flush_result {
  lc_pouch_generation index_seq;
  int repaired;
} lc_pouch_query_index_flush_result;

typedef struct lc_pouch_query_index_row_view {
  const char *key;
  const char *key_hex;
  unsigned long doc_id;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_query_index_row_view;

typedef int (*lc_pouch_query_index_row_visit_fn)(
    const lc_pouch_query_index_row_view *row, void *context, lc_error *error);

typedef struct lc_pouch_query_index_key_view {
  const char *key;
  const char *key_hex;
  unsigned long doc_id;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  size_t value_index;
  int candidate_exact;
} lc_pouch_query_index_key_view;

typedef lc_pouch_index_plain_term lc_pouch_query_index_scalar_term;

typedef struct lc_pouch_query_index_range_bounds {
  int has_gt;
  int has_gte;
  int has_lt;
  int has_lte;
  double gt;
  double gte;
  double lt;
  double lte;
} lc_pouch_query_index_range_bounds;

typedef lc_pouch_index_date_bounds lc_pouch_query_index_date_bounds;

typedef int (*lc_pouch_query_index_key_visit_fn)(
    const lc_pouch_query_index_key_view *key, void *context, lc_error *error);

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *ns,
                               lc_pouch_generation state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error);
/* Exclusive foreground threshold publication. If a pending operation becomes
 * active before the batch is taken, this returns LC_OK with index_seq zero so
 * the caller can preserve the batch for a later completion boundary. */
int lc_pouch_query_index_flush_threshold(lc_pouch *pouch, const char *ns,
                                         lc_pouch_generation state_index_seq,
                                         lc_pouch_query_index_flush_result *out,
                                         lc_error *error);
int lc_pouch_query_index_flush_validated(lc_pouch *pouch, const char *ns,
                                         lc_pouch_generation state_index_seq,
                                         lc_pouch_query_index_flush_result *out,
                                         lc_error *error);
/* Retires a namespace's derived query manifest before canonical topology can
 * discard mutations it did not capture. The next foreground flush rebuilds
 * the projection; callers must already hold namespace write authority. */
int lc_pouch_query_index_invalidate_namespace(lc_pouch *pouch, const char *ns,
                                              lc_error *error);
/** Returns whether the durable state sequence is newer than its index
 * manifest, or freshness cannot be read safely. */
int lc_pouch_query_index_has_pending(lc_pouch *pouch, const char *ns);
int lc_pouch_query_index_manifest_seq(lc_pouch *pouch, const char *ns,
                                      lc_pouch_generation *index_seq,
                                      lc_error *error);
int lc_pouch_query_index_ensure_current(lc_pouch *pouch, const char *ns,
                                        lc_pouch_generation state_index_seq,
                                        int validate_current,
                                        lc_pouch_query_index_flush_result *out,
                                        lc_error *error);
/* Preloads the manifest, document table, and query projections for a namespace.
 */
int lc_pouch_query_index_warm_namespace(lc_pouch *pouch, const char *ns,
                                        lc_error *error);
void lc_pouch_query_index_note_state_write(
    lc_pouch *pouch, const char *ns, const char *key, const char *content_type,
    lc_source *body, const lc_pouch_state_write_result *result,
    int operation_active);
void lc_pouch_query_index_note_state_delete(
    lc_pouch *pouch, const char *ns, const char *key,
    const lc_pouch_state_write_result *result, int operation_active);
/** Installs an exclusive-writer publication guard before a lease- or
 * transaction-owned state mutation becomes visible. `operation_id` identifies
 * that exact operation; a positive expiration is pruned after lease expiry.
 * `started_out` is non-zero only when a guard was installed. */
int lc_pouch_query_index_operation_begin(lc_pouch *pouch, const char *ns,
                                         const char *key,
                                         const char *operation_id,
                                         lc_pouch_unix_seconds expires_at_unix,
                                         int *started_out, lc_error *error);
/** Cancels a guard for a mutation that did not become durable. */
void lc_pouch_query_index_operation_cancel(lc_pouch *pouch, const char *ns,
                                           const char *key,
                                           const char *operation_id);
/** Extends an existing lease guard without creating one for metadata-only
 * keepalive traffic. */
void lc_pouch_query_index_operation_refresh(
    lc_pouch *pouch, const char *ns, const char *key, const char *operation_id,
    lc_pouch_unix_seconds expires_at_unix);
/** Returns the earliest live guard expiration in `ns`. The caller
 * holds `pouch->indexer_mutex`; zero means no expiring guard is present. */
lc_pouch_unix_seconds
lc_pouch_query_index_next_operation_expiry_locked(lc_pouch *pouch,
                                                  const char *ns);
/** Returns whether a complete exclusive-writer pending memtable has reached
 * `document_limit`. The caller must hold `pouch->indexer_mutex`. */
int lc_pouch_query_index_pending_document_limit_reached_locked(
    lc_pouch *pouch, const char *ns, uint64_t document_limit);
int lc_pouch_query_index_visit(lc_pouch *pouch, const char *ns,
                               lc_pouch_query_index_row_visit_fn visit,
                               void *context, lc_pouch_generation *index_seq,
                               lc_error *error);
int lc_pouch_query_index_visit_scalar(
    lc_pouch *pouch, const char *ns, const char *field, const char *value,
    char value_type, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_scalar_candidates_exact(const char *value,
                                                 char value_type);
int lc_pouch_query_index_visit_scalar_any(
    lc_pouch *pouch, const char *ns, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_terms(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_terms_docids(
    lc_pouch *pouch, const char *ns,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_any_docids(
    lc_pouch *pouch, const char *ns, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_prefix(
    lc_pouch *pouch, const char *ns, const char *field, const char *prefix,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_prefix_candidates_exact(const char *prefix);
int lc_pouch_query_index_visit_prefix_candidates(
    lc_pouch *pouch, const char *ns, const char *field, const char *prefix,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
/* Uses a caller-held exclusive-Pouch query snapshot. The snapshot must come
 * from lc_pouch_state_query_index_seq() and have been made current before this
 * call. Shared-writer Pouch retains its ordinary durable-read behavior. */
int lc_pouch_query_index_visit_contains_complete_at_snapshot(
    lc_pouch *pouch, const char *ns, const char *field, const char *needle,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    const lc_pouch_generation *state_index_seq_snapshot,
    lc_pouch_generation *index_seq, int *complete, lc_error *error);
int lc_pouch_query_index_visit_contains_candidates(
    lc_pouch *pouch, const char *ns, const char *field, const char *needle,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_range(
    lc_pouch *pouch, const char *ns, const char *field,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_date(
    lc_pouch *pouch, const char *ns, const char *field,
    const lc_pouch_query_index_date_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_exists(lc_pouch *pouch, const char *ns,
                                      const char *field,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      lc_pouch_generation *index_seq,
                                      lc_error *error);

#endif
