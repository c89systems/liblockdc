#include "lc_collection_index.h"
#include "lc_api_internal.h"
#include "lc_pouch_query_terms.h"

#include <string.h>
#include <zlib.h>

#define LC_CI_HEADER_BYTES 64U
#define LC_CI_PROOF_HEADER_BYTES                                               \
  (LC_CI_HEADER_BYTES + LC_COLLECTION_PAGE_PROOF_BYTES)

static int lc_ci_error(lc_error *error, int rc, const char *message) {
  return lc_error_set(error, rc, 0L, message, NULL,
                      rc == LC_ERR_PROTOCOL ? "collection_index_corrupt" : NULL,
                      "pouch");
}

static lc_u64 lc_ci_mask(unsigned int rows) {
  return ((lc_u64)1U << rows) - 1U;
}

static void lc_ci_put64(unsigned char *bytes, lc_u64 value) {
  unsigned int i;
  for (i = 0U; i < 8U; ++i) {
    bytes[i] = (unsigned char)value;
    value >>= 8U;
  }
}

static lc_u64 lc_ci_get64(const unsigned char *bytes) {
  lc_u64 value;
  unsigned int i;
  value = 0U;
  for (i = 8U; i > 0U; --i)
    value = (value << 8U) | bytes[i - 1U];
  return value;
}

int lc_collection_index_init(lc_collection_index *index,
                             const lc_allocator *allocator,
                             const char *collection_id, const char *query_id,
                             lc_u64 ordinal, unsigned long row_hash,
                             lc_error *error) {
  if (index == NULL)
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection index requires output storage");
  memset(index, 0, sizeof(*index));
  if (allocator != NULL)
    index->allocator = *allocator;
  if (collection_id == NULL || collection_id[0] == '\0' ||
      strlen(collection_id) >= LC_XID_STRING_SIZE ||
      !lc_xid_is_valid(query_id) || row_hash > 0xffffffffUL)
    return lc_ci_error(
        error, LC_ERR_INVALID,
        "collection index requires bounded identity and row hash");
  index->exact.ns = lc_strdup_with_allocator(&index->allocator, collection_id);
  index->presence.ns =
      lc_strdup_with_allocator(&index->allocator, collection_id);
  if (index->exact.ns == NULL || index->presence.ns == NULL) {
    index->failed = 1;
    return lc_ci_error(error, LC_ERR_NOMEM,
                       "failed to allocate collection index identity");
  }
  index->exact.index_seq = index->presence.index_seq = ordinal;
  index->exact.row_hash = index->presence.row_hash = row_hash;
  memcpy(index->query_id, query_id, LC_XID_STRING_SIZE);
  index->exact.terms_sorted = index->presence.terms_sorted = 1;
  return LC_OK;
}

void lc_collection_index_cleanup(lc_collection_index *index) {
  if (index == NULL)
    return;
  lc_pouch_index_term_generation_cleanup(&index->allocator, &index->exact);
  lc_pouch_index_term_generation_cleanup(&index->allocator, &index->presence);
  memset(index, 0, sizeof(*index));
}

static int lc_ci_term(void *context, const lc_pouch_index_term_key *term,
                      int presence, lc_error *error) {
  lc_collection_index *index;
  lc_pouch_index_term_generation *generation;
  lc_pouch_index_term_posting_table *postings;
  unsigned long term_id;
  unsigned long doc_id;
  size_t bytes;
  int added;
  int rc;
  index = (lc_collection_index *)context;
  if (index->incomplete)
    return LC_OK;
  generation = presence ? &index->presence : &index->exact;
  postings = &generation->postings;
  doc_id = (unsigned long)index->rows;
  if (!lc_pouch_index_term_table_find(&generation->terms, term->field_hex,
                                      term->value_hex, term->value_type,
                                      &term_id)) {
    bytes = strlen(term->field_hex) + strlen(term->value_hex) + 2U;
    if (index->exact.terms.count + index->presence.terms.count >=
            LC_COLLECTION_INDEX_MAX_TERMS ||
        bytes > LC_COLLECTION_INDEX_TERM_BYTES - index->term_bytes) {
      index->incomplete = 1;
      return LC_OK;
    }
    rc = lc_pouch_index_term_table_find_or_add_trusted(
        &index->allocator, &generation->terms, term->field_hex, term->value_hex,
        term->value_type, &term_id, error);
    if (rc != LC_OK)
      return rc;
    index->term_bytes += bytes;
    return lc_pouch_index_term_posting_table_append_sorted_unique_trusted(
        &index->allocator, postings, term_id, &doc_id, 1U, error);
  }
  /* Terms acquire increasing IDs and exactly one posting when first seen.
   * The term dictionary sorts keys independently without renumbering IDs. */
  if (term_id == 0UL || term_id > postings->count ||
      postings->items[term_id - 1UL].term_id != term_id)
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection index posting identity is invalid");
  return lc_pouch_index_adaptive_posting_append_sorted_unique(
      &postings->items[term_id - 1UL].posting, doc_id, &added,
      &index->allocator, error);
}

int lc_collection_index_add(lc_collection_index *index, lc_source *source,
                            lc_error *error) {
  int rc;
  if (index == NULL || index->failed || index->sealed ||
      index->exact.ns == NULL || index->rows >= LC_COLLECTION_ORDER_PAGE_ROWS)
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection index is full or unavailable");
  index->incomplete = 0;
  rc = source == NULL ? LC_OK
                      : lc_pouch_query_terms_visit(source, &index->allocator,
                                                   lc_ci_term, index, error);
  if (rc == LC_ERR_UNSUPPORTED && error != NULL && error->server_code != NULL &&
      strcmp(error->server_code, LC_POUCH_QUERY_NUMBER_UNINDEXED) == 0) {
    lc_error_cleanup(error);
    lc_error_init(error);
    index->incomplete = 1;
    rc = LC_OK;
  }
  if (rc != LC_OK) {
    index->failed = 1;
    return rc;
  }
  if (source != NULL && !index->incomplete)
    index->covered |= (lc_u64)1U << index->rows;
  ++index->rows;
  index->exact.row_count = index->presence.row_count = index->rows;
  if (index->incomplete) {
    rc = lc_collection_index_prune(index, (lc_u64)1U << (index->rows - 1U),
                                   error);
    if (rc != LC_OK)
      index->failed = 1;
  }
  return rc;
}

typedef struct lc_ci_posting {
  lc_u64 mask;
  unsigned int rows;
} lc_ci_posting;

static int lc_ci_slot(void *context, unsigned long doc_id, lc_error *error) {
  lc_ci_posting *posting;
  posting = (lc_ci_posting *)context;
  if (doc_id >= posting->rows)
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection index posting exceeds its page");
  posting->mask |= (lc_u64)1U << doc_id;
  return LC_OK;
}

static int lc_ci_exact(const lc_pouch_index_term_key *key, int presence) {
  size_t length;
  if (strstr(key->field_hex, "5b5d") != NULL)
    return 0;
  if (presence) {
    /* Existing recursive presences include the container itself. LQL's
     * recursive existence also requires a descendant, so empty containers
     * are candidates requiring verification. */
    length = strlen(key->field_hex);
    return length < 6U || strcmp(key->field_hex + length - 6U, "2f2a2a") != 0;
  }
  return key->value_type == 's' || key->value_type == 'b' ||
         key->value_type == 'z';
}

static int lc_ci_lookup(const lc_collection_index *index,
                        const lc_pouch_index_term_key *key, int presence,
                        int prefix, lc_u64 *possible, lc_u64 *proven,
                        lc_error *error) {
  const lc_pouch_index_term_generation *generation;
  lc_ci_posting posting;
  unsigned long term_id;
  size_t first, last, mid, length;
  int rc;
  if (possible != NULL)
    *possible = 0U;
  if (proven != NULL)
    *proven = 0U;
  if (index == NULL || index->failed || index->exact.ns == NULL ||
      index->rows > LC_COLLECTION_ORDER_PAGE_ROWS || key == NULL ||
      key->field_hex == NULL || key->value_hex == NULL || possible == NULL ||
      proven == NULL)
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection index lookup arguments are invalid");
  generation = presence ? &index->presence : &index->exact;
  posting.rows = index->rows;
  posting.mask = 0U;
  rc = LC_OK;
  if (prefix) {
    first = 0U;
    last = generation->terms.count;
    length = strlen(key->field_hex);
    while (first < last) {
      mid = first + (last - first) / 2U;
      if (strcmp(generation->terms.items[mid].field_hex, key->field_hex) < 0)
        first = mid + 1U;
      else
        last = mid;
    }
    for (; rc == LC_OK && first < generation->terms.count; ++first) {
      const lc_pouch_index_term_entry *term;
      term = generation->terms.items + first;
      if (strncmp(term->field_hex, key->field_hex, length) != 0)
        break;
      if (term->value_type == key->value_type &&
          strcmp(term->value_hex, key->value_hex) == 0)
        rc = lc_pouch_index_term_posting_table_visit(
            &generation->postings, term->term_id, lc_ci_slot, &posting, error);
    }
  } else if (lc_pouch_index_term_table_find(&generation->terms, key->field_hex,
                                            key->value_hex, key->value_type,
                                            &term_id))
    rc = lc_pouch_index_term_posting_table_visit(&generation->postings, term_id,
                                                 lc_ci_slot, &posting, error);
  if (rc == LC_OK) {
    *possible = posting.mask | (lc_ci_mask(index->rows) & ~index->covered);
    if (!prefix && lc_ci_exact(key, presence))
      *proven = posting.mask & index->covered;
  }
  return rc;
}

int lc_collection_index_lookup(const lc_collection_index *index,
                               const lc_pouch_index_term_key *key, int presence,
                               lc_u64 *possible, lc_u64 *proven,
                               lc_error *error) {
  return lc_ci_lookup(index, key, presence, 0, possible, proven, error);
}

int lc_collection_index_lookup_prefix(const lc_collection_index *index,
                                      const lc_pouch_index_term_key *key,
                                      int presence, lc_u64 *possible,
                                      lc_u64 *proven, lc_error *error) {
  return lc_ci_lookup(index, key, presence, 1, possible, proven, error);
}

static int lc_ci_prune_terms(lc_collection_index *destination,
                             lc_pouch_index_term_generation *output,
                             const lc_pouch_index_term_generation *input,
                             lc_u64 removed, lc_error *error) {
  lc_ci_posting posting;
  unsigned long slots[LC_COLLECTION_ORDER_PAGE_ROWS];
  unsigned long term_id;
  size_t i, count;
  unsigned int slot;
  int rc;
  posting.rows = destination->rows;
  for (i = 0U; i < input->terms.count; ++i) {
    const lc_pouch_index_term_entry *term;
    term = input->terms.items + i;
    posting.mask = 0U;
    rc = lc_pouch_index_term_posting_table_visit(
        &input->postings, term->term_id, lc_ci_slot, &posting, error);
    if (rc != LC_OK)
      return rc;
    posting.mask &= ~removed;
    if (posting.mask == 0U)
      continue;
    count = 0U;
    for (slot = 0U; slot < posting.rows; ++slot)
      if ((posting.mask & ((lc_u64)1U << slot)) != 0U)
        slots[count++] = slot;
    rc = lc_pouch_index_term_table_find_or_add_trusted(
        &destination->allocator, &output->terms, term->field_hex,
        term->value_hex, term->value_type, &term_id, error);
    if (rc == LC_OK)
      rc = lc_pouch_index_term_posting_table_append_sorted_unique_trusted(
          &destination->allocator, &output->postings, term_id, slots, count,
          error);
    if (rc != LC_OK)
      return rc;
    destination->term_bytes +=
        strlen(term->field_hex) + strlen(term->value_hex) + 2U;
  }
  return LC_OK;
}

int lc_collection_index_prune(lc_collection_index *index, lc_u64 removed,
                              lc_error *error) {
  lc_collection_index replacement;
  int rc;
  if (index == NULL || index->failed || index->exact.ns == NULL ||
      index->rows > LC_COLLECTION_ORDER_PAGE_ROWS ||
      (removed & ~lc_ci_mask(index->rows)) != 0U)
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection index pruning requires valid page slots");
  if (removed == 0U)
    return LC_OK;
  rc = lc_collection_index_init(
      &replacement, &index->allocator, index->exact.ns, index->query_id,
      index->exact.index_seq, index->exact.row_hash, error);
  if (rc == LC_OK) {
    memcpy(replacement.page_proof, index->page_proof,
           sizeof(replacement.page_proof));
    replacement.rows = index->rows;
    replacement.covered = index->covered & ~removed;
    replacement.exact.row_count = replacement.presence.row_count = index->rows;
    rc = lc_ci_prune_terms(&replacement, &replacement.exact, &index->exact,
                           removed, error);
  }
  if (rc == LC_OK)
    rc = lc_ci_prune_terms(&replacement, &replacement.presence,
                           &index->presence, removed, error);
  if (rc == LC_OK) {
    lc_collection_index_cleanup(index);
    *index = replacement;
  } else
    lc_collection_index_cleanup(&replacement);
  return rc;
}

int lc_collection_index_encode(lc_collection_index *index, char **bytes,
                               size_t *length, lc_error *error) {
  char *exact, *presence;
  unsigned char *encoded;
  static const unsigned char empty_proof[LC_COLLECTION_PAGE_PROOF_BYTES] = {0};
  size_t exact_len, presence_len, total, header;
  unsigned long crc;
  int rc;
  if (bytes != NULL)
    *bytes = NULL;
  if (length != NULL)
    *length = 0U;
  if (index == NULL || index->failed || index->exact.ns == NULL ||
      index->rows > LC_COLLECTION_ORDER_PAGE_ROWS || bytes == NULL ||
      length == NULL)
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection index encoding arguments are invalid");
  exact = presence = NULL;
  exact_len = presence_len = 0U;
  rc = lc_pouch_index_term_generation_encode(&index->exact, &index->allocator,
                                             &exact, &exact_len, error);
  if (rc == LC_OK)
    rc = lc_pouch_index_term_generation_encode(
        &index->presence, &index->allocator, &presence, &presence_len, error);
  header = memcmp(index->page_proof, empty_proof, sizeof(empty_proof))
               ? LC_CI_PROOF_HEADER_BYTES
               : LC_CI_HEADER_BYTES;
  total = header + exact_len + presence_len + 4U;
  if (rc == LC_OK && total > LC_COLLECTION_INDEX_MAX_BYTES)
    rc = lc_ci_error(error, LC_ERR_INVALID,
                     "collection index page exceeds metadata limit");
  encoded = NULL;
  if (rc == LC_OK) {
    encoded =
        (unsigned char *)lc_alloc_with_allocator(&index->allocator, total);
    if (encoded == NULL)
      rc = lc_ci_error(error, LC_ERR_NOMEM,
                       "failed to allocate encoded collection index");
  }
  if (rc == LC_OK) {
    memset(encoded, 0, header);
    memcpy(encoded, "LCQPAGE1", 8U);
    lc_ci_put64(encoded + 8U, index->rows);
    lc_ci_put64(encoded + 16U, index->covered);
    lc_ci_put64(encoded + 24U, exact_len);
    lc_ci_put64(encoded + 32U, presence_len);
    memcpy(encoded + 40U, index->query_id, LC_XID_STRING_SIZE - 1U);
    if (header == LC_CI_PROOF_HEADER_BYTES) {
      encoded[60] = 1U;
      memcpy(encoded + LC_CI_HEADER_BYTES, index->page_proof,
             sizeof(index->page_proof));
    }
    memcpy(encoded + header, exact, exact_len);
    memcpy(encoded + header + exact_len, presence, presence_len);
    crc = (unsigned long)crc32(0L, encoded, (uInt)(total - 4U));
    encoded[total - 4U] = (unsigned char)crc;
    encoded[total - 3U] = (unsigned char)(crc >> 8U);
    encoded[total - 2U] = (unsigned char)(crc >> 16U);
    encoded[total - 1U] = (unsigned char)(crc >> 24U);
    *bytes = (char *)encoded;
    *length = total;
    index->sealed = 1;
  }
  lc_free_with_allocator(&index->allocator, exact);
  lc_free_with_allocator(&index->allocator, presence);
  return rc;
}

static int lc_ci_validate(lc_collection_index *index,
                          const lc_pouch_index_term_generation *generation,
                          const char *collection_id, lc_error *error) {
  lc_ci_posting posting;
  size_t i;
  int rc;
  if (generation->ns == NULL || strcmp(generation->ns, collection_id) != 0 ||
      generation->terms.count > LC_COLLECTION_INDEX_MAX_TERMS ||
      generation->terms.count != generation->postings.count)
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection index binding or term count is invalid");
  posting.mask = 0U;
  posting.rows = index->rows;
  for (i = 0U; i < generation->postings.count; ++i) {
    rc = lc_pouch_index_adaptive_posting_visit(
        &generation->postings.items[i].posting, lc_ci_slot, &posting, error);
    if (rc != LC_OK)
      return rc;
  }
  if ((posting.mask & ~index->covered) != 0U)
    return lc_ci_error(
        error, LC_ERR_PROTOCOL,
        "collection index contains terms for an uncovered state");
  return LC_OK;
}

int lc_collection_index_decode(lc_collection_index *index,
                               const lc_allocator *allocator,
                               const char *collection_id, const char *query_id,
                               lc_u64 ordinal, unsigned long row_hash,
                               const char *bytes, size_t length,
                               lc_error *error) {
  const unsigned char *encoded;
  lc_pouch_index_term_limits limits;
  lc_u64 exact_len, presence_len, rows;
  size_t header;
  unsigned long crc, stored;
  int rc;
  if (index == NULL)
    return lc_ci_error(error, LC_ERR_INVALID,
                       "collection index requires output storage");
  memset(index, 0, sizeof(*index));
  if (allocator != NULL)
    index->allocator = *allocator;
  index->failed = 1;
  index->sealed = 1;
  if (bytes == NULL || collection_id == NULL || !lc_xid_is_valid(query_id) ||
      length < LC_CI_HEADER_BYTES + 4U ||
      length > LC_COLLECTION_INDEX_MAX_BYTES)
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection index metadata length is invalid");
  encoded = (const unsigned char *)bytes;
  crc = (unsigned long)crc32(0L, encoded, (uInt)(length - 4U));
  stored = (unsigned long)encoded[length - 4U] |
           ((unsigned long)encoded[length - 3U] << 8U) |
           ((unsigned long)encoded[length - 2U] << 16U) |
           ((unsigned long)encoded[length - 1U] << 24U);
  rows = lc_ci_get64(encoded + 8U);
  exact_len = lc_ci_get64(encoded + 24U);
  presence_len = lc_ci_get64(encoded + 32U);
  header = encoded[60] ? LC_CI_PROOF_HEADER_BYTES : LC_CI_HEADER_BYTES;
  if (memcmp(encoded, "LCQPAGE1", 8U) != 0 || crc != stored ||
      memcmp(encoded + 40U, query_id, LC_XID_STRING_SIZE - 1U) != 0 ||
      rows > LC_COLLECTION_ORDER_PAGE_ROWS || encoded[60] > 1U ||
      length < header + 4U || exact_len > length - header - 4U ||
      presence_len != length - header - 4U - exact_len)
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection index envelope is invalid");
  index->rows = (unsigned int)rows;
  memcpy(index->query_id, query_id, LC_XID_STRING_SIZE);
  if (header == LC_CI_PROOF_HEADER_BYTES)
    memcpy(index->page_proof, encoded + LC_CI_HEADER_BYTES,
           sizeof(index->page_proof));
  index->covered = lc_ci_get64(encoded + 16U);
  if ((index->covered & ~lc_ci_mask(index->rows)) != 0U)
    return lc_ci_error(error, LC_ERR_PROTOCOL,
                       "collection index coverage exceeds its page");
  limits.max_terms = LC_COLLECTION_INDEX_MAX_TERMS;
  limits.max_term_bytes = LC_COLLECTION_INDEX_TERM_BYTES;
  rc = lc_pouch_index_term_generation_decode(
      &index->allocator, bytes + header, (size_t)exact_len, ordinal,
      (unsigned long)rows, row_hash, &limits, &index->exact, error);
  if (rc == LC_OK)
    rc = lc_pouch_index_term_generation_decode(
        &index->allocator, bytes + header + (size_t)exact_len,
        (size_t)presence_len, ordinal, (unsigned long)rows, row_hash, &limits,
        &index->presence, error);
  if (rc == LC_OK)
    rc = lc_ci_validate(index, &index->exact, collection_id, error);
  if (rc == LC_OK)
    rc = lc_ci_validate(index, &index->presence, collection_id, error);
  if (rc == LC_OK && index->exact.terms.count + index->presence.terms.count >
                         LC_COLLECTION_INDEX_MAX_TERMS)
    rc = lc_ci_error(error, LC_ERR_PROTOCOL,
                     "collection index exceeds combined term limit");
  if (rc == LC_OK)
    index->failed = 0;
  return rc;
}
