#include "lc_collection_query.h"
#include "lc_pouch_crypto.h"
#include <stdio.h>
#include <string.h>

#define LC_CQS_DESCRIPTOR_BYTES 4096U
#define LC_CQS_STORED_BYTES (LC_COLLECTION_INDEX_MAX_BYTES + 65536U)

#ifdef LOCKDC_TEST_BUILD
lc_collection_query_hook_fn lc_collection_query_test_hook;
void *lc_collection_query_test_context;
#endif

typedef struct query_publication {
  lc_collection_handle *collection;
  lc_collection_index *index;
  const char *bytes;
  size_t length;
  char binding[128];
  int encrypted;
} query_publication;

static int query_binding(lc_collection_handle *collection, const char *query_id,
                         lc_u64 ordinal, char binding[128], lc_error *e) {
  char number[32];
  int n;
  if (!lc_xid_is_valid(query_id) ||
      lc_u64_format_base10(ordinal, number, sizeof(number)) < 0)
    return lc_collection_failure(e, LC_ERR_INVALID, NULL,
                                 "collection query page identity is invalid");
  n = snprintf(binding, 128U, "collection-index:%s:%s:%s", collection->id,
               query_id, number);
  return n < 0 || n >= 128
             ? lc_collection_failure(e, LC_ERR_INVALID, NULL,
                                     "collection query binding is too long")
             : LC_OK;
}

static int query_select(void *context, lc_pouch_index_commit_fn commit,
                        void *publication, lc_error *e) {
  query_publication *p = context;
  lc_collection_handle *c = p->collection;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  size_t count = 0U;
  int skip, rc = lc_collection_metadata_enter(c, 1, e);
  if (rc != LC_OK)
    return rc;
  rc = lc_collection_refresh_view(c, e);
  skip = rc != LC_OK || strcmp(c->order.query_id, p->index->query_id) != 0;
  if (!skip) {
    rc = lc_collection_order_rows(&c->order, p->index->exact.index_seq, rows,
                                  LC_COLLECTION_ORDER_PAGE_ROWS, &count, e);
    skip =
        rc != LC_OK || count != p->index->rows ||
        lc_collection_query_row_hash(rows, count) != p->index->exact.row_hash;
  }
  if (rc == LC_OK && !skip)
    rc = commit(publication, e);
  lc_collection_metadata_leave(c);
  return rc;
}

static int query_produce(void *context, lc_sink *sink, lc_error *e) {
  query_publication *p = context;
  return lc_pouch_index_metadata_encode(p->collection->pouch, p->binding,
                                        p->bytes, p->length, p->encrypted, sink,
                                        e);
}

int lc_collection_query_store(lc_collection_handle *collection,
                              lc_collection_index *index, lc_error *error) {
  query_publication p;
  char *bytes = NULL;
  uint64_t bound = 0U;
  int rc;
  memset(&p, 0, sizeof(p));
  p.collection = collection;
  p.index = index;
  p.encrypted = lc_pouch_crypto_enabled(collection->pouch->crypto);
  rc = query_binding(collection, index->query_id, index->exact.index_seq,
                     p.binding, error);
  if (rc == LC_OK)
    rc = lc_collection_index_encode(index, &bytes, &p.length, error);
  p.bytes = bytes;
  if (rc == LC_OK)
    rc = lc_pouch_index_metadata_bound(collection->pouch, p.length, p.encrypted,
                                       &bound, error);
  if (rc == LC_OK && bound > LC_CQS_STORED_BYTES)
    rc = lc_collection_failure(error, LC_ERR_PROTOCOL, NULL,
                               "collection query encoded bound is too large");
  if (rc == LC_OK)
    rc = collection->queries.replace(
        collection->queries.context, index->query_id, index->exact.index_seq,
        bound, query_produce, &p, query_select, &p, error);
  lc_free_with_allocator(&index->allocator, bytes);
  return rc;
}

int lc_collection_query_load(lc_collection_handle *collection,
                             const char *query_id, lc_u64 ordinal,
                             unsigned long row_hash, lc_collection_index *index,
                             int *found, lc_error *error) {
  lc_pouch_index_input input;
  lc_error failure;
  char binding[128], *bytes = NULL;
  size_t length = 0U;
  int rc;
  memset(index, 0, sizeof(*index));
  memset(&input, 0, sizeof(input));
  *found = 0;
  lc_error_init(&failure);
  rc = query_binding(collection, query_id, ordinal, binding, &failure);
  if (rc == LC_OK)
    rc = collection->queries.open(collection->queries.context, query_id,
                                  ordinal, &input, &failure);
  if (rc == LC_OK && input.signature.present)
    rc = lc_pouch_index_metadata_decode(
        collection->pouch, &input, binding,
        lc_pouch_crypto_enabled(collection->pouch->crypto), LC_CQS_STORED_BYTES,
        LC_COLLECTION_INDEX_MAX_BYTES, LC_CQS_DESCRIPTOR_BYTES, &bytes, &length,
        &failure);
  if (rc == LC_OK && input.signature.present)
    rc = lc_collection_index_decode(index, &collection->allocator,
                                    collection->id, query_id, ordinal, row_hash,
                                    bytes, length, &failure);
  if (rc == LC_OK && input.signature.present)
    *found = 1;
  else
    lc_collection_index_cleanup(index);
  collection->queries.close(collection->queries.context, &input);
  lc_free_with_allocator(&collection->pouch->allocator, bytes);
  if (rc == LC_ERR_NOMEM && error != NULL) {
    lc_error_cleanup(error);
    *error = failure;
    memset(&failure, 0, sizeof(failure));
  }
  lc_error_cleanup(&failure);
  return rc == LC_ERR_NOMEM ? rc : LC_OK;
}
