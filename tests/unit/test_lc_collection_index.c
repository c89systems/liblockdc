#include <cmocka.h>
#include <lql/lql.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#include "lc_api_internal.h"
#include "lc_collection_index.h"

#define QUERY_ID "00000000000000000000"

typedef struct fixture {
  lc_collection_index index;
  lc_collection_index decoded;
  char *encoded;
  size_t length;
  lc_error error;
  lql *runtime;
  lql_selector *selector;
} fixture;

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  assert_non_null(f);
  lc_error_init(&f->error);
  *state = f;
  assert_int_equal(lc_collection_index_init(&f->index, NULL, "collection-id",
                                            QUERY_ID, 63, 41, &f->error),
                   LC_OK);
  return 0;
}
static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_index_cleanup(&f->index);
  lc_collection_index_cleanup(&f->decoded);
  lc_free_with_allocator(NULL, f->encoded);
  lc_error_cleanup(&f->error);
  if (f->selector)
    f->runtime->selector_destroy(f->runtime, f->selector);
  if (f->runtime)
    f->runtime->destroy(f->runtime);
  free(f);
  return 0;
}

static int add(lc_collection_index *index, const char *json, lc_error *error) {
  lc_source *source = NULL;
  int rc;
  if (json == NULL)
    return lc_collection_index_add(index, NULL, error);
  rc = lc_source_from_memory(json, strlen(json), &source, error);
  if (rc == LC_OK)
    rc = lc_collection_index_add(index, source, error);
  lc_source_close(source);
  return rc;
}

static void lookup(fixture *f, lc_collection_index *index, const char *field,
                   const char *value, char type, int presence, lc_u64 possible,
                   lc_u64 proven) {
  lc_pouch_index_plain_term plain = {field, value, type};
  lc_pouch_index_term_key *key = NULL;
  size_t count = 0;
  lc_u64 actual_possible, actual_proven;
  assert_int_equal(lc_pouch_index_term_keys_build_exact(&plain, 1, &key, &count,
                                                        NULL, &f->error),
                   LC_OK);
  assert_int_equal(count, 1);
  assert_int_equal(lc_collection_index_lookup(index, key, presence,
                                              &actual_possible, &actual_proven,
                                              &f->error),
                   LC_OK);
  lc_pouch_index_term_keys_cleanup(NULL, key, count);
  assert_true(actual_possible == possible);
  assert_true(actual_proven == proven);
}

static void
test_exact_terms_and_uncovered_tail_share_bounded_masks(void **state) {
  fixture *f = *state;
  assert_int_equal(
      add(&f->index, "{\"k\":\"x\",\"n\":10,\"b\":true}", &f->error), LC_OK);
  assert_int_equal(add(&f->index, "{\"k\":\"y\",\"n\":1e1}", &f->error), LC_OK);
  assert_int_equal(add(&f->index, NULL, &f->error), LC_OK);
  assert_true(f->index.covered == 3);
  lookup(f, &f->index, "/k", "x", 's', 0, 5, 1);
  lookup(f, &f->index, "/k", "y", 's', 0, 6, 2);
  lookup(f, &f->index, "/k", "absent", 's', 0, 4, 0);
  lookup(f, &f->index, "/n", "10.0", 'n', 0, 7, 0);
  lookup(f, &f->index, "/b", "true", 'b', 0, 5, 1);
  lookup(f, &f->index, "/k", "", 's', 1, 7, 3);
  lookup(f, &f->index, "/absent", "", 's', 1, 4, 0);
}

typedef struct lql_input {
  const char *json;
  size_t offset;
} lql_input;
static lql_status lql_read(void *context, unsigned char *bytes, size_t capacity,
                           size_t *got, lql_error *error) {
  lql_input *input = context;
  size_t length = strlen(input->json) - input->offset;
  (void)error;
  if (length > capacity)
    length = capacity;
  if (length > 3)
    length = 3;
  memcpy(bytes, input->json + input->offset, length);
  input->offset += length;
  *got = length;
  return LQL_STATUS_OK;
}

static void test_index_masks_agree_with_streaming_lql_semantics(void **state) {
  fixture *f = *state;
  const char *documents[] = {"{}",
                             "{\"k\":\"x\"}",
                             "{\"k\":\"y\",\"k\":\"x\"}",
                             "{\"k\":\"x\",\"k\":\"y\"}",
                             "{\"k\":\"x\",\"k\":null}",
                             "{\"k\":true}",
                             "{\"k\":\"true\"}",
                             "{\"a\":[\"x\",\"x\"]}",
                             "{\"a\":{\"0\":\"x\"}}",
                             "{\"a\":[]}",
                             "{\"a\":[{\"b\":null}]}",
                             "{\"a\":{\"b\":null}}",
                             "{\"n\":1}",
                             "{\"n\":1.00000000000000000000000001}",
                             "{\"n\":1.0000000000000002}",
                             "{\"n\":1e0}"};
  const struct {
    const char *lql, *field, *value;
    char type;
    int presence;
  } predicates[] = {{"eq{field=/k,value=\"x\"}", "/k", "x", 's', 0},
                    {"eq{field=/k,value=true}", "/k", "true", 'b', 0},
                    {"eq{field=/k,value=\"true\"}", "/k", "true", 's', 0},
                    {"eq{field=/k,value=null}", "/k", "null", 'z', 0},
                    {"exists{/k}", "/k", "", 's', 1},
                    {"eq{field=/a/[],value=\"x\"}", "/a/[]", "x", 's', 0},
                    {"exists{/a/**}", "/a/**", "", 's', 1},
                    {"eq{field=/n,value=1}", "/n", "1", 'n', 0}};
  lql *runtime = NULL;
  lql_error error;
  lql_error_init(&error);
  assert_int_equal(lql_new(&runtime, &error), LQL_STATUS_OK);
  f->runtime = runtime;
  for (size_t i = 0; i < sizeof(documents) / sizeof(documents[0]); ++i)
    assert_int_equal(add(&f->index, documents[i], &f->error), LC_OK);
  for (size_t i = 0; i < sizeof(predicates) / sizeof(predicates[0]); ++i) {
    lc_pouch_index_plain_term plain = {predicates[i].field, predicates[i].value,
                                       predicates[i].type};
    lc_pouch_index_term_key *key = NULL;
    size_t count;
    lc_u64 possible, proven;
    lql_selector *selector = NULL;
    assert_int_equal(lc_pouch_index_term_keys_build_exact(
                         &plain, 1, &key, &count, NULL, &f->error),
                     LC_OK);
    assert_int_equal(lc_collection_index_lookup(&f->index, key,
                                                predicates[i].presence,
                                                &possible, &proven, &f->error),
                     LC_OK);
    lc_pouch_index_term_keys_cleanup(NULL, key, count);
    {
      lql_status status = runtime->selector_parse(runtime, predicates[i].lql,
                                                  &selector, &error);
      f->selector = selector;
      if (status != LQL_STATUS_OK)
        print_error("selector %s: %s\n", predicates[i].lql, error.message);
      assert_int_equal(status, LQL_STATUS_OK);
    }
    for (size_t row = 0; row < sizeof(documents) / sizeof(documents[0]);
         ++row) {
      lql_stream_request request = {0};
      lql_stream_result result;
      lql_input input = {documents[row], 0};
      lc_u64 bit = (lc_u64)1 << row;
      request.reader = lql_read;
      request.reader_user = &input;
      request.selector = selector;
      assert_int_equal(lql_stream_apply(runtime, &request, &result, &error),
                       LQL_STATUS_OK);
      if (result.records_matched)
        assert_true((possible & bit) != 0);
      if (proven & bit) {
        if (!result.records_matched)
          print_error("inexact indexed proof for %s against %s\n",
                      predicates[i].lql, documents[row]);
        assert_true(result.records_matched != 0);
      }
    }
    runtime->selector_destroy(runtime, selector);
    f->selector = NULL;
  }
}

static void test_arrays_deduplicate_within_each_slot(void **state) {
  fixture *f = *state;
  assert_int_equal(add(&f->index, "{\"a\":[\"x\",\"x\",\"y\"]}", &f->error),
                   LC_OK);
  assert_int_equal(add(&f->index, "{\"a\":[\"y\"]}", &f->error), LC_OK);
  lookup(f, &f->index, "/a/[]", "x", 's', 0, 1, 0);
  lookup(f, &f->index, "/a/[]", "y", 's', 0, 3, 0);
  lookup(f, &f->index, "/a/1", "x", 's', 0, 1, 1);
  lookup(f, &f->index, "/a/**", "", 's', 1, 3, 0);
}

static void
test_prefix_candidates_include_mixed_array_and_object_paths(void **state) {
  fixture *f = *state;
  lc_pouch_index_plain_term term = {"/a/", "x", 's'};
  lc_pouch_index_term_key *key = NULL;
  size_t count;
  lc_u64 possible, proven;
  assert_int_equal(add(&f->index, "{\"a\":[[\"x\"]]}", &f->error), LC_OK);
  assert_int_equal(add(&f->index, "{\"a\":{\"[]\":\"x\"}}", &f->error), LC_OK);
  assert_int_equal(add(&f->index, "{\"a\":[\"y\"]}", &f->error), LC_OK);
  assert_int_equal(add(&f->index, NULL, &f->error), LC_OK);
  assert_int_equal(lc_pouch_index_term_keys_build_exact(&term, 1, &key, &count,
                                                        NULL, &f->error),
                   LC_OK);
  assert_int_equal(lc_collection_index_lookup_prefix(
                       &f->index, key, 0, &possible, &proven, &f->error),
                   LC_OK);
  lc_pouch_index_term_keys_cleanup(NULL, key, count);
  assert_true(possible == 11 && proven == 0);
}

static void test_last_slot_roundtrip_and_page_binding(void **state) {
  fixture *f = *state;
  lc_u64 high = (lc_u64)1 << (LC_COLLECTION_ORDER_PAGE_ROWS - 1);
  for (size_t i = 0; i < LC_COLLECTION_ORDER_PAGE_ROWS; ++i)
    assert_int_equal(add(&f->index,
                         i == 31 ? NULL
                         : i == 0 || i == LC_COLLECTION_ORDER_PAGE_ROWS - 1U
                             ? "{\"k\":\"x\"}"
                             : "{\"k\":\"y\"}",
                         &f->error),
                     LC_OK);
  assert_int_equal(add(&f->index, "{}", &f->error), LC_ERR_INVALID);
  lookup(f, &f->index, "/k", "x", 's', 0, high | 1 | ((lc_u64)1 << 31),
         high | 1);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_OK);
  assert_true(f->length < LC_COLLECTION_INDEX_MAX_BYTES);
  assert_int_equal(lc_collection_index_decode(&f->decoded, NULL,
                                              "collection-id", QUERY_ID, 63, 41,
                                              f->encoded, f->length, &f->error),
                   LC_OK);
  lookup(f, &f->decoded, "/k", "x", 's', 0, high | 1 | ((lc_u64)1 << 31),
         high | 1);
  assert_int_equal(add(&f->decoded, "{}", &f->error), LC_ERR_INVALID);
  for (int wrong = 0; wrong < 4; ++wrong) {
    lc_collection_index_cleanup(&f->decoded);
    assert_true(lc_collection_index_decode(
                    &f->decoded, NULL,
                    wrong == 0 ? "replacement" : "collection-id",
                    wrong == 3 ? "11111111111111111111" : QUERY_ID,
                    wrong == 1 ? 126 : 63, wrong == 2 ? 42 : 41, f->encoded,
                    f->length, &f->error) != LC_OK);
  }
}

static void test_overflow_never_establishes_a_negative(void **state) {
  fixture *f = *state;
  char *json = malloc(40000);
  size_t offset = 0;
  assert_non_null(json);
  assert_int_equal(add(&f->index, "{\"k\":\"x\"}", &f->error), LC_OK);
  size_t exact_terms = f->index.exact.terms.count;
  size_t presence_terms = f->index.presence.terms.count;
  json[offset++] = '{';
  for (size_t i = 0; i < 2000; ++i)
    offset += (size_t)snprintf(json + offset, 40000 - offset,
                               "%s\"field%04zu\":1", i ? "," : "", i);
  json[offset++] = '}';
  json[offset] = 0;
  assert_int_equal(add(&f->index, json, &f->error), LC_OK);
  free(json);
  assert_true(f->index.covered == 1 && f->index.rows == 2);
  /* Uncovered states must not retain partial/private values or consume the
   * remaining page budget after extraction exceeded its limit. */
  assert_int_equal(f->index.exact.terms.count, exact_terms);
  assert_int_equal(f->index.presence.terms.count, presence_terms);
  assert_true(f->index.exact.terms.count + f->index.presence.terms.count <=
              LC_COLLECTION_INDEX_MAX_TERMS);
  assert_int_equal(add(&f->index, "{\"k\":\"x\"}", &f->error), LC_OK);
  lookup(f, &f->index, "/k", "x", 's', 0, 7, 5);
  lookup(f, &f->index, "/absent", "", 's', 1, 2, 0);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_OK);
  assert_int_equal(lc_collection_index_decode(&f->decoded, NULL,
                                              "collection-id", QUERY_ID, 63, 41,
                                              f->encoded, f->length, &f->error),
                   LC_OK);
  lookup(f, &f->decoded, "/k", "x", 's', 0, 7, 5);
}

static void
test_pruning_removes_values_without_proving_historical_negatives(void **state) {
  fixture *f = *state;
  assert_int_equal(
      add(&f->index, "{\"old\":\"private\",\"k\":\"x\"}", &f->error), LC_OK);
  assert_int_equal(add(&f->index, NULL, &f->error), LC_OK);
  assert_int_equal(add(&f->index, "{\"k\":\"y\"}", &f->error), LC_OK);
  assert_int_equal(lc_collection_index_prune(&f->index, 1U, &f->error), LC_OK);
  assert_int_equal(f->index.exact.terms.count, 1);
  assert_true(f->index.covered == 4U && f->index.rows == 3U);
  lookup(f, &f->index, "/old", "private", 's', 0, 3, 0);
  lookup(f, &f->index, "/k", "y", 's', 0, 7, 4);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_OK);
  assert_int_equal(lc_collection_index_decode(&f->decoded, NULL,
                                              "collection-id", QUERY_ID, 63, 41,
                                              f->encoded, f->length, &f->error),
                   LC_OK);
  lookup(f, &f->decoded, "/old", "private", 's', 0, 3, 0);
  assert_int_equal(lc_collection_index_prune(&f->decoded, 4U, &f->error),
                   LC_OK);
  assert_int_equal(f->decoded.exact.terms.count, 0);
  assert_int_equal(f->decoded.presence.terms.count, 0);
  assert_true(f->decoded.covered == 0U && f->decoded.rows == 3U);
}

static void
test_unrepresentable_numbers_are_validated_and_left_uncovered(void **state) {
  fixture *f = *state;
  assert_int_equal(
      add(&f->index, "{\"early\":1,\"n\":1e99999,\"late\":\"x\"}", &f->error),
      LC_OK);
  assert_int_equal(f->error.code, LC_OK);
  assert_int_equal(add(&f->index, "{\"n\":1e-99999}", &f->error), LC_OK);
  assert_int_equal(add(&f->index, "{\"k\":\"x\"}", &f->error), LC_OK);
  assert_true(f->index.covered == 4 && f->index.rows == 3);
  assert_int_equal(f->index.exact.terms.count, 1);
  lookup(f, &f->index, "/k", "x", 's', 0, 7, 4);
  assert_true(add(&f->index, "{\"n\":1e99999,\"broken\":}", &f->error) !=
              LC_OK);
  assert_true(f->index.failed);
  assert_int_equal(f->index.rows, 3);
}

static void seal(char *bytes, size_t length) {
  unsigned long crc =
      crc32(0L, (const unsigned char *)bytes, (uInt)(length - 4));
  for (unsigned int i = 0; i < 4; ++i)
    bytes[length - 4 + i] = (char)(crc >> (8 * i));
}

static void
test_truncation_corruption_and_impossible_coverage_are_rejected(void **state) {
  fixture *f = *state;
  assert_int_equal(add(&f->index, "{\"k\":\"x\"}", &f->error), LC_OK);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_OK);
  for (size_t length = 0; length < f->length; ++length) {
    assert_true(lc_collection_index_decode(&f->decoded, NULL, "collection-id",
                                           QUERY_ID, 63, 41, f->encoded, length,
                                           &f->error) != LC_OK);
    lc_collection_index_cleanup(&f->decoded);
  }
  f->encoded[90] ^= 1;
  assert_true(lc_collection_index_decode(&f->decoded, NULL, "collection-id",
                                         QUERY_ID, 63, 41, f->encoded,
                                         f->length, &f->error) != LC_OK);
  lc_collection_index_cleanup(&f->decoded);
  f->encoded[90] ^= 1;
  f->encoded[16] |= 2;
  seal(f->encoded, f->length);
  assert_true(lc_collection_index_decode(&f->decoded, NULL, "collection-id",
                                         QUERY_ID, 63, 41, f->encoded,
                                         f->length, &f->error) != LC_OK);
}

static void test_postings_cannot_escape_page_or_disappear(void **state) {
  fixture *f = *state;
  int added;
  assert_int_equal(add(&f->index, "{\"k\":\"x\"}", &f->error), LC_OK);
  assert_int_equal(lc_pouch_index_adaptive_posting_append_sorted_unique(
                       &f->index.exact.postings.items[0].posting, 99, &added,
                       &f->index.allocator, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_OK);
  assert_true(lc_collection_index_decode(&f->decoded, NULL, "collection-id",
                                         QUERY_ID, 63, 41, f->encoded,
                                         f->length, &f->error) != LC_OK);
  lc_collection_index_cleanup(&f->decoded);
  lc_free_with_allocator(NULL, f->encoded);
  f->encoded = NULL;
  lc_pouch_index_term_posting_table_cleanup(&f->index.allocator,
                                            &f->index.exact.postings);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_OK);
  assert_true(lc_collection_index_decode(&f->decoded, NULL, "collection-id",
                                         QUERY_ID, 63, 41, f->encoded,
                                         f->length, &f->error) != LC_OK);
}

static void
test_dictionary_expansion_obeys_explicit_decode_budget(void **state) {
  fixture *f = *state;
  lc_pouch_index_term_limits limits = {LC_COLLECTION_INDEX_MAX_TERMS, 4};
  assert_int_equal(
      add(&f->index, "{\"long-field-name\":[\"x\",\"y\",\"z\"]}", &f->error),
      LC_OK);
  assert_int_equal(lc_pouch_index_term_generation_encode(&f->index.exact, NULL,
                                                         &f->encoded,
                                                         &f->length, &f->error),
                   LC_OK);
  assert_int_equal(lc_pouch_index_term_generation_decode(
                       NULL, f->encoded, f->length, 63, 1, 41, &limits,
                       &f->decoded.exact, &f->error),
                   LC_ERR_INVALID);
  assert_non_null(strstr(f->error.message, "bounded term bytes"));
  limits.max_term_bytes = LC_COLLECTION_INDEX_TERM_BYTES;
  limits.max_terms = 1;
  assert_int_equal(lc_pouch_index_term_generation_decode(
                       NULL, f->encoded, f->length, 63, 1, 41, &limits,
                       &f->decoded.exact, &f->error),
                   LC_ERR_INVALID);
  limits.max_terms = LC_COLLECTION_INDEX_MAX_TERMS;
  assert_int_equal(lc_pouch_index_term_generation_decode(
                       NULL, f->encoded, f->length, 63, 1, 41, &limits,
                       &f->decoded.exact, &f->error),
                   LC_OK);
}

static void test_failed_extraction_cannot_publish_partial_terms(void **state) {
  fixture *f = *state;
  lc_u64 possible = 99, proven = 99;
  lc_pouch_index_term_key key = {"2f6b", "78", 's'};
  assert_true(add(&f->index, "{\"k\":\"x\",\"broken\":", &f->error) != LC_OK);
  assert_int_equal(lc_collection_index_lookup(&f->index, &key, 0, &possible,
                                              &proven, &f->error),
                   LC_ERR_INVALID);
  assert_true(possible == 0 && proven == 0 && f->index.covered == 0);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_ERR_INVALID);
  assert_null(f->encoded);
  assert_int_equal(add(&f->index, "{}", &f->error), LC_ERR_INVALID);
}

typedef struct allocation_fault {
  size_t calls, fail_at, live;
} allocation_fault;
static void *fault_malloc(void *context, size_t size) {
  allocation_fault *fault = context;
  void *ptr;
  if (++fault->calls == fault->fail_at)
    return NULL;
  ptr = malloc(size);
  if (ptr)
    ++fault->live;
  return ptr;
}
static void fault_free(void *context, void *ptr) {
  allocation_fault *fault = context;
  if (!ptr)
    return;
  --fault->live;
  free(ptr);
}
static void *fault_realloc(void *context, void *ptr, size_t size) {
  allocation_fault *fault = context;
  void *next;
  if (ptr == NULL)
    return fault_malloc(context, size);
  if (++fault->calls == fault->fail_at)
    return NULL;
  next = realloc(ptr, size);
  return next;
}

static void
test_allocation_failures_release_build_encode_and_decode(void **state) {
  fixture *f = *state;
  size_t build_calls = 0, decode_calls = 0;
  assert_int_equal(add(&f->index, "{\"k\":[\"x\",\"y\"],\"n\":12}", &f->error),
                   LC_OK);
  assert_int_equal(
      lc_collection_index_encode(&f->index, &f->encoded, &f->length, &f->error),
      LC_OK);
  for (int decoding = 0; decoding <= 1; ++decoding) {
    for (size_t fail_at = 0; fail_at <= (decoding ? decode_calls : build_calls);
         ++fail_at) {
      lc_collection_index page;
      allocation_fault fault = {0};
      lc_allocator allocator = {fault_malloc, fault_realloc, fault_free,
                                &fault};
      char *bytes = NULL;
      size_t length = 0;
      int rc;
      fault.fail_at = fail_at;
      if (decoding)
        rc = lc_collection_index_decode(&page, &allocator, "collection-id",
                                        QUERY_ID, 63, 41, f->encoded, f->length,
                                        &f->error);
      else {
        rc = lc_collection_index_init(&page, &allocator, "collection-id",
                                      QUERY_ID, 63, 41, &f->error);
        if (rc == LC_OK)
          rc = add(&page, "{\"k\":[\"x\",\"y\"],\"n\":12}", &f->error);
        if (rc == LC_OK)
          rc = lc_collection_index_encode(&page, &bytes, &length, &f->error);
      }
      assert_int_equal(rc, fail_at ? LC_ERR_NOMEM : LC_OK);
      if (!fail_at) {
        if (decoding)
          decode_calls = fault.calls;
        else
          build_calls = fault.calls;
      }
      lc_free_with_allocator(&allocator, bytes);
      lc_collection_index_cleanup(&page);
      assert_int_equal(fault.live, 0);
    }
  }
}

static void test_failed_pruning_preserves_the_usable_generation(void **state) {
  fixture *f = *state;
  size_t allocations = 0;
  for (size_t fail_at = 0; fail_at <= allocations; ++fail_at) {
    allocation_fault fault = {0};
    lc_allocator allocator = {fault_malloc, fault_realloc, fault_free, &fault};
    lc_collection_index index;
    size_t before;
    assert_int_equal(lc_collection_index_init(&index, &allocator,
                                              "collection-id", QUERY_ID, 63, 41,
                                              &f->error),
                     LC_OK);
    assert_int_equal(
        add(&index, "{\"old\":\"private\",\"k\":\"x\"}", &f->error), LC_OK);
    assert_int_equal(add(&index, "{\"k\":\"y\"}", &f->error), LC_OK);
    before = fault.calls;
    fault.fail_at = fail_at ? before + fail_at : 0;
    int rc = lc_collection_index_prune(&index, 1, &f->error);
    if (!fail_at) {
      assert_int_equal(rc, LC_OK);
      allocations = fault.calls - before;
      assert_true(index.covered == 2);
    } else {
      assert_int_equal(rc, LC_ERR_NOMEM);
      lc_error_cleanup(&f->error);
      lc_error_init(&f->error);
      assert_false(index.failed);
      assert_true(index.covered == 3 && index.rows == 2);
      lookup(f, &index, "/old", "private", 's', 0, 1, 1);
    }
    lc_collection_index_cleanup(&index);
    assert_int_equal(fault.live, 0);
  }
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_failed_pruning_preserves_the_usable_generation, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_unrepresentable_numbers_are_validated_and_left_uncovered, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_pruning_removes_values_without_proving_historical_negatives,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_exact_terms_and_uncovered_tail_share_bounded_masks, setup,
          teardown),
      cmocka_unit_test_setup_teardown(test_arrays_deduplicate_within_each_slot,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_prefix_candidates_include_mixed_array_and_object_paths, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_index_masks_agree_with_streaming_lql_semantics, setup, teardown),
      cmocka_unit_test_setup_teardown(test_last_slot_roundtrip_and_page_binding,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_overflow_never_establishes_a_negative, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_truncation_corruption_and_impossible_coverage_are_rejected,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_postings_cannot_escape_page_or_disappear, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_dictionary_expansion_obeys_explicit_decode_budget, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_extraction_cannot_publish_partial_terms, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_allocation_failures_release_build_encode_and_decode, setup,
          teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
