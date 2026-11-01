#include <cmocka.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../support/lc_test_tmp.h"
#include "lc_collection_order.h"

#define ORDER_PREFIX LC_COLLECTION_TMP_PARENT "/collection-order-"

typedef struct fixture {
  char path[512];
  lc_collection_order order;
  lc_error error;
} fixture;

static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  int fd;
  assert_non_null(f);
  strcpy(f->path, ORDER_PREFIX "XXXXXX");
  fd = lc_test_tmp_mkstemp(f->path, ORDER_PREFIX);
  assert_true(fd >= 0);
  assert_int_equal(close(fd), 0);
  lc_error_init(&f->error);
  assert_int_equal(lc_collection_order_open(f->path, 1, &f->order, &f->error),
                   LC_OK);
  *state = f;
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_order_close(&f->order);
  lc_error_cleanup(&f->error);
  lc_test_tmp_cleanup_path(f->path, ORDER_PREFIX);
  free(f);
  return 0;
}

static void append(fixture *f, lc_u64 position) {
  lc_collection_order_row row;
  memset(&row, 0, sizeof(row));
  row.position = position;
  row.published_at = f->order.generation + 1;
  row.segment = 1;
  row.record_offset = f->order.count * 100;
  row.bytes = 2;
  assert_int_equal(lc_collection_order_append(&f->order, &row, 1,
                                              f->order.replay_offset + 100,
                                              &f->error),
                   LC_OK);
}

static void remove_position(fixture *f, lc_u64 position) {
  assert_int_equal(
      lc_collection_order_delete(&f->order, position, f->order.generation + 1,
                                 1, f->order.replay_offset + 100, &f->error),
      LC_OK);
}

static void test_empty_and_exclusive_bounds(void **state) {
  fixture *f = *state;
  lc_collection_order_iter it;
  lc_collection_order_row row;
  int found;
  assert_int_equal(
      lc_collection_order_seek(&f->order, 0, LC_U64_MAX, 0, 0, &it, &f->error),
      LC_OK);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_false(found);
  append(f, 2);
  append(f, 5);
  append(f, 9);
  for (int reverse = 0; reverse <= 1; ++reverse) {
    assert_int_equal(
        lc_collection_order_seek(&f->order, 2, 5, 3, reverse, &it, &f->error),
        LC_OK);
    assert_int_equal(
        lc_collection_order_next(&f->order, &it, &row, &found, &f->error),
        LC_OK);
    assert_true(found);
    assert_true(row.position == 5);
    assert_int_equal(
        lc_collection_order_next(&f->order, &it, &row, &found, &f->error),
        LC_OK);
    assert_false(found);
    assert_int_equal(
        lc_collection_order_seek(&f->order, 5, 5, 3, reverse, &it, &f->error),
        LC_OK);
    assert_int_equal(
        lc_collection_order_next(&f->order, &it, &row, &found, &f->error),
        LC_OK);
    assert_false(found);
  }
}

static void test_full_unsigned_positions_are_keys(void **state) {
  fixture *f = *state;
  lc_collection_order_iter it;
  lc_collection_order_row row;
  struct stat st;
  int found;
  append(f, 1);
  append(f, (lc_u64)LC_I64_MAX + 1);
  append(f, LC_U64_MAX);
  assert_int_equal(
      lc_collection_order_find(&f->order, LC_U64_MAX, &row, &found, &f->error),
      LC_OK);
  assert_true(found);
  assert_true(row.position == LC_U64_MAX);
  assert_int_equal(lc_collection_order_seek(&f->order, LC_I64_MAX, LC_U64_MAX,
                                            3, 1, &it, &f->error),
                   LC_OK);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_true(found);
  assert_true(row.position == LC_U64_MAX);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_true(found);
  assert_true(row.position == (lc_u64)LC_I64_MAX + 1);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_false(found);
  assert_int_equal(fstat(f->order.fd, &st), 0);
  assert_true(st.st_size < 4096);
}

static void test_deleted_pages_preserve_snapshot_and_order(void **state) {
  fixture *f = *state;
  lc_collection_order_iter it;
  lc_collection_order_row row;
  const lc_u64 count = LC_COLLECTION_ORDER_PAGE_ROWS * 3;
  int found;
  for (lc_u64 i = 1; i <= count; ++i)
    append(f, i * 3);
  for (lc_u64 i = 1; i <= LC_COLLECTION_ORDER_PAGE_ROWS * 2; ++i)
    remove_position(f, i * 3);
  for (int old_view = 0; old_view <= 1; ++old_view) {
    for (int reverse = 0; reverse <= 1; ++reverse) {
      const lc_u64 start = old_view ? 1 : LC_COLLECTION_ORDER_PAGE_ROWS * 2 + 1;
      lc_u64 expected = reverse ? count : start;
      size_t seen = 0;
      assert_int_equal(
          lc_collection_order_seek(&f->order, 0, count * 3,
                                   old_view ? count : f->order.generation,
                                   reverse, &it, &f->error),
          LC_OK);
      do {
        assert_int_equal(
            lc_collection_order_next(&f->order, &it, &row, &found, &f->error),
            LC_OK);
        if (found) {
          assert_true(row.position == expected * 3);
          ++seen;
          expected = reverse ? expected - 1 : expected + 1;
        }
      } while (found);
      assert_int_equal(seen, count - start + 1);
    }
  }
  lc_collection_order_close(&f->order);
  assert_int_equal(lc_collection_order_open(f->path, 0, &f->order, &f->error),
                   LC_OK);
  assert_true(f->order.upper_position == count * 3);
  assert_int_equal(
      lc_collection_order_find(&f->order, 3, &row, &found, &f->error), LC_OK);
  assert_true(found && row.deleted_at > count);
}

static void test_captured_iteration_excludes_later_appends(void **state) {
  fixture *f = *state;
  lc_collection_order_iter it;
  lc_collection_order_row row;
  int found;
  append(f, 10);
  append(f, 20);
  assert_int_equal(lc_collection_order_seek(&f->order, 0, LC_U64_MAX,
                                            f->order.generation, 0, &it,
                                            &f->error),
                   LC_OK);
  remove_position(f, 20);
  append(f, 30);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_true(found && row.position == 10);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_true(found && row.position == 20);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_false(found);
}

static void
test_batches_bound_deleted_work_and_keep_captured_visibility(void **state) {
  fixture *f = *state;
  lc_collection_order_iter old, it;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  const lc_u64 total = LC_COLLECTION_ORDER_PAGE_ROWS * 4 + 7;
  lc_u64 examined, previous;
  size_t count, calls;
  for (lc_u64 i = 1; i <= total; ++i)
    append(f, i);
  assert_int_equal(lc_collection_order_seek(&f->order, 0, total,
                                            f->order.generation, 0, &old,
                                            &f->error),
                   LC_OK);
  for (lc_u64 i = 1; i <= total; ++i)
    remove_position(f, i);
  /* A small caller buffer may split a page, without losing or reordering rows.
   * Deletions published after this read's view must remain invisible. */
  previous = 0;
  while (!old.done) {
    assert_int_equal(lc_collection_order_batch(&f->order, &old, rows, 7, &count,
                                               &examined, &f->error),
                     LC_OK);
    assert_true(count > 0 && count <= 7);
    for (size_t i = 0; i < count; ++i)
      assert_true(rows[i].position == ++previous);
    assert_true(examined == previous);
  }
  assert_true(previous == total);
  for (int reverse = 0; reverse <= 1; ++reverse) {
    assert_int_equal(lc_collection_order_seek(&f->order, 1, total - 1,
                                              f->order.generation, reverse, &it,
                                              &f->error),
                     LC_OK);
    calls = 0;
    previous = reverse ? total : 1;
    while (!it.done) {
      assert_int_equal(lc_collection_order_batch(&f->order, &it, rows,
                                                 LC_COLLECTION_ORDER_PAGE_ROWS,
                                                 &count, &examined, &f->error),
                       LC_OK);
      assert_int_equal(count, 0);
      assert_true(reverse ? examined < previous : examined > previous);
      assert_true(reverse
                      ? previous - examined <= LC_COLLECTION_ORDER_PAGE_ROWS
                      : examined - previous <= LC_COLLECTION_ORDER_PAGE_ROWS);
      previous = examined;
      ++calls;
    }
    assert_int_equal(calls, 5);
    assert_true(previous == (reverse ? 1 : total - 1));
  }
}

static void test_delete_all_keeps_highwater_and_retry_binding(void **state) {
  fixture *f = *state;
  lc_collection_order_row row;
  lc_collection_order_iter it;
  lc_u64 generation;
  int found;
  append(f, 3);
  append(f, 42);
  remove_position(f, 3);
  remove_position(f, 42);
  generation = f->order.generation;
  remove_position(f, 42);
  assert_true(f->order.generation == generation);
  assert_int_equal(
      lc_collection_order_seek(&f->order, 0, 42, generation, 0, &it, &f->error),
      LC_OK);
  assert_int_equal(
      lc_collection_order_next(&f->order, &it, &row, &found, &f->error), LC_OK);
  assert_false(found);
  lc_collection_order_close(&f->order);
  assert_int_equal(lc_collection_order_open(f->path, 0, &f->order, &f->error),
                   LC_OK);
  assert_true(f->order.upper_position == 42);
  assert_true(f->order.generation == generation);
  assert_true(f->order.count == 2);
  append(f, 100);
  assert_int_equal(
      lc_collection_order_find(&f->order, 42, &row, &found, &f->error), LC_OK);
  assert_true(found && row.deleted_at != 0);
  assert_int_equal(
      lc_collection_order_find(&f->order, 41, &row, &found, &f->error), LC_OK);
  assert_false(found);
}

static void corrupt_byte(fixture *f, off_t offset) {
  unsigned char byte;
  assert_int_equal(pread(f->order.fd, &byte, 1, offset), 1);
  byte ^= 1;
  assert_int_equal(pwrite(f->order.fd, &byte, 1, offset), 1);
}

static void test_corruption_is_explicit_and_open_is_bounded(void **state) {
  fixture *f = *state;
  lc_collection_order other;
  lc_collection_order_iter it;
  lc_collection_order_row row;
  int found;
  for (lc_u64 i = 1; i <= 1000; ++i)
    append(f, i);
  /* A distant row is validated on demand, not scanned during cold open. */
  corrupt_byte(f, LC_COLLECTION_ORDER_ROOT_BYTES + 4096 * 10 + 64);
  assert_int_equal(lc_collection_order_open(f->path, 0, &other, &f->error),
                   LC_OK);
  assert_int_equal(lc_collection_order_seek(&other, 630, 631, other.generation,
                                            0, &it, &f->error),
                   LC_ERR_PROTOCOL);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(
      lc_collection_order_find(&other, 631, &row, &found, &f->error),
      LC_ERR_PROTOCOL);
  lc_collection_order_close(&other);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  corrupt_byte(f, 0);
  assert_int_equal(lc_collection_order_open(f->path, 1, &other, &f->error),
                   LC_ERR_PROTOCOL);
  assert_int_equal(other.fd, -1);
}

static void test_truncation_missing_and_invalid_mutations(void **state) {
  fixture *f = *state;
  lc_collection_order other;
  lc_collection_order_row row;
  lc_collection_order_iter it;
  int found;
  append(f, 7);
  memset(&row, 0, sizeof(row));
  row.position = 7;
  row.published_at = 2;
  row.segment = 1;
  assert_int_equal(
      lc_collection_order_append(&f->order, &row, 1, 200, &f->error),
      LC_ERR_INVALID);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(
      lc_collection_order_delete(&f->order, 6, 2, 1, 200, &f->error),
      LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "entry_not_found");
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(
      lc_collection_order_seek(&f->order, 0, 7, 2, 0, &it, &f->error),
      LC_ERR_INVALID);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(
      lc_collection_order_find(&f->order, 0, &row, &found, &f->error),
      LC_ERR_INVALID);
  assert_int_equal(
      ftruncate(f->order.fd, LC_COLLECTION_ORDER_ROOT_BYTES + 64 + 32), 0);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(lc_collection_order_open(f->path, 1, &other, &f->error),
                   LC_ERR_PROTOCOL);
}

static void
test_snapshot_rebuild_keeps_interleaved_deletion_generations(void **state) {
  fixture *f = *state;
  lc_collection_order_row rows[3] = {
      {1, 1, 10, 1, 0, 2},
      {2, 2, 7, 1, 100, 2},
      {3, 4, 0, 1, 200, 2},
  };
  const lc_u64 generations[] = {3, 8, 10};
  const lc_u64 expected[][3] = {{1, 2, 0}, {1, 3, 0}, {3, 0, 0}};
  lc_collection_order_iter it;
  lc_collection_order_row row;
  int found;
  for (size_t i = 0; i < 3; ++i) {
    assert_int_equal(lc_collection_order_append_recovered(
                         &f->order, &rows[i], 1, (i + 1) * 100, &f->error),
                     LC_OK);
  }
  assert_true(f->order.generation == 10 && f->order.upper_position == 3);
  lc_collection_order_close(&f->order);
  assert_int_equal(lc_collection_order_open(f->path, 0, &f->order, &f->error),
                   LC_OK);
  for (size_t view = 0; view < 3; ++view) {
    size_t i = 0;
    assert_int_equal(lc_collection_order_seek(
                         &f->order, 0, 3, generations[view], 0, &it, &f->error),
                     LC_OK);
    do {
      assert_int_equal(
          lc_collection_order_next(&f->order, &it, &row, &found, &f->error),
          LC_OK);
      if (found) {
        assert_true(i < 3 && expected[view][i] == row.position);
        ++i;
      }
    } while (found);
    assert_true(expected[view][i] == 0);
  }
  append(f, 5);
  assert_true(f->order.generation == 11);
}

static void test_oldest_live_hint_skips_gaps_and_survives_reopen(void **state) {
  fixture *f = *state;
  lc_collection_order_row row;
  const lc_u64 total = LC_COLLECTION_ORDER_PAGE_ROWS * 3 + 7;
  int found;
  for (lc_u64 i = 1; i <= total; ++i)
    append(f, i);
  /* Leave a deleted prefix with a deliberately conservative lower bound. */
  for (lc_u64 i = 2; i < total; ++i)
    remove_position(f, i);
  remove_position(f, 1);
  assert_int_equal(
      lc_collection_order_oldest(&f->order, &row, &found, &f->error), LC_OK);
  assert_true(found && row.position == total &&
              f->order.first_live == total - 1);
  remove_position(f, total);
  lc_collection_order_close(&f->order);
  assert_int_equal(lc_collection_order_open(f->path, 0, &f->order, &f->error),
                   LC_OK);
  assert_true(f->order.first_live == total && f->order.live_count == 0);
  assert_int_equal(
      lc_collection_order_oldest(&f->order, &row, &found, &f->error), LC_OK);
  assert_false(found);
  append(f, LC_U64_MAX);
  assert_int_equal(
      lc_collection_order_oldest(&f->order, &row, &found, &f->error), LC_OK);
  assert_true(found && row.position == LC_U64_MAX &&
              f->order.first_live == total);
}

static void
test_dense_seek_checks_rows_and_preserves_deleted_views(void **state) {
  fixture *f = *state;
  lc_collection_order_iter *it = calloc(1, sizeof(*it));
  lc_collection_order_row row;
  int found;
  assert_non_null(it);
  for (lc_u64 i = 1; i <= 128; ++i)
    append(f, i);
  for (lc_u64 i = 1; i <= 63; ++i)
    remove_position(f, i);
  for (lc_u64 after = 0; after <= 128; ++after) {
    for (int old = 0; old <= 1; ++old) {
      lc_u64 expected = old || after >= 63 ? after + 1 : 64;
      assert_int_equal(lc_collection_order_seek(&f->order, after, LC_U64_MAX,
                                                old ? 128 : f->order.generation,
                                                0, it, &f->error),
                       LC_OK);
      assert_int_equal(
          lc_collection_order_next(&f->order, it, &row, &found, &f->error),
          LC_OK);
      assert_true(found == (expected <= 128));
      if (found)
        assert_true(row.position == expected);
    }
  }
  corrupt_byte(f, LC_COLLECTION_ORDER_ROOT_BYTES + 4096 + 64);
  assert_int_equal(lc_collection_order_seek(&f->order, 63, 128,
                                            f->order.generation, 0, it,
                                            &f->error),
                   LC_ERR_PROTOCOL);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  assert_int_equal(
      lc_collection_order_find(&f->order, 64, &row, &found, &f->error),
      LC_ERR_PROTOCOL);
  free(it);
}

static void test_range_counts_and_cuts_match_sparse_model(void **state) {
  fixture *f = *state;
  enum { TOTAL = LC_COLLECTION_ORDER_PAGE_ROWS * 4 + 11 };
  unsigned char live[TOTAL];
  lc_collection_edit_range ranges[LC_COLLECTION_EDIT_MAX_RANGES];
  lc_allocator allocator;
  lc_collection_order_iter *old = calloc(1, sizeof(*old));
  lc_collection_order_row row;
  lc_u64 expected, counted, removed, before, generation;
  size_t count, i, r, round;
  int found;
  assert_non_null(old);
  lc_allocator_init(&allocator);
  memset(live, 1, sizeof(live));
  for (i = 0; i < TOTAL; ++i)
    append(f, (i + 1) * 3);
  for (i = 0; i < TOTAL; ++i)
    if (i % 5 == 0 || (i >= 63 && i < 126)) {
      remove_position(f, (i + 1) * 3);
      live[i] = 0;
    }
  for (round = 0; round < 5; ++round) {
    count = 2;
    ranges[0] = (lc_collection_edit_range){4, 41};
    ranges[1] = (lc_collection_edit_range){43, 48};
    if (round == 1) {
      count = LC_COLLECTION_EDIT_MAX_RANGES;
      for (r = 0; r < count; ++r)
        ranges[r] = (lc_collection_edit_range){r * 9 + 1, r * 9 + 6};
    } else if (round == 2 || round == 3) {
      count = 1;
      ranges[0] = (lc_collection_edit_range){0, LC_U64_MAX};
    } else if (round == 4)
      count = 0;
    expected = 0;
    for (i = 0; i < TOTAL; ++i)
      for (r = 0; r < count; ++r)
        if (live[i] && (i + 1) * 3 > ranges[r].after &&
            (i + 1) * 3 <= ranges[r].through)
          ++expected;
    before = f->order.live_count;
    generation = f->order.generation;
    assert_int_equal(lc_collection_order_count_ranges(&f->order, &allocator,
                                                      ranges, count, &counted,
                                                      &f->error),
                     LC_OK);
    assert_true(counted == expected);
    assert_int_equal(lc_collection_order_refresh(&f->order, &f->error), LC_OK);
    assert_true(f->order.live_count == before &&
                f->order.generation == generation);
    assert_int_equal(lc_collection_order_seek(&f->order, 0, LC_U64_MAX,
                                              generation, 0, old, &f->error),
                     LC_OK);
    assert_int_equal(
        lc_collection_order_edit_begin(&f->order, generation + 1, &f->error),
        LC_OK);
    assert_int_equal(lc_collection_order_cut(&f->order, &allocator, ranges,
                                             count, generation + 1, &removed,
                                             &f->error),
                     LC_OK);
    assert_true(removed == expected &&
                f->order.live_count == before - expected);
    assert_int_equal(lc_collection_order_checkpoint(
                         &f->order, 1, f->order.replay_offset, &f->error),
                     LC_OK);
    counted = 0;
    do {
      assert_int_equal(
          lc_collection_order_next(&f->order, old, &row, &found, &f->error),
          LC_OK);
      if (found)
        ++counted;
    } while (found);
    assert_true(counted == before);
    for (i = 0; i < TOTAL; ++i) {
      for (r = 0; r < count; ++r)
        if ((i + 1) * 3 > ranges[r].after && (i + 1) * 3 <= ranges[r].through)
          live[i] = 0;
      assert_int_equal(lc_collection_order_find(&f->order, (i + 1) * 3, &row,
                                                &found, &f->error),
                       LC_OK);
      assert_true(found && (row.deleted_at == 0) == (live[i] != 0));
    }
  }
  free(old);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_dense_seek_checks_rows_and_preserves_deleted_views, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_range_counts_and_cuts_match_sparse_model, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_oldest_live_hint_skips_gaps_and_survives_reopen, setup,
          teardown),
      cmocka_unit_test_setup_teardown(test_empty_and_exclusive_bounds, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_full_unsigned_positions_are_keys,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_deleted_pages_preserve_snapshot_and_order, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_captured_iteration_excludes_later_appends, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_batches_bound_deleted_work_and_keep_captured_visibility, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_delete_all_keeps_highwater_and_retry_binding, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_corruption_is_explicit_and_open_is_bounded, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_truncation_missing_and_invalid_mutations, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_snapshot_rebuild_keeps_interleaved_deletion_generations, setup,
          teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}
