#ifndef POUCH_SPACE_ORDERED_READ_CASES_H
#define POUCH_SPACE_ORDERED_READ_CASES_H

typedef struct ordered_view {
  lc_pouch *pouch;
  unsigned char seen[1024];
  char last[64];
  size_t count, metadata, deleted;
} ordered_view;
static void ordered_key(size_t index, char key[32]) {
  REQUIRE(snprintf(key, 32, "p%zu/%04zu", index / 256, index % 256) < 32);
}
static int ordered_live(size_t index) {
  return index % 11 || index == 0 || index == 1023;
}
static void ordered_seen(ordered_view *view, const char *key, int body) {
  REQUIRE(!view->last[0] || strcmp(view->last, key) < 0);
  REQUIRE(strlen(key) < sizeof(view->last));
  strcpy(view->last, key);
  if (!strcmp(key, "p1/zz-meta")) {
    REQUIRE(!body);
    ++view->metadata;
  } else {
    unsigned group, number;
    char extra;
    REQUIRE(sscanf(key, "p%u/%u%c", &group, &number, &extra) == 2);
    size_t index = group * 256U + number;
    REQUIRE(group < 4 && number < 256 && ordered_live(index) &&
            !view->seen[index] && body);
    view->seen[index] = 1;
  }
  ++view->count;
}
static int ordered_summary(const lc_pouch_state_scan_summary_entry *entry,
                           void *context, lc_error *e) {
  (void)e;
  ordered_seen(context, entry->key, entry->bytes == 17);
  return LC_OK;
}
static int ordered_prefix(const lc_pouch_state_visit_entry *entry,
                          void *context, lc_error *e) {
  (void)e;
  REQUIRE(!strncmp(entry->key, "p1/", 3));
  ordered_seen(context, entry->key, entry->has_payload);
  return LC_OK;
}
static int ordered_consume(void *context, lc_error *e) {
  ordered_view *view = context;
  char *keys[LC_POUCH_STATE_KEY_WINDOW], cursor[64] = {0};
  size_t count = 0;
  int rc;
  do {
    rc = lc_pouch_state_payload_keys_locked(
        view->pouch, "ordered", "p1/", cursor, keys, LC_POUCH_STATE_KEY_WINDOW,
        &count, e);
    if (rc != LC_OK)
      return rc;
    for (size_t i = 0; i < count; ++i) {
      lc_pouch_state_write_result written = {0};
      REQUIRE(strcmp(keys[i], cursor) > 0 && strlen(keys[i]) < sizeof(cursor));
      strcpy(cursor, keys[i]);
      ok(lc_pouch_state_delete(view->pouch, "ordered", keys[i], NULL, &written,
                               e),
         e);
      lc_pouch_state_write_result_cleanup(&allocator, &written);
      lc_free_with_allocator(&allocator, keys[i]);
      ++view->deleted;
    }
    if (view->deleted == LC_POUCH_STATE_KEY_WINDOW) {
      /* Rebalancing after insertion cannot invalidate an owned key cursor. */
      input in = source(17, 91);
      lc_pouch_state_write_result written = {0};
      ok(put(view->pouch, "ordered", "p1/9999", &in, NULL, &written, e), e);
      lc_pouch_state_write_result_cleanup(&allocator, &written);
    }
  } while (count);
  return LC_OK;
}
static void ordered_reads(const char *path, const char *directory,
                          int benchmark) {
  lc_error e;
  lc_error_init(&e);
  for (int multifile = 0; multifile < 2; ++multifile)
    for (int crypto = 0; crypto <= 3; crypto += 3) {
      ps_pouch_state_options o = options(crypto, 0);
      lc_pouch *p = NULL;
      char native[1400];
      const char *root = path;
      if (multifile) {
        REQUIRE(snprintf(native, sizeof(native), "%s/ordered-%d", directory,
                         crypto) < (int)sizeof(native));
        root = native;
        ok(lc_pouch_open(root, &allocator, &o.logical, &p, &e), &e);
      } else
        ok(ps_pouch_state_open(root, 1, &allocator, &o, &p, &e), &e);
      /* Ascending, descending, alternating extremes and a full permutation. */
      for (size_t group = 0; group < 4; ++group)
        for (size_t i = 0; i < 256; ++i) {
          size_t number = group == 0   ? i
                          : group == 1 ? 255 - i
                          : group == 2 ? (i % 2 ? 255 - i / 2 : i / 2)
                                       : (73 * i) % 256;
          char key[32];
          ordered_key(256 * group + number, key);
          input in = source(17, (unsigned char)number);
          lc_pouch_state_write_result written = {0};
          ok(put(p, "ordered", key, &in, NULL, &written, &e), &e);
          REQUIRE(in.offset == 17 && in.calls > 1);
          lc_pouch_state_write_result_cleanup(&allocator, &written);
        }
      for (size_t i = 0; i < 1024; i += 11) {
        char key[32];
        ordered_key(i, key);
        lc_pouch_state_write_result written = {0};
        ok(lc_pouch_state_delete(p, "ordered", key, NULL, &written, &e), &e);
        lc_pouch_state_write_result_cleanup(&allocator, &written);
        if (i == 0 || i == 1023) {
          input in = source(17, (unsigned char)(i % 256));
          ok(put(p, "ordered", key, &in, NULL, &written, &e), &e);
          lc_pouch_state_write_result_cleanup(&allocator, &written);
        }
      }
      lc_pouch_state_write_options metadata = {0};
      lc_pouch_state_write_result written = {0};
      metadata.has_metadata = 1;
      metadata.metadata = (const unsigned char *)"metadata";
      metadata.metadata_length = 8;
      ok(lc_pouch_state_update_metadata(p, "ordered", "p1/zz-meta", &metadata,
                                        &written, &e),
         &e);
      lc_pouch_state_write_result_cleanup(&allocator, &written);
      for (int cold = 0; cold < 2; ++cold) {
        struct timespec reopen_begin = {0}, reopen_end = {0};
        if (cold) {
          lc_pouch_close(p);
          p = NULL;
          REQUIRE(!clock_gettime(CLOCK_THREAD_CPUTIME_ID, &reopen_begin));
          if (multifile)
            ok(lc_pouch_open(root, &allocator, &o.logical, &p, &e), &e);
          else
            ok(ps_pouch_state_open(root, 0, &allocator, &o, &p, &e), &e);
        }
        read_value(p, "ordered", "p0/0000", 17, 0, 1, &e);
        if (cold) {
          REQUIRE(!clock_gettime(CLOCK_THREAD_CPUTIME_ID, &reopen_end));
          printf(
              "ordered_reopen multifile=%d crypto=%d cpu_ms=%.6f verified=1\n",
              multifile, crypto,
              (reopen_end.tv_sec - reopen_begin.tv_sec) * 1000.0 +
                  (reopen_end.tv_nsec - reopen_begin.tv_nsec) / 1e6);
        }
        ordered_view *view = calloc(1, sizeof(*view));
        REQUIRE(view);
        lc_pouch_state_scan_summaries_result page = {0};
        char *cursor = NULL;
        do {
          ok(lc_pouch_state_scan_summaries(p, "ordered", cursor, 7,
                                           ordered_summary, view, &page, &e),
             &e);
          lc_free_with_allocator(&allocator, cursor);
          cursor = page.next_start_after;
          page.next_start_after = NULL;
        } while (page.truncated);
        REQUIRE(view->count == 932 && view->metadata == 0);
        for (size_t i = 0; i < 1024; ++i)
          REQUIRE(view->seen[i] == (ordered_live(i) ? 1 : 0));
        free(view);
        if (!benchmark) {
          view = calloc(1, sizeof(*view));
          REQUIRE(view);
          ok(lc_pouch_state_visit_prefix(p, "ordered", "p1/", ordered_prefix,
                                         view, &e),
             &e);
          REQUIRE(view->count == 234 && view->metadata == 1);
          free(view);
        }
        __atomic_store_n(&measured_max, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&measure, 1, __ATOMIC_RELAXED);
        struct timespec begin, end;
        REQUIRE(!clock_gettime(CLOCK_THREAD_CPUTIME_ID, &begin));
        for (size_t repeat = 0; repeat < 128; ++repeat) {
          view = calloc(1, sizeof(*view));
          REQUIRE(view);
          ok(lc_pouch_state_scan_summaries(p, "ordered", "p1/0100", 7,
                                           ordered_summary, view, &page, &e),
             &e);
          REQUIRE(view->count == 7 && page.truncated);
          lc_pouch_state_scan_summaries_result_cleanup(&allocator, &page);
          free(view);
        }
        REQUIRE(!clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end));
        __atomic_store_n(&measure, 0, __ATOMIC_RELAXED);
        size_t maximum = __atomic_load_n(&measured_max, __ATOMIC_RELAXED);
        printf(
            "ordered_scan multifile=%d crypto=%d cold=%d records=1025 limit=7 "
            "repeats=128 cpu_ms=%.6f maximum_allocation=%zu verified=1\n",
            multifile, crypto, cold,
            (end.tv_sec - begin.tv_sec) * 1000.0 +
                (end.tv_nsec - begin.tv_nsec) / 1e6,
            maximum);
        fflush(stdout);
        REQUIRE(benchmark || maximum < 8192);
      }
      ordered_view view = {0};
      view.pouch = p;
      ok(lc_pouch_state_with_namespace_lock(p, "ordered", ordered_consume,
                                            &view, &e),
         &e);
      REQUIRE(view.deleted == 234);
      lc_pouch_close(p);
      REQUIRE(!live);
      if (!multifile)
        REQUIRE(!unlink(path));
    }
  lc_error_cleanup(&e);
}
#endif
