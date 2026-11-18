#ifndef POUCH_SPACE_INDEX_OPEN_CASES_H
#define POUCH_SPACE_INDEX_OPEN_CASES_H
#include "index_cases.h"

static size_t index_namespace_mutex_faults(const char *directory) {
  char path[1400];
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-namespace-mutex", directory) <
          (int)sizeof(path));
  size_t cuts = 0;
  for (int sync = 0; sync < 2; ++sync) {
    size_t sites = 0;
    for (size_t cut = 0; cut <= sites; ++cut)
      for (int no_error = 0; no_error < (cut ? 2 : 1); ++no_error) {
        lc_client *client = index_client(path, sync, 1, 1, NULL, &e);
        lc_pouch_index_artifact *artifact = index_artifact(
            client, "query/admission", LC_POUCH_INDEX_PACKED, 1, &e);
        mutex_init_calls = 0;
        mutex_init_fail = cut;
        mutex_init_armed = 1;
        int rc = lc_pouch_index_artifact_write(artifact, "one", 3, 1, 0,
                                               no_error ? NULL : &e);
        mutex_init_armed = 0;
        if (!cut) {
          ok(rc, &e);
          sites = mutex_init_calls;
          REQUIRE(sites);
        } else {
          REQUIRE(mutex_init_calls >= cut && rc == LC_ERR_TRANSPORT);
          clear(&e);
          ++cuts;
        }
        ok(lc_pouch_index_artifact_write(artifact, "retry", 5, 0, 0, &e), &e);
        index_artifact_expect(artifact, "retry", 5, &e);
        lc_free_with_allocator(&allocator, artifact);
        lc_client_close(client);
        REQUIRE(!live && !unlink(path));
      }
  }
  lc_error_cleanup(&e);
  return cuts;
}
static void index_open_faults(const char *directory) {
  char path[1400];
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-open", directory) <
          (int)sizeof(path));
  size_t cuts = 0U;
  for (int sync = 0; sync < 2; ++sync) {
    lc_client *client = index_client(path, sync, 1, 1, NULL, &e);
    lc_client_close(client);
    REQUIRE(!live);
    ps_pouch_state_options options = {0};
    options.logical.single_writer = options.logical.single_writer_set = 1;
    options.logical.background_compaction_enabled_set = 1;
    options.logical.query_indexing_enabled =
        options.logical.query_indexing_enabled_set = 1;
    options.logical.query_engine = "index";
    options.logical.indexer_flush_docs = UINT64_MAX;
    options.logical.indexer_flush_interval_seconds = 3600U;
    options.logical.durable_sync = sync;
    options.logical.compression = index_codec() & 2 ? "zlib" : "none";
    options.logical.crypto_key = index_codec() & 1 ? index_key : NULL;
    lc_pouch *pouch = NULL;
    allocation = 0U;
    fail_at = 0U;
    mutex_init_calls = mutex_init_fail = 0U;
    mutex_init_armed = armed = 1;
    int rc = ps_pouch_state_open(path, 0, &allocator, &options, &pouch, &e);
    mutex_init_armed = armed = 0;
    size_t sites = allocation, mutexes = mutex_init_calls;
    ok(rc, &e);
    REQUIRE(pouch->indexer_thread_started && sites && mutexes);
    lc_pouch_close(pouch);
    REQUIRE(!live);
    for (size_t cut = 1U; cut <= sites; ++cut)
      for (int no_error = 0; no_error < 2; ++no_error) {
        pouch = NULL;
        allocation = 0U;
        fail_at = cut;
        armed = 1;
        rc = ps_pouch_state_open(path, 0, &allocator, &options, &pouch,
                                 no_error ? NULL : &e);
        armed = 0;
        REQUIRE(allocation >= cut && (rc == LC_OK || rc == LC_ERR_NOMEM));
        REQUIRE((rc == LC_OK) == (pouch != NULL));
        lc_pouch_close(pouch);
        REQUIRE(!live);
        clear(&e);
        unlocked(path);
        ++cuts;
      }
    /* The index worker starts after the already-qualified runtime/owner
     * mutexes. Fail its final initialization with both error-output modes. */
    for (int no_error = 0; no_error < 2; ++no_error) {
      pouch = NULL;
      mutex_init_calls = 0U;
      mutex_init_fail = mutexes;
      mutex_init_armed = 1;
      rc = ps_pouch_state_open(path, 0, &allocator, &options, &pouch,
                               no_error ? NULL : &e);
      mutex_init_armed = 0;
      REQUIRE(rc == LC_ERR_TRANSPORT && !pouch && !live);
      REQUIRE(no_error ||
              (e.message &&
               !strcmp(e.message, "failed to initialize pouch indexer mutex")));
      clear(&e);
      unlocked(path);
      ++cuts;
    }
    ok(ps_pouch_state_open(path, 0, &allocator, &options, &pouch, &e), &e);
    REQUIRE(pouch->indexer_thread_started);
    lc_pouch_close(pouch);
    REQUIRE(!live);
    REQUIRE(!unlink(path));
  }
  cuts += index_namespace_mutex_faults(directory);
  printf("index startup and namespace allocation/mutex cuts=%zu passed\n",
         cuts);
  lc_error_cleanup(&e);
}
#endif
