#ifndef POUCH_SPACE_INDEX_COMPARE_H
#define POUCH_SPACE_INDEX_COMPARE_H
static int index_one(worker *w, size_t row, producer *source, consumer *sink,
                     lc_error *e) {
  lc_acquire_req request;
  lc_release_req release;
  lc_update_opts options = {0};
  lc_lease *lease = NULL;
  lc_get_res result = {0};
  lc_client *producer =
      w->producer_client ? w->producer_client : w->run->client;
  char key[64];
  snprintf(key, sizeof(key), "row-%lu", (unsigned long)row);
  lc_acquire_req_init(&request);
  lc_release_req_init(&release);
  request.ns = w->ns;
  request.key = key;
  request.owner = "index-benchmark";
  request.ttl_seconds = 3600L;
  options.content_type = "application/json";
  int rc = lc_acquire(producer, &request, &lease, e);
  if (rc == LC_OK)
    rc = lc_lease_update(lease, &source->source, &options, e);
  if (rc == LC_OK) {
    rc = lc_lease_release(lease, &release, e);
    if (rc == LC_OK)
      lease = NULL;
  }
  lc_lease_close(lease);
  if (rc == LC_OK)
    rc = lc_get_in_namespace(w->run->client, w->ns, key, NULL, &sink->sink,
                             &result, e);
  lc_get_res_cleanup(&result);
  return rc;
}
typedef struct indexed_keys {
  worker *worker;
  char key[64];
  size_t length;
  uint64_t seen;
} indexed_keys;
static int indexed_begin(void *context, lc_error *e) {
  indexed_keys *keys = context;
  (void)e;
  keys->length = 0U;
  return 1;
}
static int indexed_chunk(void *context, const char *bytes, size_t length,
                         lc_error *e) {
  indexed_keys *keys = context;
  (void)e;
  require(length < sizeof(keys->key) - keys->length, "bounded query key");
  memcpy(keys->key + keys->length, bytes, length);
  keys->length += length;
  keys->key[keys->length] = '\0';
  return 1;
}
static int indexed_end(void *context, lc_error *e) {
  indexed_keys *keys = context;
  char *end;
  (void)e;
  require(!strncmp(keys->key, "row-", 4U), "indexed key identity");
  unsigned long row = strtoul(keys->key + 4U, &end, 10);
  require(!*end && row < keys->worker->run->rows &&
              row % keys->worker->run->writers == keys->worker->ordinal &&
              !(keys->seen & (UINT64_C(1) << row)),
          "unique namespace query row");
  keys->seen |= UINT64_C(1) << row;
  return 1;
}
static void index_phase_begin(index_phase *phase) {
  require(!pthread_getspecific(sync_phase_key), "index phases do not overlap");
  require(!pthread_setspecific(sync_phase_key, phase), "start index phase");
  phase->started_cpu = now(CLOCK_THREAD_CPUTIME_ID);
  phase->started_seconds = now(CLOCK_MONOTONIC);
}
static void index_phase_end(index_phase *phase) {
  phase->seconds += now(CLOCK_MONOTONIC) - phase->started_seconds;
  phase->cpu_seconds += now(CLOCK_THREAD_CPUTIME_ID) - phase->started_cpu;
  require(!pthread_setspecific(sync_phase_key, NULL), "finish index phase");
}
static int index_queries(worker *w, int flush, lc_error *e) {
  static const char *const selectors[] = {
      "{\"exists\":\"/v\"}",
      "{\"prefix\":{\"field\":\"/v\",\"value\":\"ghij\"}}",
      "{\"icontains\":{\"field\":\"/...\",\"value\":\"UVWXYZ\"}}"};
  char current[48], previous[48];
  lc_index_flush_req request;
  lc_index_flush_res result = {0};
  uint64_t expected = 0U;
  if (w->revision) {
    snprintf(current, sizeof(current), "eq{field=/r,value=%c}", w->revision);
    snprintf(previous, sizeof(previous), "eq{field=/r,value=%c}",
             w->revision == 'a' ? 'z' : w->revision - 1);
  }
  lc_index_flush_req_init(&request);
  request.ns = w->ns;
  request.mode = "sync";
  int rc = LC_OK;
  if (flush) {
    index_phase_begin(&w->index_phases[0]);
    /* Compare the same canonical recovery boundary when requested. Native
     * revision publication already stabilizes the completed prefix even on
     * NoSync roots. Multifile flush publishes derived files without forcing
     * canonical records; its explicit namespace handoff supplies that boundary.
     * Durable mutations have already supplied it in either owner. */
    if (w->run->stable_index && !w->run->container && !w->run->sync)
      rc = lc_pouch_state_sync_namespace(
          ((lc_client_handle *)w->run->client)->pouch, w->ns, e);
    if (rc == LC_OK)
      rc = lc_flush_index(w->run->client, &request, &result, e);
    if (rc == LC_OK)
      require(result.flushed && !result.pending, "completed index flush");
    if (rc == LC_OK) {
      require(result.index_seq > w->index_seq,
              "each update round advances the published index");
      w->index_seq = result.index_seq;
    }
    lc_index_flush_res_cleanup(&result);
    index_phase_end(&w->index_phases[0]);
  }
  for (size_t row = w->ordinal; row < w->run->rows; row += w->run->writers)
    expected |= UINT64_C(1) << row;
  for (int pass = 0; rc == LC_OK && pass < 2; ++pass) {
    index_phase *phase =
        flush ? &w->index_phases[1 + pass] : &w->reopen_phases[pass];
    index_phase_begin(phase);
    for (size_t i = 0U;
         rc == LC_OK && i < sizeof(selectors) / sizeof(selectors[0]); ++i) {
      lc_query_req query;
      lc_query_res queried = {0};
      lc_query_key_handler handler = {indexed_begin, indexed_chunk,
                                      indexed_end};
      indexed_keys keys = {0};
      keys.worker = w;
      lc_query_req_init(&query);
      query.ns = w->ns;
      query.engine = "index";
      if (w->revision && i)
        query.selector_lql = i == 1U ? current : previous;
      else
        query.selector_json = selectors[i];
      rc = lc_query_keys(w->run->client, &query, &handler, &keys, &queried, e);
      if (rc == LC_OK)
        require(keys.seen == (w->revision && i == 2U ? 0U : expected) &&
                    queried.metadata_json &&
                    strstr(queried.metadata_json, "\"engine\":\"index\""),
                "indexed query matches the current revision");
      if (rc == LC_OK)
        ++phase->queries;
      lc_query_res_cleanup(&queried);
    }
    index_phase_end(phase);
  }
  return rc;
}
static void index_verify_reopen(worker *w, lc_error *e) {
  comparison *run = w->run;
  lc_pouch_generation state = 0U, indexed = 0U;
  lc_pouch *pouch = ((lc_client_handle *)run->client)->pouch;
  check(lc_pouch_state_query_index_seq(pouch, w->ns, &state, e), e);
  check(lc_pouch_query_index_manifest_seq(pouch, w->ns, &indexed, e), e);
  require(state == indexed && state > 0U,
          "published index survives cold reopen");
  if (w->revision)
    require(indexed == w->index_seq, "cold reopen retains last explicit flush");
  check(index_queries(w, 0, e), e);
  for (size_t row = w->ordinal; row < run->rows; row += run->writers) {
    lc_get_res result = {0};
    consumer sink = {0};
    char key[64];
    snprintf(key, sizeof(key), "row-%lu", (unsigned long)row);
    sink.sink.impl = &sink;
    sink.sink.write = consume;
    sink.size = run->bytes;
    sink.revision = w->revision;
    check(lc_get_in_namespace(run->client, w->ns, key, NULL, &sink.sink,
                              &result, e),
          e);
    require(sink.offset == run->bytes, "indexed value survives reopen");
    lc_get_res_cleanup(&result);
  }
}
#endif
