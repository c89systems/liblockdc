/* Exercise real streaming on both sides with no body-sized test buffer. */
enum { CHURN_BYTES = 384 * 1024, CHURN_CYCLES = 28 };

typedef struct churn_stream {
  lc_source source;
  lc_sink sink;
  uint64_t state;
  size_t offset;
} churn_stream;

static unsigned char churn_byte(churn_stream *stream) {
  uint64_t x = stream->state;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  stream->state = x;
  return (unsigned char)x;
}

static size_t churn_produce(lc_source *source, void *bytes, size_t count,
                            lc_error *e) {
  churn_stream *stream = source->impl;
  unsigned char *buffer = bytes;
  (void)e;
  if (count > CHURN_BYTES - stream->offset)
    count = CHURN_BYTES - stream->offset;
  for (size_t i = 0; i < count; ++i)
    buffer[i] = churn_byte(stream);
  stream->offset += count;
  return count;
}

static int churn_consume(lc_sink *sink, const void *bytes, size_t count,
                         lc_error *e) {
  churn_stream *stream = sink->impl;
  const unsigned char *buffer = bytes;
  (void)e;
  REQUIRE(count <= CHURN_BYTES - stream->offset);
  for (size_t i = 0; i < count; ++i)
    if (buffer[i] != churn_byte(stream))
      REQUIRE(0);
  stream->offset += count;
  return 1;
}

static void churn_stream_init(churn_stream *stream, uint64_t seed) {
  memset(stream, 0, sizeof(*stream));
  stream->state = seed;
  stream->source.impl = stream;
  stream->source.read = churn_produce;
  stream->sink.impl = stream;
  stream->sink.write = churn_consume;
}

static void churn_read_state(fixture *f, const char *key, uint64_t seed,
                             lc_error *e) {
  lc_get_res result = {0};
  churn_stream reader;
  churn_stream_init(&reader, seed);
  ok(lc_get(f->client, key, NULL, &reader.sink, &result, e), e);
  REQUIRE(!result.no_content && reader.offset == CHURN_BYTES);
  lc_get_res_cleanup(&result);
}

static void churn_expiry(fixture *writer, fixture *peer, uint64_t seed,
                         int last, long *fence,
                         lc_pouch_state_read_result *held, lc_error *e) {
  lc_update_opts options = {0};
  lc_lease *lease = acquire(writer, "churn", NULL, e);
  churn_stream body;
  REQUIRE(*fence < lease->fencing_token);
  *fence = lease->fencing_token;
  options.content_type = "application/octet-stream";
  options.has_retention_seconds = 1;
  options.retention_seconds = 2;
  churn_stream_init(&body, seed);
  ok(lc_lease_update(lease, &body.source, &options, e), e);
  REQUIRE(body.offset == CHURN_BYTES);
  ok(lc_lease_release(lease, NULL, e), e);
  churn_read_state(peer, "churn", seed, e);
  if (held) {
    ok(lc_pouch_state_read(store(writer), "orders", "churn", held, e), e);
    REQUIRE(held->body);
  }
  if (!last) {
    now(1000U + seed * 10U + 2U);
    REQUIRE(clean(peer, 1, e).retention_deleted_state_count == 1);
    parent(writer, "churn", 0, 0, e);
  }
}

static void churn_queue(fixture *writer, fixture *peer, uint64_t seed,
                        lc_error *e) {
  lc_enqueue_req request = {0};
  lc_enqueue_res result = {0};
  lc_dequeue_req dequeue = shared_queue_request();
  lc_message *message = NULL;
  churn_stream body;
  size_t written = 0;
  request.ns = dequeue.ns = "queues";
  request.queue = dequeue.queue;
  request.content_type = "application/octet-stream";
  churn_stream_init(&body, seed);
  ok(lc_enqueue(writer->client, &request, &body.source, &result, e), e);
  REQUIRE(body.offset == CHURN_BYTES && result.payload_bytes == CHURN_BYTES);
  ok(lc_dequeue(peer->client, &dequeue, &message, e), e);
  REQUIRE(message && !strcmp(message->message_id, result.message_id));
  churn_stream_init(&body, seed);
  ok(lc_message_write_payload(message, &body.sink, &written, e), e);
  REQUIRE(body.offset == CHURN_BYTES && written == CHURN_BYTES);
  ok(lc_message_ack(message, e), e);
  shared_queue_stats(writer, request.ns, 0, 0, e);
  lc_enqueue_res_cleanup(&result);
}

static void churn_outbox(lc_outbox *writer, lc_outbox *peer, uint64_t seed,
                         lc_error *e) {
  lc_outbox_entry entry;
  lc_outbox_transaction *transaction = NULL;
  lc_outbox_commit_result committed = {0};
  lc_outbox_receipt receipt = {0};
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_outbox_job *job = NULL;
  lc_i64 next_due = 0;
  size_t total = 0;
  char id[48];
  churn_stream body;
  REQUIRE(snprintf(id, sizeof(id), "capacity-%llu", (unsigned long long)seed) <
          (int)sizeof(id));
  lc_outbox_entry_init(&entry);
  entry.operation_id = entry.effect_id = entry.effect_key = id;
  entry.kind = "capacity";
  entry.destination = "local";
  entry.content_type = "application/octet-stream";
  entry.payload_digest = id;
  churn_stream_init(&body, seed);
  ok(lc_outbox_append(writer, &entry, &body.source, &transaction, &receipt, e),
     e);
  REQUIRE(transaction && !receipt.duplicate && body.offset == CHURN_BYTES);
  ok(lc_outbox_transaction_commit(transaction, &committed, e), e);
  REQUIRE(committed.outbox_receipt_count == 1);
  lc_outbox_transaction_close(transaction);
  transaction = NULL;
  ok(lc_outbox_dispatcher_get_or_start(peer, &dispatcher, e), e);
  ok(lc_outbox_dispatcher_notify_outbox_key(
         dispatcher, committed.outbox_receipts[0].outbox_key, e),
     e);
  ok(lc_outbox_dispatcher_next_with_state(dispatcher, 5000, &job, e), e);
  REQUIRE(job && !strcmp(job->operation_id, id) && lc_outbox_job_state(job));
  churn_stream_init(&body, seed);
  ok(lc_outbox_job_write_payload(job, &body.sink, &total, e), e);
  REQUIRE(body.offset == CHURN_BYTES && total == CHURN_BYTES);
  producer checkpoint = source("{\"processed\":true}");
  checkpoint.pub.impl = &checkpoint;
  ok(lc_lease_update(lc_outbox_job_state(job), &checkpoint.pub, NULL, e), e);
  REQUIRE(checkpoint.at == checkpoint.length);
  ok(lc_outbox_job_complete(job, NULL, e), e);
  ok(lc_outbox_dispatcher_stop(dispatcher, -1, e), e);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_receipt_cleanup(&receipt);
  churn_stream_init(&body, seed);
  ok(lc_outbox_append(writer, &entry, &body.source, &transaction, &receipt, e),
     e);
  REQUIRE(!transaction && receipt.duplicate && !body.offset &&
          !strcmp(receipt.outbox_key, committed.outbox_receipts[0].outbox_key));
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_commit_result_cleanup(&committed);
  /* Advance only the reclaimer's selection clock. The normal dispatcher is
   * stopped and cannot race completion, duplicate lookup or reclamation. */
  ok(lc_outbox_test_reclaim_terminal(writer, LC_POUCH_RETENTION_COMPLETED,
                                     (lc_i64)time(NULL) + 86400L * 31L, &total,
                                     &next_due, e),
     e);
  REQUIRE(total == 1);
}

static ps_space *fixture_space(fixture *f) {
  return ps_root_space(ps_pouch_state_root(store(f)));
}

static void churn_reclaim(fixture *f, lc_error *e) {
  const char *const names[] = {"orders", "queues", "effects", ".txns"};
  for (size_t n = 0; n < sizeof(names) / sizeof(*names); ++n) {
    lc_pouch_maintenance_options request = {0};
    lc_pouch_maintenance_result result = {0};
    request.ns = names[n];
    request.force = 1;
    ok(lc_pouch_maintenance_run(store(f), &request, &result, e), e);
    lc_pouch_maintenance_result_cleanup(&store(f)->allocator, &result);
  }
  ok(ps_checkpoint(fixture_space(f), e), e);
}

static void shared_messaging_capacity(const char *directory, int codec,
                                      int sync) {
  fixture *owners = calloc(2, sizeof(*owners));
  lc_outbox *outbox[2];
  lc_error e = {0};
  long fence = 0;
  uint64_t peak = 0;
  REQUIRE(owners);
  shared_lease_fixture = 1;
  setup(&owners[0], directory, 0, sync, codec, &e);
  strcpy(owners[1].path, owners[0].path);
  configure(&owners[1], 0, sync, codec);
  open_store(&owners[1], 0, &e);
  for (size_t i = 0; i < 2; ++i)
    outbox[i] = shared_outbox_open(&owners[i], "effects", "capacity", 300, &e);
  for (uint64_t cycle = 1; cycle <= CHURN_CYCLES; ++cycle) {
    size_t writer = cycle % 2, peer = 1 - writer;
    ps_stats stats;
    lc_pouch_state_read_result held = {0};
    REQUIRE(snprintf(scenario, sizeof(scenario), "codec=%d sync=%d cycle=%llu",
                     codec, sync, (unsigned long long)cycle) > 0);
    now(1000U + cycle * 10U);
    size_t phase = strlen(scenario);
    strcpy(scenario + phase, " lease");
    churn_expiry(&owners[writer], &owners[peer], cycle, cycle == CHURN_CYCLES,
                 &fence, cycle == 1 ? &held : NULL, &e);
    strcpy(scenario + phase, " queue");
    churn_queue(&owners[writer], &owners[peer], cycle, &e);
    strcpy(scenario + phase, " outbox");
    churn_outbox(outbox[writer], outbox[peer], cycle, &e);
    strcpy(scenario + phase, " reclaim");
    churn_reclaim(&owners[peer], &e);
    if (held.body) {
      churn_stream reader;
      size_t copied = 0;
      churn_stream_init(&reader, cycle);
      ok(lc_copy(held.body, &reader.sink, &copied, &e), &e);
      REQUIRE(copied == CHURN_BYTES && reader.offset == CHURN_BYTES);
      lc_pouch_state_read_result_cleanup(&store(&owners[writer])->allocator,
                                         &held);
    }
    ok(ps_status(fixture_space(&owners[peer]), &stats, &e), &e);
    REQUIRE(stats.capacity == PS_DEFAULT_CAPACITY);
    if (stats.allocated_bytes > peak)
      peak = stats.allocated_bytes;
    if (getenv("POUCH_SPACE_CHURN_TRACE")) {
      for (size_t i = 0; i < 2; ++i) {
        ok(ps_status(fixture_space(&owners[i]), &stats, &e), &e);
        fprintf(stderr,
                "cycle=%llu owner=%zu allocated=%llu live=%llu "
                "retired=%llu retained=%llu reserved=%llu entries=%llu\n",
                (unsigned long long)cycle, i,
                (unsigned long long)stats.allocated_bytes,
                (unsigned long long)stats.live_bytes,
                (unsigned long long)stats.retired_bytes,
                (unsigned long long)stats.retained_payload_bytes,
                (unsigned long long)stats.reserved_payload_bytes,
                (unsigned long long)stats.entries);
      }
    }
  }
  lc_outbox_close(outbox[1]);
  lc_outbox_close(outbox[0]);
  lc_client_close(owners[1].client);
  close_store(&owners[0]);
  open_store(&owners[0], 0, &e);
  churn_read_state(&owners[0], "churn", CHURN_CYCLES, &e);
  shared_queue_stats(&owners[0], "queues", 0, 0, &e);
  ok(ps_check(fixture_space(&owners[0]), &e), &e);
  close_store(&owners[0]);
  printf("shared messaging capacity codec=%d sync=%d cycles=%d bytes=%llu "
         "peak_after_maintenance=%llu verified=1\n",
         codec, sync, CHURN_CYCLES,
         (unsigned long long)CHURN_CYCLES * CHURN_BYTES * 3,
         (unsigned long long)peak);
  shared_lease_fixture = 0;
  free(owners);
  clear(&e);
}

static void shared_lease_pressure(const char *directory, int codec, int sync) {
  shared_lease_fixture = 1;
  lease_capacity_case(directory, sync, codec, 1, 16, 0, 1);
  lease_capacity_case(directory, sync, codec, 1, 16, LC_POUCH_RETENTION_STAGED,
                      1);
  shared_lease_fixture = 0;
}
