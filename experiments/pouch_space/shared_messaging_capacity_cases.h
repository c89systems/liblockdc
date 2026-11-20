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
                         int last, int manual, long *fence,
                         lc_pouch_state_read_result *held, lc_error *e) {
  lc_update_opts options = {0};
  lc_lease *lease = acquire_with_wait(writer, "churn", NULL, 2, e);
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
    if (manual) {
      REQUIRE(clean(peer, 1, e).retention_deleted_state_count == 1);
      parent(writer, "churn", 0, 0, e);
    }
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

static ps_budget *pressure_rival(fixture *writer, fixture *worker,
                                 lc_error *e) {
  ps_budget *rival = lease_capacity_peer(fixture_space(writer), e);
  ps_budget *extra = NULL;
  ps_budget_plan plan = {0};
  ok(ps_budget_status(rival, &plan, e), e);
  REQUIRE(plan.payload_bytes > 8 * PS_MIB);
  printf("completion pressure: competing ordinary bytes=%llu\n",
         (unsigned long long)plan.payload_bytes);
  plan.payload_bytes = PS_PAGE;
  REQUIRE(ps_budget_open(fixture_space(worker), &plan, &extra, e) ==
              LC_ERR_INVALID &&
          !extra);
  clear(e);
  return rival;
}

static void pressure_queue(fixture *writer, fixture *worker, lc_error *e) {
  lc_dequeue_req request = shared_queue_request();
  lc_message *message = NULL;
  lc_ack_res acknowledged = {0};
  lc_txn_decision_res decided = {0};
  lc_txn_decision_req decision = {0};
  lc_txn_replay_req replay = {0};
  lc_txn_replay_res applied = {0};
  char xid[LC_XID_STRING_SIZE], key[256];
  lc_txn_participant participant = {"orders", key, NULL};
  int recover_on_open = !worker->sync && worker->crypto == 3;
  ok(lc_xid_new(xid, e), e);
  request.txn_id = decision.txn_id = replay.txn_id = xid;
  shared_queue_enqueue(writer, NULL, e);
  ok(lc_dequeue(worker->client, &request, &message, e), e);
  REQUIRE(message && message->txn_id && !strcmp(message->txn_id, xid));
  REQUIRE(snprintf(key, sizeof(key), "q/jobs/msg/%s", message->message_id) <
          (int)sizeof(key));
  decision.participants = &participant;
  decision.participant_count = 1;
  ok(lc_txn_prepare(worker->client, &decision, &decided, e), e);
  REQUIRE(decided.state && !strcmp(decided.state, "prepare"));
  lc_txn_decision_res_cleanup(&decided);
  ps_budget *rival = pressure_rival(writer, worker, e);
  lc_ack_op ack = shared_queue_ack(message);
  strcpy(scenario, "capacity queue ack");
  ok(worker->client->queue_ack(worker->client, &ack, &acknowledged, e), e);
  REQUIRE(acknowledged.acked);
  lc_ack_res_cleanup(&acknowledged);
  shared_queue_stats(writer, NULL, 1, 0, e);
  strcpy(scenario, "capacity queue commit");
  if (!worker->sync) {
    uint64_t ready, durable;
    lc_pouch_test_after_txn_decision_hook = lose_decision_ack;
    REQUIRE(lc_txn_commit(worker->client, &decision, &decided, e) ==
            LC_ERR_TRANSPORT);
    lc_pouch_test_after_txn_decision_hook = NULL;
    clear(e);
    ok(ps_publication_prefix(fixture_space(worker), &ready, &durable, e), e);
    REQUIRE(ready == durable);
    if (!recover_on_open) {
      strcpy(scenario, "capacity queue replay under pressure");
      ok(lc_txn_replay(worker->client, &replay, &applied, e), e);
      REQUIRE(applied.state && !strcmp(applied.state, "commit"));
      lc_txn_replay_res_cleanup(&applied);
    }
  } else {
    ok(lc_txn_commit(worker->client, &decision, &decided, e), e);
    REQUIRE(decided.state && !strcmp(decided.state, "commit"));
  }
  lc_txn_decision_res_cleanup(&decided);
  shared_queue_stats(writer, NULL, recover_on_open, 0, e);
  shared_queue_read(message, e);
  message->close(message);
  ok(ps_budget_close(rival, e), e);
  lc_client_close(worker->client);
  worker->client = NULL;
  close_store(writer);
  open_store(writer, 0, e);
  strcpy(scenario, "capacity queue cold recovery");
  /* Startup applies and retires native-only terminal transaction records. */
  REQUIRE(lc_txn_replay(writer->client, &replay, &applied, e) ==
          LC_ERR_INVALID);
  REQUIRE(e->message &&
          !strcmp(e->message, "pouch transaction replay record not found"));
  clear(e);
  lc_txn_replay_res_cleanup(&applied);
  shared_queue_stats(writer, NULL, 0, 0, e);
}

static int cut_outbox_checkpoint_cleanup(void *context, lc_error *e) {
  int *cuts = context;
  ++*cuts;
  return lc_error_set(e, LC_ERR_TRANSPORT, 0,
                      "injected cut after durable outbox completion", NULL,
                      NULL, "test");
}

static void pressure_outbox(fixture *writer, fixture *worker, lc_error *e) {
  lc_outbox *publisher =
      shared_outbox_open(writer, "orders", "publisher", 300, e);
  lc_outbox *owner = shared_outbox_open(worker, "orders", "worker", 300, e);
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_outbox_receipt first = {0}, duplicate = {0};
  lc_outbox_job *job = NULL;
  producer checkpoint = source("{\"saved\":true}");
  lc_outbox_completion completion = {0};
  char *evidence = malloc(LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES + 1U);
  size_t reclaimed = 0;
  lc_i64 next_due = 0;
  int cuts = 0;
  int cut = !worker->sync && worker->crypto == 3;
  REQUIRE(evidence);
  memset(evidence, '\\', LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES);
  evidence[LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES] = '\0';
  completion.delivery_reference = evidence;
  shared_outbox_append(publisher, "capacity", &first, 0, e);
  ok(lc_outbox_dispatcher_get_or_start(owner, &dispatcher, e), e);
  ok(lc_outbox_dispatcher_notify_outbox_key(dispatcher, first.outbox_key, e),
     e);
  ok(lc_outbox_dispatcher_next_with_state(dispatcher, 5000, &job, e), e);
  REQUIRE(job && lc_outbox_job_state(job));
  shared_outbox_check_body(job, e);
  checkpoint.pub.impl = &checkpoint;
  ok(lc_lease_update(lc_outbox_job_state(job), &checkpoint.pub, NULL, e), e);
  REQUIRE(checkpoint.length == checkpoint.at);
  ps_budget *rival = pressure_rival(writer, worker, e);
  strcpy(scenario, "capacity outbox complete");
  if (cut) {
    lc_outbox_test_before_stateful_terminal_cleanup_hook =
        cut_outbox_checkpoint_cleanup;
    lc_outbox_test_before_stateful_terminal_cleanup_context = &cuts;
  }
  ok(lc_outbox_job_complete(job, &completion, e), e);
  lc_outbox_test_before_stateful_terminal_cleanup_hook = NULL;
  lc_outbox_test_before_stateful_terminal_cleanup_context = NULL;
  REQUIRE(cuts == cut);
  free(evidence);
  ok(lc_outbox_dispatcher_stop(dispatcher, -1, e), e);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(owner);
  lc_outbox_close(publisher);
  ok(ps_budget_close(rival, e), e);
  lc_client_close(worker->client);
  worker->client = NULL;
  close_store(writer);
  open_store(writer, 0, e);
  strcpy(scenario, "capacity outbox reopen and duplicate");
  publisher = shared_outbox_open(writer, "orders", "publisher", 300, e);
  shared_outbox_append(publisher, "capacity", &duplicate, 1, e);
  REQUIRE(!strcmp(duplicate.outbox_key, first.outbox_key));
  ok(lc_outbox_test_reclaim_terminal(publisher, LC_POUCH_RETENTION_COMPLETED,
                                     (lc_i64)time(NULL) + 86400L * 31L,
                                     &reclaimed, &next_due, e),
     e);
  REQUIRE(reclaimed == 1);
  lc_outbox_close(publisher);
  lc_outbox_receipt_cleanup(&first);
  lc_outbox_receipt_cleanup(&duplicate);
}

static void shared_messaging_pressure(const char *directory, int codec,
                                      int sync, int outbox) {
  fixture *owners = calloc(2, sizeof(*owners));
  lc_error e = {0};
  REQUIRE(owners);
  shared_lease_fixture = 1;
  now(1000);
  setup(&owners[0], directory, 0, sync, codec, &e);
  strcpy(owners[1].path, owners[0].path);
  configure(&owners[1], 0, sync, codec);
  open_store(&owners[1], 0, &e);
  if (outbox)
    pressure_outbox(&owners[0], &owners[1], &e);
  else
    pressure_queue(&owners[0], &owners[1], &e);
  close_store(&owners[0]);
  retire(&owners[0]);
  shared_lease_fixture = 0;
  free(owners);
  clear(&e);
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
  fixture *idle = calloc(1, sizeof(*idle));
  lc_outbox *outbox[2];
  lc_error e = {0};
  long fence = 0;
  uint64_t peak = 0;
  REQUIRE(owners && idle);
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
                 1, &fence, cycle == 1 ? &held : NULL, &e);
    strcpy(scenario + phase, " queue");
    churn_queue(&owners[writer], &owners[peer], cycle, &e);
    strcpy(scenario + phase, " outbox");
    churn_outbox(outbox[writer], outbox[peer], cycle, &e);
    if (cycle == 1) {
      strcpy(scenario + phase, " open idle peer");
      strcpy(idle->path, owners[0].path);
      configure(idle, 0, sync, codec);
      open_store(idle, 0, &e);
    }
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
    strcpy(scenario + phase, " idle physical retention");
    for (size_t i = 0; i < 2; ++i) {
      ok(ps_status(fixture_space(&owners[i]), &stats, &e), &e);
      REQUIRE(!stats.retained_payload_bytes);
    }
    ok(ps_status(fixture_space(idle), &stats, &e), &e);
    REQUIRE(!stats.retained_payload_bytes);
  }
  churn_read_state(idle, "churn", CHURN_CYCLES, &e);
  lc_client_close(idle->client);
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
  free(idle);
  free(owners);
  clear(&e);
}

static int churn_expired(fixture *owner, lc_error *e) {
  lc_pouch_state_read_result result = {0};
  lc_pouch *pouch = store(owner);
  int rc = lc_pouch_state_read_metadata(pouch, "orders", "churn", &result, e);
  int expired = rc == LC_OK && !result.has_body && !result.retention;
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &result);
  if (rc == LC_ERR_PENDING)
    clear(e); /* The background peer can own the namespace during capture. */
  else
    ok(rc, e);
  return expired;
}
static void shared_autonomous_capacity(const char *directory, int codec,
                                       int sync, int unpaced) {
  enum { CYCLES = 16, BURST = 4 };
  fixture *owners = calloc(2, sizeof(*owners));
  lc_pouch *worker = NULL;
  lc_outbox *outbox[2];
  ps_pouch_state_options options = {0};
  ps_space *space = NULL;
  ps_stats stats;
  lc_error e = {0};
  uint64_t checkpoints = 0, checkpoints_during_work = 0, peak = 0;
  double longest_wait = 0, work_seconds = 0;
  long fence = 0;
  REQUIRE(owners);
  shared_lease_fixture = 1;
  setup(&owners[0], directory, 0, sync, codec, &e);
  strcpy(owners[1].path, owners[0].path);
  configure(&owners[1], 0, sync, codec);
  open_store(&owners[1], 0, &e);
  options.maximum_capacity = PS_DEFAULT_CAPACITY;
  options.logical.single_writer_set = 1;
  options.logical.query_indexing_enabled_set = 1;
  options.logical.background_compaction_enabled_set = 1;
  options.logical.background_compaction_enabled = 1;
  options.logical.durable_sync = sync;
  options.logical.crypto_key = codec & 1 ? crypto_key : NULL;
  options.logical.compression = codec & 2 ? "zlib" : "none";
  ok(ps_pouch_state_open(owners[0].path, 0, &allocator, &options, &worker, &e),
     &e);
  REQUIRE(worker->compaction_interval_seconds ==
          LC_POUCH_DEFAULT_COMPACTION_INTERVAL_SECONDS);
  space = ps_root_space(ps_pouch_state_root(worker));
  ok(ps_status(space, &stats, &e), &e);
  REQUIRE(!stats.checkpoints);
  for (size_t i = 0; i < 2; ++i)
    outbox[i] = shared_outbox_open(&owners[i], "effects", "capacity", 300, &e);

  /* Keep a live encoded body in the same source as expiring bodies. */
  now(1000);
  lc_lease *stable = acquire(&owners[0], "stable", NULL, &e);
  churn_stream body;
  churn_stream_init(&body, 1000);
  ok(lc_lease_update(stable, &body.source, NULL, &e), &e);
  REQUIRE(body.offset == CHURN_BYTES);
  ok(lc_lease_release(stable, NULL, &e), &e);

  for (uint64_t cycle = 1; cycle <= CYCLES; ++cycle) {
    size_t writer = cycle % 2, peer = 1 - writer;
    double cycle_start = real_clock(CLOCK_MONOTONIC);
    snprintf(scenario, sizeof(scenario), "codec=%d sync=%d cycle=%llu", codec,
             sync, (unsigned long long)cycle);
    size_t phase = strlen(scenario);
    now(1000U + cycle * 10U);
    strcpy(scenario + phase, " lease");
    churn_expiry(&owners[writer], &owners[peer], cycle, cycle == CYCLES, 0,
                 &fence, NULL, &e);
    strcpy(scenario + phase, " queue");
    churn_queue(&owners[writer], &owners[peer], cycle, &e);
    strcpy(scenario + phase, " outbox");
    churn_outbox(outbox[writer], outbox[peer], cycle, &e);
    work_seconds += real_clock(CLOCK_MONOTONIC) - cycle_start;
    if (cycle == CYCLES) {
      ok(ps_status(space, &stats, &e), &e);
      checkpoints_during_work = stats.checkpoints;
      if (unpaced)
        REQUIRE(checkpoints_during_work > 0);
    }
    if (unpaced ? cycle != CYCLES : cycle % BURST != 0)
      continue;
    strcpy(scenario + phase, " worker");
    double start = real_clock(CLOCK_MONOTONIC);
    for (;;) {
      ok(ps_status(space, &stats, &e), &e);
      REQUIRE(stats.capacity == PS_DEFAULT_CAPACITY);
      if (stats.checkpoints > checkpoints &&
          stats.allocated_bytes <= 3 * PS_MIB &&
          (cycle == CYCLES || churn_expired(&owners[peer], &e)))
        break;
      if (real_clock(CLOCK_MONOTONIC) - start >= 24) {
        fprintf(stderr, "worker: allocated=%llu checkpoints=%llu pressure=%d\n",
                (unsigned long long)stats.allocated_bytes,
                (unsigned long long)stats.checkpoints,
                ps_maintenance_pressure_requested(space));
        REQUIRE(0);
      }
      const struct timespec pause = {0, 100000000};
      (void)nanosleep(&pause, NULL);
    }
    double waited = real_clock(CLOCK_MONOTONIC) - start;
    if (waited > longest_wait)
      longest_wait = waited;
    checkpoints = stats.checkpoints;
    if (getenv("POUCH_SPACE_CHURN_TRACE"))
      fprintf(stderr, "worker cycle=%llu allocated=%llu checkpoints=%llu\n",
              (unsigned long long)cycle,
              (unsigned long long)stats.allocated_bytes,
              (unsigned long long)checkpoints);
    if (stats.allocated_bytes > peak)
      peak = stats.allocated_bytes;
    churn_read_state(&owners[peer], "stable", 1000, &e);
  }
  lc_pouch_close(worker);
  lc_outbox_close(outbox[1]);
  lc_outbox_close(outbox[0]);
  lc_client_close(owners[1].client);
  close_store(&owners[0]);
  open_store(&owners[0], 0, &e);
  churn_read_state(&owners[0], "stable", 1000, &e);
  churn_read_state(&owners[0], "churn", CYCLES, &e);
  shared_queue_stats(&owners[0], "queues", 0, 0, &e);
  close_store(&owners[0]);
  printf(
      "shared autonomous capacity codec=%d sync=%d unpaced=%d cycles=%d "
      "bytes=%llu peak_after_worker=%llu checkpoints=%llu "
      "checkpoints_during_work=%llu work_ms=%.3f max_wait_ms=%.3f verified=1\n",
      codec, sync, unpaced, CYCLES,
      (unsigned long long)CYCLES * CHURN_BYTES * 3, (unsigned long long)peak,
      (unsigned long long)checkpoints,
      (unsigned long long)checkpoints_during_work, work_seconds * 1000,
      longest_wait * 1000);
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
