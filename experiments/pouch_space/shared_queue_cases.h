/* Public queue operations through independently opened container handles. */
static void shared_queue_stats(fixture *f, const char *ns, int pending,
                               int available, lc_error *e) {
  lc_queue_stats_req request = {0};
  lc_queue_stats_res result = {0};
  request.ns = ns;
  request.queue = "jobs";
  ok(f->client->queue_stats(f->client, &request, &result, e), e);
  REQUIRE(result.pending_candidates == pending &&
          result.available == available);
  lc_queue_stats_res_cleanup(&result);
}
static void shared_queue_enqueue(fixture *f, const char *ns, lc_error *e) {
  lc_enqueue_req request = {0};
  lc_enqueue_res result = {0};
  producer body = source("message streamed through the shared container");
  request.ns = ns;
  request.queue = "jobs";
  body.pub.impl = &body;
  ok(f->client->enqueue(f->client, &request, &body.pub, &result, e), e);
  REQUIRE(body.at == body.length && body.calls >= 3 &&
          result.payload_bytes == (long)body.length);
  lc_enqueue_res_cleanup(&result);
}
static void shared_queue_read(lc_message *message, lc_error *e) {
  lc_source *body = message->payload_reader(message);
  const char *expected = "message streamed through the shared container";
  char bytes[7];
  size_t offset = 0, got;
  REQUIRE(body);
  while ((got = body->read(body, bytes, sizeof(bytes), e)) != 0U) {
    REQUIRE(got <= strlen(expected) - offset &&
            !memcmp(bytes, expected + offset, got));
    offset += got;
  }
  ok(e->code, e);
  REQUIRE(offset == strlen(expected));
}
static lc_dequeue_req shared_queue_request(void) {
  lc_dequeue_req request = {0};
  request.queue = "jobs";
  request.owner = "worker";
  request.visibility_timeout_seconds = 300;
  return request;
}
static void shared_queue_peer(fixture *a, fixture *b, const char *directory,
                              int sync, lc_error *e) {
  now(1000);
  setup(a, directory, 0, sync, sync ? 3 : 0, e);
  strcpy(b->path, a->path);
  configure(b, 0, a->sync, a->crypto);
  open_store(b, 0, e);
  shared_queue_stats(b, NULL, 0, 0, e);
}
static int shared_queue_cut(void *context, lc_error *e) {
  fixture *peer = context;
  lc_queue_stats_req request = {0};
  lc_queue_stats_res result = {0};
  request.queue = "jobs";
  REQUIRE(peer->client->queue_stats(peer->client, &request, &result, e) ==
          LC_ERR_PENDING);
  lc_queue_stats_res_cleanup(&result);
  clear(e);
  shared_queue_enqueue(peer, "independent", e);
  return lc_error_set(e, LC_ERR_TRANSPORT, 0L, "delivery cut after lease claim",
                      NULL, NULL, "test");
}
typedef struct queue_application_hold {
  fixture *peer;
  rendezvous ready;
  pthread_t thread;
} queue_application_hold;
static int hold_queue_application(void *context, lc_error *e) {
  queue_application_hold *hold = context;
  (void)e;
  gate_signal(&hold->ready, 0);
  gate_wait(&hold->ready, 1);
  return LC_OK;
}
static void *hold_queue_peer(void *context) {
  queue_application_hold *hold = context;
  lc_error e = {0};
  ok(lc_pouch_state_with_namespace_lock(store(hold->peer), "orders",
                                        hold_queue_application, hold, &e),
     &e);
  clear(&e);
  return NULL;
}
static int hold_queue_after_decision(void *context, lc_error *e) {
  queue_application_hold *hold = context;
  (void)e;
  REQUIRE(!pthread_create(&hold->thread, NULL, hold_queue_peer, hold));
  gate_wait(&hold->ready, 0);
  return LC_OK;
}
static void finish_queue_hold(queue_application_hold *hold) {
  gate_signal(&hold->ready, 1);
  REQUIRE(!pthread_join(hold->thread, NULL));
  gate_destroy(&hold->ready);
}
static void release_queue_for_retry(const struct timespec *delay,
                                    void *context) {
  queue_application_hold *hold = context;
  REQUIRE(delay->tv_nsec > 0 && hold);
  finish_queue_hold(hold);
  lc_pouch_test_queue_poll_delay = NULL;
  lc_pouch_test_queue_time_context = NULL;
}
static int observe_queue_change(void *context, const lc_watch_event *event,
                                lc_error *e) {
  int *observed = context;
  REQUIRE(event->available == 1 && event->head_message_id);
  ++*observed;
  lc_error_set(e, LC_ERR_TRANSPORT, 0L, "watch observed queue", NULL, NULL,
               "test");
  return 0;
}
static void shared_queue_watch_retry(fixture *a, fixture *b, lc_error *e) {
  queue_application_hold hold = {0};
  lc_watch_queue_req request = {0};
  int observed = 0;
  lc_watch_handler handler = {0};
  hold.peer = a;
  request.queue = "jobs";
  handler.context = &observed;
  handler.handle = observe_queue_change;
  gate_init(&hold.ready);
  ok(hold_queue_after_decision(&hold, e), e);
  lc_pouch_test_queue_time_context = &hold;
  lc_pouch_test_queue_poll_delay = release_queue_for_retry;
  REQUIRE(b->client->watch_queue(b->client, &request, &handler, e) ==
          LC_ERR_TRANSPORT);
  REQUIRE(observed == 1 && lc_pouch_test_queue_poll_delay == NULL &&
          e->message && !strcmp(e->message, "watch observed queue"));
  clear(e);
}
static int shared_batch_cut(void *context, lc_error *e) {
  int *count = context;
  if (++*count == 2)
    return lc_error_set(e, LC_ERR_TRANSPORT, 0L, "batch cut at second delivery",
                        NULL, NULL, "test");
  return LC_OK;
}
static void shared_queue_delivery(const char *directory) {
  fixture *a = calloc(1, sizeof(*a)), *b = calloc(1, sizeof(*b));
  REQUIRE(a && b);
  shared_lease_fixture = 1;
  for (int sync = 0; sync <= 1; ++sync) {
    lc_error e = {0};
    lc_dequeue_req request = shared_queue_request();
    lc_message *message = NULL;
    lc_dequeue_batch_res page = {0};
    shared_queue_peer(a, b, directory, sync, &e);
    shared_queue_enqueue(a, NULL, &e);
    shared_queue_stats(b, NULL, 1, 1, &e);
    shared_queue_watch_retry(a, b, &e);

    lc_pouch_test_after_queue_lease_claim_context = b;
    lc_pouch_test_after_queue_lease_claim_hook = shared_queue_cut;
    REQUIRE(a->client->dequeue(a->client, &request, &message, &e) ==
                LC_ERR_TRANSPORT &&
            !message);
    REQUIRE(e.message && !strcmp(e.message, "delivery cut after lease claim"));
    lc_pouch_test_after_queue_lease_claim_hook = NULL;
    lc_pouch_test_after_queue_lease_claim_context = NULL;
    clear(&e);
    shared_queue_stats(b, "independent", 1, 1, &e);

    ok(b->client->dequeue_with_state(b->client, &request, &message, &e), &e);
    REQUIRE(message && message->attempts == 1 && message->state(message));
    shared_queue_read(message, &e);
    lc_extend_req extension = {0};
    extension.extend_by_seconds = 600;
    ok(lc_message_extend(message, &extension, &e), &e);
    lc_nack_req nack = {0};
    nack.intent = LC_NACK_INTENT_DEFER;
    ok(lc_message_nack(message, &nack, &e), &e);
    shared_queue_enqueue(a, NULL, &e);
    request.page_size = 4;
    int delivery_calls = 0;
    lc_pouch_test_after_queue_batch_message_build_hook = shared_batch_cut;
    lc_pouch_test_after_queue_batch_message_build_context = &delivery_calls;
    REQUIRE(a->client->dequeue_batch(a->client, &request, &page, &e) ==
                LC_ERR_TRANSPORT &&
            !page.count && !page.messages && delivery_calls == 2);
    lc_pouch_test_after_queue_batch_message_build_hook = NULL;
    lc_pouch_test_after_queue_batch_message_build_context = NULL;
    clear(&e);
    shared_queue_stats(b, NULL, 2, 2, &e);
    request.wait_seconds = 1;
    queue_application_hold hold = {0};
    hold.peer = b;
    gate_init(&hold.ready);
    ok(hold_queue_after_decision(&hold, &e), &e);
    lc_pouch_test_queue_time_context = &hold;
    lc_pouch_test_queue_poll_delay = release_queue_for_retry;
    ok(a->client->dequeue_batch(a->client, &request, &page, &e), &e);
    REQUIRE(page.count == 2 && lc_pouch_test_queue_poll_delay == NULL);
    for (size_t i = 0; i < page.count; ++i) {
      shared_queue_read(page.messages[i], &e);
      ok(lc_message_ack(page.messages[i], &e), &e);
    }
    lc_dequeue_batch_cleanup(&page);
    shared_queue_stats(b, NULL, 0, 0, &e);
    lc_client_close(b->client);
    b->client = NULL;
    close_store(a);
    clear(&e);
  }
  shared_lease_fixture = 0;
  free(b);
  free(a);
}

static lc_ack_op shared_queue_ack(const lc_message *message) {
  lc_ack_op ack = {0};
  ack.message.ns = message->ns;
  ack.message.queue = message->queue;
  ack.message.message_id = message->message_id;
  ack.message.lease_id = message->lease_id;
  ack.message.txn_id = message->txn_id;
  ack.message.fencing_token = message->fencing_token;
  ack.message.meta_etag = message->meta_etag;
  return ack;
}
static void shared_queue_transaction(const char *directory) {
  fixture *a = calloc(1, sizeof(*a)), *b = calloc(1, sizeof(*b));
  REQUIRE(a && b);
  shared_lease_fixture = 1;
  for (int sync = 0; sync <= 1; ++sync) {
    lc_error e = {0};
    lc_dequeue_req request = shared_queue_request();
    lc_txn_decision_req decision = {0};
    lc_txn_decision_res decided = {0};
    lc_txn_replay_req replay = {0};
    lc_txn_replay_res applied = {0};
    lc_ack_res acknowledged = {0};
    lc_message *message = NULL, *redelivery = NULL;
    char xid[LC_XID_STRING_SIZE], key[256];
    lc_txn_participant participant = {"orders", key, NULL};
    queue_application_hold hold = {0};
    hold.peer = b;
    gate_init(&hold.ready);
    shared_queue_peer(a, b, directory, sync, &e);
    parent(b, "order", 0, 0, &e);
    ok(lc_xid_new(xid, &e), &e);
    lc_lease *owner = acquire(a, "order", xid, &e);
    payload(owner, NULL, &e);
    shared_queue_enqueue(a, NULL, &e);
    request.txn_id = xid;
    ok(b->client->dequeue(b->client, &request, &message, &e), &e);
    REQUIRE(message && message->txn_id && !strcmp(message->txn_id, xid));
    lc_ack_op ack = shared_queue_ack(message);
    ok(b->client->queue_ack(b->client, &ack, &acknowledged, &e), &e);
    REQUIRE(acknowledged.acked);
    lc_ack_res_cleanup(&acknowledged);
    shared_queue_stats(a, NULL, 1, 0, &e);
    parent(b, "order", 0, 0, &e);
    REQUIRE(snprintf(key, sizeof(key), "q/jobs/msg/%s", message->message_id) <
            (int)sizeof(key));
    decision.txn_id = replay.txn_id = xid;
    decision.participants = &participant;
    decision.participant_count = 1;
    lc_pouch_test_after_txn_decision_context = &hold;
    lc_pouch_test_after_txn_decision_hook = hold_queue_after_decision;
    REQUIRE(a->client->txn_commit(a->client, &decision, &decided, &e) ==
            LC_ERR_PENDING);
    REQUIRE(decided.state && !strcmp(decided.state, "commit"));
    lc_pouch_test_after_txn_decision_hook = NULL;
    lc_pouch_test_after_txn_decision_context = NULL;
    lc_txn_decision_res_cleanup(&decided);
    clear(&e);
    uint64_t selected, durable;
    ok(ps_publication_prefix(ps_root_space(ps_pouch_state_root(store(a))),
                             &selected, &durable, &e),
       &e);
    REQUIRE(selected == durable);
    finish_queue_hold(&hold);

    REQUIRE(b->client->queue_ack(b->client, &ack, &acknowledged, &e) ==
                LC_ERR_INVALID &&
            !acknowledged.acked);
    REQUIRE(e.message &&
            !strcmp(e.message, "pouch queue transaction already decided"));
    clear(&e);
    for (int retry = 0; retry < 2; ++retry) {
      ok(b->client->txn_replay(b->client, &replay, &applied, &e), &e);
      REQUIRE(applied.state && !strcmp(applied.state, "commit"));
      lc_txn_replay_res_cleanup(&applied);
    }
    shared_queue_stats(a, NULL, 0, 0, &e);
    parent(a, "order", 1, 0, &e);
    shared_queue_read(message, &e);
    message->close(message);
    owner->close(owner);

    ok(lc_xid_new(xid, &e), &e);
    shared_queue_enqueue(b, NULL, &e);
    ok(a->client->dequeue(a->client, &request, &message, &e), &e);
    REQUIRE(message && message->txn_id && !strcmp(message->txn_id, xid));
    ack = shared_queue_ack(message);
    ok(a->client->queue_ack(a->client, &ack, &acknowledged, &e), &e);
    lc_ack_res_cleanup(&acknowledged);
    REQUIRE(snprintf(key, sizeof(key), "q/jobs/msg/%s", message->message_id) <
            (int)sizeof(key));
    ok(b->client->txn_rollback(b->client, &decision, &decided, &e), &e);
    REQUIRE(decided.state && !strcmp(decided.state, "rollback"));
    lc_txn_decision_res_cleanup(&decided);
    request.txn_id = NULL;
    ok(b->client->dequeue(b->client, &request, &redelivery, &e), &e);
    REQUIRE(redelivery &&
            !strcmp(redelivery->message_id, message->message_id) &&
            redelivery->fencing_token > message->fencing_token);
    shared_queue_read(redelivery, &e);
    message->close(message);
    ok(lc_message_ack(redelivery, &e), &e);
    lc_client_close(b->client);
    b->client = NULL;
    close_store(a);
    open_store(a, 0, &e);
    shared_queue_stats(a, NULL, 0, 0, &e);
    parent(a, "order", 1, 0, &e);
    close_store(a);
    clear(&e);
  }
  shared_lease_fixture = 0;
  free(b);
  free(a);
}
