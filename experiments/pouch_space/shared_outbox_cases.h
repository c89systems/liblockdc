static const char shared_outbox_body[] =
    "{\"payload\":\"streamed across independently opened Pouch handles\"}";

static lc_outbox *shared_outbox_open(fixture *f, const char *ns,
                                     const char *owner, long ttl, lc_error *e) {
  lc_outbox_config config;
  lc_outbox *out = NULL;
  lc_outbox_config_init(&config);
  config.ns = ns;
  config.owner = owner;
  config.claim_ttl_seconds = ttl;
  ok(lc_client_new_outbox(f->client, &config, &out, e), e);
  return out;
}

static void shared_outbox_append(lc_outbox *outbox, const char *id,
                                 lc_outbox_receipt *receipt, int duplicate,
                                 lc_error *e) {
  lc_outbox_entry entry;
  lc_outbox_transaction *transaction = NULL;
  lc_outbox_commit_result committed = {0};
  producer body = source(shared_outbox_body);
  lc_outbox_entry_init(&entry);
  entry.operation_id = entry.effect_id = entry.effect_key = id;
  entry.kind = "shared-outbox";
  entry.destination = "local";
  entry.content_type = "application/json";
  entry.payload_digest = "sha256:shared-outbox-fixture";
  body.pub.impl = &body;
  ok(lc_outbox_append(outbox, &entry, &body.pub, &transaction, receipt, e), e);
  REQUIRE(receipt->duplicate == duplicate);
  if (duplicate) {
    REQUIRE(!transaction && !body.calls);
    return;
  }
  REQUIRE(transaction && body.at == body.length && body.calls >= 3);
  ok(lc_outbox_transaction_commit(transaction, &committed, e), e);
  REQUIRE(!receipt->outbox_key && committed.outbox_receipt_count == 1 &&
          committed.outbox_receipts[0].outbox_key);
  *receipt = committed.outbox_receipts[0];
  memset(committed.outbox_receipts, 0, sizeof(*receipt));
  lc_outbox_commit_result_cleanup(&committed);
  lc_outbox_transaction_close(transaction);
}

static void shared_outbox_check_body(lc_outbox_job *job, lc_error *e) {
  lc_sink *sink = NULL;
  const void *bytes;
  size_t length, written;
  ok(lc_sink_to_memory(&sink, e), e);
  ok(lc_outbox_job_write_payload(job, sink, &written, e), e);
  ok(lc_sink_memory_bytes(sink, &bytes, &length, e), e);
  REQUIRE(length == sizeof(shared_outbox_body) - 1 && written == length &&
          !memcmp(bytes, shared_outbox_body, length));
  lc_sink_close(sink);
}

typedef struct outbox_handoff_cut {
  fixture *publisher;
  const char *key;
  int calls;
} outbox_handoff_cut;

static int shared_outbox_cut_handoff(void *context, lc_error *e) {
  outbox_handoff_cut *cut = context;
  lc_pouch_state_read_result borrowed = {0};
  lc_outbox_receipt independent = {0};
  REQUIRE(++cut->calls == 1);
  lc_outbox_test_before_stateful_handoff_hook = NULL;
  REQUIRE(lc_pouch_state_read_metadata(store(cut->publisher), "orders",
                                       cut->key, &borrowed,
                                       e) == LC_ERR_PENDING);
  clear(e);
  lc_pouch_state_read_result_cleanup(&store(cut->publisher)->allocator,
                                     &borrowed);
  lc_outbox *other =
      shared_outbox_open(cut->publisher, "independent", "other", 300, e);
  shared_outbox_append(other, "independent", &independent, 0, e);
  lc_outbox_close(other);
  lc_outbox_receipt_cleanup(&independent);
  return lc_error_set(e, LC_ERR_TRANSPORT, 0L,
                      "stateful hand-off cut after durable claim", NULL, NULL,
                      "test");
}

static void shared_outbox_handoff(const char *directory) {
  fixture *a = calloc(1, sizeof(*a)), *b = calloc(1, sizeof(*b));
  REQUIRE(a && b);
  shared_lease_fixture = 1;
  for (int sync = 0; sync <= 1; ++sync) {
    lc_error e = {0};
    lc_outbox_receipt first = {0}, duplicate = {0};
    lc_outbox_dispatcher *dispatcher = NULL, *observer = NULL;
    lc_outbox_job *job = NULL, *second = NULL;
    setup(a, directory, 0, sync, sync ? 3 : 0, &e);
    strcpy(b->path, a->path);
    configure(b, 0, a->sync, a->crypto);
    open_store(b, 0, &e);
    lc_outbox *publisher =
        shared_outbox_open(a, "orders", "publisher", 300, &e);
    lc_outbox *owner = shared_outbox_open(b, "orders", "worker", 300, &e);
    shared_outbox_append(publisher, "deliver", &first, 0, &e);
    ok(lc_outbox_dispatcher_get_or_start(owner, &dispatcher, &e), &e);
    ok(lc_outbox_dispatcher_notify_outbox_key(dispatcher, first.outbox_key, &e),
       &e);
    outbox_handoff_cut cut = {a, first.outbox_key, 0};
    lc_outbox_test_before_stateful_handoff_context = &cut;
    lc_outbox_test_before_stateful_handoff_hook = shared_outbox_cut_handoff;
    ok(lc_outbox_dispatcher_next_with_state(dispatcher, 5000, &job, &e), &e);
    REQUIRE(job && job->attempt == 1 && cut.calls == 1);
    lc_outbox_test_before_stateful_handoff_context = NULL;
    REQUIRE(!strcmp(job->outbox_key, first.outbox_key) &&
            !strcmp(job->operation_id, "deliver") && lc_outbox_job_state(job));
    shared_outbox_check_body(job, &e);

    ok(lc_outbox_dispatcher_get_or_start(publisher, &observer, &e), &e);
    ok(lc_outbox_dispatcher_notify_outbox_key(observer, first.outbox_key, &e),
       &e);
    ok(lc_outbox_dispatcher_next(observer, 0, &second, &e), &e);
    REQUIRE(!second);
    producer checkpoint = source("{\"saved\":true}");
    checkpoint.pub.impl = &checkpoint;
    ok(lc_lease_update(lc_outbox_job_state(job), &checkpoint.pub, NULL, &e),
       &e);
    REQUIRE(checkpoint.length == checkpoint.at);
    ok(lc_outbox_job_complete(job, NULL, &e), &e);
    ok(lc_outbox_dispatcher_stop(observer, -1, &e), &e);
    ok(lc_outbox_dispatcher_stop(dispatcher, -1, &e), &e);
    lc_outbox_dispatcher_close(observer);
    lc_outbox_dispatcher_close(dispatcher);
    lc_outbox_close(owner);
    lc_outbox_close(publisher);
    lc_client_close(b->client);
    b->client = NULL;
    close_store(a);
    open_store(a, 0, &e);
    publisher = shared_outbox_open(a, "orders", "publisher", 300, &e);
    shared_outbox_append(publisher, "deliver", &duplicate, 1, &e);
    REQUIRE(!strcmp(duplicate.outbox_key, first.outbox_key));
    lc_outbox_close(publisher);
    lc_outbox_receipt_cleanup(&first);
    lc_outbox_receipt_cleanup(&duplicate);
    close_store(a);
    retire(a);
  }
  shared_lease_fixture = 0;
  free(a);
  free(b);
}

static const char shared_outbox_checkpoint[] =
    "{\"delivered\":false,\"step\":2}";

static int shared_outbox_crash_before_state(void *context, lc_error *e) {
  int notify = *(int *)context;
  (void)e;
  REQUIRE(write(notify, "A", 1) == 1);
  _exit(0);
}

static void shared_outbox_crash_child(const char *path, int sync, int notify,
                                      const char *key, int before_state) {
  fixture *f = calloc(1, sizeof(*f));
  lc_error e = {0};
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_outbox_job *job = NULL;
  REQUIRE(f && strlen(path) < sizeof(f->path));
  strcpy(f->path, path);
  shared_lease_fixture = 1;
  configure(f, 0, sync, sync ? 3 : 0);
  open_store(f, 0, &e);
  lc_outbox *outbox = shared_outbox_open(f, "orders", "crashed", 2, &e);
  ok(lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &e), &e);
  ok(lc_outbox_dispatcher_notify_outbox_key(dispatcher, key, &e), &e);
  if (before_state) {
    lc_outbox_test_before_stateful_handoff_context = &notify;
    lc_outbox_test_before_stateful_handoff_hook =
        shared_outbox_crash_before_state;
  }
  ok(lc_outbox_dispatcher_next_with_state(dispatcher, 5000, &job, &e), &e);
  REQUIRE(job && job->attempt == 1 && lc_outbox_job_state(job));
  producer checkpoint = source(shared_outbox_checkpoint);
  checkpoint.pub.impl = &checkpoint;
  ok(lc_lease_update(lc_outbox_job_state(job), &checkpoint.pub, NULL, &e), &e);
  REQUIRE(checkpoint.at == checkpoint.length);
  REQUIRE(write(notify, "C", 1) == 1);
  _exit(0);
}

static void shared_outbox_check_record(fixture *f, const char *key,
                                       const char *expected, const char *also,
                                       lc_error *e) {
  lc_pouch_state_read_result parent = {0};
  lc_sink *sink = NULL;
  const void *bytes;
  size_t length;
  ok(lc_pouch_state_read(store(f), "orders", key, &parent, e), e);
  REQUIRE(parent.found && parent.body);
  ok(lc_sink_to_memory(&sink, e), e);
  ok(lc_copy(parent.body, sink, NULL, e), e);
  ok(lc_sink_memory_bytes(sink, &bytes, &length, e), e);
  REQUIRE(memmem(bytes, length, expected, strlen(expected)) &&
          (!also || memmem(bytes, length, also, strlen(also))));
  lc_sink_close(sink);
  lc_pouch_state_read_result_cleanup(&store(f)->allocator, &parent);
}

static void shared_outbox_recovery_case(fixture *f, const char *directory,
                                        int sync, int before_state) {
  lc_error e = {0};
  lc_outbox_receipt receipt = {0};
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_outbox_job *job = NULL;
  int ready[2], exited;
  char fd[24], policy[2] = {(char)('0' + sync), 0}, byte;
  const char *cut = before_state ? "claim" : "checkpoint";
  setup(f, directory, 0, sync, sync ? 3 : 0, &e);
  lc_outbox *outbox = shared_outbox_open(f, "orders", "publisher", 300, &e);
  shared_outbox_append(outbox, "recover", &receipt, 0, &e);
  lc_outbox_close(outbox);
  REQUIRE(!pipe(ready));
  snprintf(fd, sizeof(fd), "%d", ready[1]);
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    close(ready[0]);
    execl(executable_path, executable_path, "--shared-outbox-crash-child",
          f->path, policy, fd, receipt.outbox_key, crypto_key, cut,
          (char *)NULL);
    _exit(127);
  }
  REQUIRE(!close(ready[1]));
  REQUIRE(read(ready[0], &byte, 1) == 1 && byte == (before_state ? 'A' : 'C') &&
          !close(ready[0]));
  REQUIRE(waitpid(child, &exited, 0) == child && WIFEXITED(exited) &&
          WEXITSTATUS(exited) == 0);
  close_store(f);
  open_store(f, 0, &e);
  shared_outbox_check_record(f, receipt.outbox_key, "\"checkpoint_present\":1",
                             "\"dispatch_state\":\"claimed\"", &e);
  outbox = shared_outbox_open(f, "orders", "replacement", 300, &e);
  ok(lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &e), &e);
  ok(lc_outbox_dispatcher_notify_outbox_key(dispatcher, receipt.outbox_key, &e),
     &e);
  ok(before_state
         ? lc_outbox_dispatcher_next(dispatcher, 10000, &job, &e)
         : lc_outbox_dispatcher_next_with_state(dispatcher, 10000, &job, &e),
     &e);
  REQUIRE(job && job->attempt == 2 &&
          !strcmp(job->outbox_key, receipt.outbox_key));
  REQUIRE((lc_outbox_job_state(job) != NULL) == !before_state);
  shared_outbox_check_body(job, &e);
  if (!before_state) {
    lc_sink *sink = NULL;
    lc_get_res got = {0};
    const void *bytes;
    size_t length;
    ok(lc_sink_to_memory(&sink, &e), &e);
    ok(lc_lease_get(lc_outbox_job_state(job), sink, NULL, &got, &e), &e);
    ok(lc_sink_memory_bytes(sink, &bytes, &length, &e), &e);
    REQUIRE(!got.no_content && length == sizeof(shared_outbox_checkpoint) - 1 &&
            !memcmp(bytes, shared_outbox_checkpoint, length));
    lc_sink_close(sink);
    lc_get_res_cleanup(&got);
  }
  ok(lc_outbox_job_complete(job, NULL, &e), &e);
  char *marker = NULL, *parent = NULL, *state = NULL;
  ok(lc_pouch_outbox_state_reclaim_marker_take(store(f), "orders", NULL,
                                               &marker, &parent, &state, &e),
     &e);
  REQUIRE(!marker && !parent && !state);
  ok(lc_outbox_dispatcher_stop(dispatcher, -1, &e), &e);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_outbox_receipt_cleanup(&receipt);
  close_store(f);
  retire(f);
}

static void shared_outbox_recovery(const char *directory) {
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  shared_lease_fixture = 1;
  for (int sync = 0; sync <= 1; ++sync)
    for (int before_state = 0; before_state <= 1; ++before_state)
      shared_outbox_recovery_case(f, directory, sync, before_state);
  shared_lease_fixture = 0;
  free(f);
}

static int shared_outbox_begin_cleanup(void *context, lc_error *e) {
  barrier_disk *disk = context;
  (void)e;
  REQUIRE(!disk->armed);
  disk->armed = 1;
  return LC_OK;
}

static void shared_outbox_completion_case(fixture *f, const char *directory,
                                          int with_attachment,
                                          enum barrier_outcome outcome) {
  barrier_disk disk = {0};
  lc_outbox_receipt receipt = {0};
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_outbox_job *job = NULL;
  lc_error e = {0};
  char *marker = NULL, *parent_key = NULL, *state_key = NULL;

  snprintf(scenario, sizeof(scenario),
           "shared outbox completion attachment=%d outcome=%d", with_attachment,
           (int)outcome);
  disk.outcome = outcome;
  disk.disk.stable = calloc(1, (size_t)PS_DEFAULT_CAPACITY);
  disk.disk.dirty = calloc(1, (size_t)(PS_DEFAULT_CAPACITY / PS_PAGE));
  disk.disk.scratch = malloc(PS_PAGE);
  REQUIRE(disk.disk.stable && disk.disk.dirty && disk.disk.scratch);
  ps_io io = {&disk, disk_read, disk_write, barrier_sync, disk_reserve, NULL};
  setup_io(f, directory, 0, 1, with_attachment ? 3 : 0, &io, &e);
  lc_outbox *outbox = shared_outbox_open(f, "orders", "worker", 300, &e);
  shared_outbox_append(outbox, "complete", &receipt, 0, &e);
  ok(lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &e), &e);
  ok(lc_outbox_dispatcher_notify_outbox_key(dispatcher, receipt.outbox_key, &e),
     &e);
  ok(lc_outbox_dispatcher_next_with_state(dispatcher, 5000, &job, &e), &e);
  REQUIRE(job && lc_outbox_job_state(job));
  char *checkpoint_key = strdup(lc_outbox_job_state(job)->key);
  REQUIRE(checkpoint_key);
  producer checkpoint = source(shared_outbox_checkpoint);
  checkpoint.pub.impl = &checkpoint;
  ok(lc_lease_update(lc_outbox_job_state(job), &checkpoint.pub, NULL, &e), &e);
  REQUIRE(checkpoint.at == checkpoint.length);
  if (with_attachment) {
    lc_attach_req attach;
    lc_attach_res attached = {0};
    producer proof = source("cleanup must also retire this attachment");
    proof.pub.impl = &proof;
    lc_attach_req_init(&attach);
    attach.name = "proof.txt";
    attach.content_type = "text/plain";
    ok(lc_lease_attach(lc_outbox_job_state(job), &attach, &proof.pub, &attached,
                       &e),
       &e);
    REQUIRE(proof.at == proof.length);
    lc_attach_res_cleanup(&attached);
  }
  lc_outbox_test_before_stateful_terminal_cleanup_context = &disk;
  lc_outbox_test_before_stateful_terminal_cleanup_hook =
      shared_outbox_begin_cleanup;
  ok(lc_outbox_job_complete(job, NULL, &e), &e);
  lc_outbox_test_before_stateful_terminal_cleanup_hook = NULL;
  lc_outbox_test_before_stateful_terminal_cleanup_context = NULL;
  REQUIRE(disk.armed && disk.syncs == (outcome == BARRIER_DURABLE ? 2U : 1U));
  disk.armed = 0;
  disk.disk.freeze = 1;
  ok(lc_outbox_dispatcher_stop(dispatcher, -1, &e), &e);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  close_store(f);
  restore_disk(&disk.disk, f->path);
  open_store(f, 0, &e);

  shared_outbox_check_record(f, receipt.outbox_key,
                             "\"dispatch_state\":\"completed\"", NULL, &e);
  ok(lc_pouch_outbox_state_reclaim_marker_take(
         store(f), "orders", NULL, &marker, &parent_key, &state_key, &e),
     &e);
  if (outcome != BARRIER_DURABLE) {
    REQUIRE(marker && parent_key && state_key &&
            !strcmp(parent_key, receipt.outbox_key) &&
            !strcmp(state_key, checkpoint_key));
  } else {
    REQUIRE(!marker && !parent_key && !state_key);
  }
  if (outcome == BARRIER_LOST_SYNC) {
    shared_outbox_check_record(f, checkpoint_key, shared_outbox_checkpoint,
                               NULL, &e);
    attachments(f, checkpoint_key, with_attachment ? 1 : 0, &e);
  } else {
    parent(f, checkpoint_key, 0, 0, &e);
    attachments(f, checkpoint_key, 0, &e);
  }
  outbox = shared_outbox_open(f, "orders", "replacement", 300, &e);
  dispatcher = NULL;
  job = NULL;
  ok(lc_outbox_dispatcher_get_or_start(outbox, &dispatcher, &e), &e);
  ok(lc_outbox_dispatcher_notify_outbox_key(dispatcher, receipt.outbox_key, &e),
     &e);
  ok(lc_outbox_dispatcher_next_with_state(dispatcher, 0, &job, &e), &e);
  REQUIRE(!job);
  parent(f, checkpoint_key, 0, 0, &e);
  attachments(f, checkpoint_key, 0, &e);
  if (marker) {
    lc_free_with_allocator(&store(f)->allocator, marker);
    lc_free_with_allocator(&store(f)->allocator, parent_key);
    lc_free_with_allocator(&store(f)->allocator, state_key);
    ok(lc_pouch_outbox_state_reclaim_marker_take(
           store(f), "orders", NULL, &marker, &parent_key, &state_key, &e),
       &e);
    REQUIRE(!marker && !parent_key && !state_key);
  }
  ok(lc_outbox_dispatcher_stop(dispatcher, -1, &e), &e);
  lc_outbox_dispatcher_close(dispatcher);
  lc_outbox_close(outbox);
  lc_outbox_receipt_cleanup(&receipt);
  close_store(f);
  retire(f);
  free(checkpoint_key);
  free(disk.disk.scratch);
  free(disk.disk.dirty);
  free(disk.disk.stable);
}

static void shared_outbox_completion(const char *directory) {
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  shared_lease_fixture = 1;
  for (int attachment = 0; attachment <= 1; ++attachment) {
    shared_outbox_completion_case(f, directory, attachment, BARRIER_DURABLE);
    shared_outbox_completion_case(f, directory, attachment, BARRIER_LOST_SYNC);
    shared_outbox_completion_case(f, directory, attachment, BARRIER_LOST_ACK);
  }
  shared_lease_fixture = 0;
  free(f);
}
