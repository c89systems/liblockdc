#ifndef POUCH_SPACE_LEASE_CAPACITY_CASES_H
#define POUCH_SPACE_LEASE_CAPACITY_CASES_H

/* Public workflow characterization, not a waiver of complete admission.
 * capacity-required runs exactly this workload but rejects capacity denial.
 * Neither mode invents a larger floor or grows the physical container. */
typedef struct lease_capacity_trace {
  ps_space *space;
  ps_space *peer_space;
  ps_budget *peer;
  ps_stats phases[LC_POUCH_RETENTION_PHASE_COUNT];
  unsigned long seen;
  uint64_t peer_reserved;
  int fill_phase;
  size_t children_applied;
} lease_capacity_trace;

static ps_budget *lease_capacity_peer(ps_space *space, lc_error *e) {
  ps_budget_plan plan = {0};
  ps_budget *budget = NULL;
  uint64_t low = 0, high = PS_DEFAULT_CAPACITY / PS_PAGE;
  /* An ordinary peer preserves every computed maintenance obligation. A
   * maintenance peer could consume that capacity and would prove less. */
  while (low + 1 < high) {
    uint64_t mid = (low + high) / 2;
    plan.payload_bytes = mid * PS_PAGE;
    int rc = ps_budget_open(space, &plan, &budget, e);
    if (rc == LC_OK) {
      low = mid;
      ok(ps_budget_close(budget, e), e);
    } else {
      REQUIRE(rc == LC_ERR_INVALID);
      clear(e);
      high = mid;
    }
  }
  plan.payload_bytes = low * PS_PAGE;
  ok(ps_budget_open(space, &plan, &budget, e), e);
  return budget;
}
static void lease_capacity_claim(lease_capacity_trace *profile, lc_error *e) {
  ps_stats stats;
  profile->peer = lease_capacity_peer(profile->peer_space, e);
  ok(ps_status(profile->peer_space, &stats, e), e);
  REQUIRE(stats.reserved_payload_bytes);
  profile->peer_reserved = stats.reserved_payload_bytes;
}
static int lease_capacity_note(int phase, void *context, lc_error *e) {
  lease_capacity_trace *profile = context;
  if (phase == LC_POUCH_RETENTION_ATTACHMENT_APPLIED)
    ++profile->children_applied;
  int rc = note_phase(phase, NULL, e);
  if (rc == LC_OK && phase == profile->fill_phase && !profile->peer)
    lease_capacity_claim(profile, e);
  if (rc == LC_OK) {
    rc = ps_status(profile->space, &profile->phases[phase], e);
    profile->seen |= 1UL << phase;
  }
  return rc;
}
static void lease_capacity_snapshot(const lease_capacity_trace *profile,
                                    int phase, int sync, int crypto,
                                    size_t key_bytes, size_t attached) {
  const ps_stats *s = &profile->phases[phase];
  printf(
      "expiry_capacity_phase sync=%d crypto=%d key_bytes=%zu attachments=%zu "
      "fill_phase=%d phase=%d allocated=%llu live=%llu metadata=%llu "
      "reserved=%llu floor=%llu workspace=%llu entries=%llu budgets=%llu "
      "checkpoints=%llu syncs=%llu\n",
      sync, crypto, key_bytes, attached, profile->fill_phase, phase,
      (unsigned long long)s->allocated_bytes, (unsigned long long)s->live_bytes,
      (unsigned long long)s->metadata_bytes,
      (unsigned long long)s->reserved_payload_bytes,
      (unsigned long long)s->reserve_payload_bytes,
      (unsigned long long)s->metadata_workspace_bytes,
      (unsigned long long)s->entries, (unsigned long long)s->budgets,
      (unsigned long long)s->checkpoints, (unsigned long long)s->syncs);
}
static void lease_capacity_seed(fixture *f, const char *key, size_t attached,
                                lc_error *e) {
  lc_lease *lease = acquire(f, key, NULL, e);
  lc_update_opts options = {0};
  options.has_retention_seconds = 1;
  options.retention_seconds = 50;
  payload(lease, &options, e);
  for (size_t i = 0; i < attached; ++i) {
    char name[48];
    lc_attach_req request;
    lc_attach_res result = {0};
    lc_attach_req_init(&request);
    REQUIRE(snprintf(name, sizeof(name), "receipt-%zu", i) < (int)sizeof(name));
    request.name = name;
    request.content_type = "text/plain";
    producer input = source("streamed capacity fixture attachment");
    input.pub.impl = &input;
    ok(lease->attach(lease, &request, &input.pub, &result, e), e);
    REQUIRE(input.at == input.length && input.calls > 1);
    lc_attach_res_cleanup(&result);
  }
  f->fencing = lease->fencing_token;
  ok(lease->release(lease, NULL, e), e);
}
static int lease_capacity_case(const char *directory, int sync, int crypto,
                               int large, size_t attached, int fill_phase,
                               int required) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  fixture *peer = shared_lease_fixture ? calloc(1, sizeof(*peer)) : NULL;
  size_t key_bytes = large ? 8192 : 5;
  char *key = malloc(key_bytes + 1);
  REQUIRE(f && key && (!shared_lease_fixture || peer));
  memset(key, 'k', key_bytes);
  key[key_bytes] = 0;
  now(1000);
  setup(f, directory, 0, sync, crypto, &e);
  lease_capacity_seed(f, key, attached, &e);
  if (attached > LC_POUCH_STATE_KEY_WINDOW)
    write_parent(f, "other", 0, 1, &e);
  /* Recovery establishes a reproducible canonical prefix and all outstanding
   * checkpoint debt before either the peer or the expiry operation starts. */
  close_store(f);
  open_store(f, 0, &e);
  parent(f, key, 1, 1050, &e);
  attachments(f, key, attached, &e);
  lc_pouch_state_read_result pinned = {0};
  ok(lc_pouch_state_read(store(f), "orders", key, &pinned, &e), &e);
  REQUIRE(pinned.body);
  lease_capacity_trace profile = {0};
  profile.space = ps_root_space(ps_pouch_state_root(store(f)));
  profile.peer_space = profile.space;
  if (peer) {
    REQUIRE(!phase_fail);
    strcpy(peer->path, f->path);
    configure(peer, 0, sync, crypto);
    open_store(peer, 0, &e);
    parent(peer, key, 1, 1050, &e);
    profile.peer_space = ps_root_space(ps_pouch_state_root(store(peer)));
    REQUIRE(profile.peer_space != profile.space);
  }
  profile.fill_phase = fill_phase;
  if (!fill_phase)
    lease_capacity_claim(&profile, &e);
  ok(ps_status(profile.space, &profile.phases[0], &e), &e);
  profile.seen = 1;
  now(1050);
  int fault_phase = phase_fail;
  phase_hits = 0;
  lc_pouch_test_retention_hook = lease_capacity_note;
  lc_pouch_test_retention_context = &profile;
  double started = real_clock(CLOCK_MONOTONIC);
  double cpu_started = real_clock(CLOCK_PROCESS_CPUTIME_ID);
  lc_pouch_maintenance_result result = {0};
  int rc = lc_pouch_retention_run(store(f), "orders", 1, &result, &e);
  double elapsed = real_clock(CLOCK_MONOTONIC) - started;
  double cpu = real_clock(CLOCK_PROCESS_CPUTIME_ID) - cpu_started;
  lc_pouch_test_retention_hook = note_phase;
  lc_pouch_test_retention_context = NULL;
  if (fault_phase) {
    REQUIRE(rc == LC_ERR_TRANSPORT && phase_hits == 1 && !phase_fail);
  } else if (rc != LC_OK)
    REQUIRE(rc == LC_ERR_INVALID && result.retention_failed_count == 1);
  else
    REQUIRE(result.retention_deleted_state_count == 1 &&
            !result.retention_failed_count);
  int completed = rc == LC_OK;
  printf(
      "expiry_capacity_result sync=%d crypto=%d key_bytes=%zu attachments=%zu "
      "fill_phase=%d fault_phase=%d complete=%d rc=%d phases=%lu "
      "peer_reserved=%llu work_ms=%.6f reason=%s\n",
      sync, crypto, key_bytes, attached, fill_phase, fault_phase, completed, rc,
      profile.seen, (unsigned long long)profile.peer_reserved, elapsed * 1e3,
      e.message ? e.message : "complete");
  for (int phase = 0; phase < LC_POUCH_RETENTION_PHASE_COUNT; ++phase)
    if (profile.seen & (1UL << phase))
      lease_capacity_snapshot(&profile, phase, sync, crypto, key_bytes,
                              attached);
  fflush(stdout);
  clear(&e);
  ok(ps_check(profile.space, &e), &e);
  /* The body admitted before cleanup still denotes the original value. */
  const char *expected =
      "{\"value\":42,\"description\":\"expiry streaming fixture\"}";
  unsigned char buffer[128];
  size_t got = pinned.body->read(pinned.body, buffer, sizeof(buffer), &e);
  ok(e.code, &e);
  REQUIRE(got == strlen(expected) && !memcmp(buffer, expected, got));
  REQUIRE(!pinned.body->read(pinned.body, buffer, sizeof(buffer), &e));
  ok(e.code, &e);
  lc_pouch_state_read_result_cleanup(&store(f)->allocator, &pinned);
  if (fault_phase) {
    if (fault_phase == LC_POUCH_RETENTION_ATTACHMENT_APPLIED) {
      REQUIRE(profile.children_applied && profile.children_applied <= attached);
      parent(f, key, 0, 0, &e);
      attachments(f, key, attached - profile.children_applied, &e);
    }
    lc_pouch_maintenance_result_cleanup(&store(f)->allocator, &result);
    if (!(fault_phase % 2) ||
        fault_phase == LC_POUCH_RETENTION_ATTACHMENT_APPLIED) {
      ok(ps_budget_close(profile.peer, &e), &e);
      profile.peer = NULL;
      close_store(f);
      open_store(f, 0, &e);
      profile.space = ps_root_space(ps_pouch_state_root(store(f)));
      profile.peer = lease_capacity_peer(profile.space, &e);
    }
    lc_pouch_retention_work work = {0};
    ok(lc_pouch_retention_work_read(store(f), "orders", &work, &e), &e);
    REQUIRE(work.present && work.phase == LC_POUCH_RETENTION_ACTIVE &&
            !strcmp(work.key, key));
    lc_pouch_retention_work_cleanup(store(f), &work);
    lc_acquire_req request;
    lc_lease *blocked = NULL;
    lc_acquire_req_init(&request);
    request.key = key;
    request.owner = "replacement";
    request.ttl_seconds = 300;
    REQUIRE(f->client->acquire(f->client, &request, &blocked, &e) ==
            LC_ERR_PENDING);
    REQUIRE(!blocked);
    clear(&e);
    /* A committed outcome remains binding even when the wall clock retreats.
     * Retry its unfinished application with the ordinary peer still held. */
    now(900);
    (void)clean(f, 1, &e);
    parent(f, key, 0, 0, &e);
    attachments(f, key, 0, &e);
  }
  ok(ps_budget_close(profile.peer, &e), &e);
  if (peer) {
    parent(peer, key, !completed, completed ? 0 : 1050, &e);
    if (completed)
      attachments(peer, key, 0, &e);
    lc_client_close(peer->client);
    free(peer);
  }
  lc_pouch_maintenance_result_cleanup(&store(f)->allocator, &result);
  /* Characterization always proves full recovery after the peer releases its
   * claim; it does not relabel that retry as progress under the held peer. */
  double closed = real_clock(CLOCK_MONOTONIC);
  close_store(f);
  closed = real_clock(CLOCK_MONOTONIC) - closed;
  double opened = real_clock(CLOCK_MONOTONIC);
  open_store(f, 0, &e);
  double ready = real_clock(CLOCK_MONOTONIC) - opened;
  (void)clean(f, 64, &e);
  parent(f, key, 0, 0, &e);
  attachments(f, key, 0, &e);
  double verified = real_clock(CLOCK_MONOTONIC) - opened;
  close_store(f);
  open_store(f, 0, &e);
  parent(f, key, 0, 0, &e);
  attachments(f, key, 0, &e);
  if (attached > LC_POUCH_STATE_KEY_WINDOW) {
    parent(f, "other", 1, 0, &e);
    attachments(f, "other", 1, &e);
  }
  close_store(f);
  retire(f);
  printf("expiry_capacity_observation sync=%d crypto=%d key_bytes=%zu "
         "attachments=%zu fill_phase=%d fault_phase=%d complete=%d "
         "work_ms=%.6f cpu_ms=%.6f close_ms=%.6f reopen_ms=%.6f "
         "reopen_verify_ms=%.6f verified=1\n",
         sync, crypto, key_bytes, attached, fill_phase, fault_phase, completed,
         elapsed * 1e3, cpu * 1e3, closed * 1e3, ready * 1e3, verified * 1e3);
  free(key);
  free(f);
  lc_error_cleanup(&e);
  REQUIRE(!required || completed);
  return completed;
}
static void lease_capacity_completion(const char *directory) {
  for (int sync = 0; sync < 2; ++sync)
    for (int crypto = 0; crypto < 4; ++crypto) {
      lease_capacity_case(directory, sync, crypto, 0, 0, 0, 1);
      lease_capacity_case(directory, sync, crypto, 1, 0,
                          LC_POUCH_RETENTION_STAGED, 1);
      lease_capacity_case(directory, sync, crypto, 0, 16,
                          LC_POUCH_RETENTION_STAGED, 1);
      for (int cut = LC_POUCH_RETENTION_DECIDED;
           cut <= LC_POUCH_RETENTION_DATA_DURABLE; ++cut) {
        phase_fail = cut;
        lease_capacity_case(directory, sync, crypto, 0, 0, 0, 0);
      }
    }
}
static void lease_capacity_profile(const char *directory, int required) {
  size_t completed = 0, total = 0;
  for (int sync = 0; sync < 2; ++sync)
    for (int crypto = 0; crypto < 4; ++crypto)
      for (int large = 0; large < 2; ++large)
        for (size_t attached = 0; attached <= 16; attached += 16)
          for (int where = 0; where < 3; ++where) {
            int phase = where == 0   ? -1
                        : where == 1 ? 0
                                     : LC_POUCH_RETENTION_STAGED;
            completed += lease_capacity_case(directory, sync, crypto, large,
                                             attached, phase, required);
            ++total;
          }
  printf("expiry_capacity complete=%zu cases=%zu recovery_verified=1\n",
         completed, total);
}
/* Exercise more than two key windows, with cuts immediately after the first
 * child and after entering the second window. Native cases hold an ordinary
 * peer throughout application/recovery; multifile remains a semantic reference.
 */
static void lease_attachment_windows(const char *directory) {
  lc_error e;
  lc_error_init(&e);
  fixture *f = calloc(1, sizeof(*f));
  REQUIRE(f);
  for (int sync = 0; sync < 2; ++sync)
    for (int crypto = 0; crypto < 4; ++crypto)
      for (int cut = 0; cut < 3; ++cut) {
        phase_fail = cut ? LC_POUCH_RETENTION_ATTACHMENT_APPLIED : 0;
        attachment_cut_skip = cut == 2 ? 16 : 0;
        lease_capacity_case(directory, sync, crypto, 1, 33,
                            LC_POUCH_RETENTION_STAGED, !cut);
        REQUIRE(!attachment_cut_skip);
        now(1000);
        setup(f, directory, 1, sync, crypto, &e);
        lease_capacity_seed(f, "order", 33, &e);
        write_parent(f, "other", 0, 1, &e);
        now(1050);
        phase_fail = cut ? LC_POUCH_RETENTION_ATTACHMENT_APPLIED : 0;
        attachment_cut_skip = cut == 2 ? 16 : 0;
        phase_hits = 0;
        lc_pouch_maintenance_result result = {0};
        int rc = lc_pouch_retention_run(store(f), "orders", 1, &result, &e);
        if (cut) {
          REQUIRE(rc == LC_ERR_TRANSPORT && phase_hits == 1 && !phase_fail &&
                  !attachment_cut_skip);
          clear(&e);
          parent(f, "order", 0, 0, &e);
          attachments(f, "order", cut == 1 ? 32 : 16, &e);
        } else {
          ok(rc, &e);
          REQUIRE(result.retention_deleted_state_count == 1);
        }
        lc_pouch_maintenance_result_cleanup(&store(f)->allocator, &result);
        close_store(f);
        open_store(f, 0, &e);
        now(900);
        (void)clean(f, 1, &e);
        settled(f, &e);
        parent(f, "other", 1, 0, &e);
        attachments(f, "other", 1, &e);
        close_store(f);
        retire(f);
      }
  free(f);
  lc_error_cleanup(&e);
}
#endif
