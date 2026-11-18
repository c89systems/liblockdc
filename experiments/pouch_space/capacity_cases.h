#ifndef POUCH_SPACE_CAPACITY_CASES_H
#define POUCH_SPACE_CAPACITY_CASES_H
#include "checkpoint_cases.h"

static void obligation_entry(lc_pouch_state_entry *entry, char *name,
                             char *large, unsigned int i) {
  checkpoint_fixture(entry, name, i);
  if (!i)
    entry->key = large;
}
static void obligation_group(ps_namespace *ns, char *large, size_t keys) {
  lc_allocator allocator = {0};
  lc_pouch_state_entry entry;
  ps_namespace_append *append = NULL;
  uint64_t bytes, total = 0;
  char name[32];
  for (size_t i = 0; i < keys; ++i) {
    obligation_entry(&entry, name, large, (unsigned int)i);
    OK(lc_pouch_state_record_measure_snapshot(&allocator, NULL, &entry, 0,
                                              &bytes, &error));
    total += bytes;
  }
  OK(ps_namespace_append_begin(ns, 0, total, PS_APPEND_COMPLETE, &append,
                               &error));
  for (size_t i = 0; i < keys; ++i) {
    obligation_entry(&entry, name, large, (unsigned int)i);
    OK(lc_pouch_state_record_write_snapshot(
        &allocator, ps_namespace_append_output(append),
        ps_namespace_append_leaf(append), &entry, NULL, NULL, 0, &error));
  }
  OK(ps_namespace_append_commit(append, PS_COMMIT_DEFAULT, &error));
  ps_namespace_append_close(append);
}
static void root_obligations(int durable, int partial, int full) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *cold = NULL, *peer = NULL;
  ps_namespace_status info;
  ps_stats before, recovered, after;
  checkpoint_observation observation = {0};
  ps_checkpoint_observer observer = {checkpoint_prepare, checkpoint_finish,
                                     &observation};
  ps_values *values = NULL;
  ps_key k;
  ps_ref ref;
  uint64_t bytes, next = 100;
  char *large = malloc((size_t)PS_MIB + 1);
  unsigned char filler[PS_PAGE];
  size_t keys = partial ? 65 : 1;
  int selected_large = 0;
  CHECK(large);
  printf("root obligations sync%d partial%d full%d\n", durable, partial, full);
  memset(large, 'k', (size_t)PS_MIB);
  large[PS_MIB] = 0;
  memset(filler, 0x5b, sizeof(filler));
  clean();
  options.storage.initial_capacity = PS_DEFAULT_CAPACITY;
  options.storage.durable_sync = durable;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "checkpoint", 1, &cold, &error));
  OK(ps_root_namespace_acquire(root, "peer", 1, &peer, &error));
  checkpoint_put(root, peer, "lease/0", 1, 1, 1, 1, 0);
  OK(ps_namespace_checkpoint_flush(peer, &error));
  obligation_group(cold, large, keys);
  if (partial) {
    OK(ps_namespace_checkpoint_observe(cold, &observer, &error));
    observation.reject = 2;
    CHECK(ps_namespace_checkpoint_flush(cold, &error) == LC_ERR_NOMEM);
    lc_error_cleanup(&error);
    CHECK(observation.published == 1);
    ps_namespace_info identity;
    OK(ps_namespace_identity(cold, &identity, &error));
    OK(ps_values_capture(ps_root_space(root), identity.owner, PS_KEY_METADATA,
                         PS_KEY_METADATA, &values, &error));
    CHECK(ps_values_count(values) == 64);
    for (size_t i = 0; i < ps_values_count(values); ++i) {
      OK(ps_values_info(values, i, &k, &ref, &bytes, &error));
      selected_large += bytes > PS_MIB;
    }
    CHECK(selected_large == 1);
    ps_values_close(values);
  }
  if (full)
    budget_fill(ps_root_space(root), &next, filler);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &before, &error));
  CHECK(before.budgets == 1 && before.reserved_payload_bytes >= 2 * PS_PAGE);
  if (!partial)
    CHECK(before.reserved_payload_bytes > PS_MIB);
  ps_namespace_release(cold);
  ps_namespace_release(peer);
  cold = peer = NULL;
  OK(ps_root_close(root, &error));
  root = NULL;
  /* Root return is the admission barrier. No caller has acquired the cold
   * namespace after reopen, yet a peer must already be unable to spend its
   * completion capacity. This also covers forward key slots from an
   * interrupted checkpoint whose coverage control still names the old prefix.
   */
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_status(ps_root_space(root), &recovered, &error));
  CHECK(recovered.budgets == 1 &&
        recovered.reserved_payload_bytes == before.reserved_payload_bytes &&
        recovered.capacity == PS_DEFAULT_CAPACITY);
  budget_fill(ps_root_space(root), &next, filler);
  OK(ps_root_namespace_acquire(root, "checkpoint", 0, &cold, &error));
  OK(ps_namespace_status_get(cold, &info, &error));
  CHECK(info.pending_keys == (partial ? 1 : keys) &&
        info.checkpoint_keys == keys);
  OK(ps_namespace_checkpoint_flush(cold, &error));
  OK(ps_namespace_status_get(cold, &info, &error));
  CHECK(info.checkpoint_epoch && !info.pending_keys &&
        info.covered_segment == info.active_segment &&
        info.covered_offset == info.tail);
  OK(ps_status(ps_root_space(root), &after, &error));
  CHECK(!after.budgets && !after.reserved_payload_bytes &&
        !after.reserved_metadata_bytes && !after.reserved_entries &&
        after.capacity == PS_DEFAULT_CAPACITY);
  ps_namespace_release(cold);
  OK(ps_root_close(root, &error));
  free(large);
  clean();
}

static void root_obligation_faults(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_view *view = NULL;
  ps_stats stats;
  unsigned char *baseline = malloc((size_t)PS_MIB);
  const char *names[] = {"first", "last"};
  size_t sites;
  CHECK(baseline);
  clean();
  options.storage.initial_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  OK(ps_root_open(path, 1, &options, &root, &error));
  for (size_t i = 0; i < 2; ++i) {
    OK(ps_root_namespace_acquire(root, names[i], 1, &ns, &error));
    checkpoint_put(root, ns, "lease/0", 1, 1, 1, 1, 0);
    ps_namespace_release(ns);
  }
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_root_close(root, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  for (int kind = 0; kind < 3; ++kind) {
    sites = 0;
    for (size_t cut = 0; cut <= sites; ++cut) {
      memcpy(m.stable, baseline, (size_t)PS_MIB);
      m.stable_bytes = PS_MIB;
      crash(&m);
      m.operations = m.reads = allocation_call = 0;
      m.fail_at = kind == 1 ? cut : 0;
      m.fail_read = kind == 2 ? cut : 0;
      m.tear = m.persist_torn = 1;
      allocation_fail = kind == 0 ? (cut ? cut : (size_t)-1) : 0;
      root = NULL;
      int rc = ps_root_open(path, 0, &options, &root, &error);
      allocation_fail = m.fail_at = m.fail_read = 0;
      if (!cut) {
        OK(rc);
        sites = kind == 0   ? allocation_call
                : kind == 1 ? m.operations
                            : m.reads;
        CHECK(sites);
      } else {
        CHECK(!root && rc == (kind == 0 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT));
        lc_error_cleanup(&error);
        /* Failed bootstrap releases every partial owner and capacity claim,
         * including the file lock. A same-path retry needs no extra capacity
         * and retains both namespaces' canonical state. */
        OK(ps_root_open(path, 0, &options, &root, &error));
      }
      OK(ps_status(ps_root_space(root), &stats, &error));
      CHECK(stats.budgets == 2 && stats.reserved_payload_bytes == 4 * PS_PAGE);
      for (size_t i = 0; i < 2; ++i) {
        checkpoint_seen seen = {0};
        OK(ps_root_namespace_acquire(root, names[i], 0, &ns, &error));
        OK(ps_namespace_view_open(ns, &view, &error));
        OK(ps_namespace_view_visit(view, 1, checkpoint_visit, &seen, &error));
        CHECK(seen.count == 1 && seen.maximum == 1 && seen.first_version == 1);
        ps_namespace_view_close(view);
        ps_namespace_release(ns);
      }
      OK(ps_root_close(root, &error));
    }
    printf("root obligation bootstrap sync%d %s: %zu failure sites\n", durable,
           kind == 0   ? "allocation"
           : kind == 1 ? "write/sync"
                       : "read",
           sites);
  }
  free(baseline);
  free(m.stable);
  clean();
}

#endif
