#ifndef POUCH_SPACE_ABANDONED_CASES_H
#define POUCH_SPACE_ABANDONED_CASES_H
#include "checkpoint_cases.h"
#include "tail_cases.h"

typedef struct abandoned_io {
  uint64_t reads, writes, syncs, read_bytes, write_bytes;
  int enabled;
} abandoned_io;
static ssize_t abandoned_disk_read(void *context, int fd, void *bytes,
                                   size_t count, off_t offset) {
  abandoned_io *audit = context;
  if (__atomic_load_n(&audit->enabled, __ATOMIC_ACQUIRE)) {
    __atomic_add_fetch(&audit->reads, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&audit->read_bytes, count, __ATOMIC_RELAXED);
  }
  return pread(fd, bytes, count, offset);
}
static ssize_t abandoned_disk_write(void *context, int fd, const void *bytes,
                                    size_t count, off_t offset) {
  abandoned_io *audit = context;
  if (__atomic_load_n(&audit->enabled, __ATOMIC_ACQUIRE)) {
    __atomic_add_fetch(&audit->writes, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&audit->write_bytes, count, __ATOMIC_RELAXED);
  }
  return pwrite(fd, bytes, count, offset);
}
static int abandoned_disk_sync(void *context, int fd) {
  abandoned_io *audit = context;
  if (__atomic_load_n(&audit->enabled, __ATOMIC_ACQUIRE))
    __atomic_add_fetch(&audit->syncs, 1, __ATOMIC_RELAXED);
  return fdatasync(fd);
}
static int abandoned_disk_reserve(void *context, int fd, off_t offset,
                                  off_t bytes) {
  (void)context;
  return fallocate(fd, 0, offset, bytes);
}
static double abandoned_clock(clockid_t kind) {
  struct timespec now;
  CHECK(!clock_gettime(kind, &now));
  return (double)now.tv_sec + (double)now.tv_nsec / 1e9;
}

/* A nearly full live piece prevents profitable partial-piece relocation from
 * hiding the zero-completed suffix case. The helper owns no application body
 * buffer and uses the ordinary canonical streaming producer. */
static size_t abandoned_dense_bytes(ps_root *root, uint64_t *target,
                                    size_t bytes) {
  ps_namespace *measure = NULL;
  tail_storage storage = {0};
  OK(ps_root_namespace_acquire(root, "measure", 1, &measure, &error));
  uint64_t written =
      namespace_churn_put(root, measure, "stable", 1, 1, 1, bytes);
  OK(ps_namespace_checkpoint_flush(measure, &error));
  OK(ps_namespace_sources_visit(measure, tail_storage_visit, &storage, &error));
  CHECK(storage.runs > 1 && storage.capacity > bytes);
  ps_namespace_release(measure);
  *target = storage.capacity - 1;
  return (size_t)(*target - (written - bytes));
}
static void abandoned_reserve(ps_root *root, ps_namespace *ns, int wrote,
                              uint64_t minimum) {
  ps_namespace_append *append = NULL;
  OK(ps_namespace_append_begin(ns, 0, minimum, PS_APPEND_PREFIX, NULL, &append,
                               &error));
  if (wrote) {
    lc_allocator allocator = {0};
    lc_pouch_state_entry request = {0}, result = {0};
    checkpoint_history_input input = {0};
    input.source.impl = &input;
    input.source.read = checkpoint_history_read;
    input.remaining = PS_CHUNK + 17;
    request.key = "hot";
    request.content_type = "application/test";
    request.payload_context = "abandoned";
    request.record_type = LC_POUCH_STATE_RECORD_STATE_PUT;
    request.index_seq = request.version = request.lifetime = 2;
    OK(lc_pouch_state_record_append(&allocator, ps_root_crypto(root),
                                    ps_namespace_append_output(append),
                                    ps_namespace_append_leaf(append), &request,
                                    &input.source, 0, &result, &error));
    CHECK(!input.remaining && input.calls > 1);
    lc_pouch_state_entry_cleanup(&allocator, &result);
  }
  ps_namespace_append_close(append);
}
static void abandoned_read(ps_namespace *ns, churn_pin *pin, size_t expected) {
  ps_namespace_view *view = NULL;
  pin->ns = ns;
  OK(ps_namespace_view_open(ns, &view, &error));
  OK(ps_namespace_view_visit(view, 1, namespace_churn_visit, pin, &error));
  ps_namespace_view_close(view);
  CHECK(pin->count == expected);
}
static void abandoned_live_seed(ps_root *root, ps_namespace *ns, size_t live,
                                uint64_t target) {
  ps_namespace_repack_result result;
  tail_storage storage = {0};
  CHECK(namespace_churn_put(root, ns, "stable", 1, 1, 1, live) == target);
  OK(ps_namespace_checkpoint_flush(ns, &error));
  OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
  OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, (size_t)storage.runs, NULL,
                         NULL, &result, &error));
  CHECK(!result.finished_tail);
}
static void namespace_abandoned_case(int durable, int empty, int wrote,
                                     int reopened, int full, int large,
                                     int comparison) {
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_status before, after;
  ps_namespace_repack_result result;
  tail_storage storage = {0};
  churn_pin pin = {0};
  ps_stats allocated, reclaimed;
  abandoned_io audit = {0}, work;
  ps_io io = {&audit,
              abandoned_disk_read,
              abandoned_disk_write,
              abandoned_disk_sync,
              abandoned_disk_reserve,
              NULL};
  double elapsed, cpu, checkpoint, checkpoint_cpu;
  size_t live;
  uint64_t replaced = 0, original_runs, original_bytes, next = 1, target;
  uint64_t kept_runs, kept_bytes;
  unsigned char filler[PS_PAGE] = {0};
  clean();
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  options.storage.maximum_capacity = PS_DEFAULT_CAPACITY;
  OK(ps_root_open(path, 1, &options, &root, &error));
  live = abandoned_dense_bytes(root, &target, large ? PS_MIB : PS_CHUNK);
  OK(ps_root_namespace_acquire(root, "abandoned", 1, &ns, &error));
  if (empty)
    OK(ps_namespace_rotate(ns, 0, &error));
  else
    abandoned_live_seed(root, ns, live, target);
  OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
  kept_runs = storage.runs;
  kept_bytes = storage.bytes;
  memset(&storage, 0, sizeof(storage));
  OK(ps_namespace_status_get(ns, &before, &error));
  CHECK(before.active_segment == 1 && (empty || before.tail));
  abandoned_reserve(root, ns, wrote, PS_MIB / 4);
  OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
  original_runs = storage.runs;
  original_bytes = storage.bytes;
  CHECK(storage.bytes >= PS_MIB / 4 && original_runs > 2);
  if (reopened) {
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    OK(ps_root_open(path, 0, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "abandoned", 0, &ns, &error));
  }
  if (!empty)
    abandoned_read(ns, &pin, 1);
  if (full)
    budget_fill(ps_root_space(root), &next, filler);
  OK(ps_checkpoint(ps_root_space(root), &error));
  OK(ps_status(ps_root_space(root), &allocated, &error));
  elapsed = abandoned_clock(CLOCK_MONOTONIC);
  cpu = abandoned_clock(CLOCK_PROCESS_CPUTIME_ID);
  __atomic_store_n(&audit.enabled, 1, __ATOMIC_RELEASE);
  /* One piece per call, including the unprofitable live piece. Bounded calls
   * must eventually reach and retire every unused grant without copying it. */
  for (uint64_t pass = 0; pass <= original_runs + 1; ++pass) {
    OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL, &result,
                           &error));
    CHECK(result.visited <= 1 && !result.body_bytes && !result.mapping_bytes);
    replaced += result.replaced;
    if (result.finished_tail) {
      OK(ps_namespace_status_get(ns, &after, &error));
      CHECK(after.active_segment == 2 && !after.tail);
    }
    memset(&storage, 0, sizeof(storage));
    OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
    if (storage.runs == kept_runs)
      break;
  }
  if (!comparison)
    CHECK(storage.sources == 2 && storage.runs == kept_runs &&
          storage.bytes == kept_bytes && replaced == original_runs - kept_runs);
  __atomic_store_n(&audit.enabled, 0, __ATOMIC_RELEASE);
  elapsed = abandoned_clock(CLOCK_MONOTONIC) - elapsed;
  cpu = abandoned_clock(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  work = audit;
  CHECK(!work.reads && !work.read_bytes);
  checkpoint = abandoned_clock(CLOCK_MONOTONIC);
  checkpoint_cpu = abandoned_clock(CLOCK_PROCESS_CPUTIME_ID);
  __atomic_store_n(&audit.enabled, 1, __ATOMIC_RELEASE);
  OK(ps_checkpoint(ps_root_space(root), &error));
  __atomic_store_n(&audit.enabled, 0, __ATOMIC_RELEASE);
  checkpoint = abandoned_clock(CLOCK_MONOTONIC) - checkpoint;
  checkpoint_cpu = abandoned_clock(CLOCK_PROCESS_CPUTIME_ID) - checkpoint_cpu;
  if (pin.body) {
    churn_read(pin.body, pin.offset, pin.bytes, pin.crc, &error);
    ps_namespace_read_close(pin.body);
  }
  OK(ps_status(ps_root_space(root), &reclaimed, &error));
  CHECK(reclaimed.capacity == PS_DEFAULT_CAPACITY);
  if (!comparison)
    CHECK(reclaimed.allocated_bytes + original_bytes <=
          allocated.allocated_bytes + kept_bytes + 2 * PS_PAGE);
  namespace_churn_put(root, ns, "hot", 3, 3, 3, 1000);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  OK(ps_root_open(path, 0, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "abandoned", 0, &ns, &error));
  memset(&pin, 0, sizeof(pin));
  abandoned_read(ns, &pin, empty ? 1 : 2);
  ps_namespace_read_close(pin.body);
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  printf("abandoned_suffix sync=%d empty=%d wrote=%d reopened=%d full=%d "
         "large=%d body_bytes=%zu removed_grants=%llu reclaimed_bytes=%llu "
         "retained_unused_bytes=%llu work_ms=%.6f cpu_ms=%.6f reads=%llu "
         "writes=%llu written_bytes=%llu syncs=%llu checkpoint_ms=%.6f "
         "checkpoint_cpu_ms=%.6f checkpoint_reads=%llu checkpoint_writes=%llu "
         "checkpoint_syncs=%llu verified=1\n",
         durable, empty, wrote, reopened, full, large, empty ? 0 : live,
         (unsigned long long)replaced,
         (unsigned long long)(original_bytes - storage.bytes),
         (unsigned long long)(storage.bytes - kept_bytes), elapsed * 1e3,
         cpu * 1e3, (unsigned long long)work.reads,
         (unsigned long long)work.writes, (unsigned long long)work.write_bytes,
         (unsigned long long)work.syncs, checkpoint * 1e3, checkpoint_cpu * 1e3,
         (unsigned long long)(audit.reads - work.reads),
         (unsigned long long)(audit.writes - work.writes),
         (unsigned long long)(audit.syncs - work.syncs));
  clean();
}
static void namespace_abandoned(void) {
  for (int durable = 0; durable < 2; ++durable) {
    for (int empty = 0; empty < 2; ++empty)
      for (int wrote = 0; wrote < 2; ++wrote)
        for (int reopened = 0; reopened < 2; ++reopened)
          namespace_abandoned_case(durable, empty, wrote, reopened, 0, 0, 0);
    namespace_abandoned_case(durable, 0, 1, 1, 1, 0, 0);
    namespace_abandoned_case(durable, 0, 1, 1, 1, 1, 0);
  }
}
static void abandoned_owned(int durable) {
  model m;
  ps_io io = model_init(&m);
  ps_space *space;
  ps_owned_head *head = NULL;
  ps_owned_run *run = NULL, *replacement = NULL;
  ps_owned_status before, after;
  ps_owned_range impossible = {0, 1};
  unsigned char bytes[128], readback[128];
  ps_stats held, released;
  int found;
  clean();
  memset(bytes, 0x73, sizeof(bytes));
  space = create(PS_MIB, durable, &io);
  put(space, state(999), "keeper", 6, 6);
  OK(ps_owned_open(space, NULL, stream_key(1), 1, NULL, &head, &before,
                   &error));
  OK(ps_owned_grant(head, NULL, 0, PS_PAGE, &run, &found, &error));
  CHECK(found);
  OK(ps_owned_write(run, 0, bytes, sizeof(bytes), &error));
  BAD(ps_owned_run_repack(head, run, NULL, NULL, 0, NULL, NULL, &replacement,
                          &error));
  CHECK(!replacement);
  OK(ps_owned_seal(head, &error));
  BAD(ps_owned_run_repack(head, run, NULL, &impossible, 1, NULL, NULL,
                          &replacement, &error));
  CHECK(!replacement);
  OK(ps_owned_status_get(head, &before, &error));
  m.reads = 0;
  m.fail_read = 1;
  OK(ps_owned_run_repack(head, run, NULL, NULL, 0, NULL, NULL, &replacement,
                         &error));
  CHECK(!replacement && !m.reads);
  m.fail_read = 0;
  OK(ps_owned_status_get(head, &after, &error));
  CHECK(after.sealed && !after.tail && !after.runs &&
        after.capacity == before.capacity);
  OK(ps_owned_read(run, 0, readback, sizeof(readback), &error));
  CHECK(!memcmp(bytes, readback, sizeof(bytes)));
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &held, &error));
  ps_owned_run_close(run);
  OK(ps_checkpoint(space, &error));
  OK(ps_status(space, &released, &error));
  CHECK(released.allocated_bytes + PS_PAGE <= held.allocated_bytes);
  ps_owned_close(head);
  OK(ps_close(space, &error));
  space = reopen(&io);
  OK(ps_owned_open(space, NULL, stream_key(1), 0, NULL, &head, &after, &error));
  CHECK(after.sealed && !after.tail && !after.runs &&
        after.capacity == before.capacity);
  expect(space, state(999), NULL, "keeper", 6);
  ps_owned_close(head);
  OK(ps_close(space, &error));
  free(m.stable);
  clean();
}
static int abandoned_reap(ps_root *root, ps_namespace *ns, lc_error *e) {
  uint64_t closed, pending, reclaimed, skipped;
  int rc = ps_namespace_reap(ns, &closed, &pending, e);
  if (rc == LC_OK)
    rc = ps_root_reclaim_orphans(root, &reclaimed, &skipped, e);
  if (rc == LC_OK)
    rc = ps_checkpoint(ps_root_space(root), e);
  return rc;
}
static void namespace_abandoned_empty(void) {
  for (int durable = 0; durable < 2; ++durable) {
    ps_root_options options = {0};
    ps_root *root = NULL;
    ps_namespace *ns = NULL;
    ps_namespace_status status;
    ps_namespace_repack_result result;
    uint64_t active = 1, entries = 0, next = 1;
    unsigned char filler[PS_PAGE] = {0};
    clean();
    options.storage.durable_sync = durable;
    options.storage.maximum_capacity = PS_DEFAULT_CAPACITY;
    OK(ps_root_open(path, 1, &options, &root, &error));
    OK(ps_root_namespace_acquire(root, "empty", 1, &ns, &error));
    OK(ps_namespace_rotate(ns, 0, &error));
    for (unsigned int cycle = 0; cycle < 24; ++cycle) {
      /* Preserve fragmentation margins as well as physical payload capacity.
       * Only the initial half-MiB abandoned reservation is larger; subsequent
       * quarter-MiB operations reuse its capacity while other data stays live.
       */
      abandoned_reserve(root, ns, 0, cycle ? PS_MIB / 4 : PS_MIB / 2);
      if (!cycle)
        budget_fill(ps_root_space(root), &next, filler);
      OK(ps_namespace_checkpoint_flush(ns, &error));
      OK(ps_namespace_status_get(ns, &status, &error));
      CHECK(status.checkpoint_epoch && status.covered_segment == active &&
            !status.covered_offset && !status.checkpoint_keys);
      OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, active, 16, NULL, NULL,
                             &result, &error));
      CHECK(result.finished_tail && result.pending && result.replaced == 16);
      OK(ps_namespace_reclaim(ns, PS_CATALOG_SEGMENT, active, &error));
      ++active;
      OK(ps_checkpoint(ps_root_space(root), &error));
      OK(ps_namespace_status_get(ns, &status, &error));
      CHECK(status.active_segment == active &&
            status.covered_segment == active && status.bound_segments == 1 &&
            !status.runs && !status.tail && !status.pending_keys &&
            !status.checkpoint_keys);
      ps_key_checkpoint *selected = NULL;
      ps_checkpoint_info coverage;
      OK(ps_key_checkpoint_open(ns, &selected, &error));
      ps_key_checkpoint_info(selected, &coverage);
      CHECK(!coverage.state_index && !coverage.query_index);
      ps_key_checkpoint_close(selected);
      ps_stats stats;
      OK(ps_status(ps_root_space(root), &stats, &error));
      if (!cycle)
        entries = stats.entries;
      CHECK(stats.entries == entries && !stats.budgets &&
            stats.capacity == PS_DEFAULT_CAPACITY);
      if (cycle % 4 == 3) {
        ps_namespace_release(ns);
        OK(ps_root_close(root, &error));
        OK(ps_root_open(path, 0, &options, &root, &error));
        OK(ps_root_namespace_acquire(root, "empty", 0, &ns, &error));
      }
    }
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    printf("empty namespace sync%d: 24 full-store cycles, stable entries, "
           "zero state/query watermarks\n",
           durable);
    clean();
  }
}
static void namespace_abandoned_faults(int durable, int unselected) {
  model m;
  ps_io io = model_init(&m);
  ps_root_options options = {0};
  ps_root *root = NULL;
  ps_namespace *ns = NULL;
  ps_namespace_append *append = NULL;
  ps_namespace_repack_result result;
  ps_namespace_status status;
  unsigned char *baseline = malloc((size_t)PS_MIB);
  CHECK(baseline);
  clean();
  options.storage.initial_capacity = options.storage.maximum_capacity = PS_MIB;
  options.storage.durable_sync = durable;
  options.storage.io = &io;
  OK(ps_root_open(path, 1, &options, &root, &error));
  OK(ps_root_namespace_acquire(root, "abandoned", 1, &ns, &error));
  if (!unselected)
    OK(ps_namespace_rotate(ns, 0, &error));
  OK(ps_namespace_append_begin(ns, 0, 1, PS_APPEND_PREFIX, NULL, &append,
                               &error));
  ps_namespace_append_close(append);
  OK(ps_checkpoint(ps_root_space(root), &error));
  ps_namespace_release(ns);
  OK(ps_root_close(root, &error));
  memcpy(baseline, m.stable, (size_t)PS_MIB);
  for (int kind = 0; kind < 3; ++kind) {
    size_t sites = 0, optional = 0;
    for (size_t cut = 0; cut <= sites; ++cut) {
      memcpy(m.stable, baseline, (size_t)PS_MIB);
      m.stable_bytes = PS_MIB;
      crash(&m);
      OK(ps_root_open(path, 0, &options, &root, &error));
      OK(ps_root_namespace_acquire(root, "abandoned", 0, &ns, &error));
      m.operations = m.reads = allocation_call = 0;
      m.tear = m.persist_torn = 1;
      m.fail_at = kind == 1 ? cut : 0;
      m.fail_read = kind == 2 ? cut : 0;
      allocation_fail = kind == 0 ? (cut ? cut : (size_t)-1) : 0;
      int rc;
      if (unselected)
        rc = abandoned_reap(root, ns, &error);
      else {
        rc = ps_namespace_checkpoint_flush(ns, &error);
        if (rc == LC_OK)
          rc = ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 1, NULL, NULL,
                                   &result, &error);
      }
      allocation_fail = m.fail_at = m.fail_read = 0;
      if (!cut) {
        OK(rc);
        sites = kind == 0   ? allocation_call
                : kind == 1 ? m.operations
                            : m.reads;
        if (!unselected)
          CHECK(result.finished_tail && result.replaced == 1 &&
                !result.body_bytes && !result.mapping_bytes);
      } else {
        CHECK(rc == (kind == 0 ? LC_ERR_NOMEM : LC_ERR_TRANSPORT) ||
              (kind == 0 && rc == LC_OK));
        if (rc == LC_OK)
          ++optional; /* Best-effort interval collection can defer allocation.
                       */
        lc_error_cleanup(&error);
        if (kind == 0) {
          if (unselected)
            OK(abandoned_reap(root, ns, &error));
          else {
            OK(ps_namespace_checkpoint_flush(ns, &error));
            OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL,
                                   &result, &error));
          }
        }
      }
      ps_namespace_release(ns);
      OK(ps_root_close(root, &error));
      crash(&m);
      OK(ps_root_open(path, 0, &options, &root, &error));
      OK(ps_root_namespace_acquire(root, "abandoned", 0, &ns, &error));
      if (unselected)
        OK(abandoned_reap(root, ns, &error));
      else {
        OK(ps_namespace_checkpoint_flush(ns, &error));
        OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 2, NULL, NULL,
                               &result, &error));
      }
      OK(ps_namespace_status_get(ns, &status, &error));
      CHECK(status.active_segment == (uint64_t)(unselected ? 0 : 2) &&
            !status.tail);
      if (!unselected)
        CHECK(!result.pending && !result.body_bytes && !result.mapping_bytes);
      tail_storage storage = {0};
      OK(ps_namespace_sources_visit(ns, tail_storage_visit, &storage, &error));
      CHECK(storage.sources == (uint64_t)(unselected ? 0 : 2) &&
            !storage.runs && !storage.bytes);
      namespace_churn_put(root, ns, "hot", 1, 1, 1, 1000);
      ps_namespace_release(ns);
      OK(ps_root_close(root, &error));
    }
    printf("abandoned suffix sync%d unselected%d %s: %zu sites, "
           "%zu optional completions\n",
           durable, unselected,
           kind == 0   ? "allocation"
           : kind == 1 ? "write/sync"
                       : "read",
           sites, optional);
  }
  free(baseline);
  free(m.stable);
  clean();
}
typedef struct abandoned_gate {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  size_t calls, stop;
  int entered, released, finished;
} abandoned_gate;
static int abandoned_select(void *context, ps_piece_publish_fn publish,
                            void *publication, lc_error *e) {
  abandoned_gate *gate = context;
  pthread_mutex_lock(&gate->mutex);
  if (++gate->calls == gate->stop) {
    gate->entered = 1;
    pthread_cond_broadcast(&gate->changed);
    while (!gate->released)
      pthread_cond_wait(&gate->changed, &gate->mutex);
  }
  pthread_mutex_unlock(&gate->mutex);
  return publish(publication, e);
}
typedef struct abandoned_actor {
  ps_namespace *ns;
  abandoned_gate *gate;
  ps_namespace_repack_result result;
  int rc;
} abandoned_actor;
static void *abandoned_worker(void *context) {
  abandoned_actor *actor = context;
  lc_error e;
  lc_error_init(&e);
  for (unsigned int pass = 0; pass < 8; ++pass) {
    actor->rc =
        ps_namespace_repack(actor->ns, PS_CATALOG_SEGMENT, 1, 1,
                            abandoned_select, actor->gate, &actor->result, &e);
    pthread_mutex_lock(&actor->gate->mutex);
    int entered = actor->gate->entered;
    pthread_mutex_unlock(&actor->gate->mutex);
    if (actor->rc != LC_OK || entered)
      break;
  }
  pthread_mutex_lock(&actor->gate->mutex);
  actor->gate->finished = 1;
  pthread_cond_broadcast(&actor->gate->changed);
  pthread_mutex_unlock(&actor->gate->mutex);
  lc_error_cleanup(&e);
  return NULL;
}
static void namespace_abandoned_concurrency(int durable) {
  for (int which = 0; which < 3; ++which) {
    ps_root_options options = {0};
    ps_root *root = NULL;
    ps_namespace *ns = NULL;
    ps_namespace_view *view = NULL;
    ps_namespace_repack_result result;
    ps_namespace_status status;
    churn_pin pin = {0};
    uint64_t target;
    abandoned_gate gate = {PTHREAD_MUTEX_INITIALIZER,
                           PTHREAD_COND_INITIALIZER,
                           0,
                           which ? 2 : 1,
                           0,
                           0};
    abandoned_actor actor = {0};
    pthread_t thread;
    clean();
    options.storage.durable_sync = durable;
    OK(ps_root_open(path, 1, &options, &root, &error));
    size_t live = abandoned_dense_bytes(root, &target, PS_CHUNK);
    OK(ps_root_namespace_acquire(root, "abandoned", 1, &ns, &error));
    abandoned_live_seed(root, ns, live, target);
    abandoned_reserve(root, ns, 1, PS_MIB / 4);
    abandoned_read(ns, &pin, 1);
    actor.ns = ns;
    actor.gate = &gate;
    CHECK(!pthread_create(&thread, NULL, abandoned_worker, &actor));
    pthread_mutex_lock(&gate.mutex);
    while (!gate.entered && !gate.finished)
      pthread_cond_wait(&gate.changed, &gate.mutex);
    CHECK(gate.entered);
    pthread_mutex_unlock(&gate.mutex);
    OK(ps_namespace_status_get(ns, &status, &error));
    CHECK(status.active_segment == (uint64_t)(which ? 2 : 1));
    namespace_churn_put(root, ns, "hot", 2, 2, 2, 1000);
    put(ps_root_space(root), state(999), "peer", 4, 4);
    churn_read(pin.body, pin.offset, pin.bytes, pin.crc, &error);
    if (which == 2)
      OK(ps_namespace_view_open(ns, &view, &error));
    pthread_mutex_lock(&gate.mutex);
    gate.released = 1;
    pthread_cond_broadcast(&gate.changed);
    pthread_mutex_unlock(&gate.mutex);
    CHECK(!pthread_join(thread, NULL) && actor.rc == LC_OK);
    CHECK(actor.result.finished_tail == !!which &&
          actor.result.replaced == (uint64_t)(which == 1) &&
          actor.result.pending); /* The bounded suffix still has more grants. */
    ps_namespace_view_close(view);
    OK(ps_namespace_checkpoint_flush(ns, &error));
    for (unsigned int pass = 0; pass < 4; ++pass) {
      OK(ps_namespace_repack(ns, PS_CATALOG_SEGMENT, 1, 32, NULL, NULL, &result,
                             &error));
      if (!result.pending)
        break;
    }
    CHECK(!result.pending);
    churn_read(pin.body, pin.offset, pin.bytes, pin.crc, &error);
    ps_namespace_read_close(pin.body);
    memset(&pin, 0, sizeof(pin));
    abandoned_read(ns, &pin, 2);
    ps_namespace_read_close(pin.body);
    ps_namespace_release(ns);
    OK(ps_root_close(root, &error));
    CHECK(!pthread_cond_destroy(&gate.changed) &&
          !pthread_mutex_destroy(&gate.mutex));
    clean();
  }
}
#endif
