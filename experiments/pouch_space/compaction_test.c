#define _XOPEN_SOURCE 700
#include "../../tests/support/lc_test_tmp.h"
#include "budget.h"
#include "lc_api_internal.h"
#include "lc_collection_internal.h"
#include "lc_pouch_record.h"
#include "pouch_internal.h"
#include "space_internal.h"
#include <errno.h>
#ifdef __linux__
#include <ftw.h>
#include <sys/resource.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define REQUIRE(condition)                                                     \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

static void ok(int rc, const lc_error *error) {
  if (rc != LC_OK)
    fprintf(stderr, "Pouch error %d: %s\n", rc,
            error->message ? error->message : "");
  REQUIRE(rc == LC_OK);
}

typedef struct observations {
  unsigned int attempts, pending, reclaimed, settled, errors;
} observations;

static pthread_mutex_t observation_lock = PTHREAD_MUTEX_INITIALIZER;
static observations observed;
static int observe_all_namespaces;
static const char work_namespace[] = "churn";

static void observe(const char *ns, const lc_pouch_maintenance_result *result,
                    int rc) {
  REQUIRE(!pthread_mutex_lock(&observation_lock));
  ++observed.attempts;
  if (rc == LC_OK && (observe_all_namespaces || !strcmp(ns, work_namespace))) {
    if (result->cleanup_pending_count)
      ++observed.pending;
    else
      ++observed.settled;
    if (result->cleanup_deleted_count)
      ++observed.reclaimed;
  } else if (rc != LC_OK && rc != LC_ERR_PENDING) {
    fprintf(stderr, "Background maintenance for %s returned %d\n", ns, rc);
    ++observed.errors;
  }
  REQUIRE(!pthread_mutex_unlock(&observation_lock));
}

static observations sample(void) {
  REQUIRE(!pthread_mutex_lock(&observation_lock));
  observations result = observed;
  REQUIRE(!pthread_mutex_unlock(&observation_lock));
  return result;
}

static uint64_t ns_now(clockid_t clock) {
  struct timespec now;
  REQUIRE(!clock_gettime(clock, &now));
  return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static void pause_ms(unsigned int milliseconds) {
  struct timespec delay = {milliseconds / 1000U,
                           (long)(milliseconds % 1000U) * 1000000L};
  while (nanosleep(&delay, &delay) != 0)
    REQUIRE(errno == EINTR);
}

typedef struct admission {
  int single_file, create;
  unsigned int interval;
  const ps_io *io;
} admission;

static int admit(void *context, const char *path, const lc_allocator *allocator,
                 const lc_pouch_open_options *logical, lc_pouch **out,
                 lc_error *error) {
  const admission *mode = context;
  ps_pouch_state_options options = {.logical = *logical};
  options.logical.compaction_interval_seconds = mode->interval;
  options.logical.compaction_delete_grace_seconds = 1;
  options.logical.compaction_min_segment_count = 1;
  options.logical.compaction_min_reclaimable_bytes = 1;
  options.logical.compaction_throttling_disabled = 1;
  options.maximum_capacity = PS_DEFAULT_CAPACITY;
  options.io = mode->io;
  return mode->single_file
             ? ps_pouch_state_open(path, mode->create, allocator, &options, out,
                                   error)
             : lc_pouch_open(path, allocator, &options.logical, out, error);
}

static lc_client *open_client(const char *path, int single_file, int create,
                              int worker, unsigned int interval, int durable,
                              const ps_io *io, lc_error *error) {
  char endpoint[1400];
  lc_client_config config;
  lc_pouch_settings settings;
  lc_client *client = NULL;
  admission mode = {single_file, create, interval, io};
  const char *endpoints[] = {endpoint};
  REQUIRE(snprintf(endpoint, sizeof(endpoint),
                   "pouch://%s?single_writer=false&background_compaction=%s&"
                   "query_indexing=false&queue_watch=false",
                   path, worker ? "true" : "false") < (int)sizeof(endpoint));
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask = LC_POUCH_SETTING_DURABLE_SYNC;
  settings.durable_sync = durable;
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.pouch_settings = &settings;
  ok(lc_client_open_with_pouch_factory(&config, admit, &mode, &client, error),
     error);
  return client;
}

static lc_pouch *runtime(lc_client *client) {
  return ((lc_client_handle *)client)->pouch;
}

static lc_collection *named_collection(lc_client *client, const char *ns,
                                       const char *name, int create,
                                       lc_error *error) {
  lc_collection_config config = {ns, name};
  lc_collection *out = NULL;
  ok(create ? lc_client_create_collection(client, &config, &out, error)
            : lc_client_open_collection(client, &config, &out, error),
     error);
  return out;
}

static lc_collection *collection(lc_client *client, const char *ns, int create,
                                 lc_error *error) {
  return named_collection(client, ns, "events", create, error);
}

static lc_collection_append_res
append_with_receipt(lc_collection *out, uint64_t entry, const char *body,
                    size_t bytes, lc_error *error) {
  char id[32];
  lc_collection_append_req request;
  lc_collection_append_res result = {0};
  lc_source *source = NULL;
  REQUIRE(snprintf(id, sizeof(id), "%llu", (unsigned long long)entry) > 0);
  lc_collection_append_req_init(&request);
  request.append_id = id;
  ok(lc_source_from_memory(body, bytes, &source, error), error);
  ok(lc_collection_append(out, &request, source, &result, error), error);
  lc_source_close(source);
  return result;
}

static uint64_t append(lc_collection *out, uint64_t entry, const char *body,
                       size_t bytes, lc_error *error) {
  lc_collection_append_res result =
      append_with_receipt(out, entry, body, bytes, error);
  REQUIRE(!result.duplicate && result.bytes == bytes);
  return result.position;
}

static void expect_collection(lc_collection *out, uint64_t live,
                              uint64_t position, lc_error *error) {
  lc_collection_head_res head = {0};
  ok(lc_collection_head(out, &head, error), error);
  REQUIRE(head.live_count == live && head.upper_position == position);
}

static void regression(const char *path, int single_file, int pressure,
                       lc_error *error) {
  static const char body[] = "{\"n\":1}";
  lc_client *owner = open_client(path, single_file, 1, 0, 1, 0, NULL, error);
  lc_collection *pinned = collection(owner, work_namespace, 1, error);
  lc_collection_handle *handle = pinned->impl;
  REQUIRE(append(pinned, 1, body, sizeof(body) - 1, error) == 1);
  REQUIRE(append(pinned, 2, body, sizeof(body) - 1, error) == 2);
  lc_collection_delete_res deleted;
  ok(lc_collection_delete_entry(pinned, 1, &deleted, error), error);
  lc_collection_lock pin = {0};
  ok(lc_collection_metadata_enter(handle, 0, error), error);
  ok(lc_collection_read_pin(handle, handle->head.snapshot, &pin, error), error);
  lc_collection_metadata_leave(handle);

  lc_pouch_maintenance_options options = {.ns = work_namespace, .force = 1};
  lc_pouch_maintenance_result result = {0};
  ok(lc_pouch_maintenance_run(runtime(owner), &options, &result, error), error);
  REQUIRE(result.compacted_collection_count == 1 &&
          result.cleanup_pending_count);
  lc_pouch_maintenance_result_cleanup(NULL, &result);

  ps_budget *claim = NULL;
  ps_space *space = NULL;
  if (pressure) {
    ps_budget_plan plan = {.payload_bytes = 5 * PS_MIB};
    space = ps_root_space(ps_pouch_state_root(runtime(owner)));
    ok(ps_budget_open(space, &plan, &claim, error), error);
    int level;
    uint64_t available;
    ok(ps_maintenance_pressure(space, 0, &level, &available, error), error);
    REQUIRE(level == PS_PRESSURE_ADVISORY);
  }
  lc_pouch_test_compaction_observe = observe;
  lc_client *worker = open_client(path, single_file, 0, 1, 1, 0, NULL, error);
  uint64_t deadline = ns_now(CLOCK_MONOTONIC) + UINT64_C(10000000000);
  while (sample().pending == 0 && ns_now(CLOCK_MONOTONIC) < deadline)
    pause_ms(10);
  REQUIRE(sample().pending > 0);
  lc_collection *other = collection(worker, "independent", 1, error);
  REQUIRE(append(other, 1, body, sizeof(body) - 1, error) == 1);
  expect_collection(other, 1, 1, error);
  uint64_t started = ns_now(CLOCK_MONOTONIC);
  observations before = sample();
  pause_ms(2200);
  observations after = sample();
  unsigned int attempts = after.attempts - before.attempts;
  uint64_t seconds = (ns_now(CLOCK_MONOTONIC) - started) / UINT64_C(1000000000);
  /* Advisory pressure shares the regular cadence from its initial pass.
   * Allow a pass already in flight when sampling began. */
  uint64_t limit = 1 + (seconds + 1);
  printf("Pinned window: %u passes, %u pending, %u errors, limit %llu\n",
         attempts, after.pending - before.pending, after.errors,
         (unsigned long long)limit);
  REQUIRE(!after.errors && after.pending > before.pending);
  REQUIRE(attempts <= limit);
  if (claim) {
    REQUIRE(ps_maintenance_pressure_requested(space));
    ok(ps_budget_close(claim, error), error);
    ps_stats status;
    ok(ps_status(space, &status, error), error);
    REQUIRE(status.capacity == PS_DEFAULT_CAPACITY);
  }

  lc_collection_lock_leave(&pin);
  deadline = ns_now(CLOCK_MONOTONIC) + UINT64_C(10000000000);
  observations done;
  do {
    done = sample();
    if (done.reclaimed > after.reclaimed && done.settled > after.settled)
      break;
    pause_ms(10);
  } while (ns_now(CLOCK_MONOTONIC) < deadline);
  REQUIRE(!done.errors && done.reclaimed > after.reclaimed &&
          done.settled > after.settled);
  expect_collection(pinned, 1, 2, error);
  expect_collection(other, 1, 1, error);
  lc_collection_close(other);
  lc_client_close(worker);
  lc_pouch_test_compaction_observe = NULL;
  lc_collection_close(pinned);
  lc_client_close(owner);
}

static ssize_t count_record_reads(void *context, int fd, void *bytes, size_t n,
                                  off_t offset) {
  size_t *count = context;
  ssize_t rc = ps_native_io.read(NULL, fd, bytes, n, offset);
  if (n == LC_POUCH_RECORD_HEADER_BYTES && rc == (ssize_t)n &&
      !memcmp(bytes, "LHCP", 4))
    ++*count;
  return rc;
}

static void projection_reuse(const char *path, lc_error *error) {
  static const char body[] = "{\"n\":1}";
  size_t reads = 0;
  ps_io io = ps_native_io;
  io.context = &reads;
  io.read = count_record_reads;
  lc_client *worker = open_client(path, 1, 1, 0, 1, 1, &io, error);
  lc_client *peer = open_client(path, 1, 0, 0, 1, 1, NULL, error);
  lc_collection *events = collection(peer, work_namespace, 1, error);
  lc_pouch_maintenance_options background = {.ns = work_namespace,
                                             .background = 1};
  lc_pouch_maintenance_options forced = {.ns = work_namespace, .force = 1};
  lc_pouch_maintenance_result result = {0};
  for (uint64_t i = 1; i <= 48; ++i)
    REQUIRE(append(events, i, body, sizeof(body) - 1, error) == i);
  /* A snapshot with an empty active segment isolates projection replay from
   * the canonical recovery that a checkpoint generation change can require. */
  ok(lc_pouch_maintenance_run(runtime(peer), &forced, &result, error), error);
  REQUIRE(result.compacted_collection_count == 1);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  reads = 0;
  ok(lc_pouch_maintenance_run(runtime(worker), &background, NULL, error),
     error);
  size_t cold = reads;
  reads = 0;
  ok(lc_pouch_maintenance_run(runtime(worker), &background, NULL, error),
     error);
  printf("Maintenance canonical header reads: cold=%zu warm=%zu\n", cold,
         reads);
  REQUIRE(cold >= 40 && reads < cold / 4);
  ps_space *space = ps_root_space(ps_pouch_state_root(runtime(worker)));
  ps_stats status;
  ok(ps_status(space, &status, error), error);
  REQUIRE(!status.retained_payload_bytes);

  REQUIRE(append(events, 49, body, sizeof(body) - 1, error) == 49);
  lc_collection_metadata_req policy = {.has_max_entries = 1, .max_entries = 48};
  lc_collection_info info;
  ok(lc_collection_metadata(events, &policy, &info, error), error);
  ok(lc_pouch_maintenance_run(runtime(worker), &background, &result, error),
     error);
  REQUIRE(result.retention_deleted_state_count == 1);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  expect_collection(events, 48, 49, error);

  REQUIRE(append(events, 50, body, sizeof(body) - 1, error) == 50);
  ok(lc_pouch_maintenance_run(runtime(peer), &forced, &result, error), error);
  REQUIRE(result.compacted_collection_count == 1);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  reads = 0;
  ok(lc_pouch_maintenance_run(runtime(worker), &background, NULL, error),
     error);
  REQUIRE(reads >= 40);
  expect_collection(events, 48, 50, error);

  ok(lc_collection_drop(events, error), error);
  lc_collection_close(events);
  events = collection(peer, work_namespace, 1, error);
  REQUIRE(append(events, 1, body, sizeof(body) - 1, error) == 1);
  ok(lc_pouch_maintenance_run(runtime(worker), &background, NULL, error),
     error);
  expect_collection(events, 1, 1, error);
  lc_collection_close(events);
  lc_client_close(peer);
  lc_client_close(worker);
}

static int stop_compactor(void *context, lc_collection *events, int phase,
                          lc_error *error) {
  (void)events;
  (void)error;
  if (phase == *(const int *)context)
    _exit(73);
  return LC_OK;
}

static void projection_recovery(const char *path, int durable, int phase,
                                lc_error *error) {
  static const char body[] = "{\"n\":1}";
  const size_t body_bytes = sizeof(body) - 1;
  lc_pouch_maintenance_options background = {.ns = work_namespace,
                                             .background = 1};
  int start[2];
  char ready;
  REQUIRE(!pipe(start));
  /* Fork before opening either client. The child opens an independent root
   * after the parent has populated its resident maintenance projection. */
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    REQUIRE(!close(start[1]));
    REQUIRE(read(start[0], &ready, 1) == 1);
    REQUIRE(!close(start[0]));
    lc_client *peer = open_client(path, 1, 0, 0, 1, durable, NULL, error);
    lc_collection *events = collection(peer, work_namespace, 0, error);
    REQUIRE(append(events, 2, body, body_bytes, error) == 2);
    /* Promise the same append prefix at both compaction interruption points. */
    ok(ps_flush(ps_root_space(ps_pouch_state_root(runtime(peer))), error),
       error);
    lc_collection_compact_test_hook = stop_compactor;
    lc_collection_compact_test_context = &phase;
    ok(lc_collection_compact(events, 1, 0, NULL, error), error);
    _exit(74);
  }
  REQUIRE(!close(start[0]));
  lc_client *worker = open_client(path, 1, 1, 0, 1, durable, NULL, error);
  lc_collection *events = collection(worker, work_namespace, 1, error);
  lc_collection_metadata_req policy = {.has_max_entries = 1, .max_entries = 1};
  lc_collection_info info;
  REQUIRE(append(events, 1, body, body_bytes, error) == 1);
  ok(lc_collection_metadata(events, &policy, &info, error), error);
  ok(lc_collection_compact(events, 1, 0, NULL, error), error);
  for (int pass = 0; pass < 2; ++pass)
    ok(lc_pouch_maintenance_run(runtime(worker), &background, NULL, error),
       error);
  REQUIRE(write(start[1], "r", 1) == 1);
  REQUIRE(!close(start[1]));
  int exit_status;
  REQUIRE(waitpid(child, &exit_status, 0) == child && WIFEXITED(exit_status) &&
          WEXITSTATUS(exit_status) == 73);

  lc_pouch_maintenance_result result = {0};
  ok(lc_pouch_maintenance_run(runtime(worker), &background, &result, error),
     error);
  REQUIRE(result.retention_deleted_state_count == 1);
  unsigned long reclaimed = result.cleanup_deleted_count;
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  expect_collection(events, 1, 2, error);
  lc_collection_append_res old =
      append_with_receipt(events, 1, body, body_bytes, error);
  lc_collection_append_res latest =
      append_with_receipt(events, 2, body, body_bytes, error);
  REQUIRE(old.duplicate && old.deleted && old.position == 1);
  REQUIRE(latest.duplicate && !latest.deleted && latest.position == 2);
  ps_space *space = ps_root_space(ps_pouch_state_root(runtime(worker)));
  uint64_t deadline = ns_now(CLOCK_MONOTONIC) + UINT64_C(8000000000);
  ps_stats status;
  do {
    ok(lc_pouch_maintenance_run(runtime(worker), &background, &result, error),
       error);
    reclaimed += result.cleanup_deleted_count;
    ok(ps_status(space, &status, error), error);
    int settled = !result.cleanup_pending_count;
    lc_pouch_maintenance_result_cleanup(NULL, &result);
    if (settled && reclaimed && !status.retained_payload_bytes)
      break;
    REQUIRE(ns_now(CLOCK_MONOTONIC) < deadline);
    pause_ms(100);
  } while (1);
  printf("Projection recovery durable=%d phase=%d reclaimed=%lu\n", durable,
         phase, reclaimed);
  lc_collection_close(events);
  lc_client_close(worker);
}

static int stop_provisional_edit(void *context, lc_collection *events,
                                 int phase, lc_error *error) {
  (void)context;
  (void)events;
  (void)error;
  if (phase == LC_COLLECTION_EDIT_BODY)
    _exit(73);
  return LC_OK;
}

static void append_admission(const char *path, int durable, lc_error *error) {
  static const char body[] = "{\"n\":1}";
  int start[2], exit_status;
  char ready;
  REQUIRE(!pipe(start));
  pid_t child = fork();
  REQUIRE(child >= 0);
  if (!child) {
    REQUIRE(!close(start[1]));
    REQUIRE(read(start[0], &ready, 1) == 1);
    REQUIRE(!close(start[0]));
    lc_client *editor = open_client(path, 1, 0, 0, 1, durable, NULL, error);
    lc_collection *events = collection(editor, work_namespace, 0, error);
    lc_collection_head_res head = {0};
    ok(lc_collection_head(events, &head, error), error);
    lc_collection_edit_range cut = {0, 1};
    lc_collection_append_req replacement;
    lc_collection_append_req_init(&replacement);
    replacement.append_id = "interrupted-body";
    lc_collection_edit_req edit = {.operation_id = "interrupted-edit",
                                   .expected_head = head.etag,
                                   .ranges = &cut,
                                   .range_count = 1,
                                   .replacement = &replacement};
    lc_source *source = NULL;
    lc_collection_edit_res result;
    ok(lc_source_from_memory(body, sizeof(body) - 1, &source, error), error);
    lc_collection_edit_test_hook = stop_provisional_edit;
    ok(lc_collection_edit(events, &edit, source, &result, error), error);
    _exit(74);
  }
  REQUIRE(!close(start[0]));
  lc_client *owner = open_client(path, 1, 1, 0, 1, durable, NULL, error);
  lc_collection *events = collection(owner, work_namespace, 1, error);
  REQUIRE(append(events, 1, body, sizeof(body) - 1, error) == 1);
  lc_collection_close(events);
  lc_client *peer = open_client(path, 1, 0, 0, 1, durable, NULL, error);
  ps_space *space = ps_root_space(ps_pouch_state_root(runtime(owner)));
  ps_space *peer_space = ps_root_space(ps_pouch_state_root(runtime(peer)));
  ps_stats before, after;
  ok(ps_refresh(peer_space, error), error);
  ok(ps_metadata_repack(peer_space, error), error);
  ok(ps_status(space, &before, error), error);
  events = collection(owner, work_namespace, 0, error);
  expect_collection(events, 1, 1, error);
  ok(ps_status(space, &after, error), error);
  REQUIRE(after.syncs == before.syncs);
  lc_collection_close(events);

  REQUIRE(write(start[1], "r", 1) == 1);
  REQUIRE(!close(start[1]));
  REQUIRE(waitpid(child, &exit_status, 0) == child && WIFEXITED(exit_status) &&
          WEXITSTATUS(exit_status) == 73);
  /* Even a later peer checkpoint must preserve revocation by the edit. */
  ok(ps_refresh(peer_space, error), error);
  ok(ps_metadata_repack(peer_space, error), error);
  ok(ps_status(space, &before, error), error);
  events = collection(owner, work_namespace, 0, error);
  ok(ps_status(space, &after, error), error);
  REQUIRE(after.syncs > before.syncs);
  expect_collection(events, 1, 1, error);
  lc_collection_edit_status_res receipt = {0};
  ok(lc_collection_edit_status(events, "interrupted-edit", &receipt, error),
     error);
  REQUIRE(!receipt.found);
  REQUIRE(append(events, 2, body, sizeof(body) - 1, error) == 2);
  ok(ps_flush(space, error), error);
  lc_collection_close(events);
  lc_client_close(peer);
  lc_client_close(owner);

  owner = open_client(path, 1, 0, 0, 1, durable, NULL, error);
  space = ps_root_space(ps_pouch_state_root(runtime(owner)));
  ok(ps_status(space, &before, error), error);
  events = collection(owner, work_namespace, 0, error);
  ok(ps_status(space, &after, error), error);
  REQUIRE(after.syncs > before.syncs);
  expect_collection(events, 2, 2, error);
  lc_collection_close(events);
  lc_client_close(owner);
}

#ifdef __linux__
static uint64_t file_blocks;

static int count_file_blocks(const char *path, const struct stat *status,
                             int kind, struct FTW *walk) {
  (void)path;
  (void)walk;
  if (kind == FTW_F)
    file_blocks += (uint64_t)status->st_blocks;
  return 0;
}

static uint64_t allocated_bytes(lc_client *client, const char *path,
                                int single_file, lc_error *error) {
  if (single_file) {
    ps_space *space = ps_root_space(ps_pouch_state_root(runtime(client)));
    ps_stats status;
    ok(ps_status(space, &status, error), error);
    return status.allocated_bytes;
  }
  file_blocks = 0;
  REQUIRE(nftw(path, count_file_blocks, 16, FTW_PHYS) == 0);
  return file_blocks * 512;
}

static int compare_latency(const void *a, const void *b) {
  uint64_t left = *(const uint64_t *)a, right = *(const uint64_t *)b;
  return (left > right) - (left < right);
}

static void soak(const char *path, int single_file, int durable, int mixed,
                 lc_error *error) {
  enum { windows = 30, per_window = 16, kept = 32, payload_size = 8192 };
  static const char *const namespaces[] = {work_namespace, "independent"};
  static const char *const names[] = {"events", "more-events"};
  lc_collection *events[4];
  const size_t collections = mixed ? 4 : 1;
  const size_t samples = windows * per_window;
  char *body = malloc(payload_size);
  uint64_t *latencies = malloc(samples * sizeof(*latencies));
  REQUIRE(body && latencies);
  memcpy(body, "{\"v\":\"", 6);
  memset(body + 6, 'x', payload_size - 8);
  memcpy(body + payload_size - 2, "\"}", 2);

  observe_all_namespaces = 1;
  lc_pouch_test_compaction_observe = observe;
  lc_client *client =
      open_client(path, single_file, 1, 1, 1, durable, NULL, error);
  for (size_t i = 0; i < collections; ++i)
    events[i] =
        named_collection(client, namespaces[i / 2], names[i % 2], 1, error);
  clockid_t compactor;
  REQUIRE(
      !pthread_getcpuclockid(runtime(client)->compaction_thread, &compactor));
  printf("backend=%s durable=%d interval_s=1 collections=%zu "
         "live_per_collection=%d "
         "payload_bytes=%d\n",
         single_file ? "single-file" : "multifile", durable, collections, kept,
         payload_size);
  uint64_t cpu_start = ns_now(compactor);
  uint64_t write_start = ns_now(CLOCK_MONOTONIC);
  for (size_t window = 0; window < windows; ++window) {
    uint64_t cpu_before = ns_now(compactor);
    uint64_t wall_before = ns_now(CLOCK_MONOTONIC);
    observations before = sample();
    for (size_t i = 0; i < per_window; ++i) {
      size_t seq = window * per_window + i;
      lc_collection *target = events[seq % collections];
      size_t next_position = seq / collections + 1;
      uint64_t begin = ns_now(CLOCK_MONOTONIC);
      uint64_t position =
          append(target, next_position, body, payload_size, error);
      REQUIRE(position == next_position);
      if (position > kept) {
        lc_collection_delete_res deleted;
        ok(lc_collection_delete_entry(target, position - kept, &deleted, error),
           error);
      }
      latencies[seq] = ns_now(CLOCK_MONOTONIC) - begin;
      pause_ms(50);
    }
    observations after = sample();
    REQUIRE(!after.errors);
    printf("window=%zu phase=writer wall_ms=%.3f compactor_cpu_ms=%.3f "
           "passes=%u reclaim_passes=%u\n",
           window + 1, (ns_now(CLOCK_MONOTONIC) - wall_before) / 1e6,
           (ns_now(compactor) - cpu_before) / 1e6,
           after.attempts - before.attempts,
           after.reclaimed - before.reclaimed);
  }
  uint64_t write_wall = ns_now(CLOCK_MONOTONIC) - write_start;
  uint64_t write_cpu = ns_now(compactor) - cpu_start;
  uint64_t bytes_before = allocated_bytes(client, path, single_file, error);
  uint64_t drain_start = ns_now(compactor);
  for (unsigned int window = 0; window < 6; ++window) {
    uint64_t cpu_before = ns_now(compactor);
    observations before = sample();
    pause_ms(1000);
    observations after = sample();
    REQUIRE(!after.errors);
    printf("window=%u phase=drain compactor_cpu_ms=%.3f passes=%u "
           "reclaim_passes=%u\n",
           window + 1, (ns_now(compactor) - cpu_before) / 1e6,
           after.attempts - before.attempts,
           after.reclaimed - before.reclaimed);
  }
  uint64_t drain_cpu = ns_now(compactor) - drain_start;
  uint64_t bytes_after = allocated_bytes(client, path, single_file, error);
  struct rusage usage;
  REQUIRE(!getrusage(RUSAGE_SELF, &usage));
  REQUIRE(sample().reclaimed > 0);
  qsort(latencies, samples, sizeof(*latencies), compare_latency);
  printf("writer_wall_ms=%.3f writer_phase_compactor_cpu_ms=%.3f "
         "append_delete_p95_ms=%.3f append_delete_p99_ms=%.3f "
         "drain_compactor_cpu_ms=%.3f occupied_before=%llu occupied_after=%llu "
         "peak_rss_kib=%ld\n",
         write_wall / 1e6, write_cpu / 1e6,
         latencies[(95 * samples + 99) / 100 - 1] / 1e6,
         latencies[(99 * samples + 99) / 100 - 1] / 1e6, drain_cpu / 1e6,
         (unsigned long long)bytes_before, (unsigned long long)bytes_after,
         usage.ru_maxrss);
  if (bytes_after < bytes_before)
    printf("drain_cpu_ms_per_net_MiB_released=%.3f\n",
           (drain_cpu / 1e6) / ((bytes_before - bytes_after) / (double)PS_MIB));

  for (size_t i = 0; i < collections; ++i) {
    expect_collection(events[i], kept, samples / collections, error);
    lc_collection_close(events[i]);
  }
  lc_client_close(client);
  lc_pouch_test_compaction_observe = NULL;
  client = open_client(path, single_file, 0, 0, 1, durable, NULL, error);
  for (size_t i = 0; i < collections; ++i) {
    events[i] =
        named_collection(client, namespaces[i / 2], names[i % 2], 0, error);
    expect_collection(events[i], kept, samples / collections, error);
    lc_collection_close(events[i]);
  }
  lc_client_close(client);
  free(latencies);
  free(body);
}
#endif

int main(int argc, char **argv) {
  char prefix[1024], pattern[1050], scratch[1050], path[1100];
  lc_error error = {0};
  REQUIRE(argc == 3 || argc == 4);
  int pressure = !strcmp(argv[2], "single-file-pressure");
  int projection = !strcmp(argv[2], "single-file-projection");
  int recovery = !strcmp(argv[2], "single-file-projection-recovery");
  int admission = !strcmp(argv[2], "single-file-append-admission");
  int single_file = pressure || projection || recovery || admission ||
                    !strcmp(argv[2], "single-file");
  REQUIRE(single_file || !strcmp(argv[2], "multifile"));
  REQUIRE(snprintf(prefix, sizeof(prefix), "%s/compaction-", argv[1]) <
          (int)sizeof(prefix));
  REQUIRE(snprintf(pattern, sizeof(pattern), "%sXXXXXX", prefix) <
          (int)sizeof(pattern));
  REQUIRE(lc_test_tmp_mkdtemp(pattern, scratch, sizeof(scratch), prefix));
  REQUIRE(snprintf(path, sizeof(path), "%s/store", scratch) <
          (int)sizeof(path));
  if (argc == 3) {
    if (recovery) {
      static const int phases[] = {LC_COLLECTION_COMPACT_COPIED,
                                   LC_COLLECTION_COMPACT_HEAD_PUBLISHED};
      char case_path[1150];
      for (int durable = 0; durable <= 1; ++durable) {
        for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); ++i) {
          REQUIRE(snprintf(case_path, sizeof(case_path), "%s-%d-%zu", path,
                           durable, i) < (int)sizeof(case_path));
          projection_recovery(case_path, durable, phases[i], &error);
        }
      }
    } else if (admission) {
      char case_path[1150];
      for (int durable = 0; durable <= 1; ++durable) {
        REQUIRE(snprintf(case_path, sizeof(case_path), "%s-%d", path, durable) <
                (int)sizeof(case_path));
        append_admission(case_path, durable, &error);
      }
    } else if (projection)
      projection_reuse(path, &error);
    else
      regression(path, single_file, pressure, &error);
    printf("%s background compaction checks passed\n", argv[2]);
  } else {
#ifdef __linux__
    int mixed = !strcmp(argv[3], "soak-mixed") ||
                !strcmp(argv[3], "soak-mixed-durable");
    int durable = !strcmp(argv[3], "soak-durable") ||
                  !strcmp(argv[3], "soak-mixed-durable");
    REQUIRE(!pressure && !projection && !recovery && !admission &&
            (mixed || durable || !strcmp(argv[3], "soak")));
    soak(path, single_file, durable, mixed, &error);
#else
    REQUIRE(0 && "the soak requires a Linux worker CPU clock");
#endif
  }
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(scratch, prefix);
  return 0;
}
