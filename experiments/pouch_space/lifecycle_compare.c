#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "coordination.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_query_index.h"
#include "lc_xa_internal.h"
#include "pouch.h"
#include "pouch_internal.h"
#include <dirent.h>
#include <errno.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

typedef struct comparison comparison;
enum { ROUNDS_PER_WINDOW = 16, MAX_ROUNDS = 128, MAX_WRITERS = 16 };
typedef struct work_window {
  double started, finished;
} work_window;
typedef struct open_phases {
  double owner, joins, joins_cpu, slowest_join;
  uint64_t parallel_sync_calls;
} open_phases;
typedef struct index_phase {
  double seconds, cpu_seconds, sync_seconds;
  double started_seconds, started_cpu;
  uint64_t sync_calls;
  size_t queries;
} index_phase;
typedef struct worker {
  comparison *run;
  size_t ordinal;
  char ns[64];
  lc_client *producer_client;
  lc_outbox *outbox, *producer_outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_xa_store *journal;
  char owner[LC_XID_STRING_SIZE], resource[LC_XID_STRING_SIZE];
  double index_seconds;
  index_phase mutations;
  lc_index_seq index_seq;
  size_t revision;
  work_window windows[MAX_ROUNDS / ROUNDS_PER_WINDOW];
  index_phase index_phases[3], reopen_phases[2];
  int result;
} worker;
struct comparison {
  size_t rows, bytes, writers, rounds;
  int container, outbox, xa, index, sync, started, preopened, stable_index;
  int shared, warm_queues, reclaim, parallel_joins;
  lc_client *client;
  worker *workers;
  double *latencies;
  char endpoint[1400], digest[72];
  pthread_mutex_t mutex;
  pthread_cond_t condition;
};
typedef struct producer {
  lc_source source;
  size_t size, offset;
  size_t revision;
} producer;
typedef struct consumer {
  lc_sink sink;
  size_t size, offset;
  size_t revision;
} consumer;
static uint64_t data_syncs, full_syncs;
static double sync_seconds;
static pthread_mutex_t sync_mutex = PTHREAD_MUTEX_INITIALIZER;
/* Attribute synchronous calls only to the thread executing a measured phase.
 * Process-wide snapshots around overlapping writers would count peer work. */
static pthread_key_t sync_phase_key;
static int sync_phase_key_initialized;
static double now(clockid_t clock) {
  struct timespec time;
  if (clock_gettime(clock, &time) != 0)
    abort();
  return (double)time.tv_sec + (double)time.tv_nsec / 1e9;
}
int __real_fdatasync(int);
int __real_fsync(int);
int __wrap_fdatasync(int fd) {
  double start = now(CLOCK_MONOTONIC);
  int rc = __real_fdatasync(fd);
  double elapsed = now(CLOCK_MONOTONIC) - start;
  index_phase *phase =
      sync_phase_key_initialized ? pthread_getspecific(sync_phase_key) : NULL;
  if (phase) {
    ++phase->sync_calls;
    phase->sync_seconds += elapsed;
  }
  pthread_mutex_lock(&sync_mutex);
  ++data_syncs;
  sync_seconds += elapsed;
  pthread_mutex_unlock(&sync_mutex);
  return rc;
}
int __wrap_fsync(int fd) {
  double start = now(CLOCK_MONOTONIC);
  int rc = __real_fsync(fd);
  double elapsed = now(CLOCK_MONOTONIC) - start;
  index_phase *phase =
      sync_phase_key_initialized ? pthread_getspecific(sync_phase_key) : NULL;
  if (phase) {
    ++phase->sync_calls;
    phase->sync_seconds += elapsed;
  }
  pthread_mutex_lock(&sync_mutex);
  ++full_syncs;
  sync_seconds += elapsed;
  pthread_mutex_unlock(&sync_mutex);
  return rc;
}
static void require(int valid, const char *message) {
  if (!valid) {
    fprintf(stderr, "Pouch lifecycle comparison: %s\n", message);
    exit(1);
  }
}
static void check(int rc, lc_error *e) {
  if (rc != LC_OK) {
    fprintf(stderr, "Pouch lifecycle comparison %d: %s [%s]\n", rc,
            e->message ? e->message : "", e->server_code ? e->server_code : "");
    exit(1);
  }
}
static size_t number(const char *text, size_t minimum, size_t maximum) {
  char *end;
  unsigned long result;
  errno = 0;
  result = strtoul(text, &end, 10);
  require(!errno && end != text && !*end && text[0] != '-' &&
              result >= minimum && result <= maximum,
          "invalid numeric argument");
  return (size_t)result;
}
static void revision_text(size_t revision, char text[3]) {
  if (revision <= 26U) {
    text[0] = (char)('a' + revision - 1U);
    text[1] = '\0';
  } else {
    revision -= 27U;
    text[0] = (char)('a' + revision / 26U);
    text[1] = (char)('a' + revision % 26U);
    text[2] = '\0';
  }
}
static unsigned char byte_at(size_t offset, size_t size, size_t revision) {
  const char *prefix = revision > 26U ? "{\"r\":\"aa\",\"v\":\""
                       : revision     ? "{\"r\":\"a\",\"v\":\""
                                      : "{\"v\":\"";
  size_t prefix_bytes =
      revision ? sizeof("{\"r\":\"a\",\"v\":\"") - 1U + (revision > 26U)
               : sizeof("{\"v\":\"") - 1U;
  if (revision && (offset == 6U || (revision > 26U && offset == 7U))) {
    char text[3];
    revision_text(revision, text);
    return (unsigned char)text[offset - 6U];
  }
  if (offset < prefix_bytes)
    return (unsigned char)prefix[offset];
  return offset == size - 2U   ? '"'
         : offset == size - 1U ? '}'
                               : (unsigned char)('a' + offset % 26U);
}
static size_t produce(lc_source *self, void *bytes, size_t size, lc_error *e) {
  producer *source = self->impl;
  (void)e;
  if (size > 4096U)
    size = 4096U;
  if (size > source->size - source->offset)
    size = source->size - source->offset;
  for (size_t i = 0U; i < size; ++i)
    ((unsigned char *)bytes)[i] =
        byte_at(source->offset + i, source->size, source->revision);
  source->offset += size;
  return size;
}
static void producer_init(producer *source, size_t bytes) {
  memset(source, 0, sizeof(*source));
  source->source.impl = source;
  source->source.read = produce;
  source->size = bytes;
}
static int consume(lc_sink *self, const void *bytes, size_t size, lc_error *e) {
  consumer *sink = self->impl;
  const unsigned char *data = bytes;
  int valid = size <= sink->size - sink->offset;
  for (size_t i = 0U; valid && i < size; ++i)
    valid = data[i] == byte_at(sink->offset + i, sink->size, sink->revision);
  if (!valid) {
    lc_error_set(e, LC_ERR_PROTOCOL, 0L, "benchmark payload differs", NULL,
                 NULL, "pouch");
    return 0;
  }
  sink->offset += size;
  return 1;
}
static void payload_digest(comparison *run) {
  static const char hex[] = "0123456789abcdef";
  unsigned char scratch[512], digest[32];
  unsigned int length = 0U;
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  require(context && EVP_DigestInit_ex(context, EVP_sha256(), NULL) == 1,
          "digest initialization");
  for (size_t at = 0U; at < run->bytes;) {
    size_t count =
        run->bytes - at < sizeof(scratch) ? run->bytes - at : sizeof(scratch);
    for (size_t i = 0U; i < count; ++i)
      scratch[i] = byte_at(at + i, run->bytes, 0);
    require(EVP_DigestUpdate(context, scratch, count) == 1, "digest update");
    at += count;
  }
  require(EVP_DigestFinal_ex(context, digest, &length) == 1 && length == 32U,
          "digest finalization");
  EVP_MD_CTX_free(context);
  memcpy(run->digest, "sha256:", 7U);
  for (size_t i = 0U; i < 32U; ++i) {
    run->digest[7U + 2U * i] = hex[digest[i] >> 4U];
    run->digest[8U + 2U * i] = hex[digest[i] & 15U];
  }
  run->digest[71] = 0;
}
static void entry_init(worker *w, size_t row, lc_outbox_entry *entry,
                       char id[64]) {
  snprintf(id, 64U, "worker-%lu-row-%lu", (unsigned long)w->ordinal,
           (unsigned long)row);
  lc_outbox_entry_init(entry);
  entry->operation_id = entry->effect_id = entry->effect_key = id;
  entry->kind = "benchmark";
  entry->destination = "local";
  entry->content_type = "application/json";
  entry->payload_digest = w->run->digest;
}
static void open_client(const comparison *run, const lc_client_config *config,
                        int create, lc_client **out, lc_error *e) {
  check(run->container ? ps_pouch_client_open(config, create, NULL, out, e)
                       : lc_pouch_legacy_client_open(config, out, e),
        e);
}
typedef struct join_job {
  const comparison *run;
  const lc_client_config *config;
  lc_client **out;
  pthread_barrier_t *start;
  index_phase measured;
} join_job;
static void index_phase_begin(index_phase *);
static void index_phase_end(index_phase *);
static void join_start(pthread_barrier_t *barrier) {
  int rc = pthread_barrier_wait(barrier);
  require(!rc || rc == PTHREAD_BARRIER_SERIAL_THREAD, "start live joins");
}
static void *run_join(void *context) {
  join_job *job = context;
  lc_error e = {0};
  join_start(job->start);
  index_phase_begin(&job->measured);
  open_client(job->run, job->config, 0, job->out, &e);
  index_phase_end(&job->measured);
  lc_error_cleanup(&e);
  return NULL;
}
static void open_parallel_joins(comparison *run, const lc_client_config *config,
                                open_phases *phases) {
  join_job jobs[MAX_WRITERS] = {0};
  pthread_t threads[MAX_WRITERS];
  pthread_barrier_t barrier;
  require(!pthread_barrier_init(&barrier, NULL, (unsigned)run->writers + 1U),
          "initialize live join start");
  for (size_t i = 0; i < run->writers; ++i) {
    jobs[i].run = run;
    jobs[i].config = config;
    jobs[i].out = &run->workers[i].producer_client;
    jobs[i].start = &barrier;
    require(!pthread_create(&threads[i], NULL, run_join, &jobs[i]),
            "start live join worker");
  }
  double started = now(CLOCK_MONOTONIC);
  join_start(&barrier);
  for (size_t i = 0; i < run->writers; ++i) {
    require(!pthread_join(threads[i], NULL), "finish live join worker");
    const index_phase *measured = &jobs[i].measured;
    phases->joins_cpu += measured->cpu_seconds;
    phases->parallel_sync_calls += measured->sync_calls;
    if (measured->seconds > phases->slowest_join)
      phases->slowest_join = measured->seconds;
  }
  phases->joins = now(CLOCK_MONOTONIC) - started;
  require(!pthread_barrier_destroy(&barrier), "close live join start");
}
static void open_store(comparison *run, int create, open_phases *phases,
                       lc_error *e) {
  lc_client_config config;
  lc_pouch_settings settings;
  const char *endpoint = run->endpoint;
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_DURABLE_SYNC |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_BACKGROUND_COMPACTION |
      LC_POUCH_SETTING_QUEUE_WATCH | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_QUERY_ENGINE | LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  settings.single_writer = !run->shared;
  settings.durable_sync = run->sync;
  settings.compression = "none";
  settings.query_engine = run->index ? "index" : "scan";
  settings.query_indexing_enabled = run->index;
  settings.background_compaction_enabled = run->reclaim;
  if (run->index) {
    settings.set_mask |= LC_POUCH_SETTING_INDEXER_FLUSH_DOCS |
                         LC_POUCH_SETTING_INDEXER_FLUSH_INTERVAL_SECONDS;
    settings.indexer_flush_docs = UINT64_MAX;
    settings.indexer_flush_interval_seconds = 3600U;
  }
  settings.segment_target_bytes = PS_MIB;
  config.endpoints = &endpoint;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_settings = &settings;
  double started = phases ? now(CLOCK_MONOTONIC) : 0;
  open_client(run, &config, create, &run->client, e);
  if (phases) {
    double finished = now(CLOCK_MONOTONIC);
    phases->owner = finished - started;
    started = finished;
  }
  settings.background_compaction_enabled = 0;
  if (phases && run->parallel_joins) {
    open_parallel_joins(run, &config, phases);
  } else if (run->shared) {
    double cpu = phases ? now(CLOCK_THREAD_CPUTIME_ID) : 0;
    for (size_t i = 0U; i < run->writers; ++i) {
      double opened_at = phases ? now(CLOCK_MONOTONIC) : 0;
      open_client(run, &config, 0, &run->workers[i].producer_client, e);
      if (phases) {
        double elapsed = now(CLOCK_MONOTONIC) - opened_at;
        if (elapsed > phases->slowest_join)
          phases->slowest_join = elapsed;
      }
    }
    if (phases) {
      phases->joins = now(CLOCK_MONOTONIC) - started;
      phases->joins_cpu = now(CLOCK_THREAD_CPUTIME_ID) - cpu;
    }
  }
  for (size_t i = 0U; run->outbox && i < run->writers; ++i) {
    lc_outbox_config outbox;
    lc_outbox_config_init(&outbox);
    outbox.ns = run->workers[i].ns;
    outbox.owner = "benchmark";
    check(
        lc_client_new_outbox(run->client, &outbox, &run->workers[i].outbox, e),
        e);
    if (run->shared)
      check(lc_client_new_outbox(run->workers[i].producer_client, &outbox,
                                 &run->workers[i].producer_outbox, e),
            e);
  }
  for (size_t i = 0U; run->xa && i < run->writers; ++i)
    check(lc_xa_store_open_owned(run->client, run->workers[i].owner,
                                 &run->workers[i].journal, e),
          e);
}
static void close_store(comparison *run) {
  for (size_t i = 0U; i < run->writers; ++i) {
    lc_outbox_close(run->workers[i].producer_outbox);
    run->workers[i].producer_outbox = NULL;
    lc_client_close(run->workers[i].producer_client);
    run->workers[i].producer_client = NULL;
    lc_outbox_dispatcher_close(run->workers[i].dispatcher);
    run->workers[i].dispatcher = NULL;
    lc_outbox_close(run->workers[i].outbox);
    run->workers[i].outbox = NULL;
    lc_xa_store_close(run->workers[i].journal);
    run->workers[i].journal = NULL;
  }
  lc_client_close(run->client);
  run->client = NULL;
}
static int queue_one(worker *w, size_t row, producer *source, consumer *sink,
                     lc_error *e) {
  lc_enqueue_req enqueue;
  lc_enqueue_res enqueued = {0};
  lc_dequeue_req dequeue;
  lc_message *message = NULL;
  lc_client *producer_client =
      w->producer_client ? w->producer_client : w->run->client;
  size_t written = 0U;
  int rc;
  (void)row;
  lc_enqueue_req_init(&enqueue);
  enqueue.ns = w->ns;
  enqueue.queue = "work";
  enqueue.content_type = "application/json";
  rc = lc_enqueue(producer_client, &enqueue, &source->source, &enqueued, e);
  lc_dequeue_req_init(&dequeue);
  dequeue.ns = w->ns;
  dequeue.queue = "work";
  dequeue.owner = "benchmark";
  if (rc == LC_OK)
    rc = lc_dequeue(w->run->client, &dequeue, &message, e);
  if (rc == LC_OK) {
    require(message && !strcmp(message->message_id, enqueued.message_id),
            "queue delivery identity");
    rc = lc_message_write_payload(message, &sink->sink, &written, e);
  }
  if (rc == LC_OK)
    rc = lc_message_ack(message, e);
  if (rc == LC_OK)
    message = NULL;
  lc_message_close(message);
  lc_enqueue_res_cleanup(&enqueued);
  return rc;
}
static void warm_queues(comparison *run, lc_error *e) {
  size_t i;
  /* Prepare one active namespace and stream per writer. Timed work measures
   * steady-state body flow and handoff after cold catalog creation. */
  for (i = 0U; i < run->writers; ++i) {
    producer source;
    consumer sink = {0};
    producer_init(&source, run->bytes);
    sink.size = run->bytes;
    sink.sink.impl = &sink;
    sink.sink.write = consume;
    check(queue_one(&run->workers[i], i, &source, &sink, e), e);
    require(source.offset == run->bytes && sink.offset == run->bytes,
            "warm queue streamed payload coverage");
  }
}
static int outbox_one(worker *w, size_t row, producer *source, consumer *sink,
                      lc_error *e) {
  lc_outbox_entry entry;
  lc_outbox_transaction *transaction = NULL;
  lc_outbox_receipt receipt;
  lc_outbox_commit_result committed;
  lc_outbox_job *job = NULL;
  lc_source *checkpoint = NULL;
  char id[64];
  size_t written = 0U;
  int rc;
  entry_init(w, row, &entry, id);
  lc_outbox_receipt_init(&receipt);
  lc_outbox_commit_result_init(&committed);
  rc = lc_outbox_append(w->producer_outbox ? w->producer_outbox : w->outbox,
                        &entry, &source->source, &transaction, &receipt, e);
  if (rc == LC_OK) {
    require(transaction && !receipt.duplicate, "new outbox effect");
    rc = lc_outbox_transaction_commit(transaction, &committed, e);
  }
  if (rc == LC_OK) {
    require(committed.outbox_receipt_count == 1U, "committed receipt");
    rc = lc_outbox_dispatcher_notify_outbox_key(
        w->dispatcher, committed.outbox_receipts[0].outbox_key, e);
  }
  lc_outbox_transaction_close(transaction);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_commit_result_cleanup(&committed);
  if (rc == LC_OK)
    rc = lc_outbox_dispatcher_next_with_state(w->dispatcher, 5000L, &job, e);
  if (rc == LC_OK) {
    require(job && !strcmp(job->operation_id, id) && lc_outbox_job_state(job),
            "stateful outbox identity");
    rc = lc_outbox_job_write_payload(job, &sink->sink, &written, e);
  }
  if (rc == LC_OK)
    rc = lc_source_from_memory("{}", 2U, &checkpoint, e);
  if (rc == LC_OK)
    rc = lc_lease_update(lc_outbox_job_state(job), checkpoint, NULL, e);
  lc_source_close(checkpoint);
  if (rc == LC_OK)
    rc = lc_outbox_job_complete(job, NULL, e);
  if (rc == LC_OK)
    job = NULL;
  lc_outbox_job_close(job);
  return rc;
}
static void xa_id(size_t value, char id[LC_XID_STRING_SIZE]) {
  require(snprintf(id, LC_XID_STRING_SIZE, "0000000000%09lx0",
                   (unsigned long)value) == 20,
          "benchmark XA identity");
}
static int xa_one(worker *w, size_t row, producer *source, consumer *sink,
                  lc_error *e) {
  lc_xa_branch_record branch = {0};
  lc_xa_snapshot snapshot = {0};
  lc_acquire_req request;
  lc_lease *lease = NULL;
  lc_get_res result = {0};
  int rc;
  xa_id(1000U + row, branch.txn_id);
  strcpy(branch.branch_id, branch.txn_id);
  strcpy(branch.resource_id, w->resource);
  snprintf(branch.gid, sizeof(branch.gid), "lc-benchmark-%s", branch.txn_id);
  strcpy(branch.full_xid, "12345");
  strcpy(branch.system_id, "7695692707854696465");
  strcpy(branch.database_oid, "16384");
  strcpy(branch.role_oid, "16385");
  rc = lc_xa_store_claim_blocking(w->journal, branch.txn_id, 3600L, 0,
                                  &snapshot, e);
  lc_xa_snapshot_cleanup(&snapshot);
  if (rc == LC_OK)
    rc = lc_xa_store_enroll_blocking(w->journal, &branch, e);
  lc_acquire_req_init(&request);
  request.ns = w->ns;
  request.key = branch.txn_id;
  request.owner = w->owner;
  request.txn_id = branch.txn_id;
  request.ttl_seconds = 3600L;
  if (rc == LC_OK)
    rc = lc_acquire(w->run->client, &request, &lease, e);
  if (rc == LC_OK)
    rc = lc_lease_update(lease, &source->source, NULL, e);
  if (rc == LC_OK)
    rc = lc_xa_store_seal_blocking(w->journal, branch.txn_id, e);
  branch.phase = LC_XA_PREPARE_SENT;
  if (rc == LC_OK)
    rc = lc_xa_store_progress_blocking(w->journal, &branch, e);
  if (rc == LC_OK)
    rc = lc_xa_store_prepare_pouch_blocking(w->journal, branch.txn_id, e);
  branch.phase = LC_XA_PREPARED;
  branch.prepared_observed = 1U;
  if (rc == LC_OK)
    rc = lc_xa_store_progress_blocking(w->journal, &branch, e);
  if (rc == LC_OK) {
    rc = lc_xa_store_decide_blocking(w->journal, branch.txn_id, 1, e);
    require(rc == LC_ERR_PENDING && e->server_code &&
                !strcmp(e->server_code, "xa_pending"),
            "XA decision waits for external completion");
    lc_error_cleanup(e);
    lc_error_init(e);
    rc = LC_OK;
  }
  branch.phase = LC_XA_COMMIT_SENT;
  if (rc == LC_OK)
    rc = lc_xa_store_progress_blocking(w->journal, &branch, e);
  branch.phase = LC_XA_COMMITTED;
  branch.evidence = LC_XA_EVIDENCE_NATIVE;
  branch.completed_at_unix = (lc_unix_seconds)time(NULL);
  if (rc == LC_OK)
    rc = lc_xa_store_progress_blocking(w->journal, &branch, e);
  if (rc == LC_OK)
    rc = lc_xa_store_decide_blocking(w->journal, branch.txn_id, 1, e);
  lc_lease_close(lease);
  if (rc == LC_OK)
    rc = lc_get_in_namespace(w->run->client, w->ns, branch.txn_id, NULL,
                             &sink->sink, &result, e);
  lc_get_res_cleanup(&result);
  if (rc == LC_OK)
    rc = lc_xa_store_release_blocking(w->journal, branch.txn_id, e);
  return rc;
}
#include "index_compare.h"
static void *run_worker(void *context) {
  worker *w = context;
  comparison *run = w->run;
  lc_error e;
  lc_error_init(&e);
  pthread_mutex_lock(&run->mutex);
  while (!run->started)
    pthread_cond_wait(&run->condition, &run->mutex);
  pthread_mutex_unlock(&run->mutex);
  w->result = LC_OK;
  const char *phase = "mutation";
  for (size_t round = 0U; round < run->rounds && w->result == LC_OK; ++round) {
    work_window *window = &w->windows[round / ROUNDS_PER_WINDOW];
    phase = "mutation";
    if (run->reclaim)
      w->revision = round + 1U;
    index_phase_begin(&w->mutations);
    if (round % ROUNDS_PER_WINDOW == 0)
      window->started = w->mutations.started_seconds;
    for (size_t row = w->ordinal; row < run->rows && w->result == LC_OK;
         row += run->writers) {
      producer source;
      consumer sink;
      producer_init(&source, run->bytes);
      source.revision = w->revision;
      memset(&sink, 0, sizeof(sink));
      sink.size = run->bytes;
      sink.revision = w->revision;
      sink.sink.impl = &sink;
      sink.sink.write = consume;
      double start = now(CLOCK_MONOTONIC);
      w->result = run->index    ? index_one(w, row, &source, &sink, &e)
                  : run->xa     ? xa_one(w, row, &source, &sink, &e)
                  : run->outbox ? outbox_one(w, row, &source, &sink, &e)
                                : queue_one(w, row, &source, &sink, &e);
      run->latencies[round * run->rows + row] = now(CLOCK_MONOTONIC) - start;
      if (w->result == LC_OK)
        require(source.offset == run->bytes && sink.offset == run->bytes,
                "streamed payload coverage");
    }
    index_phase_end(&w->mutations);
    if (w->result == LC_OK && run->index) {
      phase = "index readiness";
      double start = now(CLOCK_MONOTONIC);
      w->result = index_queries(w, run->client, 1, &e);
      window->finished = now(CLOCK_MONOTONIC);
      w->index_seconds += window->finished - start;
    }
  }
  if (w->result == LC_OK && run->outbox) {
    phase = "outbox cleanup";
    lc_outbox_job *job = NULL;
    w->result =
        lc_outbox_dispatcher_next_with_state(w->dispatcher, 0L, &job, &e);
    require(!job, "stateful cleanup probe has no remaining delivery");
  }
  if (w->result != LC_OK)
    fprintf(stderr, "worker %zu in %s (revision %zu), error %d: %s [%s]%s%s\n",
            w->ordinal, phase, w->revision, w->result,
            e.message ? e.message : "", e.server_code ? e.server_code : "",
            e.detail ? " " : "", e.detail ? e.detail : "");
  lc_error_cleanup(&e);
  return NULL;
}
static int cleanup_namespace(void *context, const char *ns, lc_error *e) {
  lc_pouch *pouch = context;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  options.ns = ns;
  options.cleanup_only = 1;
  int rc = lc_pouch_maintenance_run(pouch, &options, &result, e);
  lc_pouch_maintenance_result_cleanup(&pouch->allocator, &result);
  return rc;
}
static void verify_reopen(comparison *run, lc_error *e) {
  for (size_t i = 0U; i < run->writers; ++i) {
    worker *w = &run->workers[i];
    if (run->index) {
      index_verify_reopen(w, e);
    } else if (run->xa) {
      for (size_t row = i; row < run->rows; row += run->writers) {
        char id[LC_XID_STRING_SIZE];
        lc_get_res value = {0};
        lc_xa_snapshot snapshot = {0};
        consumer sink = {0};
        sink.size = run->bytes;
        sink.sink.impl = &sink;
        sink.sink.write = consume;
        xa_id(1000U + row, id);
        check(lc_get_in_namespace(run->client, w->ns, id, NULL, &sink.sink,
                                  &value, e),
              e);
        lc_get_res_cleanup(&value);
        require(sink.offset == run->bytes,
                "XA applied body survives retirement");
        require(lc_xa_store_snapshot_blocking(w->journal, id, &snapshot, e) ==
                    LC_ERR_INVALID,
                "XA history remains retired after reopen");
        lc_xa_snapshot_cleanup(&snapshot);
        lc_error_cleanup(e);
        lc_error_init(e);
      }
    } else if (!run->outbox) {
      lc_queue_stats_req request;
      lc_queue_stats_res result = {0};
      lc_queue_stats_req_init(&request);
      request.ns = w->ns;
      request.queue = "work";
      check(lc_queue_stats(run->client, &request, &result, e), e);
      require(!result.available && !result.pending_candidates,
              "queue remains drained after reopen");
      lc_queue_stats_res_cleanup(&result);
    } else
      for (size_t row = i; row < run->rows; row += run->writers) {
        lc_outbox_entry entry;
        lc_outbox_transaction *transaction = NULL;
        lc_outbox_receipt receipt;
        char id[64];
        producer source;
        entry_init(w, row, &entry, id);
        producer_init(&source, run->bytes);
        lc_outbox_receipt_init(&receipt);
        check(lc_outbox_append(w->outbox, &entry, &source.source, &transaction,
                               &receipt, e),
              e);
        require(receipt.duplicate && !transaction,
                "completed outbox effect remains idempotent after reopen");
        lc_outbox_receipt_cleanup(&receipt);
      }
  }
}
static uint64_t count_files(const char *path) {
  struct stat status;
  require(lstat(path, &status) == 0, "inspect stored files");
  if (S_ISREG(status.st_mode))
    return 1U;
  require(S_ISDIR(status.st_mode), "stored entry is regular file or directory");
  DIR *directory = opendir(path);
  struct dirent *entry;
  uint64_t count = 0U;
  require(directory != NULL, "open stored directory");
  while ((entry = readdir(directory)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    size_t length = strlen(path) + strlen(entry->d_name) + 2U;
    char *child = malloc(length);
    require(child != NULL, "allocate stored path");
    snprintf(child, length, "%s/%s", path, entry->d_name);
    count += count_files(child);
    free(child);
  }
  require(!closedir(directory), "close stored directory");
  return count;
}
static int compare_double(const void *a, const void *b) {
  double left = *(const double *)a, right = *(const double *)b;
  return left < right ? -1 : left > right;
}
static void report_work_windows(comparison *run, double work_seconds) {
  for (size_t from = 0; from < run->rounds; from += ROUNDS_PER_WINDOW) {
    size_t rounds = run->rounds - from;
    size_t window = from / ROUNDS_PER_WINDOW;
    double first = run->workers[0].windows[window].started;
    double last = run->workers[0].windows[window].finished;
    if (rounds > ROUNDS_PER_WINDOW)
      rounds = ROUNDS_PER_WINDOW;
    for (size_t i = 1; i < run->writers; ++i) {
      const work_window *w = &run->workers[i].windows[window];
      if (w->started < first)
        first = w->started;
      if (w->finished > last)
        last = w->finished;
    }
    require(last > first && last - first <= work_seconds,
            "round window belongs to the work interval");
    double *latencies = run->latencies + from * run->rows;
    size_t items = rounds * run->rows;
    qsort(latencies, items, sizeof(*latencies), compare_double);
    fprintf(stderr,
            "rounds_first=%zu rounds_last=%zu items=%zu work_ms=%.6f "
            "p95_ms=%.6f\n",
            from + 1, from + rounds, items, (last - first) * 1e3,
            latencies[(items - 1) * 95U / 100U] * 1e3);
  }
}
static void report_capacity(lc_client *client, const char *label,
                            size_t owner) {
  if (!client)
    return;
  lc_pouch *pouch = ((lc_client_handle *)client)->pouch;
  ps_stats stats;
  if (ps_status(ps_root_space(ps_pouch_state_root(pouch)), &stats, NULL) !=
      LC_OK)
    return;
  fprintf(stderr,
          "capacity_owner=%s owner_number=%zu checkpoint_bytes=%llu "
          "checkpoint_claim_bytes=%llu workspace_bytes=%llu "
          "payload_claim_bytes=%llu retained_bytes=%llu allocated_bytes=%llu "
          "retired_bytes=%llu\n",
          label, owner, (unsigned long long)stats.metadata_available_bytes,
          (unsigned long long)stats.claimed_metadata_bytes,
          (unsigned long long)stats.metadata_workspace_bytes,
          (unsigned long long)stats.claimed_payload_bytes,
          (unsigned long long)stats.retained_payload_bytes,
          (unsigned long long)stats.allocated_bytes,
          (unsigned long long)stats.retired_bytes);
}
static uint64_t native_work_stats(comparison *run, ps_stats *primary,
                                  lc_error *e) {
  uint64_t checkpoints = 0;
  for (size_t i = 0; i <= run->writers; ++i) {
    lc_client *client = i ? run->workers[i - 1].producer_client : run->client;
    lc_pouch *pouch = ((lc_client_handle *)client)->pouch;
    ps_stats snapshot;
    check(ps_status(ps_root_space(ps_pouch_state_root(pouch)), &snapshot, e),
          e);
    require(snapshot.capacity == PS_DEFAULT_CAPACITY,
            "native reclamation participants retained fixed capacity");
    if (!i)
      *primary = snapshot;
    checkpoints += snapshot.checkpoints;
  }
  return checkpoints;
}
int main(int argc, char **argv) {
  comparison run = {0};
  lc_error e;
  pthread_t *threads;
  char pattern[1200], directory[1200], store[1300];
  double create_seconds, start, cpu, lifecycle_seconds, cpu_seconds,
      work_seconds, maintenance_started, preopen_seconds = 0.0;
  index_phase maintenance = {0}, closing = {0}, reopening = {0},
              verification = {0};
  open_phases reopened = {0};
  ps_stats native_work = {0};
  uint64_t measured_syncs, files, checkpoints_at_start = 0,
                                  work_checkpoints = 0;
  double measured_sync_seconds;
  struct rusage usage;
  require(argc >= 7,
          "MODE DIRECTORY ROWS BYTES SYNC WRITERS [--reopen] [--rounds N] "
          "[--stable-index] [--warm-queues] [--reclaim] [--parallel-joins]");
  require(
      !strcmp(argv[1], "pouch-queue") || !strcmp(argv[1], "runtime-queue") ||
          !strcmp(argv[1], "runtime-shared-queue") ||
          !strcmp(argv[1], "pouch-shared-queue") ||
          !strcmp(argv[1], "pouch-outbox") ||
          !strcmp(argv[1], "runtime-shared-outbox") ||
          !strcmp(argv[1], "pouch-shared-outbox") ||
          !strcmp(argv[1], "runtime-outbox") || !strcmp(argv[1], "pouch-xa") ||
          !strcmp(argv[1], "runtime-xa") || !strcmp(argv[1], "pouch-index") ||
          !strcmp(argv[1], "runtime-index") ||
          !strcmp(argv[1], "runtime-shared-index") ||
          !strcmp(argv[1], "pouch-shared-index"),
      "mode");
  run.container = !strncmp(argv[1], "runtime-", 8U);
  run.shared = strstr(argv[1], "-shared-") != NULL;
  run.outbox = strstr(argv[1], "outbox") != NULL;
  run.xa = strstr(argv[1], "-xa") != NULL;
  run.index = strstr(argv[1], "-index") != NULL;
  run.rounds = 1U;
  for (int i = 7; i < argc; ++i) {
    if (!strcmp(argv[i], "--reopen") && !run.preopened)
      run.preopened = 1;
    else if (!strcmp(argv[i], "--rounds") && i + 1 < argc)
      run.rounds = number(argv[++i], 1U, MAX_ROUNDS);
    else if (!strcmp(argv[i], "--stable-index") && !run.stable_index)
      run.stable_index = 1;
    else if (!strcmp(argv[i], "--warm-queues") && !run.warm_queues)
      run.warm_queues = 1;
    else if (!strcmp(argv[i], "--reclaim") && !run.reclaim)
      run.reclaim = 1;
    else if (!strcmp(argv[i], "--parallel-joins") && !run.parallel_joins)
      run.parallel_joins = 1;
    else
      require(0, "unknown or incomplete comparison option");
  }
  require(run.index || run.rounds == 1U, "rounds require an index workload");
  require(run.index || !run.stable_index,
          "stable-index requires an index workload");
  require(!run.warm_queues || (!run.outbox && !run.xa && !run.index),
          "warm-queues requires a queue workload");
  run.rows = number(argv[3], 1U, 64U);
  run.bytes = number(argv[4], 64U, PS_MIB);
  run.sync = (int)number(argv[5], 0U, 1U);
  run.writers = number(argv[6], 1U, MAX_WRITERS);
  require(!run.parallel_joins || (run.shared && run.writers > 1U),
          "parallel-joins requires a shared workload with multiple writers");
  require(run.writers <= run.rows && run.rows * (run.bytes + 8192U) *
                                             (run.reclaim ? 1 : run.rounds) *
                                             4U <
                                         PS_DEFAULT_CAPACITY - PS_DATA,
          "10 MiB development capacity");
  require(!run.reclaim ||
              (run.shared && run.index && run.writers >= 2U &&
               run.rows * run.bytes * run.rounds > PS_DEFAULT_CAPACITY),
          "reclaim requires shared indexed input exceeding 10 MiB");
  require(
      snprintf(pattern, sizeof(pattern), "%s/queue-outbox-XXXXXX", argv[2]) > 0,
      "fixture path");
  require(lc_test_tmp_mkdtemp(pattern, directory, sizeof(directory), argv[2]),
          "fixture directory");
  require(snprintf(store, sizeof(store), "%s/store", directory) > 0,
          "store path");
  require(snprintf(run.endpoint, sizeof(run.endpoint), "pouch://%s", store) > 0,
          "endpoint");
  run.workers = calloc(run.writers, sizeof(*run.workers));
  run.latencies = calloc(run.rows * run.rounds, sizeof(*run.latencies));
  threads = calloc(run.writers, sizeof(*threads));
  require(run.workers && run.latencies && threads, "worker allocation");
  require(!pthread_mutex_init(&run.mutex, NULL) &&
              !pthread_cond_init(&run.condition, NULL),
          "worker gate");
  require(!pthread_key_create(&sync_phase_key, NULL), "sync attribution key");
  sync_phase_key_initialized = 1;
  for (size_t i = 0U; i < run.writers; ++i) {
    run.workers[i].run = &run;
    run.workers[i].ordinal = i;
    snprintf(run.workers[i].ns, sizeof(run.workers[i].ns), "bench-%lu",
             (unsigned long)i);
    xa_id(100U + i, run.workers[i].owner);
    xa_id(200U + i, run.workers[i].resource);
  }
  payload_digest(&run);
  lc_error_init(&e);
  start = now(CLOCK_MONOTONIC);
  open_store(&run, 1, NULL, &e);
  create_seconds = now(CLOCK_MONOTONIC) - start;
  if (run.preopened) {
    close_store(&run);
    start = now(CLOCK_MONOTONIC);
    open_store(&run, 0, NULL, &e);
    preopen_seconds = now(CLOCK_MONOTONIC) - start;
  }
  if (run.warm_queues)
    warm_queues(&run, &e);
  for (size_t i = 0U; run.outbox && i < run.writers; ++i)
    check(lc_outbox_dispatcher_get_or_start(run.workers[i].outbox,
                                            &run.workers[i].dispatcher, &e),
          &e);
  if (run.reclaim && run.container)
    checkpoints_at_start = native_work_stats(&run, &native_work, &e);
  for (size_t i = 0U; i < run.writers; ++i)
    require(!pthread_create(&threads[i], NULL, run_worker, &run.workers[i]),
            "start writer");
  pthread_mutex_lock(&sync_mutex);
  data_syncs = full_syncs = 0U;
  sync_seconds = 0.0;
  pthread_mutex_unlock(&sync_mutex);
  cpu = now(CLOCK_PROCESS_CPUTIME_ID);
  start = now(CLOCK_MONOTONIC);
  pthread_mutex_lock(&run.mutex);
  run.started = 1;
  pthread_cond_broadcast(&run.condition);
  pthread_mutex_unlock(&run.mutex);
  for (size_t i = 0U; i < run.writers; ++i)
    require(!pthread_join(threads[i], NULL), "join writer");
  for (size_t i = 0U; i < run.writers; ++i) {
    if (run.workers[i].result == LC_OK)
      continue;
    if (run.container) {
      report_capacity(run.client, "primary", 0);
      for (size_t peer = 0; peer < run.writers; ++peer)
        report_capacity(run.workers[peer].producer_client, "writer", peer);
    }
    require(0, "completed workflow");
  }
  for (size_t i = 0U; run.outbox && i < run.writers; ++i)
    check(lc_outbox_dispatcher_stop(run.workers[i].dispatcher, -1L, &e), &e);
  work_seconds = now(CLOCK_MONOTONIC) - start;
  maintenance_started = now(CLOCK_MONOTONIC);
  index_phase_begin(&maintenance);
  lc_pouch *pouch = ((lc_client_handle *)run.client)->pouch;
  if (run.reclaim && run.container) {
    uint64_t completed = native_work_stats(&run, &native_work, &e);
    require(completed > checkpoints_at_start,
            "native root reclaimed within 10 MiB during indexed work");
    work_checkpoints = completed - checkpoints_at_start;
  }
  if (run.xa) {
    size_t retired = 0U;
    check(lc_pouch_client_xa_retire(run.client, 0L, 0U, &retired, &e), &e);
    require(retired == run.rows, "complete XA history retirement");
  }
  check(lc_pouch_storage_namespaces_visit(pouch, cleanup_namespace, pouch, &e),
        &e);
  index_phase_end(&maintenance);
  lifecycle_seconds = now(CLOCK_MONOTONIC) - start;
  require(work_seconds <= maintenance_started - start &&
              maintenance.seconds <= lifecycle_seconds - work_seconds,
          "work and maintenance timers belong to the lifecycle interval");
  cpu_seconds = now(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  pthread_mutex_lock(&sync_mutex);
  measured_syncs = data_syncs + full_syncs;
  measured_sync_seconds = sync_seconds;
  pthread_mutex_unlock(&sync_mutex);
  index_phase_begin(&closing);
  close_store(&run);
  index_phase_end(&closing);
  files = count_files(store);
  if (run.container)
    require(files == 1U, "one persistent container file");
  if (run.reclaim && run.container) {
    struct stat status;
    require(!stat(store, &status) && status.st_size == PS_DEFAULT_CAPACITY,
            "native root retained fixed physical capacity");
  }
  index_phase_begin(&reopening);
  open_store(&run, 0, &reopened, &e);
  index_phase_end(&reopening);
  require(reopened.owner + reopened.joins <= reopening.seconds,
          "owner and join timers belong to the reopen interval");
  require(!run.shared || (reopened.slowest_join > 0.0 &&
                          reopened.slowest_join <= reopened.joins &&
                          reopened.joins_cpu > 0.0),
          "all live joins completed in the measured cohort");
  /* Reopened query passes have their own thread attribution. Their parent
   * interval deliberately uses wall time only so those phases do not nest. */
  start = now(CLOCK_MONOTONIC);
  verify_reopen(&run, &e);
  verification.seconds = now(CLOCK_MONOTONIC) - start;
  close_store(&run);
  if (run.rounds > ROUNDS_PER_WINDOW)
    report_work_windows(&run, work_seconds);
  qsort(run.latencies, run.rows * run.rounds, sizeof(*run.latencies),
        compare_double);
  require(!getrusage(RUSAGE_SELF, &usage), "memory observation");
  double max_index_seconds = 0.0;
  double max_mutation_seconds = 0.0, mutation_cpu_seconds = 0.0;
  uint64_t mutation_sync_calls = 0U;
  double mutation_sync_seconds = 0.0;
  double max_phase_seconds[3] = {0}, phase_cpu_seconds[3] = {0},
         phase_sync_seconds[3] = {0}, reopen_phase_seconds[2] = {0};
  uint64_t phase_sync_calls[3] = {0};
  for (size_t i = 0U; i < run.writers; ++i)
    if (run.workers[i].index_seconds > max_index_seconds)
      max_index_seconds = run.workers[i].index_seconds;
  for (size_t i = 0U; i < run.writers; ++i) {
    worker *w = &run.workers[i];
    if (w->mutations.seconds > max_mutation_seconds)
      max_mutation_seconds = w->mutations.seconds;
    mutation_cpu_seconds += w->mutations.cpu_seconds;
    mutation_sync_calls += w->mutations.sync_calls;
    mutation_sync_seconds += w->mutations.sync_seconds;
    require(w->mutations.seconds + w->index_seconds <= work_seconds,
            "worker phases belong to the work interval");
    double phase_seconds = 0.0;
    for (size_t j = 0U; j < 3U; ++j) {
      index_phase *phase = &w->index_phases[j];
      phase_seconds += phase->seconds;
      if (phase->seconds > max_phase_seconds[j])
        max_phase_seconds[j] = phase->seconds;
      phase_cpu_seconds[j] += phase->cpu_seconds;
      phase_sync_seconds[j] += phase->sync_seconds;
      phase_sync_calls[j] += phase->sync_calls;
      require(phase->queries == (run.index && j ? 3U * run.rounds : 0U),
              "measured query pass coverage");
    }
    require(phase_seconds <= w->index_seconds,
            "phase timers are inside the worker interval");
    for (size_t j = 0U; j < 2U; ++j) {
      require(w->reopen_phases[j].queries == (run.index ? 3U : 0U),
              "reopened query pass coverage");
      reopen_phase_seconds[j] += w->reopen_phases[j].seconds;
    }
  }
  require(mutation_sync_calls + maintenance.sync_calls + phase_sync_calls[0] +
                  phase_sync_calls[1] + phase_sync_calls[2] <=
              measured_syncs,
          "attributed sync calls belong to the work interval");
  printf(
      "mode=%s rows=%zu bytes=%zu writers=%zu sync=%d preopened=%d "
      "rounds=%zu stable_index=%d warm_queues=%d chunk=4096 "
      "lifecycle_ms=%.6f cpu_ms=%.6f p50_ms=%.6f p95_ms=%.6f "
      "sync_calls=%llu sync_ms=%.6f files=%llu create_ms=%.6f close_ms=%.6f "
      "reopen_verify_ms=%.6f preopen_ms=%.6f work_ms=%.6f "
      "maintenance_ms=%.6f reopen_ms=%.6f reopen_owner_ms=%.6f "
      "reopen_joins_ms=%.6f verify_ms=%.6f "
      "mutation_worker_max_ms=%.6f mutation_thread_cpu_ms=%.6f "
      "mutation_sync_calls=%llu mutation_sync_ms=%.6f "
      "maintenance_sync_calls=%llu close_sync_calls=%llu "
      "reopen_sync_calls=%llu "
      "peak_RSS_KiB=%ld index_worker_max_ms=%.6f "
      "index_flush_max_ms=%.6f index_first_pass_max_ms=%.6f "
      "index_repeat_pass_max_ms=%.6f index_flush_thread_cpu_ms=%.6f "
      "index_first_pass_thread_cpu_ms=%.6f "
      "index_repeat_pass_thread_cpu_ms=%.6f "
      "index_flush_sync_calls=%llu index_first_pass_sync_calls=%llu "
      "index_repeat_pass_sync_calls=%llu index_flush_sync_ms=%.6f "
      "index_first_pass_sync_ms=%.6f index_repeat_pass_sync_ms=%.6f "
      "reopen_first_pass_ms=%.6f reopen_repeat_pass_ms=%.6f "
      "indexed_queries=%zu reclaim=%d work_checkpoints=%llu "
      "work_allocated_bytes=%llu parallel_joins=%d reopen_joins_cpu_ms=%.6f "
      "reopen_slowest_join_ms=%.6f verified=1\n",
      argv[1], run.rows, run.bytes, run.writers, run.sync, run.preopened,
      run.rounds, run.stable_index, run.warm_queues, lifecycle_seconds * 1e3,
      cpu_seconds * 1e3, run.latencies[(run.rows * run.rounds - 1U) / 2U] * 1e3,
      run.latencies[(run.rows * run.rounds - 1U) * 95U / 100U] * 1e3,
      (unsigned long long)measured_syncs, measured_sync_seconds * 1e3,
      (unsigned long long)files, create_seconds * 1e3, closing.seconds * 1e3,
      (reopening.seconds + verification.seconds) * 1e3, preopen_seconds * 1e3,
      work_seconds * 1e3, maintenance.seconds * 1e3, reopening.seconds * 1e3,
      reopened.owner * 1e3, reopened.joins * 1e3, verification.seconds * 1e3,
      max_mutation_seconds * 1e3, mutation_cpu_seconds * 1e3,
      (unsigned long long)mutation_sync_calls, mutation_sync_seconds * 1e3,
      (unsigned long long)maintenance.sync_calls,
      (unsigned long long)closing.sync_calls,
      (unsigned long long)(reopening.sync_calls + reopened.parallel_sync_calls),
      usage.ru_maxrss, max_index_seconds * 1e3, max_phase_seconds[0] * 1e3,
      max_phase_seconds[1] * 1e3, max_phase_seconds[2] * 1e3,
      phase_cpu_seconds[0] * 1e3, phase_cpu_seconds[1] * 1e3,
      phase_cpu_seconds[2] * 1e3, (unsigned long long)phase_sync_calls[0],
      (unsigned long long)phase_sync_calls[1],
      (unsigned long long)phase_sync_calls[2], phase_sync_seconds[0] * 1e3,
      phase_sync_seconds[1] * 1e3, phase_sync_seconds[2] * 1e3,
      reopen_phase_seconds[0] * 1e3, reopen_phase_seconds[1] * 1e3,
      run.index ? run.writers * 6U * run.rounds : 0U, run.reclaim,
      (unsigned long long)work_checkpoints,
      (unsigned long long)native_work.allocated_bytes, run.parallel_joins,
      reopened.joins_cpu * 1e3, reopened.slowest_join * 1e3);
  lc_error_cleanup(&e);
  sync_phase_key_initialized = 0;
  require(!pthread_key_delete(sync_phase_key), "close sync attribution key");
  require(!pthread_mutex_destroy(&run.mutex) &&
              !pthread_cond_destroy(&run.condition),
          "close worker gate");
  free(threads);
  free(run.latencies);
  free(run.workers);
  lc_test_tmp_cleanup_path(directory, argv[2]);
  return 0;
}
