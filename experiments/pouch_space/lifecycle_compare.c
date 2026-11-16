#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_xa_internal.h"
#include "pouch.h"
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
typedef struct worker {
  comparison *run;
  size_t ordinal;
  char ns[64];
  lc_outbox *outbox;
  lc_outbox_dispatcher *dispatcher;
  lc_xa_store *journal;
  char owner[LC_XID_STRING_SIZE], resource[LC_XID_STRING_SIZE];
  int result;
} worker;
struct comparison {
  size_t rows, bytes, writers;
  int container, outbox, xa, sync, started;
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
} producer;
typedef struct consumer {
  lc_sink sink;
  size_t size, offset;
} consumer;
static uint64_t data_syncs, full_syncs;
static double sync_seconds;
static pthread_mutex_t sync_mutex = PTHREAD_MUTEX_INITIALIZER;
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
  pthread_mutex_lock(&sync_mutex);
  ++data_syncs;
  sync_seconds += now(CLOCK_MONOTONIC) - start;
  pthread_mutex_unlock(&sync_mutex);
  return rc;
}
int __wrap_fsync(int fd) {
  double start = now(CLOCK_MONOTONIC);
  int rc = __real_fsync(fd);
  pthread_mutex_lock(&sync_mutex);
  ++full_syncs;
  sync_seconds += now(CLOCK_MONOTONIC) - start;
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
static unsigned char byte_at(size_t offset, size_t size) {
  static const unsigned char prefix[] = "{\"v\":\"";
  return offset < sizeof(prefix) - 1U ? prefix[offset]
         : offset == size - 2U        ? '"'
         : offset == size - 1U        ? '}'
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
    ((unsigned char *)bytes)[i] = byte_at(source->offset + i, source->size);
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
    valid = data[i] == byte_at(sink->offset + i, sink->size);
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
      scratch[i] = byte_at(at + i, run->bytes);
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
static void open_store(comparison *run, int create, lc_error *e) {
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
  settings.single_writer = 1;
  settings.durable_sync = run->sync;
  settings.compression = "none";
  settings.query_engine = "scan";
  settings.segment_target_bytes = PS_MIB;
  config.endpoints = &endpoint;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_settings = &settings;
  check(run->container
            ? ps_pouch_client_open(&config, create, NULL, &run->client, e)
            : lc_client_open(&config, &run->client, e),
        e);
  for (size_t i = 0U; run->outbox && i < run->writers; ++i) {
    lc_outbox_config outbox;
    lc_outbox_config_init(&outbox);
    outbox.ns = run->workers[i].ns;
    outbox.owner = "benchmark";
    check(
        lc_client_new_outbox(run->client, &outbox, &run->workers[i].outbox, e),
        e);
  }
  for (size_t i = 0U; run->xa && i < run->writers; ++i)
    check(lc_xa_store_open_owned(run->client, run->workers[i].owner,
                                 &run->workers[i].journal, e),
          e);
}
static void close_store(comparison *run) {
  for (size_t i = 0U; i < run->writers; ++i) {
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
  size_t written = 0U;
  int rc;
  (void)row;
  lc_enqueue_req_init(&enqueue);
  enqueue.ns = w->ns;
  enqueue.queue = "work";
  enqueue.content_type = "application/json";
  rc = lc_enqueue(w->run->client, &enqueue, &source->source, &enqueued, e);
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
  rc = lc_outbox_append(w->outbox, &entry, &source->source, &transaction,
                        &receipt, e);
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
  for (size_t row = w->ordinal; row < run->rows && w->result == LC_OK;
       row += run->writers) {
    producer source;
    consumer sink;
    producer_init(&source, run->bytes);
    memset(&sink, 0, sizeof(sink));
    sink.size = run->bytes;
    sink.sink.impl = &sink;
    sink.sink.write = consume;
    double start = now(CLOCK_MONOTONIC);
    w->result = run->xa       ? xa_one(w, row, &source, &sink, &e)
                : run->outbox ? outbox_one(w, row, &source, &sink, &e)
                              : queue_one(w, row, &source, &sink, &e);
    run->latencies[row] = now(CLOCK_MONOTONIC) - start;
    if (w->result == LC_OK)
      require(source.offset == run->bytes && sink.offset == run->bytes,
              "streamed payload coverage");
  }
  if (w->result == LC_OK && run->outbox) {
    lc_outbox_job *job = NULL;
    w->result =
        lc_outbox_dispatcher_next_with_state(w->dispatcher, 0L, &job, &e);
    require(!job, "stateful cleanup probe has no remaining delivery");
  }
  if (w->result != LC_OK)
    fprintf(stderr, "worker %lu: %s [%s]\n", (unsigned long)w->ordinal,
            e.message ? e.message : "", e.server_code ? e.server_code : "");
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
    if (run->xa) {
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
int main(int argc, char **argv) {
  comparison run = {0};
  lc_error e;
  pthread_t *threads;
  char pattern[1200], directory[1200], store[1300];
  double create_seconds, start, cpu, lifecycle_seconds, cpu_seconds,
      close_seconds, reopen_seconds;
  uint64_t measured_syncs, files;
  double measured_sync_seconds;
  struct rusage usage;
  require(argc == 7 || (argc == 8 && !strcmp(argv[7], "--reopen")),
          "MODE DIRECTORY ROWS BYTES SYNC WRITERS [--reopen]");
  require(!strcmp(argv[1], "pouch-queue") ||
              !strcmp(argv[1], "runtime-queue") ||
              !strcmp(argv[1], "pouch-outbox") ||
              !strcmp(argv[1], "runtime-outbox") ||
              !strcmp(argv[1], "pouch-xa") || !strcmp(argv[1], "runtime-xa"),
          "mode");
  run.container = !strncmp(argv[1], "runtime-", 8U);
  run.outbox = strstr(argv[1], "outbox") != NULL;
  run.xa = strstr(argv[1], "-xa") != NULL;
  run.rows = number(argv[3], 1U, 64U);
  run.bytes = number(argv[4], 64U, PS_MIB);
  run.sync = (int)number(argv[5], 0U, 1U);
  run.writers = number(argv[6], 1U, 16U);
  require(run.writers <= run.rows && run.rows * (run.bytes + 8192U) * 4U <
                                         PS_DEFAULT_CAPACITY - PS_DATA,
          "10 MiB development capacity");
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
  run.latencies = calloc(run.rows, sizeof(*run.latencies));
  threads = calloc(run.writers, sizeof(*threads));
  require(run.workers && run.latencies && threads, "worker allocation");
  require(!pthread_mutex_init(&run.mutex, NULL) &&
              !pthread_cond_init(&run.condition, NULL),
          "worker gate");
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
  open_store(&run, 1, &e);
  create_seconds = now(CLOCK_MONOTONIC) - start;
  if (argc == 8) {
    close_store(&run);
    open_store(&run, 0, &e);
  }
  for (size_t i = 0U; run.outbox && i < run.writers; ++i)
    check(lc_outbox_dispatcher_get_or_start(run.workers[i].outbox,
                                            &run.workers[i].dispatcher, &e),
          &e);
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
  for (size_t i = 0U; i < run.writers; ++i) {
    require(!pthread_join(threads[i], NULL), "join writer");
    require(run.workers[i].result == LC_OK, "completed workflow");
  }
  for (size_t i = 0U; run.outbox && i < run.writers; ++i)
    check(lc_outbox_dispatcher_stop(run.workers[i].dispatcher, -1L, &e), &e);
  lc_pouch *pouch = ((lc_client_handle *)run.client)->pouch;
  if (run.xa) {
    size_t retired = 0U;
    check(lc_pouch_client_xa_retire(run.client, 0L, 0U, &retired, &e), &e);
    require(retired == run.rows, "complete XA history retirement");
  }
  check(lc_pouch_storage_namespaces_visit(pouch, cleanup_namespace, pouch, &e),
        &e);
  lifecycle_seconds = now(CLOCK_MONOTONIC) - start;
  cpu_seconds = now(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  pthread_mutex_lock(&sync_mutex);
  measured_syncs = data_syncs + full_syncs;
  measured_sync_seconds = sync_seconds;
  pthread_mutex_unlock(&sync_mutex);
  start = now(CLOCK_MONOTONIC);
  close_store(&run);
  close_seconds = now(CLOCK_MONOTONIC) - start;
  files = count_files(store);
  if (run.container)
    require(files == 1U, "one persistent container file");
  start = now(CLOCK_MONOTONIC);
  open_store(&run, 0, &e);
  verify_reopen(&run, &e);
  reopen_seconds = now(CLOCK_MONOTONIC) - start;
  close_store(&run);
  qsort(run.latencies, run.rows, sizeof(*run.latencies), compare_double);
  require(!getrusage(RUSAGE_SELF, &usage), "memory observation");
  printf(
      "mode=%s rows=%zu bytes=%zu writers=%zu sync=%d preopened=%d chunk=4096 "
      "lifecycle_ms=%.6f cpu_ms=%.6f p50_ms=%.6f p95_ms=%.6f "
      "sync_calls=%llu sync_ms=%.6f files=%llu create_ms=%.6f close_ms=%.6f "
      "reopen_verify_ms=%.6f peak_RSS_KiB=%ld verified=1\n",
      argv[1], run.rows, run.bytes, run.writers, run.sync, argc == 8,
      lifecycle_seconds * 1e3, cpu_seconds * 1e3,
      run.latencies[(run.rows - 1U) / 2U] * 1e3,
      run.latencies[(run.rows - 1U) * 95U / 100U] * 1e3,
      (unsigned long long)measured_syncs, measured_sync_seconds * 1e3,
      (unsigned long long)files, create_seconds * 1e3, close_seconds * 1e3,
      reopen_seconds * 1e3, usage.ru_maxrss);
  lc_error_cleanup(&e);
  require(!pthread_mutex_destroy(&run.mutex) &&
              !pthread_cond_destroy(&run.condition),
          "close worker gate");
  free(threads);
  free(run.latencies);
  free(run.workers);
  lc_test_tmp_cleanup_path(directory, argv[2]);
  return 0;
}
