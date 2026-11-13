#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_collection_json.h"
#include "lc_pouch.h"
#include "space.h"
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Time the real synchronization calls of both engines in this executable,
 * without clocks or profiling modes in the storage implementation. */
static uint64_t fd_sync_calls, full_sync_calls, syscall_sync_ns;
int __real_fdatasync(int);
int __real_fsync(int);
static int timed_sync(int fd, int data_only) {
  struct timespec begin, end;
  int measured, rc, saved_errno;
  int64_t elapsed;
  measured = clock_gettime(CLOCK_MONOTONIC, &begin) == 0;
  rc = data_only ? __real_fdatasync(fd) : __real_fsync(fd);
  saved_errno = errno;
  if (measured && clock_gettime(CLOCK_MONOTONIC, &end) == 0) {
    elapsed = (int64_t)(end.tv_sec - begin.tv_sec) * INT64_C(1000000000) +
              (int64_t)end.tv_nsec - (int64_t)begin.tv_nsec;
    if (elapsed > 0)
      __atomic_fetch_add(&syscall_sync_ns, (uint64_t)elapsed, __ATOMIC_RELAXED);
  }
  if (data_only)
    __atomic_fetch_add(&fd_sync_calls, 1, __ATOMIC_RELAXED);
  else
    __atomic_fetch_add(&full_sync_calls, 1, __ATOMIC_RELAXED);
  errno = saved_errno;
  return rc;
}
int __wrap_fdatasync(int fd) { return timed_sync(fd, 1); }
int __wrap_fsync(int fd) { return timed_sync(fd, 0); }

typedef struct input {
  const char *body;
  size_t bytes, offset, chunk;
} input;
typedef struct comparison comparison;
typedef struct worker {
  comparison *run;
  size_t ordinal;
  lc_collection *collection;
  char *body;
  int rc;
} worker;
struct comparison {
  int container, collection, sync;
  size_t rows, bytes, writers, rounds;
  ps_space *space;
  lc_pouch *pouch;
  lc_client *client;
  worker *workers;
  pthread_t *threads;
  double *latencies;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int started;
  char root[1024], store[1100], endpoint[1200];
};
typedef struct io_counts {
  uint64_t read_calls, write_calls, read_bytes, write_bytes, written;
} io_counts;
typedef struct files {
  uint64_t regular, directories, logical, allocated;
} files;

static double now(clockid_t which) {
  struct timespec t;
  if (clock_gettime(which, &t) != 0)
    abort();
  return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}
static void check(int rc, lc_error *e) {
  if (rc != LC_OK) {
    fprintf(stderr, "comparison failed (%d): %s\n", rc,
            e->message ? e->message : "no detail");
    exit(1);
  }
}
static void require(int condition, const char *message) {
  if (!condition) {
    fprintf(stderr, "comparison verification failed: %s\n", message);
    exit(1);
  }
}
static size_t number(const char *s, size_t minimum, size_t maximum) {
  unsigned long n;
  char *end;
  errno = 0;
  n = strtoul(s, &end, 10);
  require(!errno && end != s && !*end && *s != '-' && n >= minimum &&
              n <= maximum,
          "numeric argument outside allowed range");
  return (size_t)n;
}
static size_t source_read(void *context, void *bytes, size_t count,
                          lc_error *e) {
  input *in = context;
  (void)e;
  if (count > in->chunk)
    count = in->chunk;
  if (count > in->bytes - in->offset)
    count = in->bytes - in->offset;
  memcpy(bytes, in->body + in->offset, count);
  in->offset += count;
  return count;
}
static void make_body(char *body, size_t bytes, size_t id, size_t round) {
  int prefix =
      snprintf(body, bytes, "{\"id\":\"%08lu\",\"round\":%lu,\"data\":\"",
               (unsigned long)id, (unsigned long)round);
  size_t i;
  require(prefix > 0 && (size_t)prefix + 2 < bytes, "payload too short");
  for (i = (size_t)prefix; i < bytes - 2; ++i)
    body[i] = (char)('a' + (id * 17 + round * 3 + i * 7) % 26);
  body[bytes - 2] = '"';
  body[bytes - 1] = '}';
}
static ps_key storage_key(comparison *c, size_t owner, size_t id) {
  ps_key k;
  k.kind = c->collection ? PS_COLLECTION : PS_STATE;
  k.owner = owner + 1;
  k.key = id;
  return k;
}
static void names(size_t owner, size_t id, char ns[64], char name[64]) {
  snprintf(ns, 64, "owner-%lu", (unsigned long)owner);
  snprintf(name, 64, "key-%lu", (unsigned long)id);
}
static int space_sink(lc_sink *sink, const void *bytes, size_t count,
                      lc_error *e) {
  return ps_write(sink->impl, bytes, count, e) == LC_OK;
}
static int write_one(worker *w, size_t id, lc_error *e) {
  comparison *c = w->run;
  lc_source *source = NULL;
  lc_sink sink;
  ps_writer *destination = NULL;
  input in;
  unsigned char buffer[4096];
  size_t n;
  lc_u64 written;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_collection_append_req request;
  lc_collection_append_res append;
  char ns[64], name[64];
  int rc;
  names(w->ordinal, id, ns, name);
  in.body = w->body;
  in.bytes = c->bytes;
  in.offset = 0;
  in.chunk = sizeof(buffer);
  rc = lc_source_from_callbacks(source_read, NULL, NULL, &in, &source, e);
  if (rc != LC_OK)
    return rc;
  if (c->container) {
    rc = ps_begin(c->space, storage_key(c, w->ordinal, id), &destination, e);
    if (rc == LC_OK && c->collection) {
      memset(&sink, 0, sizeof(sink));
      sink.impl = destination;
      sink.write = space_sink;
      /* Same incremental JSON validator/compactor as real collections. */
      rc = lc_collection_json_copy(source, &sink, NULL, &written, e);
      if (rc == LC_OK && written != c->bytes)
        rc = LC_ERR_PROTOCOL;
    } else if (rc == LC_OK) {
      while ((n = source->read(source, buffer, sizeof(buffer), e)) != 0) {
        rc = ps_write(destination, buffer, n, e);
        if (rc != LC_OK)
          break;
      }
    }
    if (rc == LC_OK)
      rc = ps_publish(destination, NULL, e);
    ps_writer_close(destination);
  } else if (c->collection) {
    lc_collection_append_req_init(&request);
    request.append_id = name;
    rc = lc_collection_append(w->collection, &request, source, &append, e);
    if (rc == LC_OK && (append.duplicate || append.bytes != c->bytes))
      rc = LC_ERR_PROTOCOL;
  } else {
    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.content_type = "application/json";
    rc = lc_pouch_state_write(c->pouch, ns, name, source, &options, &result, e);
    if (rc == LC_OK && result.bytes != c->bytes)
      rc = LC_ERR_PROTOCOL;
    lc_pouch_state_write_result_cleanup(NULL, &result);
  }
  lc_source_close(source);
  return rc;
}
static void *writer_main(void *context) {
  worker *w = context;
  comparison *c = w->run;
  lc_error e;
  size_t id, round;
  double start;
  lc_error_init(&e);
  pthread_mutex_lock(&c->mutex);
  while (!c->started)
    pthread_cond_wait(&c->condition, &c->mutex);
  pthread_mutex_unlock(&c->mutex);
  w->rc = LC_OK;
  for (round = 0; round < c->rounds && w->rc == LC_OK; ++round)
    for (id = w->ordinal; id < c->rows; id += c->writers) {
      make_body(w->body, c->bytes, id, round);
      start = now(CLOCK_MONOTONIC);
      w->rc = write_one(w, id, &e);
      c->latencies[round * c->rows + id] = now(CLOCK_MONOTONIC) - start;
      if (w->rc != LC_OK)
        break;
    }
  if (w->rc != LC_OK)
    fprintf(stderr, "writer %lu: %s\n", (unsigned long)w->ordinal,
            e.message ? e.message : "failed");
  lc_error_cleanup(&e);
  return NULL;
}
static void open_stores(comparison *c, int create, lc_error *e) {
  ps_options space_options = {0};
  lc_pouch_open_options options;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_collection_config collection;
  const char *endpoint = c->endpoint;
  char name[64];
  size_t i;
  if (c->container) {
    space_options.durable_sync = c->sync;
    check(ps_open(c->store, create, &space_options, &c->space, e), e);
    return;
  }
  if (!c->collection) {
    memset(&options, 0, sizeof(options));
    options.durable_sync = c->sync;
    options.query_indexing_enabled_set = 1;
    options.background_compaction_enabled_set = 1;
    options.segment_target_bytes = PS_MIB;
    options.compression = "none";
    check(lc_pouch_open(c->store, NULL, &options, &c->pouch, e), e);
    return;
  }
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_QUERY_INDEXING |
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  settings.durable_sync = c->sync;
  settings.segment_target_bytes = PS_MIB;
  settings.compression = "none";
  config.endpoints = &endpoint;
  config.endpoint_count = 1;
  config.default_namespace = "bench";
  config.pouch_settings = &settings;
  check(lc_client_open(&config, &c->client, e), e);
  for (i = 0; i < c->writers; ++i) {
    snprintf(name, sizeof(name), "collection-%lu", (unsigned long)i);
    lc_collection_config_init(&collection);
    collection.name = name;
    if (create)
      check(lc_client_create_collection(c->client, &collection,
                                        &c->workers[i].collection, e),
            e);
    else
      check(lc_client_open_collection(c->client, &collection,
                                      &c->workers[i].collection, e),
            e);
  }
}
static void close_stores(comparison *c, lc_error *e) {
  size_t i;
  if (c->container) {
    check(ps_close(c->space, e), e);
    c->space = NULL;
  } else if (c->collection) {
    for (i = 0; i < c->writers; ++i) {
      lc_collection_close(c->workers[i].collection);
      c->workers[i].collection = NULL;
    }
    lc_client_close(c->client);
    c->client = NULL;
  } else {
    lc_pouch_close(c->pouch);
    c->pouch = NULL;
  }
}
static void verify_states(comparison *c, lc_error *e) {
  ps_reader *reader;
  lc_pouch_state_read_result result;
  char ns[64], name[64];
  unsigned char buffer[4096];
  size_t id, ordinal, offset, got;
  for (id = 0; id < c->rows; ++id) {
    ordinal = id % c->writers;
    make_body(c->workers[ordinal].body, c->bytes, id, c->rounds - 1);
    offset = 0;
    if (c->container) {
      check(
          ps_read_open(c->space, storage_key(c, ordinal, id), NULL, &reader, e),
          e);
      require(ps_read_size(reader) == c->bytes, "container entry length");
      do {
        check(ps_read(reader, buffer, sizeof(buffer), &got, e), e);
        require(got <= c->bytes - offset &&
                    !memcmp(buffer, c->workers[ordinal].body + offset, got),
                "container entry bytes");
        offset += got;
      } while (got);
      ps_reader_close(reader);
    } else {
      names(ordinal, id, ns, name);
      memset(&result, 0, sizeof(result));
      check(lc_pouch_state_read(c->pouch, ns, name, &result, e), e);
      require(result.found && result.body && result.bytes == c->bytes,
              "Pouch state metadata");
      while ((got = result.body->read(result.body, buffer, sizeof(buffer),
                                      e)) != 0) {
        require(got <= c->bytes - offset &&
                    !memcmp(buffer, c->workers[ordinal].body + offset, got),
                "Pouch state bytes");
        offset += got;
      }
      lc_pouch_state_read_result_cleanup(NULL, &result);
    }
    require(offset == c->bytes, "complete entry read");
  }
}
typedef struct output {
  worker *worker;
  size_t id, offset, records;
} output;
static int verify_jsonl(lc_sink *sink, const void *bytes, size_t count,
                        lc_error *e) {
  output *o = sink->impl;
  comparison *c = o->worker->run;
  const unsigned char *p = bytes;
  size_t take;
  (void)e;
  while (count) {
    require(o->id < c->rows, "unexpected collection entry");
    if (o->offset == 0)
      make_body(o->worker->body, c->bytes, o->id, 0);
    if (o->offset == c->bytes) {
      require(*p == '\n', "JSONL delimiter");
      ++o->records;
      o->id += c->writers;
      o->offset = 0;
      ++p;
      --count;
      continue;
    }
    take = c->bytes - o->offset;
    if (take > count)
      take = count;
    require(!memcmp(p, o->worker->body + o->offset, take),
            "collection payload bytes");
    o->offset += take;
    p += take;
    count -= take;
  }
  return 1;
}
static void verify_all(comparison *c, lc_error *e) {
  size_t i;
  output out;
  lc_sink sink;
  lc_collection_read_opts options;
  lc_collection_read_res result;
  if (c->container || !c->collection) {
    verify_states(c, e);
    return;
  }
  for (i = 0; i < c->writers; ++i) {
    memset(&out, 0, sizeof(out));
    out.worker = &c->workers[i];
    out.id = i;
    memset(&sink, 0, sizeof(sink));
    sink.impl = &out;
    sink.write = verify_jsonl;
    lc_collection_read_opts_init(&options);
    lc_collection_read_res_init(&result);
    check(lc_collection_read(c->workers[i].collection, &options, &sink, &result,
                             e),
          e);
    require(result.complete && out.offset == 0 &&
                result.completed_records == out.records &&
                out.records == (c->rows + c->writers - 1 - i) / c->writers,
            "complete collection read");
    lc_collection_read_res_cleanup(&result);
  }
}
static void count_files(const char *path, files *out) {
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  char child[1500];
  require(lstat(path, &st) == 0, "stat storage artifact");
  if (S_ISREG(st.st_mode)) {
    ++out->regular;
    out->logical += (uint64_t)st.st_size;
    out->allocated += (uint64_t)st.st_blocks * 512;
  } else if (S_ISDIR(st.st_mode)) {
    ++out->directories;
    dir = opendir(path);
    require(dir != NULL, "open storage directory");
    while ((entry = readdir(dir)) != NULL) {
      if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
        continue;
      require(snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) <
                  (int)sizeof(child),
              "artifact path length");
      count_files(child, out);
    }
    require(closedir(dir) == 0, "close storage directory");
  } else
    require(0, "unexpected storage artifact type");
}
static io_counts io_sample(void) {
  FILE *fp = fopen("/proc/self/io", "r");
  io_counts out = {0};
  char name[64];
  unsigned long value;
  require(fp != NULL, "read process I/O counters");
  while (fscanf(fp, "%63s %lu", name, &value) == 2) {
    if (!strcmp(name, "syscr:"))
      out.read_calls = (uint64_t)value;
    if (!strcmp(name, "syscw:"))
      out.write_calls = (uint64_t)value;
    if (!strcmp(name, "read_bytes:"))
      out.read_bytes = (uint64_t)value;
    if (!strcmp(name, "write_bytes:"))
      out.write_bytes = (uint64_t)value;
    if (!strcmp(name, "wchar:"))
      out.written = (uint64_t)value;
  }
  require(fclose(fp) == 0, "close I/O counters");
  return out;
}
static int compare_double(const void *a, const void *b) {
  double x = *(const double *)a, y = *(const double *)b;
  return (x > y) - (x < y);
}
int main(int argc, char **argv) {
  comparison c;
  lc_error e;
  io_counts before, after;
  files artifacts = {0}, audit = {0};
  ps_stats stats, initial_stats = {0};
  uint64_t fd_before, full_before, ns_before, fd_calls, full_calls, sync_ns;
  long max_sync_batch = -1;
  long checkpoints = -1, directory_writes = -1, descriptor_writes = -1;
  long metadata_pages = -1;
  struct rusage usage;
  char prefix[1024], template_path[1100];
  size_t i, count;
  double start, cpu, seconds, cpu_seconds, read_seconds, close_seconds,
      open_seconds, create_seconds, reopen_read_seconds;
  if (argc != 8) {
    fprintf(
        stderr,
        "usage: %s space-state|pouch-state|space-collection|pouch-collection "
        "DIRECTORY ROWS BYTES SYNC WRITERS CHUNK\n",
        argv[0]);
    return 2;
  }
  memset(&c, 0, sizeof(c));
  require(!strcmp(argv[1], "space-state") || !strcmp(argv[1], "pouch-state") ||
              !strcmp(argv[1], "space-collection") ||
              !strcmp(argv[1], "pouch-collection"),
          "mode");
  c.container = !strncmp(argv[1], "space-", 6);
  c.collection = strstr(argv[1], "collection") != NULL;
  c.rows = number(argv[3], 1, PS_DEFAULT_CAPACITY / PS_PAGE);
  c.bytes = number(argv[4], 64, 2 * PS_MIB);
  c.sync = (int)number(argv[5], 0, 1);
  c.writers = number(argv[6], 1, 16);
  /* argv[7] is the input chunk size, deliberately shared by both engines. */
  require(!strcmp(argv[7], "4096"), "input chunk must currently be 4096");
  c.rounds = c.collection ? 1 : 2;
  require(c.writers <= c.rows &&
              c.rows * ((c.bytes + PS_PAGE - 1) / PS_PAGE * PS_PAGE) *
                      c.rounds <
                  PS_DEFAULT_CAPACITY - PS_DATA,
          "workload must fit fixed default capacity including old state "
          "generations");
  require(argv[2][0] == '/', "storage directory must be absolute");
  require(snprintf(prefix, sizeof(prefix), "%s/pouch-space-bench-", argv[2]) <
              (int)sizeof(prefix),
          "storage directory length");
  require(snprintf(template_path, sizeof(template_path), "%sXXXXXX", prefix) <
              (int)sizeof(template_path),
          "temporary root length");
  require(lc_test_tmp_mkdtemp(template_path, c.root, sizeof(c.root), prefix),
          "create owned benchmark root");
  snprintf(c.store, sizeof(c.store), "%s/store", c.root);
  snprintf(c.endpoint, sizeof(c.endpoint), "pouch://%s", c.store);
  c.workers = calloc(c.writers, sizeof(*c.workers));
  c.threads = calloc(c.writers, sizeof(*c.threads));
  count = c.rows * c.rounds;
  c.latencies = calloc(count, sizeof(*c.latencies));
  require(c.workers && c.threads && c.latencies, "benchmark allocations");
  require(pthread_mutex_init(&c.mutex, NULL) == 0, "benchmark mutex");
  require(pthread_cond_init(&c.condition, NULL) == 0, "benchmark condition");
  lc_error_init(&e);
  for (i = 0; i < c.writers; ++i) {
    c.workers[i].run = &c;
    c.workers[i].ordinal = i;
    c.workers[i].body = malloc(c.bytes);
    require(c.workers[i].body != NULL, "worker payload fixture");
  }
  start = now(CLOCK_MONOTONIC);
  open_stores(&c, 1, &e);
  create_seconds = now(CLOCK_MONOTONIC) - start;
  for (i = 0; i < c.writers; ++i)
    require(pthread_create(&c.threads[i], NULL, writer_main, &c.workers[i]) ==
                0,
            "start writer");
  if (c.container)
    check(ps_status(c.space, &initial_stats, &e), &e);
  before = io_sample();
  fd_before = __atomic_load_n(&fd_sync_calls, __ATOMIC_RELAXED);
  full_before = __atomic_load_n(&full_sync_calls, __ATOMIC_RELAXED);
  ns_before = __atomic_load_n(&syscall_sync_ns, __ATOMIC_RELAXED);
  cpu = now(CLOCK_PROCESS_CPUTIME_ID);
  start = now(CLOCK_MONOTONIC);
  pthread_mutex_lock(&c.mutex);
  c.started = 1;
  pthread_cond_broadcast(&c.condition);
  pthread_mutex_unlock(&c.mutex);
  for (i = 0; i < c.writers; ++i) {
    require(pthread_join(c.threads[i], NULL) == 0, "join writer");
    check(c.workers[i].rc, &e);
  }
  seconds = now(CLOCK_MONOTONIC) - start;
  cpu_seconds = now(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  fd_calls = __atomic_load_n(&fd_sync_calls, __ATOMIC_RELAXED) - fd_before;
  full_calls =
      __atomic_load_n(&full_sync_calls, __ATOMIC_RELAXED) - full_before;
  sync_ns = __atomic_load_n(&syscall_sync_ns, __ATOMIC_RELAXED) - ns_before;
  after = io_sample();
  if (c.container) {
    check(ps_status(c.space, &stats, &e), &e);
    max_sync_batch = (long)stats.max_sync_batch;
    checkpoints = (long)(stats.checkpoints - initial_stats.checkpoints);
    directory_writes = (long)(stats.directory_page_writes -
                              initial_stats.directory_page_writes);
    descriptor_writes = (long)(stats.descriptor_page_writes -
                               initial_stats.descriptor_page_writes);
    metadata_pages = (long)stats.metadata_pages;
  }
  qsort(c.latencies, count, sizeof(*c.latencies), compare_double);
  start = now(CLOCK_MONOTONIC);
  verify_all(&c, &e);
  read_seconds = now(CLOCK_MONOTONIC) - start;
  start = now(CLOCK_MONOTONIC);
  close_stores(&c, &e);
  close_seconds = now(CLOCK_MONOTONIC) - start;
  start = now(CLOCK_MONOTONIC);
  open_stores(&c, 0, &e);
  open_seconds = now(CLOCK_MONOTONIC) - start;
  start = now(CLOCK_MONOTONIC);
  verify_all(&c, &e);
  reopen_read_seconds = now(CLOCK_MONOTONIC) - start;
  count_files(c.store, &artifacts);
  if (c.container) {
    /* The test helper owns one marker in the parent, outside the store. Audit
     * the whole parent too so an accidental store sidecar cannot hide. */
    count_files(c.root, &audit);
    require(audit.regular == 2 && audit.directories == 1 &&
                artifacts.regular == 1 && artifacts.directories == 0,
            "exactly one container and no sidecars");
    check(ps_status(c.space, &stats, &e), &e);
    require(stats.entries == c.rows, "recovered entry count");
  }
  require(getrusage(RUSAGE_SELF, &usage) == 0, "peak RSS");
  printf(
      "mode=%s rows=%lu bytes=%lu rounds=%lu writers=%lu sync=%d chunk=4096 "
      "write_ms=%.6f cpu_ms=%.6f MiB_s=%.3f p50_us=%.3f p95_us=%.3f "
      "p99_us=%.3f "
      "read_ms=%.6f close_ms=%.6f reopen_ms=%.6f create_ms=%.6f "
      "reopen_read_ms=%.6f ready_ms=%.6f "
      "write_calls=%lu read_calls=%lu syscall_written=%lu kernel_written=%lu "
      "files=%lu directories=%lu logical_bytes=%lu allocated_bytes=%lu "
      "peak_RSS_KiB=%ld fdatasync_calls=%lu fsync_calls=%lu sync_ms=%.6f "
      "max_sync_batch=%ld ",
      argv[1], (unsigned long)c.rows, (unsigned long)c.bytes,
      (unsigned long)c.rounds, (unsigned long)c.writers, c.sync, seconds * 1000,
      cpu_seconds * 1000, (double)c.bytes * count / 1048576.0 / seconds,
      c.latencies[(count - 1) / 2] * 1e6,
      c.latencies[(count * 95 + 99) / 100 - 1] * 1e6,
      c.latencies[(count * 99 + 99) / 100 - 1] * 1e6, read_seconds * 1000,
      close_seconds * 1000, open_seconds * 1000, create_seconds * 1000,
      reopen_read_seconds * 1000, (open_seconds + reopen_read_seconds) * 1000,
      (unsigned long)(after.write_calls - before.write_calls),
      (unsigned long)(after.read_calls - before.read_calls),
      (unsigned long)(after.written - before.written),
      (unsigned long)(after.write_bytes - before.write_bytes),
      (unsigned long)artifacts.regular, (unsigned long)artifacts.directories,
      (unsigned long)artifacts.logical, (unsigned long)artifacts.allocated,
      usage.ru_maxrss, (unsigned long)fd_calls, (unsigned long)full_calls,
      (double)sync_ns / 1e6, max_sync_batch);
  printf("checkpoints=%ld directory_page_writes=%ld descriptor_page_writes=%ld "
         "metadata_pages=%ld verified=1\n",
         checkpoints, directory_writes, descriptor_writes, metadata_pages);
  close_stores(&c, &e);
  for (i = 0; i < c.writers; ++i)
    free(c.workers[i].body);
  free(c.latencies);
  free(c.threads);
  free(c.workers);
  pthread_cond_destroy(&c.condition);
  pthread_mutex_destroy(&c.mutex);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(c.root, prefix);
  return 0;
}
