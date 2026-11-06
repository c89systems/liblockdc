#define _POSIX_C_SOURCE 200809L

#include "../tests/support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_pouch.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Producer-side experiment, not a replacement collection writer API. Buffered
 * input is validated by the real atomic pull append at commit. No chunked
 * canonical format or production storage behavior is introduced here. */
typedef struct layout_files {
  uint64_t files, directories, canonical, query_pages, bytes, allocated;
} layout_files;

typedef struct layout_io {
  uint64_t read_calls, write_calls, read_bytes, write_bytes;
} layout_io;

static int layout_io_read(layout_io *io) {
  FILE *fp;
  char key[64], value[64];
  uint64_t parsed;
  memset(io, 0, sizeof(*io));
  fp = fopen("/proc/self/io", "r");
  if (fp == NULL)
    return 0;
  while (fscanf(fp, "%63s %63s", key, value) == 2) {
    if (!lc_u64_parse_base10(value, &parsed)) {
      (void)fclose(fp);
      return 0;
    }
    if (!strcmp(key, "syscr:"))
      io->read_calls = parsed;
    if (!strcmp(key, "syscw:"))
      io->write_calls = parsed;
    if (!strcmp(key, "read_bytes:"))
      io->read_bytes = parsed;
    if (!strcmp(key, "write_bytes:"))
      io->write_bytes = parsed;
  }
  (void)fclose(fp);
  return 1;
}

static void layout_io_report(const char *phase, const layout_io *before) {
  layout_io after;
  if (layout_io_read(&after))
    printf("phase=%s read_calls=%lu write_calls=%lu kernel_read_bytes=%lu "
           "kernel_write_bytes=%lu\n",
           phase, (unsigned long)(after.read_calls - before->read_calls),
           (unsigned long)(after.write_calls - before->write_calls),
           (unsigned long)(after.read_bytes - before->read_bytes),
           (unsigned long)(after.write_bytes - before->write_bytes));
}

typedef struct layout_stage {
  unsigned char *bytes;
  size_t length, capacity, limit;
  int fd;
  char path[768];
} layout_stage;

typedef struct layout_run {
  const char *mode;
  char root[512];
  lc_collection *collection;
  size_t rows, body_bytes, limit, writers;
  size_t source_chunk, source_delay_us;
  int pull_compare, compact;
  double *latencies;
  int sync, raw_fd;
  pthread_mutex_t raw_mutex;
} layout_run;

typedef struct layout_worker {
  layout_run *run;
  size_t ordinal, spooled, peak_memory;
  uint64_t source_calls;
  int result;
} layout_worker;

typedef struct layout_read {
  lc_sink sink;
  uint64_t bytes;
  size_t rows, body_bytes, records, offset;
  uint64_t id;
  char *expected;
  unsigned char *seen;
  int verify;
} layout_read;

typedef struct layout_input {
  lc_source pub;
  layout_run *run;
  const char *body;
  size_t offset;
  uint64_t calls;
} layout_input;

static double layout_time(clockid_t clock_id) {
  struct timespec ts;
  if (clock_gettime(clock_id, &ts) != 0)
    return -1.0;
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static int layout_number(const char *text, size_t *out) {
  char *end;
  unsigned long value;
  errno = 0;
  value = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || text[0] == '-')
    return 0;
  *out = (size_t)value;
  return (unsigned long)*out == value;
}

static int layout_option(const char *name, size_t fallback, size_t maximum,
                         size_t *out) {
  const char *value = getenv(name);
  *out = fallback;
  return value == NULL || (layout_number(value, out) && *out <= maximum);
}

static int layout_error(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_TRANSPORT, errno, message, strerror(errno),
                      NULL, "layout-probe");
}

static int layout_write(int fd, const void *bytes, size_t count) {
  const unsigned char *p = bytes;
  ssize_t written;
  while (count != 0U) {
    written = write(fd, p, count);
    if (written < 0 && errno == EINTR)
      continue;
    if (written <= 0)
      return 0;
    p += written;
    count -= (size_t)written;
  }
  return 1;
}

static int layout_random_padding;

static void layout_body(char *body, size_t size, size_t id) {
  char prefix[64];
  size_t count;
  count = (size_t)snprintf(prefix, sizeof(prefix),
                           "{\"id\":\"%020lu\",\"data\":\"", (unsigned long)id);
  memcpy(body, prefix, count);
  if (layout_random_padding) {
    static const char alphabet[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
    unsigned long random = 17;
    size_t at;
    for (at = count; at < size - 2U; ++at) {
      random = (random * 1664525UL + 1013904223UL) & 0xffffffffUL;
      body[at] = alphabet[(random >> 24) & 63];
    }
  } else
    memset(body + count, 'x', size - count - 2U);
  memcpy(body + size - 2U, "\"}", 3U);
}

static void layout_stage_close(layout_stage *stage) {
  if (stage->fd >= 0) {
    (void)close(stage->fd);
    (void)unlink(stage->path);
  }
  lc_free_with_allocator(NULL, stage->bytes);
  memset(stage, 0, sizeof(*stage));
  stage->fd = -1;
}

static int layout_stage_write(layout_stage *stage, const char *root,
                              const void *bytes, size_t count,
                              lc_error *error) {
  unsigned char *grown;
  size_t capacity;
  if (stage->fd < 0 && count <= stage->limit - stage->length) {
    capacity = stage->capacity;
    if (capacity == 0U)
      capacity = stage->limit < 2048U ? stage->limit : 2048U;
    while (capacity < stage->length + count)
      capacity = capacity > stage->limit / 2U ? stage->limit : capacity * 2U;
    if (capacity != stage->capacity) {
      grown = lc_realloc_with_allocator(NULL, stage->bytes, capacity);
      if (grown == NULL)
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "cannot grow bounded producer buffer", NULL, NULL,
                            "layout-probe");
      stage->bytes = grown;
      stage->capacity = capacity;
    }
    memcpy(stage->bytes + stage->length, bytes, count);
  } else {
    if (stage->fd < 0) {
      (void)snprintf(stage->path, sizeof(stage->path), "%s/spool-XXXXXX", root);
      stage->fd = mkstemp(stage->path);
      if (stage->fd < 0 ||
          !layout_write(stage->fd, stage->bytes, stage->length))
        return layout_error(error, "cannot create producer spool");
      lc_free_with_allocator(NULL, stage->bytes);
      stage->bytes = NULL;
    }
    if (!layout_write(stage->fd, bytes, count))
      return layout_error(error, "cannot write producer spool");
  }
  stage->length += count;
  return LC_OK;
}

static size_t layout_stage_read(void *context, void *bytes, size_t count,
                                lc_error *error) {
  layout_stage *stage = context;
  ssize_t got;
  do {
    got = read(stage->fd, bytes, count);
  } while (got < 0 && errno == EINTR);
  if (got < 0)
    (void)layout_error(error, "cannot read producer spool");
  return got < 0 ? 0U : (size_t)got;
}

/* Both pull experiments borrow the same single-pass callback. Delay models
 * time waiting on input, without a background producer or transport cache. */
static size_t layout_input_read(lc_source *source, void *bytes, size_t count,
                                lc_error *error) {
  layout_input *input = source->impl;
  struct timespec delay, remaining;
  ++input->calls;
  if (count > input->run->source_chunk)
    count = input->run->source_chunk;
  if (count > input->run->body_bytes - input->offset)
    count = input->run->body_bytes - input->offset;
  if (count == 0U)
    return 0U;
  delay.tv_sec = 0;
  delay.tv_nsec = (long)input->run->source_delay_us * 1000L;
  if (delay.tv_nsec != 0L) {
    while (nanosleep(&delay, &remaining) != 0) {
      if (errno != EINTR) {
        (void)layout_error(error, "cannot delay comparison source");
        return 0U;
      }
      delay = remaining;
    }
  }
  memcpy(bytes, input->body + input->offset, count);
  input->offset += count;
  return count;
}

static int layout_latency_compare(const void *left, const void *right) {
  double a = *(const double *)left, b = *(const double *)right;
  return a < b ? -1 : a > b;
}

static void *layout_produce(void *context) {
  layout_worker *worker = context;
  layout_run *run = worker->run;
  lc_collection_append_req req;
  lc_collection_append_res result;
  lc_collection_writer *push;
  lc_source *source;
  lc_error error;
  layout_stage stage;
  layout_input input;
  char *body, id[48];
  size_t i, offset, count;
  double start;
  int rc = LC_OK;
  lc_error_init(&error);
  lc_collection_append_req_init(&req);
  body = lc_alloc_with_allocator(NULL, run->body_bytes + 1U);
  if (body == NULL)
    rc = LC_ERR_NOMEM;
  for (i = worker->ordinal; rc == LC_OK && i < run->rows; i += run->writers) {
    layout_body(body, run->body_bytes, i);
    (void)snprintf(id, sizeof(id), "event-%lu", (unsigned long)i);
    req.append_id = id;
    source = NULL;
    push = NULL;
    memset(&stage, 0, sizeof(stage));
    stage.fd = -1;
    stage.limit = run->limit;
    start = layout_time(CLOCK_MONOTONIC);
    if (strcmp(run->mode, "raw") == 0) {
      pthread_mutex_lock(&run->raw_mutex);
      body[run->body_bytes] = '\n';
      if (!layout_write(run->raw_fd, body, run->body_bytes + 1U))
        rc = layout_error(&error, "cannot append raw JSONL");
      pthread_mutex_unlock(&run->raw_mutex);
      if (rc == LC_OK && run->sync && fsync(run->raw_fd) != 0)
        rc = layout_error(&error, "cannot sync raw JSONL");
    } else {
      if (run->pull_compare) {
        memset(&input, 0, sizeof(input));
        input.pub.read = layout_input_read;
        input.pub.impl = &input;
        input.run = run;
        input.body = body;
        if (!strcmp(run->mode, "staged-pull"))
          rc = lc_collection_append_staged(run->collection, &req, &input.pub,
                                           &result, &error);
        else
          rc = lc_collection_append(run->collection, &req, &input.pub, &result,
                                    &error);
        worker->source_calls += input.calls;
        if (rc == LC_OK &&
            (input.offset != run->body_bytes ||
             input.calls != (run->body_bytes + run->source_chunk - 1U) /
                                    run->source_chunk +
                                1U))
          rc = lc_error_set(&error, LC_ERR_PROTOCOL, 0L,
                            "pull comparison changed source delivery", NULL,
                            NULL, "layout-probe");
      } else {
        if (strcmp(run->mode, "push") == 0)
          rc = lc_collection_append_begin(run->collection, &req, &push, &error);
        if (push != NULL || strcmp(run->mode, "buffered") == 0) {
          for (offset = 0U; rc == LC_OK && offset < run->body_bytes;
               offset += count) {
            count = run->body_bytes - offset;
            if (count > 4096U)
              count = 4096U;
            rc = push != NULL
                     ? lc_collection_writer_write(push, body + offset, count,
                                                  &error)
                     : layout_stage_write(&stage, run->root, body + offset,
                                          count, &error);
            if (stage.capacity > worker->peak_memory)
              worker->peak_memory = stage.capacity;
          }
        }
        if (rc == LC_OK && push != NULL)
          rc = lc_collection_writer_commit(push, &result, &error);
        else if (rc == LC_OK) {
          if (strcmp(run->mode, "buffered") != 0)
            rc = lc_source_from_memory(body, run->body_bytes, &source, &error);
          else if (stage.fd < 0)
            rc = lc_source_from_memory(stage.bytes, stage.length, &source,
                                       &error);
          else {
            ++worker->spooled;
            if (lseek(stage.fd, 0, SEEK_SET) < 0)
              rc = layout_error(&error, "cannot rewind producer spool");
            else
              rc = lc_source_from_callbacks(layout_stage_read, NULL, NULL,
                                            &stage, &source, &error);
          }
          if (rc == LC_OK)
            rc = lc_collection_append(run->collection, &req, source, &result,
                                      &error);
        }
      }
      if (rc == LC_OK &&
          (result.position == 0U || result.position > run->rows ||
           result.bytes != run->body_bytes || result.duplicate))
        rc = lc_error_set(&error, LC_ERR_PROTOCOL, 0L,
                          "append result differs from producer input", NULL,
                          NULL, "layout-probe");
    }
    run->latencies[i] = (layout_time(CLOCK_MONOTONIC) - start) * 1000000.0;
    lc_source_close(source);
    lc_collection_writer_close(push);
    layout_stage_close(&stage);
  }
  if (rc != LC_OK)
    fprintf(stderr, "producer failed: %d %s\n", rc,
            error.message != NULL ? error.message : "allocation failure");
  lc_free_with_allocator(NULL, body);
  lc_error_cleanup(&error);
  worker->result = rc;
  return NULL;
}

static int layout_sink(lc_sink *sink, const void *input, size_t count,
                       lc_error *error) {
  layout_read *read = sink->impl;
  const unsigned char *bytes = input;
  size_t i, at;
  for (i = 0U; read->verify && i < count; ++i) {
    at = read->offset++;
    if (at >= 7U && at < 27U) {
      if (bytes[i] < '0' || bytes[i] > '9' || read->id > read->rows)
        goto invalid;
      read->id = read->id * 10U + bytes[i] - '0';
    } else if (bytes[i] != (at == read->body_bytes ? '\n' : read->expected[at]))
      goto invalid;
    if (read->offset == read->body_bytes + 1U) {
      if (read->id >= read->rows ||
          (read->seen[read->id / 8U] & (1U << (read->id % 8U))) != 0U)
        goto invalid;
      read->seen[read->id / 8U] |= (unsigned char)(1U << (read->id % 8U));
      read->id = 0U;
      read->offset = 0U;
      ++read->records;
    }
  }
  read->bytes += count;
  return 1;
invalid:
  (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                     "read changed, duplicated or reordered state bytes", NULL,
                     NULL, "layout-probe");
  return 0;
}

static int layout_metadata(void *context,
                           const lc_collection_read_metadata *entry,
                           lc_error *error) {
  layout_read *read = context;
  if (entry->kind == LC_COLLECTION_READ_METADATA_ENTRY &&
      entry->position != read->records)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "read positions are not in commit order", NULL, NULL,
                        "layout-probe");
  return LC_OK;
}

static int layout_read_all(layout_run *run, lc_collection *collection,
                           const char *raw, int verify, const char *phase,
                           lc_error *error) {
  layout_read counters;
  layout_io io;
  struct rusage usage_before = {0}, usage_after = {0};
  lc_collection_read_opts options;
  lc_collection_read_res result;
  unsigned char *buffer = NULL;
  double start, cpu;
  ssize_t got;
  int rc = LC_OK, fd = -1;
  memset(&counters, 0, sizeof(counters));
  counters.sink.impl = &counters;
  counters.sink.write = layout_sink;
  counters.rows = run->rows;
  counters.body_bytes = run->body_bytes;
  counters.verify = verify;
  lc_collection_read_opts_init(&options);
  lc_collection_read_res_init(&result);
  if (verify) {
    counters.expected = lc_alloc_with_allocator(NULL, run->body_bytes + 1U);
    counters.seen = lc_calloc_with_allocator(NULL, (run->rows + 7U) / 8U, 1U);
    if (counters.expected == NULL || counters.seen == NULL) {
      rc = LC_ERR_NOMEM;
      goto done;
    }
    layout_body(counters.expected, run->body_bytes, 0U);
    options.on_metadata = layout_metadata;
    options.metadata_context = &counters;
  }
  if (collection == NULL) {
    buffer = lc_alloc_with_allocator(NULL, 128U * 1024U);
    fd = open(raw, O_RDONLY | O_CLOEXEC);
    if (buffer == NULL || fd < 0) {
      rc = layout_error(error, "cannot open raw read input");
      goto done;
    }
  }
  if (!layout_io_read(&io)) {
    rc = layout_error(error, "cannot read process I/O counters");
    goto done;
  }
  (void)getrusage(RUSAGE_SELF, &usage_before);
  cpu = layout_time(CLOCK_PROCESS_CPUTIME_ID);
  start = layout_time(CLOCK_MONOTONIC);
  if (collection != NULL)
    rc = lc_collection_read(collection, &options, &counters.sink, &result,
                            error);
  else {
    for (;;) {
      got = read(fd, buffer, 128U * 1024U);
      if (got < 0 && errno == EINTR)
        continue;
      if (got < 0) {
        rc = layout_error(error, "raw read failed");
        break;
      }
      if (got == 0)
        break;
      if (!layout_sink(&counters.sink, buffer, (size_t)got, error)) {
        rc = error->code;
        break;
      }
    }
  }
  start = layout_time(CLOCK_MONOTONIC) - start;
  cpu = layout_time(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  (void)getrusage(RUSAGE_SELF, &usage_after);
  if (!verify)
    layout_io_report(phase, &io);
  if (rc == LC_OK &&
      (counters.bytes != (uint64_t)run->rows * (run->body_bytes + 1U) ||
       (collection != NULL &&
        (!result.complete || result.completed_records != run->rows)) ||
       (verify && (counters.offset != 0U || counters.records != run->rows))))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L, "read is incomplete", NULL,
                      NULL, "layout-probe");
  if (!verify)
    printf(
        "phase=%s seconds=%.6f cpu_seconds=%.6f bytes=%lu MiBps=%.3f "
        "user_seconds=%.6f system_seconds=%.6f\n",
        phase, start, cpu, (unsigned long)counters.bytes,
        start > 0.0 ? (double)counters.bytes / 1048576.0 / start : 0.0,
        (double)(usage_after.ru_utime.tv_sec - usage_before.ru_utime.tv_sec) +
            (double)(usage_after.ru_utime.tv_usec -
                     usage_before.ru_utime.tv_usec) /
                1000000.0,
        (double)(usage_after.ru_stime.tv_sec - usage_before.ru_stime.tv_sec) +
            (double)(usage_after.ru_stime.tv_usec -
                     usage_before.ru_stime.tv_usec) /
                1000000.0);
done:
  if (fd >= 0)
    (void)close(fd);
  lc_free_with_allocator(NULL, buffer);
  lc_free_with_allocator(NULL, counters.expected);
  lc_free_with_allocator(NULL, counters.seen);
  lc_collection_read_res_cleanup(&result);
  return rc;
}

static int layout_walk(const char *path, layout_files *files, int cold,
                       unsigned depth) {
  struct stat st;
  struct dirent *entry;
  DIR *dir;
  char *child;
  const char *leaf;
  int fd, rc = 1;
  if (depth > 16U || lstat(path, &st) != 0)
    return 0;
  if (S_ISREG(st.st_mode)) {
    ++files->files;
    files->bytes += (uint64_t)st.st_size;
    files->allocated += (uint64_t)st.st_blocks * 512U;
    leaf = strrchr(path, '/');
    leaf = leaf != NULL ? leaf + 1 : path;
    if (strncmp(leaf, "seg-", 4U) == 0 || strncmp(leaf, "snapshot-", 9U) == 0)
      ++files->canonical;
    if (strncmp(leaf, "page-", 5U) == 0)
      ++files->query_pages;
    if (cold) {
      fd = open(path, O_RDONLY | O_CLOEXEC);
      if (fd < 0)
        return 0;
      rc = fsync(fd) == 0;
#if defined(__linux__)
      if (rc)
        rc = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) == 0;
#endif
      (void)close(fd);
    }
    return rc;
  }
  if (!S_ISDIR(st.st_mode))
    return 0;
  ++files->directories;
  dir = opendir(path);
  if (dir == NULL)
    return 0;
  while (rc) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      rc = errno == 0;
      break;
    }
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;
    child = lc_pouch_path_join(NULL, path, entry->d_name);
    rc = child != NULL && layout_walk(child, files, cold, depth + 1U);
    lc_free_with_allocator(NULL, child);
  }
  (void)closedir(dir);
  return rc;
}

static int layout_inventory(layout_run *run, const char *phase, int cold) {
  layout_files files;
  memset(&files, 0, sizeof(files));
  if (!layout_walk(run->root, &files, cold, 0U))
    return 0;
  printf("phase=%s files=%lu directories=%lu canonical=%lu query_pages=%lu "
         "bytes=%lu allocated=%lu\n",
         phase, (unsigned long)files.files, (unsigned long)files.directories,
         (unsigned long)files.canonical, (unsigned long)files.query_pages,
         (unsigned long)files.bytes, (unsigned long)files.allocated);
  return 1;
}

int main(int argc, char **argv) {
  layout_run run;
  layout_io io;
  layout_worker workers[16];
  pthread_t threads[16];
  lc_client *client = NULL;
  lc_collection *collection = NULL;
  lc_client_config config;
  lc_collection_config cc;
  lc_pouch_settings settings;
  lc_pouch_maintenance_options maintenance = {0};
  lc_pouch_maintenance_result maintained = {0};
  lc_error error;
  char prefix[512], template_path[512], endpoint[640], raw[640];
  const char *endpoints[1];
  size_t i, created = 0U, spooled = 0U, memory = 0U;
  size_t sync, index;
  size_t compact, compress, encrypt, entropy, extra_reads;
  char *crypto_key = NULL;
  uint64_t source_calls = 0U;
  double start, cpu;
  int rc = LC_OK;
  memset(&run, 0, sizeof(run));
  memset(workers, 0, sizeof(workers));
  lc_error_init(&error);
  run.raw_fd = -1;
  if (argc != 9) {
    fprintf(stderr,
            "usage: %s raw|pull|push|buffered|direct-pull|staged-pull ROWS "
            "BODY_BYTES BUFFER_LIMIT "
            "WRITERS SYNC INDEX DIRECTORY\n",
            argv[0]);
    return 2;
  }
  run.mode = argv[1];
  run.pull_compare =
      !strcmp(run.mode, "direct-pull") || !strcmp(run.mode, "staged-pull");
  if (!layout_option("LOCKDC_LAYOUT_SOURCE_CHUNK", 16384U, 16384U,
                     &run.source_chunk) ||
      run.source_chunk == 0U ||
      !layout_option("LOCKDC_LAYOUT_SOURCE_DELAY_US", 0U, 10000U,
                     &run.source_delay_us) ||
      !layout_option("LOCKDC_LAYOUT_COMPACT", 1U, 1U, &compact) ||
      !layout_option("LOCKDC_LAYOUT_COMPRESS", 0U, 1U, &compress) ||
      !layout_option("LOCKDC_LAYOUT_CRYPTO", 0U, 1U, &encrypt) ||
      !layout_option("LOCKDC_LAYOUT_ENTROPY", 0U, 1U, &entropy) ||
      !layout_option("LOCKDC_LAYOUT_EXTRA_READS", 0U, 1000U, &extra_reads)) {
    fprintf(stderr, "invalid layout source/compaction options\n");
    return 2;
  }
  run.compact = (int)compact;
  layout_random_padding = (int)entropy;
  if (!strcmp(run.mode, "raw") && (compress || encrypt)) {
    fprintf(
        stderr,
        "codec options require a collection mode; raw is plaintext JSONL\n");
    return 2;
  }
  if (!layout_number(argv[2], &run.rows) ||
      !layout_number(argv[3], &run.body_bytes) ||
      !layout_number(argv[4], &run.limit) ||
      !layout_number(argv[5], &run.writers) || !layout_number(argv[6], &sync) ||
      !layout_number(argv[7], &index) || sync > 1U || index > 1U) {
    fprintf(stderr, "layout probe expects unsigned integer arguments\n");
    return 2;
  }
  run.sync = (int)sync;
  if ((strcmp(run.mode, "raw") && strcmp(run.mode, "pull") &&
       strcmp(run.mode, "push") && strcmp(run.mode, "buffered") &&
       !run.pull_compare) ||
      run.rows == 0U || run.rows > 1000000U || run.body_bytes < 64U ||
      run.body_bytes > 16U * 1024U * 1024U || run.limit > 16U * 1024U * 1024U ||
      run.writers == 0U || run.writers > 16U || argv[8][0] != '/' ||
      (run.sync != 0 && run.sync != 1)) {
    fprintf(stderr, "invalid layout probe arguments\n");
    return 2;
  }
  if (snprintf(prefix, sizeof(prefix), "%s/layout-probe-", argv[8]) >=
          (int)sizeof(prefix) ||
      snprintf(template_path, sizeof(template_path), "%sXXXXXX", prefix) >=
          (int)sizeof(template_path) ||
      !lc_test_tmp_mkdtemp(template_path, run.root, sizeof(run.root), prefix))
    return 2;
  if (pthread_mutex_init(&run.raw_mutex, NULL) != 0) {
    lc_test_tmp_cleanup_path(run.root, prefix);
    return 2;
  }
  if (getenv("LOCKDC_TEST_ALLOCATOR_TRACKING") != NULL &&
      strcmp(getenv("LOCKDC_TEST_ALLOCATOR_TRACKING"), "1") == 0)
    rc = lc_allocator_tracking_configure(1, &error);
  run.latencies =
      lc_calloc_with_allocator(NULL, run.rows, sizeof(*run.latencies));
  if (run.latencies == NULL)
    rc = LC_ERR_NOMEM;
  (void)snprintf(raw, sizeof(raw), "%s/raw.jsonl", run.root);
  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s", run.root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_DURABLE_SYNC | LC_POUCH_SETTING_SINGLE_WRITER |
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_QUERY_INDEXING |
      LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  settings.single_writer = 0;
  settings.durable_sync = run.sync;
  settings.background_compaction_enabled = 0;
  settings.query_indexing_enabled = (int)index;
  settings.segment_target_bytes = 1024U * 1024U;
  settings.set_mask |= LC_POUCH_SETTING_COMPRESSION;
  settings.compression = compress ? "zlib" : "none";
  if (encrypt) {
    if (rc == LC_OK)
      rc = lc_pouch_crypto_generate_key_string(&crypto_key, &error);
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = crypto_key;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "layout";
  config.pouch_settings = &settings;
  lc_collection_config_init(&cc);
  cc.name = "events";
  if (rc == LC_OK && !strcmp(run.mode, "raw")) {
    run.raw_fd = open(raw, O_RDWR | O_CREAT | O_EXCL | O_APPEND, 0600);
    if (run.raw_fd < 0)
      rc = layout_error(&error, "cannot create raw baseline");
  } else if (rc == LC_OK) {
    rc = lc_client_open(&config, &client, &error);
    if (rc == LC_OK)
      rc = lc_client_create_collection(client, &cc, &run.collection, &error);
  }
  printf("mode=%s rows=%lu body_bytes=%lu buffer_limit=%lu writers=%lu sync=%d "
         "index=%d source_chunk=%lu source_delay_us=%lu compact=%d "
         "compress=%lu crypto=%lu entropy=%lu\n",
         run.mode, (unsigned long)run.rows, (unsigned long)run.body_bytes,
         (unsigned long)run.limit, (unsigned long)run.writers, run.sync,
         settings.query_indexing_enabled, (unsigned long)run.source_chunk,
         (unsigned long)run.source_delay_us, run.compact,
         (unsigned long)compress, (unsigned long)encrypt,
         (unsigned long)entropy);
  if (!layout_io_read(&io))
    rc = layout_error(&error, "cannot read process I/O counters");
  cpu = layout_time(CLOCK_PROCESS_CPUTIME_ID);
  start = layout_time(CLOCK_MONOTONIC);
  for (i = 0U; rc == LC_OK && i < run.writers; ++i) {
    workers[i].run = &run;
    workers[i].ordinal = i;
    if (pthread_create(&threads[i], NULL, layout_produce, &workers[i]) != 0)
      rc = LC_ERR_TRANSPORT;
    else
      ++created;
  }
  for (i = 0U; i < created; ++i) {
    (void)pthread_join(threads[i], NULL);
    if (workers[i].result != LC_OK)
      rc = workers[i].result;
    spooled += workers[i].spooled;
    memory += workers[i].peak_memory;
    source_calls += workers[i].source_calls;
  }
  start = layout_time(CLOCK_MONOTONIC) - start;
  cpu = layout_time(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  layout_io_report("append_io", &io);
  printf(
      "phase=append seconds=%.6f cpu_seconds=%.6f per_op_us=%.3f "
      "adapter_spooled=%lu adapter_stage_capacity_bound=%lu source_calls=%lu\n",
      start, cpu, start * 1000000.0 / (double)run.rows, (unsigned long)spooled,
      (unsigned long)memory, (unsigned long)source_calls);
  if (rc == LC_OK) {
    qsort(run.latencies, run.rows, sizeof(*run.latencies),
          layout_latency_compare);
    printf("phase=latency p50_us=%.3f p95_us=%.3f p99_us=%.3f max_us=%.3f\n",
           run.latencies[(run.rows - 1U) / 2U],
           run.latencies[(run.rows * 95U + 99U) / 100U - 1U],
           run.latencies[(run.rows * 99U + 99U) / 100U - 1U],
           run.latencies[run.rows - 1U]);
  }
  if (rc == LC_OK && !strcmp(run.mode, "buffered") &&
      (memory > run.limit * run.writers ||
       spooled != (run.body_bytes > run.limit ? run.rows : 0U)))
    rc = LC_ERR_PROTOCOL;
  (void)layout_io_read(&io);
  cpu = layout_time(CLOCK_PROCESS_CPUTIME_ID);
  start = layout_time(CLOCK_MONOTONIC);
  lc_collection_close(run.collection);
  run.collection = NULL;
  lc_client_close(client);
  client = NULL;
  if (run.raw_fd >= 0) {
    if (fsync(run.raw_fd) != 0)
      rc = layout_error(&error, "cannot checkpoint raw baseline");
    (void)close(run.raw_fd);
    run.raw_fd = -1;
  }
  printf("phase=checkpoint seconds=%.6f cpu_seconds=%.6f\n",
         layout_time(CLOCK_MONOTONIC) - start,
         layout_time(CLOCK_PROCESS_CPUTIME_ID) - cpu);
  layout_io_report("checkpoint_io", &io);
  start = layout_time(CLOCK_MONOTONIC);
  if (rc == LC_OK && strcmp(run.mode, "raw")) {
    rc = lc_client_open(&config, &client, &error);
    if (rc == LC_OK)
      rc = lc_client_open_collection(client, &cc, &collection, &error);
  }
  printf("phase=open seconds=%.6f\n", layout_time(CLOCK_MONOTONIC) - start);
  /* Recovery reads canonical metadata and may warm adjacent payload pages.
   * Advise after opening so both layouts start the read with the same policy.
   */
  if (rc == LC_OK && !layout_inventory(&run, "files_before", 1))
    rc = layout_error(&error, "cannot inspect or advise probe files");
  if (rc == LC_OK)
    rc = layout_read_all(&run, collection, raw, 0, "read_advised_cold", &error);
  if (rc == LC_OK)
    rc = layout_read_all(&run, collection, raw, 0, "read_warm", &error);
  for (i = 0; rc == LC_OK && i < extra_reads; ++i)
    rc = layout_read_all(&run, collection, raw, 0, "read_repeat", &error);
  if (rc == LC_OK)
    rc = layout_read_all(&run, collection, raw, 1, "verify", &error);
  if (rc == LC_OK && collection != NULL && run.compact) {
    ((lc_client_handle *)client)->pouch->compaction_delete_grace_seconds = 0;
    maintenance.ns = "layout";
    rc = lc_pouch_maintenance_run(((lc_client_handle *)client)->pouch,
                                  &maintenance, &maintained, &error);
    printf("phase=automatic_compaction compacted=%lu\n",
           (unsigned long)maintained.compacted_collection_count);
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    maintenance.force = 1;
    (void)layout_io_read(&io);
    cpu = layout_time(CLOCK_PROCESS_CPUTIME_ID);
    start = layout_time(CLOCK_MONOTONIC);
    if (rc == LC_OK)
      rc = lc_pouch_maintenance_run(((lc_client_handle *)client)->pouch,
                                    &maintenance, &maintained, &error);
    printf(
        "phase=forced_compaction seconds=%.6f cpu_seconds=%.6f compacted=%lu\n",
        layout_time(CLOCK_MONOTONIC) - start,
        layout_time(CLOCK_PROCESS_CPUTIME_ID) - cpu,
        (unsigned long)maintained.compacted_collection_count);
    layout_io_report("compaction_io", &io);
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    lc_collection_close(collection);
    collection = NULL;
    lc_client_close(client);
    client = NULL;
    if (rc == LC_OK)
      rc = lc_client_open(&config, &client, &error);
    if (rc == LC_OK)
      rc = lc_client_open_collection(client, &cc, &collection, &error);
    if (rc == LC_OK && !layout_inventory(&run, "files_after", 1))
      rc = layout_error(&error, "cannot inspect compacted files");
    if (rc == LC_OK)
      rc = layout_read_all(&run, collection, raw, 0, "read_packed_advised_cold",
                           &error);
    if (rc == LC_OK)
      rc =
          layout_read_all(&run, collection, raw, 0, "read_packed_warm", &error);
    if (rc == LC_OK)
      rc = layout_read_all(&run, collection, raw, 1, "verify_packed", &error);
  }
  if (rc != LC_OK)
    fprintf(stderr, "layout probe failed: %d %s\n", rc,
            error.message != NULL ? error.message : "unknown error");
  lc_collection_close(collection);
  lc_client_close(client);
  lc_pouch_crypto_key_string_free(crypto_key);
  lc_error_cleanup(&error);
  pthread_mutex_destroy(&run.raw_mutex);
  lc_free_with_allocator(NULL, run.latencies);
  lc_test_tmp_cleanup_path(run.root, prefix);
  return rc == LC_OK ? 0 : 1;
}
