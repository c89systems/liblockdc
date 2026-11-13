#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "lc_collection_json.h"
#include "lc_pouch.h"
#include "lc_pouch_record.h"
#include "space.h"
#include "stream.h"
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
#include <zlib.h>

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
  ps_stream *stream;
  size_t round, records;
  char *body;
  int rc;
} worker;
struct comparison {
  int container, collection, sync, streams, existing_records;
  size_t rows, bytes, writers, rounds;
  ps_space *space;
  lc_pouch *pouch;
  lc_client *client;
  worker *workers;
  pthread_t *threads;
  double *latencies;
  uint64_t *offsets;
  uint32_t *payload_crcs;
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
static void stream_put64(unsigned char *p, uint64_t value) {
  size_t i;
  for (i = 0; i < 8; ++i)
    p[i] = (unsigned char)(value >> (i * 8));
}
static uint64_t stream_get64(const unsigned char *p) {
  size_t i;
  uint64_t value = 0;
  for (i = 0; i < 8; ++i)
    value |= (uint64_t)p[i] << (i * 8);
  return value;
}
static ps_key stream_storage_key(size_t ordinal) {
  ps_key key;
  key.kind = PS_STREAM;
  key.owner = ordinal + 1;
  key.key = 1;
  return key;
}
/* Rebuild the benchmark's derived latest-row offsets from canonical headers
 * and metadata. Verify payload CRCs beyond the trusted checkpoint boundary.
 * This is a storage experiment, not Pouch catalog/receipt/lease semantics. */
static int stream_recover(ps_stream *stream, uint64_t checkpoint,
                          uint64_t capacity, uint64_t *accepted, void *context,
                          lc_error *e) {
  worker *w = context;
  comparison *c = w->run;
  lc_pouch_record_header record;
  unsigned char header[32], meta[80], buffer[4096], zero[32] = {0};
  uint64_t cursor = 0, payload, end, id, round;
  unsigned long crc;
  size_t take;
  char ns[64], name[64];
  int rc = LC_OK;
  w->records = 0;
  while (capacity - cursor >= sizeof(header)) {
    rc = ps_stream_recovery_read(stream, cursor, header, sizeof(header), e);
    if (rc != LC_OK || !memcmp(header, zero, sizeof(header)))
      break;
    rc = lc_pouch_record_header_decode(header, &record, e);
    if (rc == LC_ERR_PROTOCOL && cursor >= checkpoint) {
      lc_error_cleanup(e);
      rc = LC_OK;
      break;
    }
    if (rc != LC_OK)
      break;
    if (record.flags || record.type != 1 || !record.key_len ||
        record.key_len >= 64 || record.meta_len != 16 ||
        record.payload_len != c->bytes ||
        record.key_len + 48 > capacity - cursor ||
        record.payload_len > capacity - cursor - record.key_len - 48)
      break;
    rc = ps_stream_recovery_read(stream, cursor + 32, meta,
                                 (size_t)record.key_len + 16, e);
    if (rc != LC_OK)
      break;
    id = stream_get64(meta + record.key_len);
    round = stream_get64(meta + record.key_len + 8);
    require(id < c->rows && id % c->writers == w->ordinal && round < c->rounds,
            "canonical row identity");
    names(w->ordinal, (size_t)id, ns, name);
    require(strlen(name) == record.key_len &&
                !memcmp(meta, name, record.key_len),
            "canonical key binding");
    payload = cursor + record.key_len + 48;
    end = payload + record.payload_len;
    require(cursor >= checkpoint || end <= checkpoint,
            "checkpoint record boundary");
    if (cursor >= checkpoint) {
      crc = 0;
      while (payload < end && rc == LC_OK) {
        take = (size_t)(end - payload);
        if (take > sizeof(buffer))
          take = sizeof(buffer);
        rc = ps_stream_recovery_read(stream, payload, buffer, take, e);
        if (rc == LC_OK)
          crc = crc32(crc, buffer, (uInt)take);
        payload += take;
      }
      if (rc != LC_OK || crc != record.payload_crc)
        break;
    }
    c->offsets[id] = cursor + record.key_len + 48;
    c->payload_crcs[id] = (uint32_t)record.payload_crc;
    cursor = end;
    ++w->records;
  }
  *accepted = cursor;
  return rc;
}
typedef struct stream_output {
  ps_stream_writer *writer;
  uint64_t bytes;
  unsigned long crc;
} stream_output;
static int stream_sink(lc_sink *sink, const void *bytes, size_t count,
                       lc_error *e) {
  stream_output *out = sink->impl;
  if (ps_stream_write(out->writer, bytes, count, e) != LC_OK)
    return 0;
  out->crc = crc32(out->crc, bytes, (uInt)count);
  out->bytes += count;
  return 1;
}
static int stream_write_one(worker *w, size_t id, lc_source *source,
                            lc_error *e) {
  comparison *c = w->run;
  ps_stream_writer *destination = NULL;
  stream_output out;
  lc_pouch_record_header record;
  lc_sink sink;
  unsigned char header[32], meta[16], buffer[4096];
  char ns[64], name[64];
  uint64_t start = 0;
  lc_u64 written;
  size_t n;
  int rc = LC_OK;
  names(w->ordinal, id, ns, name);
  if (!w->stream)
    rc = ps_stream_open(c->space, stream_storage_key(w->ordinal), 1, NULL,
                        stream_recover, w, &w->stream, e);
  if (rc == LC_OK)
    rc = ps_stream_begin(w->stream, &destination, e);
  memset(&record, 0, sizeof(record));
  memset(&out, 0, sizeof(out));
  memset(&sink, 0, sizeof(sink));
  record.type = 1;
  record.flags = LC_POUCH_RECORD_FLAG_PENDING;
  record.key_len = (unsigned long)strlen(name);
  record.meta_len = sizeof(meta);
  lc_pouch_record_header_encode(&record, header);
  stream_put64(meta, id);
  stream_put64(meta + 8, w->round);
  if (rc == LC_OK) {
    start = ps_stream_writer_start(destination);
    rc = ps_stream_write(destination, header, sizeof(header), e);
  }
  if (rc == LC_OK)
    rc = ps_stream_write(destination, name, strlen(name), e);
  if (rc == LC_OK)
    rc = ps_stream_write(destination, meta, sizeof(meta), e);
  out.writer = destination;
  sink.impl = &out;
  sink.write = stream_sink;
  if (rc == LC_OK && c->collection) {
    rc = lc_collection_json_copy(source, &sink, NULL, &written, e);
    if (rc == LC_OK && written != c->bytes)
      rc = LC_ERR_PROTOCOL;
  } else if (rc == LC_OK)
    while ((n = source->read(source, buffer, sizeof(buffer), e)) != 0) {
      if (!stream_sink(&sink, buffer, n, e)) {
        rc = e->code;
        break;
      }
    }
  if (rc == LC_OK) {
    record.flags = 0;
    record.payload_len = out.bytes;
    record.payload_crc = out.crc;
    lc_pouch_record_header_encode(&record, header);
    rc = ps_stream_write_at(destination, 0, header, sizeof(header), e);
  }
  if (rc == LC_OK)
    rc = ps_stream_commit(destination, e);
  if (rc == LC_OK) {
    c->offsets[id] = start + strlen(name) + 48;
    c->payload_crcs[id] = (uint32_t)out.crc;
    ++w->records;
  }
  ps_stream_writer_close(destination);
  return rc;
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
  if (c->streams) {
    rc = stream_write_one(w, id, source, e);
  } else if (c->container) {
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
      w->round = round;
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
    if (c->streams) {
      for (i = 0; i < c->rows; ++i)
        c->offsets[i] = UINT64_MAX;
      for (i = 0; i < c->writers; ++i)
        if (c->existing_records || c->collection)
          check(ps_stream_open(c->space, stream_storage_key(i), create, NULL,
                               stream_recover, &c->workers[i],
                               &c->workers[i].stream, e),
                e);
    }
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
    if (c->streams)
      for (i = 0; i < c->writers; ++i) {
        check(ps_stream_close(c->workers[i].stream, e), e);
        c->workers[i].stream = NULL;
      }
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
  ps_stream_reader *stream_reader;
  lc_pouch_state_read_result result;
  char ns[64], name[64];
  unsigned char buffer[4096];
  size_t id, ordinal, offset, got;
  unsigned long crc;
  for (id = 0; id < c->rows; ++id) {
    ordinal = id % c->writers;
    make_body(c->workers[ordinal].body, c->bytes, id, c->rounds - 1);
    offset = 0;
    if (c->streams) {
      crc = 0;
      require(c->offsets[id] != UINT64_MAX, "canonical latest row exists");
      check(ps_stream_read_open(c->workers[ordinal].stream, c->offsets[id],
                                c->bytes, &stream_reader, e),
            e);
      do {
        check(ps_stream_read(stream_reader, buffer, sizeof(buffer), &got, e),
              e);
        crc = crc32(crc, buffer, (uInt)got);
        require(got <= c->bytes - offset &&
                    !memcmp(buffer, c->workers[ordinal].body + offset, got),
                "canonical stream entry bytes");
        offset += got;
      } while (got);
      ps_stream_reader_close(stream_reader);
      require(crc == c->payload_crcs[id], "canonical stream payload CRC");
    } else if (c->container) {
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
  long packed_pages = -1, packed_bytes = -1, payload_allocated = -1;
  long journal_used = -1, stream_grants = -1;
  ps_stream_info stream_info;
  struct rusage usage;
  char prefix[1024], template_path[1100];
  size_t i, count;
  double start, cpu, seconds, cpu_seconds, read_seconds, close_seconds,
      open_seconds, create_seconds, reopen_read_seconds, preopen_seconds = 0;
  int preopen;
  if (argc != 8 && argc != 9) {
    fprintf(stderr,
            "usage: %s space-state|stream-state|pouch-state|space-collection|"
            "stream-collection|pouch-collection "
            "DIRECTORY ROWS BYTES SYNC WRITERS CHUNK [--reopen]\n",
            argv[0]);
    return 2;
  }
  memset(&c, 0, sizeof(c));
  preopen = argc == 9;
  require(!preopen || !strcmp(argv[8], "--reopen"),
          "optional flag must be --reopen");
  require(!strcmp(argv[1], "space-state") || !strcmp(argv[1], "pouch-state") ||
              !strcmp(argv[1], "space-collection") ||
              !strcmp(argv[1], "pouch-collection") ||
              !strcmp(argv[1], "stream-state") ||
              !strcmp(argv[1], "stream-collection"),
          "mode");
  c.streams = !strncmp(argv[1], "stream-", 7);
  c.container = c.streams || !strncmp(argv[1], "space-", 6);
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
  c.offsets = calloc(c.rows, sizeof(*c.offsets));
  c.payload_crcs = calloc(c.rows, sizeof(*c.payload_crcs));
  require(c.workers && c.threads && c.latencies && c.offsets && c.payload_crcs,
          "benchmark allocations");
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
  if (preopen) {
    start = now(CLOCK_MONOTONIC);
    close_stores(&c, &e);
    open_stores(&c, 0, &e);
    preopen_seconds = now(CLOCK_MONOTONIC) - start;
  }
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
  c.existing_records = 1;
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
    packed_pages = (long)stats.packed_pages;
    packed_bytes = (long)stats.packed_bytes;
    payload_allocated = (long)stats.allocated_bytes;
    journal_used = (long)stats.journal_bytes;
    if (c.streams) {
      stream_grants = 0;
      for (i = 0; i < c.writers; ++i) {
        check(ps_stream_status(c.workers[i].stream, &stream_info, &e), &e);
        stream_grants += (long)stream_info.runs;
      }
    }
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
    if (c.streams) {
      require(stats.entries == (uint64_t)stream_grants + c.writers,
              "recovered physical stream/grant count");
      for (i = 0; i < c.writers; ++i)
        require(c.workers[i].records ==
                    c.rounds * ((c.rows + c.writers - 1 - i) / c.writers),
                "recovered canonical record count");
    } else
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
         "metadata_pages=%ld packed_pages=%ld packed_bytes=%ld "
         "payload_allocated=%ld journal_used=%ld stream_grants=%ld "
         "preopened=%d preopen_ms=%.6f max_us=%.3f verified=1\n",
         checkpoints, directory_writes, descriptor_writes, metadata_pages,
         packed_pages, packed_bytes, payload_allocated, journal_used,
         stream_grants, preopen, preopen_seconds * 1000,
         c.latencies[count - 1] * 1e6);
  close_stores(&c, &e);
  for (i = 0; i < c.writers; ++i)
    free(c.workers[i].body);
  free(c.latencies);
  free(c.offsets);
  free(c.payload_crcs);
  free(c.threads);
  free(c.workers);
  pthread_cond_destroy(&c.condition);
  pthread_mutex_destroy(&c.mutex);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(c.root, prefix);
  return 0;
}
