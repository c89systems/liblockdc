#define _GNU_SOURCE
#include "../../tests/support/lc_test_tmp.h"
#include "catalog.h"
#include "lc_api_internal.h"
#include "lc_collection_json.h"
#include "lc_pouch.h"
#include "lc_pouch_collection_record.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_record.h"
#include "lc_pouch_state_record.h"
#include "namespace.h"
#include "pouch.h"
#include "record_io.h"
#include "root.h"
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
  ps_namespace *namespace;
  size_t round, records;
  char *body;
  char collection_id[LC_XID_STRING_SIZE];
  int rc;
} worker;
struct comparison {
  int container, collection, sync, streams, existing_records, runtime;
  int read_method; /* 1: read-many; 2: scan bodies; 3: scan metadata; 4: points.
                    */
  int mutation; /* 1: metadata over seeded bodies; 2: tombstone seeded keys. */
  int compaction; /* Force one pass per namespace after the two PUT rounds. */
  int durable_scope; /* Force durability after both PUT rounds per writer. */
  size_t rows, bytes, writers, rounds;
  ps_space *space;
  ps_root *owner;
  lc_pouch *pouch;
  lc_pouch *record_owner;
  lc_client *client;
  worker *workers;
  pthread_t *threads;
  double *latencies;
  uint64_t *offsets, *segments;
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
static const char canonical_leaf[] = "seg-00000000000000000001.log";
/* Use the production collection decoder, including its full identity,
 * retention, descriptor and payload-span checks. The derived row offset table
 * still belongs to this benchmark, not to the public collection projection. */
static int collection_stream_recover(ps_stream *stream, uint64_t checkpoint,
                                     uint64_t capacity, uint64_t *accepted,
                                     worker *w, lc_error *e) {
  comparison *c = w->run;
  lc_pouch_record_io io = ps_record_recovery_input(stream);
  lc_pouch_collection_record record;
  unsigned char buffer[4096];
  struct iovec part;
  uint64_t cursor = 0, payload;
  unsigned long id, crc;
  char *end;
  int rc;
  w->records = 0;
  while (capacity - cursor >= LC_POUCH_RECORD_HEADER_BYTES) {
    rc = lc_pouch_collection_record_read(io, cursor, capacity, canonical_leaf,
                                         &record, e);
    if (rc != LC_OK) {
      if (cursor < checkpoint ||
          (rc != LC_ERR_PROTOCOL && rc != LC_ERR_PENDING))
        return rc;
      lc_error_cleanup(e);
      lc_error_init(e);
      break;
    }
    require(!strncmp(record.append_id, "key-", 4), "canonical append ID");
    errno = 0;
    id = strtoul(record.append_id + 4, &end, 10);
    require(!errno && !*end && end != record.append_id + 4 && id < c->rows &&
                id % c->writers == w->ordinal && record.kind == 1 &&
                record.bytes == c->bytes && record.stored_bytes == c->bytes &&
                record.position == id / c->writers + 1 &&
                record.generation == record.position,
            "canonical collection row identity");
    require(cursor >= checkpoint || record.next_offset <= checkpoint,
            "checkpoint canonical collection boundary");
    if (cursor >= checkpoint) {
      payload = record.payload_offset;
      crc = 0;
      while (payload < record.next_offset) {
        part.iov_base = buffer;
        part.iov_len = record.next_offset - payload < sizeof(buffer)
                           ? (size_t)(record.next_offset - payload)
                           : sizeof(buffer);
        rc = lc_pouch_record_io_read(io, payload, &part, 1, e);
        if (rc != LC_OK)
          return rc;
        crc = crc32(crc, buffer, (uInt)part.iov_len);
        payload += part.iov_len;
      }
      if (crc != record.stored_crc)
        break;
    }
    c->offsets[id] = record.payload_offset;
    c->payload_crcs[id] = (uint32_t)record.stored_crc;
    cursor = record.next_offset;
    ++w->records;
  }
  *accepted = cursor;
  return LC_OK;
}
static int namespace_record(void *context, uint64_t segment,
                            const lc_pouch_state_entry *entry, lc_error *e) {
  worker *w = context;
  comparison *c = w->run;
  unsigned long id;
  char ns[64], name[64], extra;
  (void)e;
  require(sscanf(entry->key, "key-%lu%c", &id, &extra) == 1 && id < c->rows &&
              id % c->writers == w->ordinal && entry->version >= 1 &&
              entry->version <= c->rounds && entry->bytes == c->bytes &&
              entry->record_type == 1,
          "canonical namespace state identity and metadata");
  names(w->ordinal, (size_t)id, ns, name);
  require(!strcmp(entry->key, name), "canonical state key");
  c->segments[id] = segment;
  c->offsets[id] = entry->payload_span.payload_offset;
  c->payload_crcs[id] = (uint32_t)entry->payload_span.payload_crc;
  ++w->records;
  return LC_OK;
}
static int stream_recover(ps_stream *stream, uint64_t checkpoint,
                          uint64_t capacity, uint64_t *accepted, void *context,
                          lc_error *e) {
  return collection_stream_recover(stream, checkpoint, capacity, accepted,
                                   context, e);
}

static int stream_named_open(worker *w, int create, lc_error *e) {
  comparison *c = w->run;
  ps_catalog_view *view = NULL;
  ps_catalog_info binding;
  ps_stream_info info;
  ps_key key = {0, 1, PS_STREAM};
  ps_namespace_info identity;
  unsigned int kind = PS_CATALOG_COLLECTION;
  char name[64], ns[64];
  int rc;
  snprintf(ns, sizeof(ns), c->collection ? "bench" : "owner-%lu",
           (unsigned long)w->ordinal);
  snprintf(name, sizeof(name), "collection-%lu", (unsigned long)w->ordinal);
  rc = ps_root_namespace_acquire(c->owner, ns, create, &w->namespace, e);
  if (rc == LC_OK && !c->collection) {
    ps_namespace_view *capture = NULL;
    w->records = 0;
    rc = ps_namespace_view_open(w->namespace, &capture, e);
    if (rc == LC_OK)
      rc = ps_namespace_view_visit(capture, 0, namespace_record, w, e);
    ps_namespace_view_close(capture);
    if (rc != LC_OK) {
      ps_namespace_release(w->namespace);
      w->namespace = NULL;
    }
    return rc;
  }
  if (rc == LC_OK)
    rc = ps_namespace_identity(w->namespace, &identity, e);
  if (rc != LC_OK)
    return rc;
  key.owner = identity.owner;
  if (create) {
    rc = ps_catalog_allocate_id(c->space, &key.owner, e);
    if (rc == LC_OK)
      rc = ps_stream_open(c->space, NULL, key, 1, NULL, stream_recover, w,
                          &w->stream, e);
    if (rc == LC_OK)
      rc = ps_stream_status(w->stream, &info, e);
    if (rc == LC_OK)
      rc = ps_catalog_put(c->space, NULL, identity.owner, kind, name,
                          strlen(name), NULL, key, &info.ref, &binding, e);
  } else {
    rc = ps_catalog_get(c->space, identity.owner, kind, name, strlen(name),
                        &view, e);
    if (rc == LC_OK && !view)
      rc = lc_error_set(e, LC_ERR_PROTOCOL, 0,
                        "comparison catalog binding missing", NULL, NULL,
                        "compare");
    if (rc == LC_OK)
      rc = ps_catalog_view_info(view, &binding, e);
    if (rc == LC_OK &&
        (binding.stream.kind != key.kind || !binding.stream.owner ||
         binding.stream.owner == identity.owner ||
         binding.stream.key != key.key))
      rc = lc_error_set(e, LC_ERR_PROTOCOL, 0,
                        "comparison catalog target mismatch", NULL, NULL,
                        "compare");
    if (rc == LC_OK)
      rc = ps_stream_open(c->space, NULL, binding.stream, 0, &binding.target,
                          stream_recover, w, &w->stream, e);
    ps_catalog_view_close(view);
  }
  if (rc != LC_OK) {
    ps_stream_close(w->stream, NULL);
    w->stream = NULL;
    ps_namespace_release(w->namespace);
    w->namespace = NULL;
  }
  return rc;
}
static int collection_stream_write(worker *w, size_t id, lc_source *source,
                                   lc_error *e) {
  comparison *c = w->run;
  ps_record_writer output;
  lc_pouch_collection_record record;
  char ns[64], name[64];
  uint64_t position = id / c->writers + 1;
  int rc = ps_record_begin(w->stream, &output, e);
  names(w->ordinal, id, ns, name);
  if (rc == LC_OK)
    rc = lc_pouch_collection_record_append(
        c->record_owner, ps_record_output(&output), canonical_leaf,
        w->collection_id, name, position, position, 0, 0, 0, source, &record,
        e);
  if (rc == LC_OK)
    rc = ps_record_commit(&output, PS_COMMIT_DEFAULT, NULL, NULL, e);
  if (rc == LC_OK) {
    require(record.bytes == c->bytes && record.stored_bytes == c->bytes,
            "canonical collection write bytes");
    c->offsets[id] = record.payload_offset;
    c->payload_crcs[id] = (uint32_t)record.stored_crc;
    ++w->records;
  }
  ps_record_abort(&output);
  return rc;
}
static int stream_write_one(worker *w, size_t id, lc_source *source,
                            lc_error *e) {
  comparison *c = w->run;
  lc_allocator allocator = {0};
  lc_pouch_state_entry request = {0}, result = {0};
  ps_namespace_append *output = NULL;
  char ns[64], name[64];
  int rc = LC_OK;
  if (c->collection)
    return collection_stream_write(w, id, source, e);
  names(w->ordinal, id, ns, name);
  request.key = name;
  request.content_type = "application/octet-stream";
  request.record_type = 1;
  request.version = w->round + 1;
  request.index_seq = (lc_pouch_generation)(w->round * c->rows + id + 1);
  request.updated_at_unix = (lc_pouch_unix_seconds)time(NULL);
  request.payload_context =
      lc_pouch_state_crypto_context(&allocator, ns, name, request.version, 1);
  if (!request.payload_context)
    return LC_ERR_NOMEM;
  if (!w->namespace)
    rc = stream_named_open(w, 1, e);
  if (rc == LC_OK)
    rc = ps_namespace_append_begin(w->namespace, PS_MIB, 0, PS_APPEND_PREFIX,
                                   &output, e);
  if (rc == LC_OK)
    rc = lc_pouch_state_record_append(&allocator, ps_root_crypto(c->owner),
                                      ps_namespace_append_output(output),
                                      ps_namespace_append_leaf(output),
                                      &request, source, 0, &result, e);
  if (rc == LC_OK)
    rc = ps_namespace_append_commit(output, PS_COMMIT_DEFAULT, e);
  if (rc == LC_OK) {
    require(result.bytes == c->bytes && result.cipher_bytes == c->bytes,
            "canonical state write bytes");
    c->offsets[id] = result.payload_span.payload_offset;
    c->segments[id] = ps_namespace_append_segment(output);
    c->payload_crcs[id] = (uint32_t)result.payload_span.payload_crc;
    ++w->records;
  }
  ps_namespace_append_close(output);
  lc_pouch_state_entry_cleanup(&allocator, &result);
  lc_free_with_allocator(&allocator, request.payload_context);
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
  if (c->mutation) {
    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.has_expected_version = 1;
    options.expected_version = 1;
    if (c->mutation == 1) {
      options.has_metadata = 1;
      options.metadata = (const unsigned char *)w->body;
      options.metadata_length = 32;
      rc = lc_pouch_state_update_metadata(c->pouch, ns, name, &options, &result,
                                          e);
      if (rc == LC_OK && (result.version != 1 || result.bytes != c->bytes))
        rc = LC_ERR_PROTOCOL;
    } else {
      rc = lc_pouch_state_delete(c->pouch, ns, name, &options, &result, e);
      if (rc == LC_OK && (result.version != 2 || result.bytes != 0))
        rc = LC_ERR_PROTOCOL;
    }
    lc_pouch_state_write_result_cleanup(NULL, &result);
    return rc;
  }
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
    rc = ps_begin(c->space, NULL, storage_key(c, w->ordinal, id), &destination,
                  e);
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
  lc_pouch_state_commit_group *scope = NULL;
  size_t id, round;
  double start;
  lc_error_init(&e);
  pthread_mutex_lock(&c->mutex);
  while (!c->started)
    pthread_cond_wait(&c->condition, &c->mutex);
  pthread_mutex_unlock(&c->mutex);
  w->rc = LC_OK;
  if (c->durable_scope)
    w->rc = lc_pouch_state_durable_scope_begin(c->pouch, &scope, &e);
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
  if (scope)
    w->rc = lc_pouch_state_durable_scope_end(scope, w->rc, &e);
  if (w->rc != LC_OK)
    fprintf(stderr, "writer %lu: %s\n", (unsigned long)w->ordinal,
            e.message ? e.message : "failed");
  lc_error_cleanup(&e);
  return NULL;
}
static void open_stores(comparison *c, int create, lc_error *e) {
  ps_options space_options = {0};
  ps_root_options root_options = {0};
  lc_pouch_open_options options;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_collection_config collection;
  const char *endpoint = c->endpoint;
  char name[64];
  size_t i;
  if (c->container) {
    space_options.durable_sync = c->sync;
    if (c->streams) {
      root_options.storage = space_options;
      check(ps_root_open(c->store, create, &root_options, &c->owner, e), e);
      c->space = ps_root_space(c->owner);
      if (c->record_owner)
        c->record_owner->crypto = ps_root_crypto(c->owner);
    } else
      check(ps_open(c->store, create, &space_options, &c->space, e), e);
    if (c->streams) {
      for (i = 0; i < c->rows; ++i)
        c->offsets[i] = UINT64_MAX;
      for (i = 0; i < c->writers; ++i)
        if (c->existing_records || c->collection)
          check(stream_named_open(&c->workers[i], create, e), e);
    }
    return;
  }
  if (!c->collection) {
    memset(&options, 0, sizeof(options));
    options.durable_sync = c->sync;
    options.query_indexing_enabled_set = 1;
    options.background_compaction_enabled_set = 1;
    options.segment_target_bytes =
        c->compaction || c->durable_scope ? 8 * PS_MIB : PS_MIB;
    options.terminal_reclaim_min_bytes = c->compaction ? 1 : 0;
    options.compaction_throttling_disabled = c->compaction;
    options.compression = "none";
    if (c->runtime) {
      ps_pouch_state_options integrated = {0};
      integrated.logical = options;
      check(ps_pouch_state_open(c->store, create, NULL, &integrated, &c->pouch,
                                e),
            e);
    } else
      check(lc_pouch_open(c->store, NULL, &options, &c->pouch, e), e);
    /* Compare completed reclamation, without the native obsolete-file grace
     * period or bandwidth throttle. These are private benchmark fixtures. */
    if (c->compaction)
      c->pouch->compaction_delete_grace_seconds = 0;
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
        ps_namespace_release(c->workers[i].namespace);
        c->workers[i].namespace = NULL;
      }
    if (c->streams) {
      if (c->record_owner)
        c->record_owner->crypto = NULL;
      check(ps_root_close(c->owner, e), e);
      c->owner = NULL;
    } else
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
static void seed_mutations(comparison *c, lc_error *e) {
  size_t id, ordinal;
  char ns[64], name[64];
  lc_source *source = NULL;
  lc_pouch_state_write_result result;
  for (id = 0; id < c->rows; ++id) {
    ordinal = id % c->writers;
    names(ordinal, id, ns, name);
    make_body(c->workers[ordinal].body, c->bytes, id, c->rounds - 1);
    check(lc_source_from_memory(c->workers[ordinal].body, c->bytes, &source, e),
          e);
    memset(&result, 0, sizeof(result));
    check(lc_pouch_state_write(c->pouch, ns, name, source, NULL, &result, e),
          e);
    require(result.version == 1 && result.bytes == c->bytes,
            "seed mutation key");
    lc_pouch_state_write_result_cleanup(NULL, &result);
    lc_source_close(source);
  }
}
typedef struct read_group {
  worker *owner;
  unsigned char *seen;
  size_t count;
} read_group;
static size_t verify_read_key(read_group *group, const char *name,
                              uint64_t version, uint64_t bytes) {
  comparison *c = group->owner->run;
  size_t id;
  require(!strncmp(name, "key-", 4), "read key prefix");
  id = number(name + 4, 0, c->rows - 1);
  require(id % c->writers == group->owner->ordinal && !group->seen[id] &&
              version == c->rounds && bytes == c->bytes,
          "read group captured metadata and unique membership");
  group->seen[id] = 1;
  ++group->count;
  return id;
}
static int verify_read_result(const char *name,
                              const lc_pouch_state_read_result *result,
                              void *context, lc_error *e) {
  read_group *group = context;
  comparison *c = group->owner->run;
  unsigned char bytes[4096];
  size_t id = verify_read_key(group, name, result->version, result->bytes);
  size_t offset = 0, got;
  require(result->found && result->has_body && result->body,
          "read group body present");
  make_body(group->owner->body, c->bytes, id, c->rounds - 1);
  while ((got = result->body->read(result->body, bytes, sizeof(bytes), e))) {
    require(got <= c->bytes - offset &&
                !memcmp(bytes, group->owner->body + offset, got),
            "read group captured body bytes");
    offset += got;
  }
  if (e->code != LC_OK)
    return e->code;
  require(offset == c->bytes, "read group complete body");
  return LC_OK;
}
static int verify_read_summary(const lc_pouch_state_scan_summary_entry *entry,
                               void *context, lc_error *e) {
  read_group *group = context;
  comparison *c = group->owner->run;
  lc_pouch_state_read_result result;
  char ns[64], name[64];
  int rc;
  if (c->read_method == 3) {
    (void)verify_read_key(group, entry->key, entry->version, entry->bytes);
    return LC_OK;
  }
  names(group->owner->ordinal, 0, ns, name);
  memset(&result, 0, sizeof(result));
  rc =
      lc_pouch_state_scan_summary_read_body(c->pouch, ns, entry, 0, &result, e);
  if (rc == LC_OK)
    rc = verify_read_result(entry->key, &result, group, e);
  lc_pouch_state_read_result_cleanup(NULL, &result);
  return rc;
}
static void *read_main(void *context) {
  worker *w = context;
  comparison *c = w->run;
  read_group group;
  lc_error e;
  char ns[64], name[64];
  size_t id = w->ordinal;
  lc_error_init(&e);
  memset(&group, 0, sizeof(group));
  group.owner = w;
  group.seen = calloc(c->rows, 1);
  require(group.seen != NULL, "read membership vector");
  names(w->ordinal, 0, ns, name);
  pthread_mutex_lock(&c->mutex);
  while (!c->started)
    pthread_cond_wait(&c->condition, &c->mutex);
  pthread_mutex_unlock(&c->mutex);
  w->rc = LC_OK;
  if (c->read_method == 1) {
    const char *keys[64];
    char (*labels)[64] = malloc(64 * sizeof(*labels));
    size_t count;
    require(labels != NULL, "bounded read-many key batch");
    while (w->rc == LC_OK && id < c->rows) {
      count = 0;
      while (count < 64 && id < c->rows) {
        names(w->ordinal, id, ns, labels[count]);
        keys[count] = labels[count];
        ++count;
        id += c->writers;
      }
      w->rc = lc_pouch_state_read_many(c->pouch, ns, keys, count,
                                       verify_read_result, &group, &e);
    }
    free(labels);
  } else if (c->read_method == 4) {
    for (; w->rc == LC_OK && id < c->rows; id += c->writers) {
      lc_pouch_state_read_result result;
      names(w->ordinal, id, ns, name);
      memset(&result, 0, sizeof(result));
      w->rc = lc_pouch_state_read(c->pouch, ns, name, &result, &e);
      if (w->rc == LC_OK)
        w->rc = verify_read_result(name, &result, &group, &e);
      lc_pouch_state_read_result_cleanup(NULL, &result);
    }
  } else {
    char *cursor = NULL;
    int more;
    do {
      lc_pouch_state_scan_summaries_result page;
      memset(&page, 0, sizeof(page));
      w->rc = lc_pouch_state_scan_summaries(
          c->pouch, ns, cursor, 256, verify_read_summary, &group, &page, &e);
      lc_free_with_allocator(NULL, cursor);
      cursor = page.next_start_after;
      page.next_start_after = NULL;
      more = page.truncated;
      lc_pouch_state_scan_summaries_result_cleanup(NULL, &page);
    } while (w->rc == LC_OK && more);
    lc_free_with_allocator(NULL, cursor);
  }
  if (w->rc == LC_OK)
    require(group.count == (c->rows + c->writers - 1 - w->ordinal) / c->writers,
            "read group complete membership");
  else
    fprintf(stderr, "read group failed (%d): %s\n", w->rc,
            e.message ? e.message : "no detail");
  free(group.seen);
  lc_error_cleanup(&e);
  return NULL;
}
static void verify_read_groups(comparison *c, lc_error *e) {
  size_t i;
  c->started = 0;
  for (i = 0; i < c->writers; ++i)
    require(pthread_create(&c->threads[i], NULL, read_main, &c->workers[i]) ==
                0,
            "start read worker");
  pthread_mutex_lock(&c->mutex);
  c->started = 1;
  pthread_cond_broadcast(&c->condition);
  pthread_mutex_unlock(&c->mutex);
  for (i = 0; i < c->writers; ++i) {
    require(pthread_join(c->threads[i], NULL) == 0, "join read worker");
    check(c->workers[i].rc, e);
  }
}
static void verify_states(comparison *c, lc_error *e) {
  ps_reader *reader;
  ps_stream_reader *stream_reader;
  ps_namespace_window *window;
  lc_pouch_state_read_result result;
  char ns[64], name[64];
  unsigned char buffer[4096];
  size_t id, ordinal, offset, got;
  unsigned long crc;
  if (c->read_method) {
    verify_read_groups(c, e);
    return;
  }
  for (id = 0; id < c->rows; ++id) {
    ordinal = id % c->writers;
    make_body(c->workers[ordinal].body, c->bytes, id, c->rounds - 1);
    offset = 0;
    if (c->streams) {
      lc_pouch_record_io io;
      struct iovec part;
      crc = 0;
      require(c->offsets[id] != UINT64_MAX, "canonical latest row exists");
      stream_reader = NULL;
      window = NULL;
      if (c->collection) {
        check(ps_stream_read_open(c->workers[ordinal].stream, c->offsets[id],
                                  c->bytes, &stream_reader, e),
              e);
      } else {
        check(ps_namespace_read_open(c->workers[ordinal].namespace,
                                     PS_CATALOG_SEGMENT, c->segments[id],
                                     c->offsets[id], c->bytes, &window, e),
              e);
        io = ps_namespace_read_input(window);
      }
      do {
        if (c->collection) {
          check(ps_stream_read(stream_reader, buffer, sizeof(buffer), &got, e),
                e);
        } else {
          got = c->bytes - offset;
          if (got > sizeof(buffer))
            got = sizeof(buffer);
          part.iov_base = buffer;
          part.iov_len = got;
          check(
              lc_pouch_record_io_read(io, c->offsets[id] + offset, &part, 1, e),
              e);
        }
        crc = crc32(crc, buffer, (uInt)got);
        require(got <= c->bytes - offset &&
                    !memcmp(buffer, c->workers[ordinal].body + offset, got),
                "canonical stream entry bytes");
        offset += got;
      } while (got);
      ps_stream_reader_close(stream_reader);
      ps_namespace_read_close(window);
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
      if (c->mutation == 2) {
        require(!result.found && !result.body, "deleted key remains absent");
        lc_pouch_state_read_result_cleanup(NULL, &result);
        continue;
      }
      require(result.found && result.body && result.bytes == c->bytes,
              "Pouch state metadata");
      if (c->mutation == 1)
        require(
            result.version == 1 && result.metadata_length == 32 &&
                !memcmp(result.metadata, c->workers[ordinal].body, 32),
            "metadata retained body/version with final application metadata");
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
static void compact_all(comparison *c, lc_error *e) {
  size_t i;
  char ns[64], name[64];
  for (i = 0; i < c->writers; ++i) {
    names(i, i, ns, name);
    if (c->runtime) {
      ps_pouch_compaction_result result;
      check(ps_pouch_state_compact(c->pouch, ns, &result, e), e);
      require(!result.skipped && result.snapshot && result.boundary == 1 &&
                  result.copied_bytes ==
                      (c->rows + c->writers - 1 - i) / c->writers * c->bytes,
              "container compaction copies the current bodies of one prefix");
    } else {
      lc_pouch_maintenance_options options = {0};
      lc_pouch_maintenance_result result = {0};
      options.ns = ns;
      options.force = 1;
      options.terminal_reclaim = 1;
      check(lc_pouch_maintenance_run(c->pouch, &options, &result, e), e);
      require(result.compacted && !result.aborted &&
                  result.candidate_segment_count == 1,
              "multifile compaction covers the same single prefix");
      lc_pouch_maintenance_result_cleanup(NULL, &result);
    }
  }
}
int main(int argc, char **argv) {
  comparison c;
  lc_error e;
  io_counts before, after;
  io_counts compact_before = {0}, compact_after = {0};
  files artifacts = {0}, audit = {0};
  ps_stats stats, initial_stats = {0};
  uint64_t fd_before, full_before, ns_before, fd_calls, full_calls, sync_ns;
  uint64_t compact_sync_calls = 0, compact_sync_ns = 0;
  long max_sync_batch = -1;
  long checkpoints = -1, directory_writes = -1, descriptor_writes = -1;
  long metadata_pages = -1;
  long packed_pages = -1, packed_bytes = -1, payload_allocated = -1;
  long journal_used = -1, stream_grants = -1;
  uint64_t logical_segments = 0;
  ps_stream_info stream_info;
  ps_namespace_status namespace_info;
  struct rusage usage;
  char prefix[1024], template_path[1100];
  size_t i, count;
  double start, cpu, seconds, cpu_seconds, read_seconds, close_seconds,
      open_seconds, create_seconds, reopen_read_seconds, preopen_seconds = 0,
                                                         seed_seconds = 0;
  double read_cpu_begin, read_cpu_seconds, reopen_read_cpu_seconds;
  double compact_seconds = 0, compact_cpu_seconds = 0;
  int preopen;
  if (argc != 8 && argc != 9) {
    fprintf(
        stderr,
        "usage: %s "
        "space-state|stream-state|runtime-state|pouch-state|space-collection|"
        "runtime-metadata|pouch-metadata|runtime-delete|pouch-delete|"
        "runtime-compact|pouch-compact|"
        "runtime-scope|pouch-scope|"
        "runtime-read-many|pouch-read-many|runtime-read-points|pouch-read-"
        "points|"
        "runtime-scan-bodies|pouch-scan-bodies|"
        "runtime-scan-metadata|pouch-scan-metadata|"
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
              !strcmp(argv[1], "runtime-state") ||
              !strcmp(argv[1], "runtime-metadata") ||
              !strcmp(argv[1], "pouch-metadata") ||
              !strcmp(argv[1], "runtime-delete") ||
              !strcmp(argv[1], "pouch-delete") ||
              !strcmp(argv[1], "runtime-compact") ||
              !strcmp(argv[1], "pouch-compact") ||
              !strcmp(argv[1], "runtime-scope") ||
              !strcmp(argv[1], "pouch-scope") ||
              !strcmp(argv[1], "runtime-read-many") ||
              !strcmp(argv[1], "pouch-read-many") ||
              !strcmp(argv[1], "runtime-read-points") ||
              !strcmp(argv[1], "pouch-read-points") ||
              !strcmp(argv[1], "runtime-scan-bodies") ||
              !strcmp(argv[1], "pouch-scan-bodies") ||
              !strcmp(argv[1], "runtime-scan-metadata") ||
              !strcmp(argv[1], "pouch-scan-metadata") ||
              !strcmp(argv[1], "stream-collection"),
          "mode");
  c.streams = !strncmp(argv[1], "stream-", 7);
  c.container = c.streams || !strncmp(argv[1], "space-", 6);
  c.runtime = !strncmp(argv[1], "runtime-", 8);
  c.compaction = strstr(argv[1], "compact") != NULL;
  c.durable_scope = strstr(argv[1], "scope") != NULL;
  c.read_method = strstr(argv[1], "read-many")       ? 1
                  : strstr(argv[1], "scan-bodies")   ? 2
                  : strstr(argv[1], "scan-metadata") ? 3
                  : strstr(argv[1], "read-points")   ? 4
                                                     : 0;
  c.mutation = c.read_method                 ? 0
               : strstr(argv[1], "metadata") ? 1
               : strstr(argv[1], "delete")   ? 2
                                             : 0;
  c.collection = strstr(argv[1], "collection") != NULL;
  c.rows = number(argv[3], 1, PS_DEFAULT_CAPACITY / PS_PAGE);
  c.bytes = number(argv[4], 64, 2 * PS_MIB);
  c.sync = (int)number(argv[5], 0, 1);
  c.writers = number(argv[6], 1, 16);
  /* argv[7] is the input chunk size, deliberately shared by both engines. */
  require(!strcmp(argv[7], "4096"), "input chunk must currently be 4096");
  c.rounds = c.collection || c.mutation == 2 ? 1 : 2;
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
  c.segments = calloc(c.rows, sizeof(*c.segments));
  c.payload_crcs = calloc(c.rows, sizeof(*c.payload_crcs));
  require(c.workers && c.threads && c.latencies && c.offsets && c.segments &&
              c.payload_crcs,
          "benchmark allocations");
  require(pthread_mutex_init(&c.mutex, NULL) == 0, "benchmark mutex");
  require(pthread_cond_init(&c.condition, NULL) == 0, "benchmark condition");
  lc_error_init(&e);
  if (c.streams && c.collection) {
    /* Canonical record codec context only: no second Pouch root or sidecars.
     * The public namespace/collection owner is still pending integration. */
    c.record_owner = calloc(1, sizeof(*c.record_owner));
    require(c.record_owner != NULL, "canonical record owner context");
  }
  for (i = 0; i < c.writers; ++i) {
    c.workers[i].run = &c;
    c.workers[i].ordinal = i;
    c.workers[i].body = malloc(c.bytes);
    require(c.workers[i].body != NULL, "worker payload fixture");
    if (c.record_owner)
      check(lc_xid_new(c.workers[i].collection_id, &e), &e);
  }
  start = now(CLOCK_MONOTONIC);
  open_stores(&c, 1, &e);
  create_seconds = now(CLOCK_MONOTONIC) - start;
  if (c.mutation) {
    start = now(CLOCK_MONOTONIC);
    seed_mutations(&c, &e);
    seed_seconds = now(CLOCK_MONOTONIC) - start;
  }
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
  if (c.compaction) {
    compact_before = io_sample();
    compact_sync_calls = __atomic_load_n(&fd_sync_calls, __ATOMIC_RELAXED) +
                         __atomic_load_n(&full_sync_calls, __ATOMIC_RELAXED);
    compact_sync_ns = __atomic_load_n(&syscall_sync_ns, __ATOMIC_RELAXED);
    cpu = now(CLOCK_PROCESS_CPUTIME_ID);
    start = now(CLOCK_MONOTONIC);
    compact_all(&c, &e);
    compact_seconds = now(CLOCK_MONOTONIC) - start;
    compact_cpu_seconds = now(CLOCK_PROCESS_CPUTIME_ID) - cpu;
    compact_sync_calls = __atomic_load_n(&fd_sync_calls, __ATOMIC_RELAXED) +
                         __atomic_load_n(&full_sync_calls, __ATOMIC_RELAXED) -
                         compact_sync_calls;
    compact_sync_ns =
        __atomic_load_n(&syscall_sync_ns, __ATOMIC_RELAXED) - compact_sync_ns;
    compact_after = io_sample();
  }
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
        if (c.collection) {
          check(ps_stream_status(c.workers[i].stream, &stream_info, &e), &e);
          stream_grants += (long)stream_info.runs;
          ++logical_segments;
        } else {
          check(ps_namespace_status_get(c.workers[i].namespace, &namespace_info,
                                        &e),
                &e);
          stream_grants += (long)namespace_info.runs;
          logical_segments += namespace_info.segments;
        }
      }
    }
  }
  if (c.runtime) {
    /* Observe the same typed owner once, outside the measured mutation loop. */
    for (i = 0; i < c.writers; ++i) {
      lc_pouch_storage_namespace_info info;
      void *ns = NULL;
      char ns_name[64], key_name[64];
      names(i, i, ns_name, key_name);
      check(c.pouch->storage.ops->namespace_open(c.pouch->storage.context,
                                                 ns_name, 0, &ns, &e),
            &e);
      check(c.pouch->storage.ops->namespace_info(c.pouch->storage.context, ns,
                                                 &info, &e),
            &e);
      logical_segments += info.segments;
      c.pouch->storage.ops->namespace_close(c.pouch->storage.context, ns);
    }
  }
  qsort(c.latencies, count, sizeof(*c.latencies), compare_double);
  read_cpu_begin = now(CLOCK_PROCESS_CPUTIME_ID);
  start = now(CLOCK_MONOTONIC);
  verify_all(&c, &e);
  read_seconds = now(CLOCK_MONOTONIC) - start;
  read_cpu_seconds = now(CLOCK_PROCESS_CPUTIME_ID) - read_cpu_begin;
  start = now(CLOCK_MONOTONIC);
  close_stores(&c, &e);
  close_seconds = now(CLOCK_MONOTONIC) - start;
  start = now(CLOCK_MONOTONIC);
  open_stores(&c, 0, &e);
  open_seconds = now(CLOCK_MONOTONIC) - start;
  read_cpu_begin = now(CLOCK_PROCESS_CPUTIME_ID);
  start = now(CLOCK_MONOTONIC);
  verify_all(&c, &e);
  reopen_read_seconds = now(CLOCK_MONOTONIC) - start;
  reopen_read_cpu_seconds = now(CLOCK_PROCESS_CPUTIME_ID) - read_cpu_begin;
  if (c.read_method == 3) {
    /* Metadata-only scan timing must stay body-free. Verify its seed bodies
     * outside both measured read phases before reporting success. */
    c.read_method = 0;
    verify_states(&c, &e);
    c.read_method = 3;
  }
  count_files(c.store, &artifacts);
  if (c.container || c.runtime) {
    /* The test helper owns one marker in the parent, outside the store. Audit
     * the whole parent too so an accidental store sidecar cannot hide. */
    count_files(c.root, &audit);
    require(audit.regular == 2 && audit.directories == 1 &&
                artifacts.regular == 1 && artifacts.directories == 0,
            "exactly one container and no sidecars");
  }
  if (c.container) {
    check(ps_status(c.space, &stats, &e), &e);
    if (c.streams) {
      require(stats.entries == (uint64_t)stream_grants + 2 * logical_segments +
                                   1 + (c.collection ? 1 : 2 * c.writers),
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
  printf(
      "checkpoints=%ld directory_page_writes=%ld descriptor_page_writes=%ld "
      "metadata_pages=%ld packed_pages=%ld packed_bytes=%ld "
      "payload_allocated=%ld journal_used=%ld stream_grants=%ld "
      "logical_segments=%lu "
      "preopened=%d preopen_ms=%.6f seed_ms=%.6f max_us=%.3f "
      "read_cpu_ms=%.6f reopen_read_cpu_ms=%.6f "
      "compact_ms=%.6f compact_cpu_ms=%.6f compact_write_calls=%lu "
      "compact_read_calls=%lu compact_written=%lu compact_sync_calls=%lu "
      "compact_sync_ms=%.6f verified=1\n",
      checkpoints, directory_writes, descriptor_writes, metadata_pages,
      packed_pages, packed_bytes, payload_allocated, journal_used,
      stream_grants, (unsigned long)logical_segments, preopen,
      preopen_seconds * 1000, seed_seconds * 1000, c.latencies[count - 1] * 1e6,
      read_cpu_seconds * 1000, reopen_read_cpu_seconds * 1000,
      compact_seconds * 1000, compact_cpu_seconds * 1000,
      (unsigned long)(compact_after.write_calls - compact_before.write_calls),
      (unsigned long)(compact_after.read_calls - compact_before.read_calls),
      (unsigned long)(compact_after.written - compact_before.written),
      (unsigned long)compact_sync_calls, (double)compact_sync_ns / 1e6);
  close_stores(&c, &e);
  for (i = 0; i < c.writers; ++i)
    free(c.workers[i].body);
  free(c.latencies);
  free(c.offsets);
  free(c.segments);
  free(c.payload_crcs);
  free(c.threads);
  free(c.workers);
  free(c.record_owner);
  pthread_cond_destroy(&c.condition);
  pthread_mutex_destroy(&c.mutex);
  lc_error_cleanup(&e);
  lc_test_tmp_cleanup_path(c.root, prefix);
  return 0;
}
