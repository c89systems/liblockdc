/* Disposable public-API diagnostic, deliberately excluded from benchmark gates.
 * The caller owns/removes the fresh root. No private state or key is printed.
 */
#include "lc/lc.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define PROBE_APPEND_COUNT 16U

typedef struct probe {
  lc_client *client;
  lc_collection *collection;
  lc_error error;
  char endpoint[1024];
  char *key;
  lc_collection_head_res head;
  lc_collection_edit_res edited;
  lc_collection_edit_range ranges[LC_COLLECTION_EDIT_MAX_RANGES];
  unsigned long bytes;
} probe;
typedef struct probe_io {
  unsigned long read_chars, write_chars, read_calls, write_calls;
} probe_io;
static void check(probe *p, int rc) {
  if (rc != LC_OK) {
    fprintf(stderr, "collection edit probe: %d: %s\n", rc,
            p->error.message ? p->error.message : "operation failed");
    exit(1);
  }
}
static double stamp(clockid_t id) {
  struct timespec t;
  if (clock_gettime(id, &t))
    exit(1);
  return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}
static void io_snapshot(probe_io *out) {
  char line[128], key[32];
  unsigned long value;
  FILE *fp = fopen("/proc/self/io", "r");
  memset(out, 0, sizeof(*out));
  if (!fp)
    return;
  while (fgets(line, sizeof(line), fp))
    if (sscanf(line, "%31s %lu", key, &value) == 2) {
      if (strcmp(key, "rchar:") == 0)
        out->read_chars = value;
      else if (strcmp(key, "wchar:") == 0)
        out->write_chars = value;
      else if (strcmp(key, "syscr:") == 0)
        out->read_calls = value;
      else if (strcmp(key, "syscw:") == 0)
        out->write_calls = value;
    }
  fclose(fp);
}
static int discard(lc_sink *sink, const void *bytes, size_t count,
                   lc_error *error) {
  probe *p = sink->impl;
  (void)bytes;
  (void)error;
  p->bytes += (unsigned long)count;
  return 1;
}
static void open_root(probe *p, const char *mode, int durable, int create) {
  const char *endpoints[1];
  lc_client_config config;
  lc_collection_config collection;
  lc_pouch_settings settings;
  endpoints[0] = p->endpoint;
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_BACKGROUND_COMPACTION | LC_POUCH_SETTING_SINGLE_WRITER |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_COMPRESSION |
      LC_POUCH_SETTING_DURABLE_SYNC;
  settings.single_writer = 0;
  settings.query_indexing_enabled = 0;
  settings.durable_sync = durable;
  settings.compression = strcmp(mode, "plain") == 0 ? "none" : "zlib";
  if (p->key) {
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = p->key;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1;
  config.default_namespace = "journal";
  config.pouch_settings = &settings;
  check(p, lc_client_open(&config, &p->client, &p->error));
  lc_collection_config_init(&collection);
  collection.name = "events";
  check(p, create ? lc_client_create_collection(p->client, &collection,
                                                &p->collection, &p->error)
                  : lc_client_open_collection(p->client, &collection,
                                              &p->collection, &p->error));
}
static void close_root(probe *p) {
  lc_collection_close(p->collection);
  lc_client_close(p->client);
  p->collection = NULL;
  p->client = NULL;
}
static void append(probe *p, const char *id, const char *body, size_t size) {
  lc_collection_append_req req;
  lc_collection_append_res result;
  lc_source *source = NULL;
  lc_collection_append_req_init(&req);
  req.append_id = id;
  check(p, lc_source_from_memory(body, size, &source, &p->error));
  check(p,
        lc_collection_append(p->collection, &req, source, &result, &p->error));
  lc_source_close(source);
}
int main(int argc, char **argv) {
  probe *p;
  lc_collection_edit_req req;
  lc_collection_append_req replacement;
  lc_collection_edit_range range;
  lc_collection_read_opts opts;
  lc_collection_read_res read;
  lc_source *source = NULL;
  lc_sink sink;
  struct stat st;
  char *body, id[48];
  const char *mode, *style;
  unsigned long random = 1234567UL;
  static const char alphabet[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
  unsigned long count, cut, size, i, writes, reads;
  probe_io before, after, edit_before, edit_after;
  double wall, cpu, append_wall, append_cpu, edit_wall, edit_cpu, read_wall,
      read_cpu, after_wall, after_cpu;
  int durable;
  if (argc != 7 && argc != 8) {
    fprintf(stderr,
            "usage: %s FRESH_ROOT ENTRIES CUT JSON_BYTES "
            "plain|zlib|encrypted-zlib nosync|durable [suffix|scatter]\n",
            argv[0]);
    return 2;
  }
  count = strtoul(argv[2], NULL, 10);
  cut = strtoul(argv[3], NULL, 10);
  size = strtoul(argv[4], NULL, 10);
  mode = argv[5];
  durable = strcmp(argv[6], "durable") == 0;
  style = argc == 8 ? argv[7] : "suffix";
  if ((strcmp(style, "suffix") && strcmp(style, "scatter")) ||
      (strcmp(style, "scatter") == 0 && cut > LC_COLLECTION_EDIT_MAX_RANGES) ||
      !count || count > 100000 || cut > count || size < 8 || size > 65536 ||
      (strcmp(mode, "plain") && strcmp(mode, "zlib") &&
       strcmp(mode, "encrypted-zlib")) ||
      (strcmp(argv[6], "nosync") && !durable) || lstat(argv[1], &st) == 0 ||
      errno != ENOENT)
    return 2;
  p = calloc(1, sizeof(*p));
  body = malloc((size_t)size);
  if (!p || !body)
    return 1;
  lc_error_init(&p->error);
  snprintf(p->endpoint, sizeof(p->endpoint), "pouch://%s", argv[1]);
  if (strcmp(mode, "encrypted-zlib") == 0)
    check(p, lc_pouch_crypto_generate_key_string(&p->key, &p->error));
  memcpy(body, "{\"s\":\"", 6);
  for (i = 6; i < size - 2; ++i) {
    random = random * 1664525UL + 1013904223UL;
    body[i] = alphabet[(random >> 24) & 63U];
  }
  memcpy(body + size - 2, "\"}", 2);
  open_root(p, mode, 0, 1);
  for (i = 0; i < count; ++i) {
    snprintf(id, sizeof(id), "entry-%lu", i);
    append(p, id, body, (size_t)size);
  }
  close_root(p);
  open_root(p, mode, durable, 0);
  wall = stamp(CLOCK_MONOTONIC);
  cpu = stamp(CLOCK_PROCESS_CPUTIME_ID);
  for (i = 0; i < PROBE_APPEND_COUNT; ++i) {
    snprintf(id, sizeof(id), "append-%lu", i);
    append(p, id, body, (size_t)size);
  }
  append_wall = stamp(CLOCK_MONOTONIC) - wall;
  append_cpu = stamp(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  check(p, lc_collection_head(p->collection, &p->head, &p->error));
  lc_collection_edit_req_init(&req);
  lc_collection_append_req_init(&replacement);
  req.operation_id = "cut";
  req.expected_head = p->head.etag;
  range.after = p->head.upper_position - cut;
  range.through = p->head.upper_position;
  req.ranges = &range;
  req.range_count = cut ? 1 : 0;
  if (strcmp(style, "scatter") == 0) {
    for (i = 0; i < cut; ++i) {
      p->ranges[i].through =
          (i + 1) * (unsigned long)p->head.upper_position / (cut + 1);
      p->ranges[i].after = p->ranges[i].through - 1;
    }
    req.ranges = p->ranges;
    req.range_count = (size_t)cut;
  }
  replacement.append_id = "replacement";
  req.replacement = &replacement;
  check(p, lc_source_from_memory(body, (size_t)size, &source, &p->error));
  io_snapshot(&edit_before);
  wall = stamp(CLOCK_MONOTONIC);
  cpu = stamp(CLOCK_PROCESS_CPUTIME_ID);
  check(p,
        lc_collection_edit(p->collection, &req, source, &p->edited, &p->error));
  edit_wall = stamp(CLOCK_MONOTONIC) - wall;
  edit_cpu = stamp(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  io_snapshot(&edit_after);
  writes = edit_after.write_chars - edit_before.write_chars;
  lc_source_close(source);
  if (p->edited.deleted_count != cut ||
      p->edited.live_count != count + PROBE_APPEND_COUNT + 1U - cut)
    return 1;
  memset(&sink, 0, sizeof(sink));
  sink.write = discard;
  sink.impl = p;
  lc_collection_read_opts_init(&opts);
  lc_collection_read_res_init(&read);
  opts.has_after = 1;
  opts.after =
      p->edited.upper_position > 256 ? p->edited.upper_position - 256 : 0;
  io_snapshot(&before);
  wall = stamp(CLOCK_MONOTONIC);
  cpu = stamp(CLOCK_PROCESS_CPUTIME_ID);
  check(p, lc_collection_read(p->collection, &opts, &sink, &read, &p->error));
  read_wall = stamp(CLOCK_MONOTONIC) - wall;
  read_cpu = stamp(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  io_snapshot(&after);
  reads = after.read_chars - before.read_chars;
  wall = stamp(CLOCK_MONOTONIC);
  cpu = stamp(CLOCK_PROCESS_CPUTIME_ID);
  for (i = 0; i < PROBE_APPEND_COUNT; ++i) {
    snprintf(id, sizeof(id), "after-%lu", i);
    append(p, id, body, (size_t)size);
  }
  after_wall = stamp(CLOCK_MONOTONIC) - wall;
  after_cpu = stamp(CLOCK_PROCESS_CPUTIME_ID) - cpu;
  printf("%s,%s,%s,%lu,%lu,%lu,%.9f,%.9f,%.9f,%.9f,%lu,%.9f,%.9f,%lu,%lu,%.9f,%"
         ".9f,%lu,%lu,%lu\n",
         mode, argv[6], style, count, cut, size,
         append_wall / PROBE_APPEND_COUNT, append_cpu / PROBE_APPEND_COUNT,
         edit_wall, edit_cpu, writes, read_wall, read_cpu, reads, p->bytes,
         after_wall / PROBE_APPEND_COUNT, after_cpu / PROBE_APPEND_COUNT,
         edit_after.read_chars - edit_before.read_chars,
         edit_after.read_calls - edit_before.read_calls,
         edit_after.write_calls - edit_before.write_calls);
  lc_collection_read_res_cleanup(&read);
  close_root(p);
  lc_pouch_crypto_key_string_free(p->key);
  lc_error_cleanup(&p->error);
  free(body);
  free(p);
  return 0;
}
