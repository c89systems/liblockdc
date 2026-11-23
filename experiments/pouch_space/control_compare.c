#define _POSIX_C_SOURCE 200809L
#include "../../tests/support/lc_test_tmp.h"
#include "control.h"
#include "lc_collection_internal.h"
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

/* Bounded control replacement only. Native uses the actual atomic head owner;
 * container uses the same codec and its typed durable slot. Caller already owns
 * metadata authority in both cases. This is not a public Pouch workload. */
static unsigned long sync_calls;
int __real_fsync(int);
int __real_fdatasync(int);
int __wrap_fsync(int fd) {
  ++sync_calls;
  return __real_fsync(fd);
}
int __wrap_fdatasync(int fd) {
  ++sync_calls;
  return __real_fdatasync(fd);
}
static double now(void) {
  struct timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t) != 0)
    abort();
  return (double)t.tv_sec + (double)t.tv_nsec / 1000000000.0;
}
static double cpu(void) {
  struct rusage r;
  if (getrusage(RUSAGE_SELF, &r) != 0)
    abort();
  return (double)r.ru_utime.tv_sec + (double)r.ru_stime.tv_sec +
         ((double)r.ru_utime.tv_usec + (double)r.ru_stime.tv_usec) / 1000000.0;
}
static void check(int rc, lc_error *e) {
  if (rc != LC_OK) {
    fprintf(stderr, "control compare: %s\n", e->message ? e->message : "error");
    exit(1);
  }
}
int main(int argc, char **argv) {
  char root[1024], prefix[1024], temporary[1100], path[1100];
  unsigned char expected[LC_COLLECTION_HEAD_BYTES],
      actual[LC_COLLECTION_HEAD_BYTES];
  lc_collection_handle *file;
  lc_collection_disk_head h, got;
  ps_options options;
  ps_space *space = NULL;
  lc_error e;
  double start, start_cpu, write_ms, read_ms, write_cpu_ms;
  unsigned long count, reads, i, barriers;
  int container;
  DIR *directory;
  struct dirent *entry;
  size_t leaves = 0;
  if (argc != 4 || (strcmp(argv[1], "space") && strcmp(argv[1], "file")))
    return 2;
  container = strcmp(argv[1], "space") == 0;
  count = strtoul(argv[3], NULL, 10);
  if (!count || count > 1000)
    return 2;
  reads = count * 8;
  if (snprintf(prefix, sizeof(prefix), "%s/control-compare-", argv[2]) <= 0 ||
      snprintf(temporary, sizeof(temporary), "%sXXXXXX", prefix) <= 0 ||
      !lc_test_tmp_mkdtemp(temporary, root, sizeof(root), prefix) ||
      snprintf(path, sizeof(path), "%s/%s", root,
               container ? "container" : "head") <= 0)
    return 1;
  file = calloc(1, sizeof(*file));
  if (!file)
    return 1;
  file->directory = root;
  file->head_path = path;
  file->head_fd = -1;
  file->control = lc_collection_file_control(file);
  memset(&options, 0, sizeof(options));
  memset(&h, 0, sizeof(h));
  h.active = h.tail_segment = 1;
  lc_error_init(&e);
  if (container)
    check(ps_open(path, 1, &options, &space, &e), &e);
  sync_calls = 0;
  start_cpu = cpu();
  start = now();
  for (i = 0; i < count; ++i) {
    h.tail_offset = i;
    h.retention_generation = i;
    h.lease.fencing_token = i + 1;
    if (container)
      check(ps_collection_head_store(space, 1, &h, NULL, &e), &e);
    else {
      file->head = h;
      check(lc_collection_head_write(file, &e), &e);
    }
  }
  write_ms = (now() - start) * 1000.0;
  write_cpu_ms = (cpu() - start_cpu) * 1000.0;
  barriers = sync_calls;
  check(lc_collection_head_encode(&h, expected, &e), &e);
  start = now();
  for (i = 0; i < reads; ++i) {
    if (container)
      check(ps_collection_head_load(space, 1, NULL, &got, &e), &e);
    else {
      file->head_loaded =
          0; /* Compare complete bounded reloads on both sides. */
      check(lc_collection_head_read(file, &e), &e);
      got = file->head;
    }
    check(lc_collection_head_encode(&got, actual, &e), &e);
    if (memcmp(expected, actual, sizeof(actual)))
      abort();
  }
  read_ms = (now() - start) * 1000.0;
  if (container)
    check(ps_close(space, &e), &e);
  if (file->head_fd >= 0 && close(file->head_fd) != 0)
    abort();
  directory = opendir(root);
  if (!directory)
    abort();
  while ((entry = readdir(directory)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    ++leaves;
    if (strcmp(entry->d_name, container ? "container" : "head"))
      abort();
  }
  if (closedir(directory) != 0 || leaves != 1)
    abort();
  printf("{\"engine\":\"%s\",\"writes\":%lu,\"reads\":%lu,\"write_ms\":%.6f,"
         "\"write_cpu_ms\":%.6f,\"reload_ms\":%.6f,\"write_syncs\":%lu,"
         "\"verified\":true}\n",
         argv[1], count, reads, write_ms, write_cpu_ms, read_ms, barriers);
  lc_error_cleanup(&e);
  free(file);
  lc_test_tmp_cleanup_path(root, prefix);
  return 0;
}
