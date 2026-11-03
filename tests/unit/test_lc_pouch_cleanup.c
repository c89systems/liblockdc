#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <pslog.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../support/lc_test_cases.h"
#include "../support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#define PREFIX "/tmp/liblockdc-cleanup-"
typedef struct fixture {
  char root[256], directory[512], manifest_path[640], segments[640];
  lc_pouch *pouch;
  lc_error error;
  lc_pouch_namespace_manifest manifest;
  FILE *log;
  pslog_logger *logger;
} fixture;

static const char *fail_unlink;
static dev_t fail_sync_device;
static ino_t fail_sync_inode;
static int fail_sync;
int __real_unlink(const char *path);
int __wrap_unlink(const char *path) {
  if (fail_unlink && !strcmp(path, fail_unlink)) {
    errno = EACCES;
    return -1;
  }
  return __real_unlink(path);
}
int __real_fsync(int fd);
int __wrap_fsync(int fd) {
  struct stat st;
  if (fail_sync && fstat(fd, &st) == 0 && st.st_dev == fail_sync_device &&
      st.st_ino == fail_sync_inode) {
    errno = EIO;
    return -1;
  }
  return __real_fsync(fd);
}
static void check(fixture *f, int rc) {
  if (rc != LC_OK)
    print_error("cleanup: %s (%s)\n", f->error.message ? f->error.message : "",
                f->error.detail ? f->error.detail : "");
  assert_int_equal(rc, LC_OK);
}
static int setup(void **state) {
  fixture *f = calloc(1, sizeof(*f));
  char pattern[] = PREFIX "XXXXXX";
  lc_pouch_open_options options = {0};
  pslog_config logging;
  assert_non_null(f);
  *state = f;
  lc_error_init(&f->error);
  f->log = tmpfile();
  assert_non_null(f->log);
  pslog_default_config(&logging);
  logging.mode = PSLOG_MODE_JSON;
  logging.min_level = PSLOG_LEVEL_TRACE;
  logging.timestamps = 0;
  logging.output = pslog_output_from_fp(f->log, 0);
  f->logger = pslog_new(&logging);
  assert_non_null(f->logger);
  assert_true(lc_test_tmp_mkdtemp(pattern, f->root, sizeof(f->root), PREFIX));
  options.background_compaction_enabled_set = 1;
  options.query_indexing_enabled_set = 1;
  options.segment_target_bytes = 1;
  options.single_writer_set = 1;
  options.logger = f->logger;
  check(f, lc_pouch_open(f->root, NULL, &options, &f->pouch, &f->error));
  check(f, lc_pouch_ensure_namespace(f->pouch, "demo", &f->error));
  snprintf(f->directory, sizeof(f->directory), "%s/namespaces/demo", f->root);
  snprintf(f->manifest_path, sizeof(f->manifest_path), "%s/manifest",
           f->directory);
  snprintf(f->segments, sizeof(f->segments), "%s/segments", f->directory);
  return 0;
}
static int teardown(void **state) {
  fixture *f = *state;
  fail_unlink = NULL;
  fail_sync = 0;
  lc_pouch_test_candidate_open_errno = NULL;
  if (f) {
    lc_pouch_namespace_manifest_cleanup(NULL, &f->manifest);
    lc_pouch_close(f->pouch);
    lc_error_cleanup(&f->error);
    f->logger->destroy(f->logger);
    fclose(f->log);
    lc_test_tmp_cleanup_path(f->root, PREFIX);
    free(f);
  }
  return 0;
}
static void leaf_path(fixture *f, unsigned long id, char path[768]) {
  snprintf(path, 768, "%s/seg-%020lu.log", f->segments, id);
}
static void seed_manifest(fixture *f) {
  char path[768];
  unsigned long i;
  const char *text = "layout=pouch-segmented\nversion=1\nnamespace=demo\n"
                     "active_segment=seg-00000000000000000005.log\n"
                     "max_segment_id=5\nstate_max_version=0\n"
                     "obsolete_segment=seg-00000000000000000001.log\t1\n"
                     "obsolete_segment=seg-00000000000000000002.log\t2\n"
                     "obsolete_segment=seg-00000000000000000003.log\t1999\n";
  for (i = 1; i <= 5; ++i) {
    leaf_path(f, i, path);
    check(f, lc_pouch_path_write_text_file(path, "", &f->error));
  }
  check(f, lc_pouch_path_write_text_file(f->manifest_path, text, &f->error));
  check(f, lc_pouch_namespace_manifest_open(NULL, f->root, "demo", &f->manifest,
                                            NULL, NULL, &f->error));
  assert_int_equal(f->manifest.segment_count, 5);
}
static void assert_candidates_exist(fixture *f) {
  unsigned long i, j;
  char path[768];
  struct stat st;
  for (i = 0; i < f->manifest.segment_count; ++i) {
    const char *leaf = f->manifest.segment_leaves[i];
    if (!strcmp(leaf, f->manifest.active_segment))
      continue;
    for (j = 0; j < f->manifest.obsolete_segment_count; ++j)
      if (!strcmp(leaf, f->manifest.obsolete_segments[j]))
        break;
    if (j != f->manifest.obsolete_segment_count)
      continue;
    snprintf(path, sizeof(path), "%s/%s", f->segments, leaf);
    assert_int_equal(stat(path, &st), 0);
  }
}
static void
test_cleanup_removes_deleted_leaves_from_candidate_view(void **state) {
  fixture *f = *state;
  unsigned long deleted, pending;
  seed_manifest(f);
  check(f, lc_pouch_namespace_manifest_cleanup_obsolete(
               NULL, "demo", &f->manifest, 10, 1000, &deleted, &pending,
               &f->error));
  assert_int_equal(deleted, 0);
  assert_int_equal(pending, 3);
  check(f, lc_pouch_namespace_manifest_cleanup_obsolete(
               NULL, "demo", &f->manifest, 2000, 1000, &deleted, &pending,
               &f->error));
  assert_int_equal(deleted, 2);
  assert_int_equal(pending, 1);
  assert_int_equal(f->manifest.segment_count, 3);
  assert_string_equal(f->manifest.segment_leaves[0],
                      "seg-00000000000000000003.log");
  assert_int_equal(f->manifest.max_segment_id, 5);
  assert_candidates_exist(f);
}
static void test_cleanup_missing_obsolete_file_is_idempotent(void **state) {
  fixture *f = *state;
  char path[768];
  seed_manifest(f);
  leaf_path(f, 1, path);
  assert_int_equal(unlink(path), 0);
  check(f, lc_pouch_namespace_manifest_cleanup_obsolete(
               NULL, "demo", &f->manifest, 2000, 1000, NULL, NULL, &f->error));
  assert_int_equal(f->manifest.segment_count, 3);
  assert_candidates_exist(f);
  check(f, lc_pouch_namespace_manifest_cleanup_obsolete(
               NULL, "demo", &f->manifest, 2000, 1000, NULL, NULL, &f->error));
  assert_int_equal(f->manifest.segment_count, 3);
}
static void test_cleanup_unlink_failure_preserves_retryable_view(void **state) {
  fixture *f = *state;
  char path[768];
  seed_manifest(f);
  leaf_path(f, 2, path);
  fail_unlink = path;
  assert_int_equal(
      lc_pouch_namespace_manifest_cleanup_obsolete(
          NULL, "demo", &f->manifest, 2000, 1000, NULL, NULL, &f->error),
      LC_ERR_TRANSPORT);
  fail_unlink = NULL;
  assert_candidates_exist(f);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, lc_pouch_namespace_manifest_cleanup_obsolete(
               NULL, "demo", &f->manifest, 2000, 1000, NULL, NULL, &f->error));
  assert_int_equal(f->manifest.segment_count, 3);
  assert_candidates_exist(f);
}
static void test_cleanup_sync_failure_keeps_retirement_markers(void **state) {
  fixture *f = *state;
  struct stat st;
  seed_manifest(f);
  assert_int_equal(stat(f->segments, &st), 0);
  fail_sync_device = st.st_dev;
  fail_sync_inode = st.st_ino;
  fail_sync = 1;
  assert_int_equal(
      lc_pouch_namespace_manifest_cleanup_obsolete(
          NULL, "demo", &f->manifest, 2000, 1000, NULL, NULL, &f->error),
      LC_ERR_TRANSPORT);
  fail_sync = 0;
  assert_int_equal(f->manifest.obsolete_segment_count, 3);
  assert_candidates_exist(f);
  lc_error_cleanup(&f->error);
  lc_error_init(&f->error);
  check(f, lc_pouch_namespace_manifest_cleanup_obsolete(
               NULL, "demo", &f->manifest, 2000, 1000, NULL, NULL, &f->error));
  assert_int_equal(f->manifest.segment_count, 3);
  assert_candidates_exist(f);
}
static void write_body(fixture *f, const char *key) {
  lc_source *source = NULL;
  lc_pouch_state_write_result written = {0};
  check(f, lc_source_from_memory(key, strlen(key), &source, &f->error));
  check(f, lc_pouch_state_write(f->pouch, "demo", key, source, NULL, &written,
                                &f->error));
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(NULL, &written);
}
static void assert_body(fixture *f, const char *key) {
  lc_sink *sink = NULL;
  lc_pouch_state_read_result read = {0};
  const void *bytes;
  size_t length;
  check(f, lc_sink_to_memory(&sink, &f->error));
  check(f, lc_pouch_state_copy(f->pouch, "demo", key, sink, &read, &f->error));
  check(f, lc_sink_memory_bytes(sink, &bytes, &length, &f->error));
  assert_true(read.found);
  assert_int_equal(length, strlen(key));
  assert_memory_equal(bytes, key, length);
  lc_pouch_state_read_result_cleanup(NULL, &read);
  lc_sink_close(sink);
}
static void
test_full_maintenance_cleanup_does_not_reselect_retired_segments(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  unsigned long i;
  check(f, lc_pouch_set_single_writer(f->pouch, 1, &f->error));
  write_body(f, "one");
  write_body(f, "two");
  write_body(f, "three");
  options.ns = "demo";
  options.force = 1;
  check(f, lc_pouch_maintenance_run(f->pouch, &options, &result, &f->error));
  assert_true(result.compacted);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  check(f, lc_pouch_namespace_manifest_open(NULL, f->root, "demo", &f->manifest,
                                            NULL, NULL, &f->error));
  assert_true(f->manifest.obsolete_segment_count >= 2);
  for (i = 0; i < f->manifest.obsolete_segment_count; ++i)
    f->manifest.obsolete_segment_marked_at[i] = 1;
  check(f, lc_pouch_namespace_manifest_save(NULL, "demo", &f->manifest,
                                            &f->error));
  check(f, lc_pouch_maintenance_run(f->pouch, &options, &result, &f->error));
  assert_false(result.compacted);
  assert_string_equal(result.diagnostic, "no-candidates");
  assert_true(result.cleanup_deleted_count >= 2);
  assert_body(f, "one");
  assert_body(f, "two");
  assert_body(f, "three");
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}
typedef struct candidate_fault {
  const char *phase;
  int cause, called;
  char path[1024];
} candidate_fault;
static candidate_fault *candidate_injection;
static int candidate_open_error(const char *path, const char *phase) {
  candidate_fault *fault = candidate_injection;
  if (strcmp(phase, fault->phase))
    return 0;
  ++fault->called;
  assert_true(strlen(path) < sizeof(fault->path));
  strcpy(fault->path, path);
  return fault->cause;
}
static void
test_candidate_open_failures_remain_actionable_and_retryable(void **state) {
  fixture *f = *state;
  const int causes[] = {ENOENT, EACCES, EMFILE};
  const char *phases[] = {"capture", "validation"};
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  candidate_fault fault;
  struct stat st;
  char number[32], *logs;
  long length;
  unsigned int i, j;
  write_body(f, "one");
  write_body(f, "two");
  write_body(f, "three");
  options.ns = "demo";
  options.force = 1;
  for (i = 0; i < 2; ++i) {
    for (j = 0; j < 3; ++j) {
      assert_int_equal(fflush(f->log), 0);
      assert_int_equal(ftruncate(fileno(f->log), 0), 0);
      rewind(f->log);
      memset(&fault, 0, sizeof(fault));
      fault.phase = phases[i];
      fault.cause = causes[j];
      candidate_injection = &fault;
      lc_pouch_test_candidate_open_errno = candidate_open_error;
      assert_int_equal(
          lc_pouch_maintenance_run(f->pouch, &options, &result, &f->error),
          LC_ERR_TRANSPORT);
      lc_pouch_test_candidate_open_errno = NULL;
      candidate_injection = NULL;
      assert_int_equal(fault.called, 1);
      assert_false(result.compacted);
      assert_string_equal(f->error.message,
                          "failed to open pouch compaction candidate");
      snprintf(number, sizeof(number), "errno=%d", causes[j]);
      assert_non_null(strstr(f->error.detail, number));
      assert_non_null(strstr(f->error.detail, phases[i]));
      assert_non_null(strstr(f->error.detail, fault.path));
      assert_int_equal(stat(fault.path, &st), 0);
      assert_int_equal(fflush(f->log), 0);
      assert_int_equal(fseek(f->log, 0, SEEK_END), 0);
      length = ftell(f->log);
      assert_true(length > 0);
      logs = malloc((size_t)length + 1);
      assert_non_null(logs);
      rewind(f->log);
      assert_int_equal(fread(logs, 1, (size_t)length, f->log), length);
      logs[length] = 0;
      assert_non_null(strstr(logs, "maintenance.error"));
      assert_non_null(strstr(logs, fault.path));
      assert_non_null(strstr(logs, number));
      free(logs);
      lc_pouch_maintenance_result_cleanup(NULL, &result);
      lc_error_cleanup(&f->error);
      lc_error_init(&f->error);
      check(f, lc_pouch_namespace_manifest_open(
                   NULL, f->root, "demo", &f->manifest, NULL, NULL, &f->error));
      assert_null(f->manifest.latest_snapshot);
      lc_pouch_namespace_manifest_cleanup(NULL, &f->manifest);
      assert_body(f, "one");
      assert_body(f, "two");
      assert_body(f, "three");
    }
  }
  check(f, lc_pouch_maintenance_run(f->pouch, &options, &result, &f->error));
  assert_true(result.compacted);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}
static void test_cleanup_protects_current_snapshot_and_grace(void **state) {
  fixture *f = *state;
  char path[768];
  unsigned long i, deleted, pending;
  FILE *fp;
  seed_manifest(f);
  lc_pouch_namespace_manifest_cleanup(NULL, &f->manifest);
  for (i = 7; i <= 9; ++i) {
    snprintf(path, sizeof(path), "%s/snapshots/snapshot-%020lu.log",
             f->directory, i);
    check(f, lc_pouch_path_write_text_file(path, "", &f->error));
  }
  fp = fopen(f->manifest_path, "ab");
  assert_non_null(fp);
  assert_true(
      fputs("snapshot=snapshot-00000000000000000009.log\n"
            "obsolete_snapshot=snapshot-00000000000000000007.log\t1\n"
            "obsolete_snapshot=snapshot-00000000000000000008.log\t1999\n",
            fp) >= 0);
  assert_int_equal(fclose(fp), 0);
  check(f, lc_pouch_namespace_manifest_open(NULL, f->root, "demo", &f->manifest,
                                            NULL, NULL, &f->error));
  check(f, lc_pouch_namespace_manifest_cleanup_obsolete(
               NULL, "demo", &f->manifest, 2000, 1000, &deleted, &pending,
               &f->error));
  assert_int_equal(deleted, 3);
  assert_int_equal(pending, 2);
  snprintf(path, sizeof(path), "%s/snapshots/snapshot-%020lu.log", f->directory,
           7UL);
  assert_int_equal(access(path, F_OK), -1);
  snprintf(path, sizeof(path), "%s/snapshots/snapshot-%020lu.log", f->directory,
           8UL);
  assert_int_equal(access(path, F_OK), 0);
  snprintf(path, sizeof(path), "%s/snapshots/snapshot-%020lu.log", f->directory,
           9UL);
  assert_int_equal(access(path, F_OK), 0);
  assert_candidates_exist(f);
}
static void
test_concurrent_process_cleanup_serializes_namespace_maintenance(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  lc_pouch_open_options open_options = {0};
  unsigned long i;
  int ready[2], start[2], status;
  char token;
  pid_t children[2];
  write_body(f, "one");
  write_body(f, "two");
  write_body(f, "three");
  options.ns = "demo";
  options.force = 1;
  check(f, lc_pouch_maintenance_run(f->pouch, &options, &result, &f->error));
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  check(f, lc_pouch_namespace_manifest_open(NULL, f->root, "demo", &f->manifest,
                                            NULL, NULL, &f->error));
  for (i = 0; i < f->manifest.obsolete_segment_count; ++i)
    f->manifest.obsolete_segment_marked_at[i] = 1;
  check(f, lc_pouch_namespace_manifest_save(NULL, "demo", &f->manifest,
                                            &f->error));
  lc_pouch_namespace_manifest_cleanup(NULL, &f->manifest);
  lc_pouch_close(f->pouch);
  f->pouch = NULL;
  open_options.single_writer_set = 1;
  open_options.background_compaction_enabled_set = 1;
  open_options.query_indexing_enabled_set = 1;
  assert_int_equal(pipe(ready), 0);
  assert_int_equal(pipe(start), 0);
  for (i = 0; i < 2; ++i) {
    children[i] = fork();
    assert_true(children[i] >= 0);
    if (children[i] == 0) {
      lc_pouch *pouch = NULL;
      int rc;
      close(ready[0]);
      close(start[1]);
      rc = lc_pouch_open(f->root, NULL, &open_options, &pouch, &f->error);
      if (rc != LC_OK || write(ready[1], "r", 1) != 1 ||
          read(start[0], &token, 1) != 1)
        _exit(42);
      rc = lc_pouch_maintenance_run(pouch, &options, &result, &f->error);
      lc_pouch_maintenance_result_cleanup(NULL, &result);
      lc_pouch_close(pouch);
      lc_error_cleanup(&f->error);
      _exit(rc == LC_OK ? 0 : 43);
    }
  }
  close(ready[1]);
  close(start[0]);
  for (i = 0; i < 2; ++i)
    assert_int_equal(read(ready[0], &token, 1), 1);
  assert_int_equal(write(start[1], "go", 2), 2);
  close(ready[0]);
  close(start[1]);
  for (i = 0; i < 2; ++i) {
    assert_int_equal(waitpid(children[i], &status, 0), children[i]);
    assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
  check(f, lc_pouch_open(f->root, NULL, &open_options, &f->pouch, &f->error));
  assert_body(f, "one");
  assert_body(f, "two");
  assert_body(f, "three");
}

int main(int argc, char **argv) {
  const struct CMUnitTest cases[] = {
      cmocka_unit_test_setup_teardown(
          test_cleanup_removes_deleted_leaves_from_candidate_view, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_cleanup_missing_obsolete_file_is_idempotent, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_cleanup_unlink_failure_preserves_retryable_view, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_cleanup_sync_failure_keeps_retirement_markers, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_full_maintenance_cleanup_does_not_reselect_retired_segments,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_candidate_open_failures_remain_actionable_and_retryable, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_cleanup_protects_current_snapshot_and_grace, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_concurrent_process_cleanup_serializes_namespace_maintenance,
          setup, teardown)};
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 2;
  return lc_test_run_cases(cases, sizeof(cases) / sizeof(cases[0]), argc, argv,
                           NULL, NULL);
}
