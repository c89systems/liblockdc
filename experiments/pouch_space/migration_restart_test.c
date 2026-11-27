#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_recode.h"
#include "migration.h"
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define REQUIRE(test)                                                          \
  do {                                                                         \
    if (!(test)) {                                                             \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #test);               \
      abort();                                                                 \
    }                                                                          \
  } while (0)

static const char *cut;

static void crash_at(const char *phase) {
  if (!strcmp(phase, cut))
    _exit(88);
}

static void ok(int rc, lc_error *error) {
  if (rc != LC_OK)
    fprintf(stderr, "pouch error %d %s (%s)\n", rc,
            error->message ? error->message : "",
            error->detail ? error->detail : "");
  REQUIRE(rc == LC_OK);
}

static void replace(lc_pouch *pouch, const char *value, lc_error *error) {
  lc_source *body = NULL;
  lc_pouch_state_write_result result = {0};
  ok(lc_source_from_memory(value, strlen(value), &body, error), error);
  ok(lc_pouch_state_write(pouch, "default", "key", body, NULL, &result, error),
     error);
  lc_source_close(body);
  lc_pouch_state_write_result_cleanup(NULL, &result);
}

static void read_value(lc_pouch *pouch, const char *value, lc_error *error) {
  lc_pouch_state_read_result result = {0};
  char bytes[64];
  size_t length;
  ok(lc_pouch_state_read(pouch, "default", "key", &result, error), error);
  REQUIRE(result.found);
  length = result.body->read(result.body, bytes, sizeof(bytes), error);
  REQUIRE(error->code == LC_OK && length == strlen(value) &&
          !memcmp(bytes, value, length));
  lc_pouch_state_read_result_cleanup(NULL, &result);
}

static void verify_cleanup(const char *directory) {
  DIR *dir = opendir(directory);
  struct dirent *entry;
  size_t count = 0;
  REQUIRE(dir != NULL);
  while ((entry = readdir(dir)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..") ||
        !strcmp(entry->d_name, ".liblockdc-test-tmp-owner"))
      continue;
    REQUIRE(!strcmp(entry->d_name, "store"));
    ++count;
  }
  REQUIRE(count == 1 && closedir(dir) == 0);
}

static void find_stage(const char *directory, char *path, size_t capacity) {
  static const char prefix[] = ".pouch-import-";
  DIR *dir = opendir(directory);
  struct dirent *entry;
  size_t count = 0;
  REQUIRE(dir != NULL);
  while ((entry = readdir(dir)) != NULL) {
    if (!strncmp(entry->d_name, prefix, sizeof(prefix) - 1)) {
      int n = snprintf(path, capacity, "%s/%s", directory, entry->d_name);
      REQUIRE(n > 0 && (size_t)n < capacity);
      ++count;
    }
  }
  REQUIRE(count == 1 && closedir(dir) == 0);
}

static void require_same_entry(const char *path, const struct stat *before) {
  struct stat after;
  REQUIRE(lstat(path, &after) == 0 && after.st_mode == before->st_mode &&
          after.st_dev == before->st_dev && after.st_ino == before->st_ino);
}

static void require_refusal(const char *root, const lc_pouch_open_options *opts,
                            int code, const char *reason, lc_error *error) {
  lc_pouch *pouch = NULL;
  int rc = ps_pouch_open_managed(root, NULL, opts, 0, &pouch, error);
  if (rc != code)
    fprintf(stderr, "expected migration error %d, got %d (%s)\n", code, rc,
            error->message ? error->message : "");
  REQUIRE(rc == code && !pouch);
  REQUIRE(!reason ||
          (error->server_code && !strcmp(error->server_code, reason)));
  lc_error_cleanup(error);
}

static void refuse_stage_directory(const char *root, const char *stage,
                                   const lc_pouch_open_options *opts,
                                   lc_error *error) {
  struct stat original, candidate, obstruction;
  char backup[1280];
  REQUIRE(lstat(root, &original) == 0 && S_ISDIR(original.st_mode));
  REQUIRE(lstat(stage, &candidate) == 0 && S_ISREG(candidate.st_mode));
  REQUIRE(snprintf(backup, sizeof(backup), "%s.saved", stage) <
          (int)sizeof(backup));
  REQUIRE(rename(stage, backup) == 0 && mkdir(stage, 0700) == 0);
  REQUIRE(lstat(stage, &obstruction) == 0);
  require_refusal(root, opts, LC_ERR_INVALID, "pouch_migration_layout", error);
  require_same_entry(root, &original);
  require_same_entry(stage, &obstruction);
  REQUIRE(rmdir(stage) == 0 && rename(backup, stage) == 0);
  require_same_entry(stage, &candidate);
}

static void write_receipt(const char *path, const char *value) {
  FILE *file = fopen(path, "wb");
  REQUIRE(file != NULL);
  REQUIRE(fwrite(value, 1, LC_XID_STRING_SIZE, file) == LC_XID_STRING_SIZE &&
          fclose(file) == 0);
}

static void read_receipt(const char *path, char value[LC_XID_STRING_SIZE]) {
  FILE *file = fopen(path, "rb");
  REQUIRE(file != NULL);
  REQUIRE(fread(value, 1, LC_XID_STRING_SIZE, file) == LC_XID_STRING_SIZE &&
          fgetc(file) == EOF && !ferror(file) && fclose(file) == 0);
}

static void refuse_wrong_receipt(const char *root, const char *stage,
                                 const lc_pouch_open_options *opts,
                                 lc_error *error) {
  struct stat published, retired, manifest;
  char path[1280], manifest_path[1280];
  char original[LC_XID_STRING_SIZE], wrong[LC_XID_STRING_SIZE];
  char actual[LC_XID_STRING_SIZE];
  REQUIRE(lstat(root, &published) == 0 && S_ISREG(published.st_mode));
  REQUIRE(lstat(stage, &retired) == 0 && S_ISDIR(retired.st_mode));
  REQUIRE(snprintf(manifest_path, sizeof(manifest_path), "%s/manifest", stage) <
          (int)sizeof(manifest_path));
  REQUIRE(lstat(manifest_path, &manifest) == 0);
  REQUIRE(snprintf(path, sizeof(path), "%s/%s", stage, PS_MIGRATION_RECEIPT) <
          (int)sizeof(path));
  read_receipt(path, original);
  ok(lc_xid_new(wrong, error), error);
  REQUIRE(strcmp(wrong, original));
  write_receipt(path, wrong);
  require_refusal(root, opts, LC_ERR_PROTOCOL, "pouch_migration_receipt",
                  error);
  require_same_entry(root, &published);
  require_same_entry(stage, &retired);
  require_same_entry(manifest_path, &manifest);
  read_receipt(path, actual);
  REQUIRE(!memcmp(actual, wrong, sizeof(actual)));
  write_receipt(path, original);
}

static int interrupt_recode(void *unused, int phase, lc_error *e) {
  (void)unused;
  return phase == LC_POUCH_RECODE_SEGMENT_READY
             ? lc_error_set(e, LC_ERR_PENDING, 0, "interrupted legacy recode",
                            NULL, NULL, "pouch")
             : LC_OK;
}

static void resume_legacy_recode(const char *root,
                                 const lc_pouch_open_options *options,
                                 lc_error *e) {
  lc_pouch_open_options recode = *options;
  lc_pouch *pouch = NULL;
  struct stat identity;
  recode.compression = "zlib";
  lc_pouch_recode_test_hook = interrupt_recode;
  REQUIRE(lc_pouch_open(root, NULL, &recode, &pouch, e) == LC_ERR_PENDING &&
          !pouch);
  lc_pouch_recode_test_hook = NULL;
  lc_error_cleanup(e);
  ok(ps_pouch_open_managed(root, NULL, options, 0, &pouch, e), e);
  REQUIRE(lstat(root, &identity) == 0 && S_ISREG(identity.st_mode));
  REQUIRE(!strcmp(lc_pouch_compression_mode(pouch), "zlib"));
  read_value(pouch, "before crash", e);
  lc_pouch_close(pouch);
}

int main(int argc, char **argv) {
  enum {
    CANDIDATE_READY,
    ROOT_EXCHANGED,
    LEGACY_RECEIPT_REMOVED,
    LEGACY_RETIRED
  };
  static const char *const phases[] = {
      [CANDIDATE_READY] = "candidate-ready",
      [ROOT_EXCHANGED] = "root-exchanged",
      [LEGACY_RECEIPT_REMOVED] = "legacy-receipt-removed",
      [LEGACY_RETIRED] = "legacy-retired",
  };
  const size_t phase_count = sizeof(phases) / sizeof(phases[0]);
  lc_pouch_open_options options = {0};
  lc_error error;
  REQUIRE(argc == 2);
  lc_error_init(&error);
  options.background_compaction_enabled_set = 1;
  options.query_indexing_enabled_set = 1;
  options.durable_sync = 1;
  for (size_t i = 0; i <= phase_count; ++i) {
    char prefix[1024], template[1100], directory[1100], root[1200];
    char stage[1200];
    lc_pouch *pouch = NULL;
    struct stat identity;
    pid_t child;
    int status;

    REQUIRE(snprintf(prefix, sizeof(prefix), "%s/migration-restart-", argv[1]) <
            (int)sizeof(prefix));
    REQUIRE(snprintf(template, sizeof(template), "%sXXXXXX", prefix) <
            (int)sizeof(template));
    REQUIRE(
        lc_test_tmp_mkdtemp(template, directory, sizeof(directory), prefix));
    REQUIRE(snprintf(root, sizeof(root), "%s/store", directory) <
            (int)sizeof(root));
    ok(lc_pouch_open(root, NULL, &options, &pouch, &error), &error);
    if (i == CANDIDATE_READY) {
      REQUIRE(lstat(root, &identity) == 0 && S_ISDIR(identity.st_mode));
      require_refusal(root, &options, LC_ERR_INVALID, NULL, &error);
      require_same_entry(root, &identity);
      verify_cleanup(directory);
    }
    replace(pouch, "before crash", &error);
    lc_pouch_close(pouch);
    if (i == phase_count) {
      resume_legacy_recode(root, &options, &error);
      verify_cleanup(directory);
      lc_test_tmp_cleanup_path(directory, prefix);
      continue;
    }
    child = fork();
    REQUIRE(child >= 0);
    if (!child) {
      cut = phases[i];
      ps_migration_test_hook = crash_at;
      ok(ps_pouch_open_managed(root, NULL, &options, 0, &pouch, &error),
         &error);
      _exit(89);
    }
    REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 88);
    REQUIRE(lstat(root, &identity) == 0);
    if (i == CANDIDATE_READY) {
      REQUIRE(S_ISDIR(identity.st_mode));
      /* The old writer's presence lease survives its process and can remain
       * valid for three seconds after the kernel lock is released. */
      sleep(4);
      ok(lc_pouch_open(root, NULL, &options, &pouch, &error), &error);
      replace(pouch, "newer legacy write", &error);
      lc_pouch_close(pouch);
      find_stage(directory, stage, sizeof(stage));
      refuse_stage_directory(root, stage, &options, &error);
    } else {
      REQUIRE(S_ISREG(identity.st_mode));
      if (i == ROOT_EXCHANGED) {
        find_stage(directory, stage, sizeof(stage));
        refuse_wrong_receipt(root, stage, &options, &error);
      } else if (i == LEGACY_RECEIPT_REMOVED) {
        char receipt[1280];
        find_stage(directory, stage, sizeof(stage));
        REQUIRE(lstat(stage, &identity) == 0 && S_ISDIR(identity.st_mode));
        REQUIRE(snprintf(receipt, sizeof(receipt), "%s/%s", stage,
                         PS_MIGRATION_RECEIPT) < (int)sizeof(receipt));
        REQUIRE(lstat(receipt, &identity) != 0 && errno == ENOENT);
      } else if (i == LEGACY_RETIRED) {
        verify_cleanup(directory);
      }
    }
    ok(ps_pouch_open_managed(root, NULL, &options, 0, &pouch, &error), &error);
    read_value(pouch,
               i == CANDIDATE_READY ? "newer legacy write" : "before crash",
               &error);
    lc_pouch_close(pouch);
    verify_cleanup(directory);
    lc_test_tmp_cleanup_path(directory, prefix);
  }
  lc_error_cleanup(&error);
  return 0;
}
