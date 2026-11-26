#include "../../tests/support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_pouch_recode.h"
#include "migration.h"
#include <dirent.h>
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
  static const char *const phases[] = {"candidate-ready", "root-exchanged",
                                       "legacy-retired"};
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
    if (i == 0) {
      REQUIRE(S_ISDIR(identity.st_mode));
      /* The old writer's presence lease survives its process and can remain
       * valid for three seconds after the kernel lock is released. */
      sleep(4);
      ok(lc_pouch_open(root, NULL, &options, &pouch, &error), &error);
      replace(pouch, "newer legacy write", &error);
      lc_pouch_close(pouch);
    } else
      REQUIRE(S_ISREG(identity.st_mode));
    ok(ps_pouch_open_managed(root, NULL, &options, 0, &pouch, &error), &error);
    read_value(pouch, i ? "before crash" : "newer legacy write", &error);
    lc_pouch_close(pouch);
    verify_cleanup(directory);
    lc_test_tmp_cleanup_path(directory, prefix);
  }
  lc_error_cleanup(&error);
  return 0;
}
