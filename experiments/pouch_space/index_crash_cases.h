#ifndef POUCH_SPACE_INDEX_CRASH_CASES_H
#define POUCH_SPACE_INDEX_CRASH_CASES_H
#include "index_cases.h"
#include "queue_outbox_crash_cases.h"

static void index_crash_admitted(void *context) {
  outbox_crash_io *io = context;
  io->armed = 1;
}
static void index_crash_execute(const char *path, int sync, int action,
                                outbox_crash_io *context, lc_error *e) {
  ps_io io = {context,           outbox_crash_read,    outbox_crash_write,
              outbox_crash_sync, outbox_crash_reserve, NULL};
  lc_client *client = index_client_admit(path, sync, 0, action != 3, 0, &io,
                                         index_crash_admitted, context, e);
  if (action)
    index_change(client, e);
  int rc = index_action(client, action, e);
  if (context->stop)
    _exit(74);
  context->armed = 0;
  ok(rc, e);
  lc_client_close(client);
}
static void index_crash_child(void) {
  const char *path = getenv("POUCH_INDEX_CRASH_PATH");
  const char *sync = getenv("POUCH_INDEX_CRASH_SYNC");
  const char *action = getenv("POUCH_INDEX_CRASH_ACTION");
  const char *stop = getenv("POUCH_INDEX_CRASH_STOP");
  const char *after = getenv("POUCH_INDEX_CRASH_AFTER");
  REQUIRE(path && sync && action && stop && after);
  lc_error e;
  lc_error_init(&e);
  outbox_crash_io context = {0};
  context.stop = (size_t)strtoul(stop, NULL, 10);
  context.after = atoi(after);
  index_crash_execute(path, atoi(sync), atoi(action), &context, &e);
  _exit(74);
}
static void index_crashes(const char *directory) {
  static const int actions[] = {0, 1, 3};
  char path[1400], sync_text[16], action_text[16], stop_text[32];
  size_t deaths = 0U;
  lc_error e;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/index-crash", directory) <
          (int)sizeof(path));
  for (int sync = 0; sync < 2; ++sync)
    for (size_t ordinal = 0U; ordinal < sizeof(actions) / sizeof(actions[0]);
         ++ordinal) {
      int action = actions[ordinal];
      size_t sites = 0U;
      for (size_t stop = 0U; stop <= sites; ++stop)
        for (int after = 0; after < (stop ? 2 : 1); ++after) {
          /* Group successful fixture setup, then stabilize its complete seed
           * once. Child admission/recovery, mutation and publication use real
           * syscall I/O. No deliberate death belongs in live sanitizer runs. */
          io_fault seed_fault = {0};
          ps_io seed_io = {&seed_fault,          disk_read,    disk_write,
                           index_immediate_sync, disk_reserve, NULL};
          lc_client *client = index_client(path, sync, 1, 1, &seed_io, &e);
          index_seed(client, action, &e);
          lc_client_close(client);
          REQUIRE(!live);
          int fd = open(path, O_RDWR);
          REQUIRE(fd >= 0 && !ps_native_io.sync(NULL, fd) && !close(fd));
          if (!stop) {
            outbox_crash_io context = {0};
            index_crash_execute(path, sync, action, &context, &e);
            sites = context.calls;
            REQUIRE(sites && !live);
            fprintf(stderr, "index crash sync=%d action=%d sites=%zu\n", sync,
                    action, sites);
          } else {
            REQUIRE(snprintf(sync_text, sizeof(sync_text), "%d", sync) > 0);
            REQUIRE(snprintf(action_text, sizeof(action_text), "%d", action) >
                    0);
            REQUIRE(snprintf(stop_text, sizeof(stop_text), "%zu", stop) > 0);
            REQUIRE(!setenv("POUCH_INDEX_CRASH_PATH", path, 1));
            REQUIRE(!setenv("POUCH_INDEX_CRASH_SYNC", sync_text, 1));
            REQUIRE(!setenv("POUCH_INDEX_CRASH_ACTION", action_text, 1));
            REQUIRE(!setenv("POUCH_INDEX_CRASH_STOP", stop_text, 1));
            REQUIRE(!setenv("POUCH_INDEX_CRASH_AFTER", after ? "1" : "0", 1));
            if (index_key)
              REQUIRE(!setenv("POUCH_INDEX_KEY", index_key, 1));
            pid_t child = fork();
            REQUIRE(child >= 0);
            if (!child) {
              execl("/proc/self/exe", "pouch_space_client_test", directory,
                    "index-crash-child", (char *)NULL);
              _exit(74);
            }
            int status;
            REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
                    WEXITSTATUS(status) == 73);
            ++deaths;
          }
          client = index_client(path, sync, 0, 1, NULL, &e);
          index_verify(client, action, stop == 0U, &e);
          lc_client_close(client);
          REQUIRE(!live);
          one_file(path);
          REQUIRE(!unlink(path));
        }
    }
  REQUIRE(!unsetenv("POUCH_INDEX_CRASH_PATH"));
  REQUIRE(!unsetenv("POUCH_INDEX_CRASH_SYNC"));
  REQUIRE(!unsetenv("POUCH_INDEX_CRASH_ACTION"));
  REQUIRE(!unsetenv("POUCH_INDEX_CRASH_STOP"));
  REQUIRE(!unsetenv("POUCH_INDEX_CRASH_AFTER"));
  REQUIRE(!unsetenv("POUCH_INDEX_KEY"));
  printf("index process deaths=%zu passed\n", deaths);
  lc_error_cleanup(&e);
}
#endif
