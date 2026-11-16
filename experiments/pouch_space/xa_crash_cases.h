#ifndef POUCH_SPACE_XA_CRASH_CASES_H
#define POUCH_SPACE_XA_CRASH_CASES_H
#include "queue_outbox_crash_cases.h"
#include "xa_managed_cases.h"

/* Root admission has separate qualification. Arm at the factory return, before
 * common SDK transaction recovery, then retain all observed handoff boundaries.
 */
static void xa_crash_admitted(void *context) {
  outbox_crash_io *io = context;
  io->armed = 1;
}
static void xa_crash_execute(const char *path, int sync, int managed,
                             int action, outbox_crash_io *context,
                             lc_error *e) {
  ps_io io = {context,           outbox_crash_read,    outbox_crash_write,
              outbox_crash_sync, outbox_crash_reserve, NULL};
  lc_client *client = xa_client_with_admission(path, sync, 0, &io,
                                               xa_crash_admitted, context, e);
  lc_xa_store *journal = NULL, *other = NULL;
  if (managed) {
    ok(lc_xa_store_open_owned(client, xa_owner_ids[0], &journal, e), e);
    ok(lc_xa_store_open_owned(client, xa_owner_ids[1], &other, e), e);
  } else if (action == 3)
    xa_stage(client, "{\"value\":84}", e);
  int rc = managed ? xa_managed_action(client, journal, other, action, e)
                   : xa_action(client, action, e);
  if (context->stop)
    _exit(74); /* Every requested boundary must be observed by this child. */
  context->armed = 0;
  if (managed)
    ok(rc, e);
  else
    xa_ok(action, rc, e);
  lc_xa_store_close(other);
  lc_xa_store_close(journal);
  lc_client_close(client);
}
static void xa_crash_child(void) {
  const char *path = getenv("POUCH_XA_CRASH_PATH");
  const char *sync = getenv("POUCH_XA_CRASH_SYNC");
  const char *managed = getenv("POUCH_XA_CRASH_MANAGED");
  const char *action = getenv("POUCH_XA_CRASH_ACTION");
  const char *stop = getenv("POUCH_XA_CRASH_STOP");
  const char *after = getenv("POUCH_XA_CRASH_AFTER");
  REQUIRE(path && sync && managed && action && stop && after);
  lc_error e;
  lc_error_init(&e);
  outbox_crash_io context = {0};
  context.stop = (size_t)strtoul(stop, NULL, 10);
  context.after = atoi(after);
  xa_crash_execute(path, atoi(sync), atoi(managed), atoi(action), &context, &e);
  _exit(74);
}
static void xa_crashes(const char *directory) {
  static const int actions[2][5] = {{0, 3, 5, 6, 10}, {0, 5, 6, 7, 0}};
  char path[1400], sync_text[16], action_text[16], stop_text[32];
  lc_error e;
  size_t deaths = 0U;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/xa-crash", directory) <
          (int)sizeof(path));
  for (int sync = 0; sync < 2; ++sync)
    for (int managed = 0; managed < 2; ++managed)
      for (size_t ordinal = 0; ordinal < (managed ? 4U : 5U); ++ordinal) {
        int action = actions[managed][ordinal];
        if (getenv("POUCH_XA_ACTION") &&
            (managed || !xa_selected("POUCH_XA_ACTION", action, 10)))
          continue;
        size_t sites = 0U;
        for (size_t stop = 0U; stop <= sites; ++stop)
          for (int after = 0; after < (stop ? 2 : 1); ++after) {
            /* No fault occurs during seed construction. Group successful
             * setup barriers, then sync its complete file once before exec.
             * Recovery and the interrupted action use real syscall I/O. */
            io_fault seed_fault = {0};
            ps_io seed_io = {&seed_fault,        disk_read,    disk_write,
                             xa_allocation_sync, disk_reserve, NULL};
            lc_client *client = xa_client(path, sync, 1, &seed_io, &e);
            lc_xa_store *journal = NULL, *other = NULL;
            if (managed) {
              ok(lc_xa_store_open_owned(client, xa_owner_ids[0], &journal, &e),
                 &e);
              xa_managed_seed(client, journal, action, &e);
            } else
              xa_seed(client, action, &e);
            lc_xa_store_close(journal);
            lc_client_close(client);
            REQUIRE(!live);
            int seed_fd = open(path, O_RDWR);
            REQUIRE(seed_fd >= 0 && !fdatasync(seed_fd) && !close(seed_fd));
            if (!stop) {
              outbox_crash_io context = {0};
              xa_crash_execute(path, sync, managed, action, &context, &e);
              sites = context.calls;
              REQUIRE(sites && !live);
              fprintf(stderr,
                      "XA crash sync=%d managed=%d action=%d sites=%zu\n", sync,
                      managed, action, sites);
            } else {
              REQUIRE(snprintf(sync_text, sizeof(sync_text), "%d", sync) > 0);
              REQUIRE(snprintf(action_text, sizeof(action_text), "%d", action) >
                      0);
              REQUIRE(snprintf(stop_text, sizeof(stop_text), "%zu", stop) > 0);
              REQUIRE(!setenv("POUCH_XA_CRASH_PATH", path, 1));
              REQUIRE(!setenv("POUCH_XA_CRASH_SYNC", sync_text, 1));
              REQUIRE(
                  !setenv("POUCH_XA_CRASH_MANAGED", managed ? "1" : "0", 1));
              REQUIRE(!setenv("POUCH_XA_CRASH_ACTION", action_text, 1));
              REQUIRE(!setenv("POUCH_XA_CRASH_STOP", stop_text, 1));
              REQUIRE(!setenv("POUCH_XA_CRASH_AFTER", after ? "1" : "0", 1));
              if (xa_test_key)
                REQUIRE(!setenv("POUCH_XA_KEY", xa_test_key, 1));
              pid_t child = fork();
              REQUIRE(child >= 0);
              if (!child) {
                execl("/proc/self/exe", "pouch_space_client_test", directory,
                      "xa-crash-child", (char *)NULL);
                _exit(74);
              }
              int status;
              REQUIRE(waitpid(child, &status, 0) == child &&
                      WIFEXITED(status) && WEXITSTATUS(status) == 73);
              ++deaths;
            }
            client = xa_client(path, sync, 0, NULL, &e);
            if (managed) {
              ok(lc_xa_store_open_owned(client, xa_owner_ids[0], &journal, &e),
                 &e);
              ok(lc_xa_store_open_owned(client, xa_owner_ids[1], &other, &e),
                 &e);
              xa_managed_verify(client, journal, other, action, &e);
            } else
              xa_verify(client, action, stop == 0U, &e);
            lc_xa_store_close(other);
            lc_xa_store_close(journal);
            lc_client_close(client);
            REQUIRE(!live);
            one_file(path);
            REQUIRE(!unlink(path));
          }
      }
  REQUIRE(!unsetenv("POUCH_XA_CRASH_PATH"));
  REQUIRE(!unsetenv("POUCH_XA_CRASH_SYNC"));
  REQUIRE(!unsetenv("POUCH_XA_CRASH_MANAGED"));
  REQUIRE(!unsetenv("POUCH_XA_CRASH_ACTION"));
  REQUIRE(!unsetenv("POUCH_XA_CRASH_STOP"));
  REQUIRE(!unsetenv("POUCH_XA_CRASH_AFTER"));
  REQUIRE(!unsetenv("POUCH_XA_KEY"));
  lc_error_cleanup(&e);
  printf("XA process deaths=%zu passed\n", deaths);
}
#endif
