#ifndef POUCH_SPACE_QUEUE_OUTBOX_CRASH_CASES_H
#define POUCH_SPACE_QUEUE_OUTBOX_CRASH_CASES_H
/* Real exec'd process death complements the separate lost/torn I/O model. */
#include "queue_outbox_marker_cases.h"
typedef struct outbox_crash_io {
  size_t calls, stop;
  int armed, after;
} outbox_crash_io;
static void outbox_crash_boundary(outbox_crash_io *io, int after) {
  if (!io->armed)
    return;
  if (!after)
    ++io->calls;
  if (io->stop && io->calls == io->stop && io->after == after)
    _exit(73);
}
static ssize_t outbox_crash_read(void *context, int fd, void *bytes,
                                 size_t size, off_t offset) {
  outbox_crash_io *io = context;
  ssize_t result;
  outbox_crash_boundary(io, 0);
  result = pread(fd, bytes, size, offset);
  outbox_crash_boundary(io, 1);
  return result;
}
static ssize_t outbox_crash_write(void *context, int fd, const void *bytes,
                                  size_t size, off_t offset) {
  outbox_crash_io *io = context;
  ssize_t result;
  outbox_crash_boundary(io, 0);
  result = pwrite(fd, bytes, size, offset);
  outbox_crash_boundary(io, 1);
  return result;
}
static int outbox_crash_sync(void *context, int fd) {
  outbox_crash_io *io = context;
  int result;
  outbox_crash_boundary(io, 0);
  result = ps_native_io.sync(NULL, fd);
  outbox_crash_boundary(io, 1);
  return result;
}
static int outbox_crash_reserve(void *context, int fd, off_t offset,
                                off_t size) {
  outbox_crash_io *io = context;
  int result;
  outbox_crash_boundary(io, 0);
  result = ps_native_io.reserve(NULL, fd, offset, size);
  outbox_crash_boundary(io, 1);
  return result;
}
static void outbox_marker_crash_child(void) {
  const char *path = getenv("POUCH_MARKER_CRASH_PATH");
  const char *operation = getenv("POUCH_MARKER_CRASH_OPERATION");
  const char *sync = getenv("POUCH_MARKER_CRASH_SYNC");
  const char *stop = getenv("POUCH_MARKER_CRASH_STOP");
  const char *after = getenv("POUCH_MARKER_CRASH_AFTER");
  outbox_crash_io context = {0};
  lc_error e;
  outbox_marker_result result;
  REQUIRE(path && operation && sync && stop && after);
  lc_error_init(&e);
  ps_io io = {&context,          outbox_crash_read,    outbox_crash_write,
              outbox_crash_sync, outbox_crash_reserve, NULL};
  lc_pouch *p = visit_open(path, 0, atoi(sync), 0, &io, &allocator, &e);
  context.stop = (size_t)strtoul(stop, NULL, 10);
  context.after = atoi(after);
  context.armed = 1;
  (void)outbox_marker_action(p, atoi(operation), &result, &e);
  _exit(74); /* Every requested boundary must be reached. */
}
static void outbox_marker_crashes(const char *directory) {
  lc_error e;
  char path[1400], operation_text[32], sync_text[32], stop_text[32];
  size_t deaths = 0U;
  lc_error_init(&e);
  REQUIRE(snprintf(path, sizeof(path), "%s/marker-crash", directory) <
          (int)sizeof(path));
  for (int sync = 0; sync < 2; ++sync)
    for (int operation = 0; operation < 12; ++operation) {
      if (outbox_marker_read_operation(operation))
        continue;
      size_t sites = 0U;
      for (size_t stop = 0U; stop <= sites; ++stop)
        for (int after = 0; after < (stop ? 2 : 1); ++after) {
          lc_pouch *p = visit_open(path, 0, sync, 1, NULL, &allocator, &e);
          outbox_marker_seed(p, operation, &e);
          lc_pouch_close(p);
          REQUIRE(!live);
          if (!stop) {
            outbox_crash_io context = {0};
            ps_io io = {
                &context,          outbox_crash_read,    outbox_crash_write,
                outbox_crash_sync, outbox_crash_reserve, NULL};
            outbox_marker_result result;
            p = visit_open(path, 0, sync, 0, &io, &allocator, &e);
            context.armed = 1;
            ok(outbox_marker_action(p, operation, &result, &e), &e);
            context.armed = 0;
            sites = context.calls;
            REQUIRE(sites);
            outbox_marker_result_clear(p, &result);
            lc_pouch_close(p);
          } else {
            REQUIRE(snprintf(operation_text, sizeof(operation_text), "%d",
                             operation) > 0);
            REQUIRE(snprintf(sync_text, sizeof(sync_text), "%d", sync) > 0);
            REQUIRE(snprintf(stop_text, sizeof(stop_text), "%zu", stop) > 0);
            REQUIRE(!setenv("POUCH_MARKER_CRASH_PATH", path, 1));
            REQUIRE(!setenv("POUCH_MARKER_CRASH_OPERATION", operation_text, 1));
            REQUIRE(!setenv("POUCH_MARKER_CRASH_SYNC", sync_text, 1));
            REQUIRE(!setenv("POUCH_MARKER_CRASH_STOP", stop_text, 1));
            REQUIRE(!setenv("POUCH_MARKER_CRASH_AFTER", after ? "1" : "0", 1));
            pid_t child = fork();
            REQUIRE(child >= 0);
            if (!child) {
              execl("/proc/self/exe", "pouch_space_client_test", directory,
                    "outbox-marker-crash-child", (char *)NULL);
              _exit(74);
            }
            int status;
            REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) &&
                    WEXITSTATUS(status) == 73);
            ++deaths;
          }
          p = visit_open(path, 0, sync, 0, NULL, &allocator, &e);
          outbox_marker_verify(p, operation, stop == 0U, &e);
          lc_pouch_close(p);
          REQUIRE(!live && !unlink(path));
        }
    }
  REQUIRE(!unsetenv("POUCH_MARKER_CRASH_PATH"));
  REQUIRE(!unsetenv("POUCH_MARKER_CRASH_OPERATION"));
  REQUIRE(!unsetenv("POUCH_MARKER_CRASH_SYNC"));
  REQUIRE(!unsetenv("POUCH_MARKER_CRASH_STOP"));
  REQUIRE(!unsetenv("POUCH_MARKER_CRASH_AFTER"));
  lc_error_cleanup(&e);
  printf("outbox marker process deaths=%zu passed\n", deaths);
}
#endif
