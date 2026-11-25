#ifndef POUCH_SPACE_FIXTURE_DIRECTORY_SYNC_H
#define POUCH_SPACE_FIXTURE_DIRECTORY_SYNC_H

#include <sys/stat.h>

static int model_fixture_directory_sync;

static void fixture_directory_sync_model(int enabled) {
  __atomic_store_n(&model_fixture_directory_sync, enabled, __ATOMIC_RELAXED);
}

static inline int fixture_complete_sync(void *context, int fd) {
  (void)context;
  (void)fd;
  return 0;
}

int __real_fsync(int);
int __wrap_fsync(int fd) {
  if (__atomic_load_n(&model_fixture_directory_sync, __ATOMIC_RELAXED)) {
    struct stat st;
    /* Simulated faults keep the fixture's directory entry. Native crash
     * tests and file barriers retain their actual system calls. */
    if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode))
      return 0;
  }
  return __real_fsync(fd);
}

#endif
