#ifndef POUCH_SPACE_FIXTURE_BARRIERS_H
#define POUCH_SPACE_FIXTURE_BARRIERS_H

#include "fixture_directory_sync.h"

#ifdef __linux__
static int model_fixture_file_sync;
int __real_fdatasync(int);
int __wrap_fdatasync(int fd) {
  if (__atomic_load_n(&model_fixture_file_sync, __ATOMIC_RELAXED))
    return 0;
  return __real_fdatasync(fd);
}
#endif

static void fixture_barriers_model(int enabled) {
  fixture_directory_sync_model(enabled);
#ifdef __linux__
  __atomic_store_n(&model_fixture_file_sync, enabled, __ATOMIC_RELAXED);
#endif
}

#endif
