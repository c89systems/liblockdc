/* Test processes opt in through CTest. This is never part of Release SDKs. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void __attribute__((constructor)) lc_test_stack_defaults(void) {
  pthread_attr_t attr;
  const char *setting;
  size_t guard;
  int rc;
  setting = getenv("LOCKDC_TEST_STACK_BYTES");
  if (setting == NULL || strcmp(setting, "131072") != 0)
    return;
  rc = pthread_getattr_default_np(&attr);
  if (rc == 0) {
    rc = pthread_attr_getguardsize(&attr, &guard);
    if (rc == 0 && guard == 0U)
      rc = pthread_attr_setguardsize(&attr, (size_t)sysconf(_SC_PAGESIZE));
    if (rc == 0)
      rc = pthread_attr_setstacksize(&attr, 128U * 1024U);
    if (rc == 0)
      rc = pthread_setattr_default_np(&attr);
    (void)pthread_attr_destroy(&attr);
  }
  if (rc != 0) {
    fprintf(stderr, "liblockdc test worker-stack setup failed: %s\n",
            strerror(rc));
    abort();
  }
}
