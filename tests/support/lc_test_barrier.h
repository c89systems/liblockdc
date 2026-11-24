#ifndef LC_TEST_BARRIER_H
#define LC_TEST_BARRIER_H

#include <pthread.h>

#if defined(__APPLE__)
#include <errno.h>
#include <stddef.h>

/* Darwin lacks pthread barriers. The tests use process-local barriers,
 * with each generation distinguished across spurious and late wakeups. */
typedef struct lc_test_barrier {
  pthread_mutex_t mutex;
  pthread_cond_t ready;
  unsigned count;
  unsigned remaining;
  unsigned generation;
} pthread_barrier_t;

#define PTHREAD_BARRIER_SERIAL_THREAD (-1)

static int pthread_barrier_init(pthread_barrier_t *barrier,
                                const void *attributes, unsigned count) {
  if (attributes != NULL || count == 0)
    return EINVAL;
  int rc = pthread_mutex_init(&barrier->mutex, NULL);
  if (rc != 0)
    return rc;
  rc = pthread_cond_init(&barrier->ready, NULL);
  if (rc != 0) {
    pthread_mutex_destroy(&barrier->mutex);
    return rc;
  }
  barrier->count = count;
  barrier->remaining = count;
  barrier->generation = 0;
  return 0;
}

static int pthread_barrier_wait(pthread_barrier_t *barrier) {
  int rc = pthread_mutex_lock(&barrier->mutex);
  if (rc != 0)
    return rc;
  unsigned generation = barrier->generation;
  if (--barrier->remaining == 0) {
    barrier->remaining = barrier->count;
    ++barrier->generation;
    rc = pthread_cond_broadcast(&barrier->ready);
    if (rc == 0)
      rc = PTHREAD_BARRIER_SERIAL_THREAD;
  } else {
    while (barrier->generation == generation && rc == 0)
      rc = pthread_cond_wait(&barrier->ready, &barrier->mutex);
  }
  pthread_mutex_unlock(&barrier->mutex);
  return rc;
}

static int pthread_barrier_destroy(pthread_barrier_t *barrier) {
  int rc = pthread_cond_destroy(&barrier->ready);
  return rc != 0 ? rc : pthread_mutex_destroy(&barrier->mutex);
}
#endif

#endif
