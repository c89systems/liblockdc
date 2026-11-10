#define _GNU_SOURCE 1
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef LC_TEST_STACK_FROM_SDK
#include "lc/lc.h"
#endif

#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
      abort();                                                                 \
    }                                                                          \
  } while (0)
typedef struct observed {
  size_t stack, guard, nested;
} observed;
static void measure(observed *out) {
  pthread_attr_t attr;
  void *base;
  CHECK(pthread_getattr_np(pthread_self(), &attr) == 0);
  CHECK(pthread_attr_getstack(&attr, &base, &out->stack) == 0);
  CHECK(pthread_attr_getguardsize(&attr, &out->guard) == 0);
  CHECK(pthread_attr_destroy(&attr) == 0);
}
static void *simple_worker(void *context) {
  measure(context);
  return NULL;
}
static void *nested_worker(void *context) {
  observed *out = context;
  observed nested = {0};
  pthread_t thread;
  measure(out);
  CHECK(pthread_create(&thread, NULL, simple_worker, &nested) == 0);
  CHECK(pthread_join(thread, NULL) == 0);
  out->nested = nested.stack;
  CHECK(nested.guard != 0);
  return NULL;
}
static void check_default(size_t size) {
#if defined(__GLIBC__)
  if (getenv("LOCKDC_TEST_STACK_BYTES") != NULL)
    CHECK(size == 128U * 1024U);
#else
  CHECK(size >= 128U * 1024U && size <= 144U * 1024U);
#endif
  CHECK(size != 0);
}
#if defined(__GNUC__) || defined(__clang__)
__attribute__((no_sanitize_address, no_sanitize_undefined))
#endif
static void *touch_guard(void *unused) {
  pthread_attr_t attr;
  void *base;
  size_t size, guard;
  volatile unsigned char byte;
  (void)unused;
  CHECK(pthread_getattr_np(pthread_self(), &attr) == 0);
  CHECK(pthread_attr_getstack(&attr, &base, &size) == 0);
  CHECK(pthread_attr_getguardsize(&attr, &guard) == 0 && guard != 0);
  CHECK(pthread_attr_destroy(&attr) == 0);
  byte = *((volatile unsigned char *)base - 1);
  (void)byte;
  _exit(10);
  return NULL;
}
static void guard_fault(void) {
  pid_t pid = fork();
  int status;
  CHECK(pid >= 0);
  if (pid == 0) {
    struct rlimit core = {0, 0};
    pthread_t thread;
    CHECK(setrlimit(RLIMIT_CORE, &core) == 0);
    signal(SIGSEGV, SIG_DFL);
    signal(SIGBUS, SIG_DFL);
    CHECK(pthread_create(&thread, NULL, touch_guard, NULL) == 0);
    CHECK(pthread_join(thread, NULL) == 0);
    _exit(11);
  }
  CHECK(waitpid(pid, &status, 0) == pid);
  CHECK(WIFSIGNALED(status));
  CHECK(WTERMSIG(status) == SIGSEGV || WTERMSIG(status) == SIGBUS);
}
int main(int argc, char **argv) {
  observed explicit_stack = {0}, defaults = {0}, warm = {0};
  pthread_attr_t attr;
  pthread_t thread;
  struct rlimit before, after;
  size_t page = (size_t)sysconf(_SC_PAGESIZE);
  int status;
  pid_t pid;
#ifdef LC_TEST_STACK_FROM_SDK
  /* Pull the required core member from a static SDK. No test initializer is
   * linked into this application; the SDK must provide its Debug policy. */
  lc_allocator allocator;
  lc_allocator_init(&allocator);
  CHECK(allocator.malloc_fn == NULL);
#endif
  CHECK(getrlimit(RLIMIT_STACK, &before) == 0);
  /* A 256 KiB cached stack is eligible for a 128 KiB request on glibc. Warm
   * it before any default workers exist, with the default guard size. */
  CHECK(pthread_attr_init(&attr) == 0);
  CHECK(pthread_attr_setstacksize(&attr, 256U * 1024U) == 0);
  CHECK(pthread_create(&thread, &attr, simple_worker, &warm) == 0);
  CHECK(pthread_attr_destroy(&attr) == 0);
  CHECK(pthread_join(thread, NULL) == 0);
  CHECK(warm.stack >= 256U * 1024U);
  /* Explicit requests remain untouched; their nested default worker must
   * receive the small stack rather than reusing the warmed larger stack. */
  CHECK(pthread_attr_init(&attr) == 0);
  CHECK(pthread_attr_setstacksize(&attr, 2U * 1024U * 1024U) == 0);
  CHECK(pthread_attr_setguardsize(&attr, 3U * page) == 0);
  CHECK(pthread_create(&thread, &attr, nested_worker, &explicit_stack) == 0);
  CHECK(pthread_attr_destroy(&attr) == 0);
  CHECK(pthread_join(thread, NULL) == 0);
  CHECK(explicit_stack.stack >= 2U * 1024U * 1024U);
  CHECK(explicit_stack.stack <= 2U * 1024U * 1024U + 16384U);
  CHECK(explicit_stack.guard == 3U * page);
  check_default(explicit_stack.nested);
  CHECK(pthread_create(&thread, NULL, nested_worker, &defaults) == 0);
  CHECK(pthread_join(thread, NULL) == 0);
  check_default(defaults.stack);
  check_default(defaults.nested);
  CHECK(defaults.guard != 0);
  guard_fault();
  if (argc == 1) {
    pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
      execl(argv[0], argv[0], "child", (char *)NULL);
      _exit(12);
    }
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
  CHECK(getrlimit(RLIMIT_STACK, &after) == 0);
  CHECK(before.rlim_cur == after.rlim_cur && before.rlim_max == after.rlim_max);
  printf("%s: default=%lu nested=%lu explicit=%lu guard=%lu\n",
         argc == 1 ? "parent" : "child", (unsigned long)defaults.stack,
         (unsigned long)defaults.nested, (unsigned long)explicit_stack.stack,
         (unsigned long)defaults.guard);
  return 0;
}
