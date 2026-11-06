#include "lc_allocation_tracking.h"
#include "lc_api_internal.h"
#include "lc_internal.h"
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Selected once at startup; the plain path points directly to libc. */
void *(*lc_builtin_malloc)(size_t) = malloc;
void *(*lc_builtin_realloc)(void *, size_t) = realloc;
void (*lc_builtin_free)(void *) = free;
static int lc_allocation_configured;

#define LC_ALLOCATION_COUNTERS 64U
typedef union lc_allocation_counter {
  struct {
    unsigned char lock;
    size_t count;
    size_t bytes;
  } value;
  unsigned char separation[128];
} lc_allocation_counter;
typedef union lc_allocation_header {
  struct {
    size_t size;
    lc_allocation_counter *counter;
  } value;
  long double alignment;
  void *pointer_alignment;
} lc_allocation_header;

/* Preserve malloc alignment and expose payload underruns to sanitizers. */
static void lc_allocation_header_hide(lc_allocation_header *header) {
#ifdef LC_ALLOCATION_ASAN
  __asan_poison_memory_region(header, sizeof(*header));
#else
  (void)header;
#endif
}

static lc_allocation_header *lc_allocation_header_get(const void *pointer) {
  lc_allocation_header *header = (lc_allocation_header *)pointer - 1;
#ifdef LC_ALLOCATION_ASAN
  __asan_unpoison_memory_region(header, sizeof(*header));
#endif
  return header;
}
static lc_allocation_counter lc_allocation_counters[LC_ALLOCATION_COUNTERS];
static int lc_allocation_ready;
static void lc_tracking_free(void *pointer);

static void lc_allocation_counter_lock(lc_allocation_counter *counter) {
  /* Hold only while updating two integers; callbacks execute outside the lock.
   */
  while (__atomic_test_and_set(&counter->value.lock, __ATOMIC_ACQUIRE))
    sched_yield();
}
static void lc_allocation_counter_unlock(lc_allocation_counter *counter) {
  __atomic_clear(&counter->value.lock, __ATOMIC_RELEASE);
}

static void lc_allocation_lock(void) {
  size_t i;
  for (i = 0; i < LC_ALLOCATION_COUNTERS; ++i)
    lc_allocation_counter_lock(&lc_allocation_counters[i]);
}
static void lc_allocation_unlock(void) {
  size_t i = LC_ALLOCATION_COUNTERS;
  while (i != 0U)
    lc_allocation_counter_unlock(&lc_allocation_counters[--i]);
}
static lc_allocation_counter *lc_allocation_counter_for_thread(void) {
  uintptr_t value = (uintptr_t)pthread_self();
  value ^= value >> 16U;
  value ^= value >> 8U;
  return &lc_allocation_counters[value % LC_ALLOCATION_COUNTERS];
}
/* Counters outlive worker threads. Retaining no payload pointers also leaves
 * leaked blocks visible to external reachability-based memory checkers. */
/* Report after SDK global/thread-runtime owners have released their state. */
static void lc_allocation_report(void) __attribute__((destructor(101)));
static void lc_allocation_report(void) {
  size_t i;
  size_t count = 0;
  size_t bytes = 0;
  if (!lc_allocation_ready)
    return;
  lc_allocation_lock();
  for (i = 0; i < LC_ALLOCATION_COUNTERS; ++i) {
    count += lc_allocation_counters[i].value.count;
    bytes += lc_allocation_counters[i].value.bytes;
  }
  lc_allocation_unlock();
  if (count != 0U || bytes != 0U)
    fprintf(stderr,
            "liblockdc allocator: %lu live allocations (%lu bytes) at exit\n",
            (unsigned long)count, (unsigned long)bytes);
}
static void *lc_tracking_malloc(size_t size) {
  lc_allocation_header *header;
  lc_allocation_counter *counter;
  if (size > (size_t)-1 - sizeof(*header))
    return NULL;
  header = malloc(size + sizeof(*header));
  if (!header)
    return NULL;
  counter = lc_allocation_counter_for_thread();
  header->value.size = size;
  header->value.counter = counter;
  lc_allocation_counter_lock(counter);
  ++counter->value.count;
  counter->value.bytes += size;
  lc_allocation_counter_unlock(counter);
  lc_allocation_header_hide(header);
  return header + 1;
}
static void *lc_tracking_realloc(void *pointer, size_t size) {
  lc_allocation_header *header;
  lc_allocation_header *resized;
  lc_allocation_counter *counter;
  size_t previous;
  int fresh = pointer == NULL;
  if (size == 0U) {
    lc_tracking_free(pointer);
    return NULL;
  }
  if (size > (size_t)-1 - sizeof(*header))
    return NULL;
  header = pointer ? lc_allocation_header_get(pointer) : NULL;
  counter = header ? header->value.counter : lc_allocation_counter_for_thread();
  previous = header ? header->value.size : 0U;
  resized = realloc(header, size + sizeof(*header));
  if (!resized) {
    if (header != NULL)
      lc_allocation_header_hide(header);
    return NULL;
  }
  resized->value.size = size;
  resized->value.counter = counter;
  lc_allocation_counter_lock(counter);
  if (fresh)
    ++counter->value.count;
  counter->value.bytes -= previous;
  counter->value.bytes += size;
  lc_allocation_counter_unlock(counter);
  lc_allocation_header_hide(resized);
  return resized + 1;
}
static void lc_tracking_free(void *pointer) {
  lc_allocation_header *header;
  lc_allocation_counter *counter;
  if (!pointer)
    return;
  header = lc_allocation_header_get(pointer);
  counter = header->value.counter;
  lc_allocation_counter_lock(counter);
  --counter->value.count;
  counter->value.bytes -= header->value.size;
  lc_allocation_counter_unlock(counter);
  free(header);
}

int lc_allocator_tracking_configure(int enabled, lc_error *error) {
  int rc;
  if ((enabled != 0 && enabled != 1) || lc_allocation_configured)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "allocator tracking requires 0 or 1, once at process startup", NULL,
        NULL, NULL);
  if (enabled) {
    rc = pthread_atfork(lc_allocation_lock, lc_allocation_unlock,
                        lc_allocation_unlock);
    if (rc != 0)
      return lc_error_set(error, LC_ERR_TRANSPORT, (long)rc,
                          "cannot initialize allocator tracking", strerror(rc),
                          NULL, NULL);
    lc_builtin_malloc = lc_tracking_malloc;
    lc_builtin_realloc = lc_tracking_realloc;
    lc_builtin_free = lc_tracking_free;
    lc_allocation_ready = 1;
  }
  lc_allocation_configured = 1;
  return LC_OK;
}

/* Only custom malloc/free pairs without realloc need size metadata. No custom
 * allocation participates in tracking, irrespective of process settings. */
void *lc_sized_malloc(void *(*allocate)(void *, size_t), void *context,
                      size_t size) {
  lc_allocation_header *header;
  if (size > (size_t)-1 - sizeof(*header))
    return NULL;
  header = allocate(context, size + sizeof(*header));
  if (header == NULL)
    return NULL;
  header->value.size = size;
  header->value.counter = NULL;
  lc_allocation_header_hide(header);
  return header + 1;
}
void lc_sized_free(void (*release)(void *, void *), void *context,
                   void *pointer) {
  lc_allocation_header *header;
  if (pointer == NULL)
    return;
  header = lc_allocation_header_get(pointer);
  if (release != NULL)
    release(context, header);
  else
    free(header);
}
void *lc_sized_realloc(void *(*allocate)(void *, size_t),
                       void (*release)(void *, void *), void *context,
                       void *pointer, size_t size) {
  void *resized;
  lc_allocation_header *header;
  size_t previous;
  if (size == 0U) {
    lc_sized_free(release, context, pointer);
    return NULL;
  }
  resized = lc_sized_malloc(allocate, context, size);
  if (resized != NULL && pointer != NULL) {
    header = lc_allocation_header_get(pointer);
    previous = header->value.size;
    memcpy(resized, pointer, previous < size ? previous : size);
    lc_sized_free(release, context, pointer);
  }
  return resized;
}
static void *lc_engine_default_malloc(void *context, size_t size) {
  (void)context;
  return lc_builtin_malloc(size);
}

static void *lc_engine_default_realloc(void *context, void *ptr, size_t size) {
  (void)context;
  return lc_builtin_realloc(ptr, size);
}

static void lc_engine_default_free(void *context, void *ptr) {
  (void)context;
  lc_builtin_free(ptr);
}

int lc_engine_client_set_allocator(lc_engine_client *client,
                                   const lc_engine_allocator *allocator,
                                   lc_engine_error *error) {
  if (client == NULL || allocator == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "set_allocator requires client, allocator, and error");
  }
  if (allocator->malloc_fn == NULL || allocator->free_fn == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "allocator requires malloc_fn and free_fn");
  }
  client->allocator = *allocator;
  return LC_ENGINE_OK;
}

int lc_engine_client_get_allocator(lc_engine_client *client,
                                   lc_engine_allocator *allocator,
                                   lc_engine_error *error) {
  if (client == NULL || allocator == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_allocator requires client, allocator, and error");
  }
  *allocator = client->allocator;
  if (allocator->malloc_fn == NULL) {
    allocator->malloc_fn = lc_engine_default_malloc;
    allocator->realloc_fn = lc_engine_default_realloc;
    allocator->free_fn = lc_engine_default_free;
    allocator->context = NULL;
  }
  return LC_ENGINE_OK;
}

void *lc_engine_client_alloc(lc_engine_client *client, size_t size) {
  if (client != NULL && client->allocator.malloc_fn != NULL) {
    return client->allocator.malloc_fn(client->allocator.context, size);
  }
  return lc_engine_default_malloc(NULL, size);
}

void lc_engine_client_free_alloc(lc_engine_client *client, void *ptr) {
  if (ptr == NULL) {
    return;
  }
  if (client != NULL && client->allocator.free_fn != NULL) {
    client->allocator.free_fn(client->allocator.context, ptr);
    return;
  }
  lc_engine_default_free(NULL, ptr);
}

char *lc_engine_client_strdup(lc_engine_client *client, const char *value) {
  char *copy;
  size_t length;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)lc_engine_client_alloc(client, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_engine_client_strdup_range(lc_engine_client *client, const char *begin,
                                    const char *end) {
  char *copy;
  size_t length;

  if (begin == NULL || end == NULL || end < begin) {
    return NULL;
  }
  length = (size_t)(end - begin);
  copy = (char *)lc_engine_client_alloc(client, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, begin, length);
  }
  copy[length] = '\0';
  return copy;
}

void lc_engine_query_stream_response_cleanup(
    lc_engine_client *client, lc_engine_query_stream_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_client_free_alloc(client, response->cursor);
  response->cursor = NULL;
  lc_engine_client_free_alloc(client, response->correlation_id);
  response->correlation_id = NULL;
  lc_engine_client_free_alloc(client, response->metadata_json);
  response->metadata_json = NULL;
  lc_engine_client_free_alloc(client, response->return_mode);
  response->return_mode = NULL;
  response->index_seq = 0UL;
  response->index_seq_present = 0;
  response->http_status = 0L;
}
