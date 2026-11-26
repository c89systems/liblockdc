#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_allocation_tracking.h"
#include "lc_api_internal.h"
#include "lc_internal.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_internal.h"

#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static int protected_header(const void *pointer) {
#ifdef LC_ALLOCATION_ASAN
  return __asan_address_is_poisoned((const unsigned char *)pointer - 1);
#else
  (void)pointer;
  return 1;
#endif
}

typedef union arena_slot {
  long double alignment;
  unsigned char bytes[1024];
} arena_slot;

struct alignment_probe {
  char first;
  long double value;
};

static int exercise_alignment(void) {
  unsigned int index;
  unsigned char *pointer;
  void *resized;

  for (index = 0U; index < 128U; ++index) {
    pointer = lc_builtin_malloc(index);
    if (pointer == NULL ||
        (uintptr_t)pointer % offsetof(struct alignment_probe, value) != 0U ||
        !protected_header(pointer))
      return 1;
    resized = lc_builtin_realloc(pointer, index + 256U);
    if (resized == NULL ||
        (uintptr_t)resized % offsetof(struct alignment_probe, value) != 0U ||
        !protected_header(resized))
      return 1;
    lc_builtin_free(resized);
  }
  return 0;
}

static int exercise_delegated_leak(void) {
  const char *endpoints[] = {"http://127.0.0.1:1"};
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_allocator allocator;
  lc_engine_error error;
  void *pointer;

  memset(&config, 0, sizeof(config));
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  lc_engine_error_init(&error);
  if (lc_engine_client_open(&config, &client, &error) != LC_ENGINE_OK)
    return 1;
  if (lc_engine_client_get_allocator(client, &allocator, &error) !=
      LC_ENGINE_OK)
    return 1;
  lc_engine_client_close(client);
  lc_engine_error_cleanup(&error);
  pointer = allocator.malloc_fn(allocator.context, 37U);
  if (pointer == NULL)
    return 1;
  return allocator.realloc_fn(allocator.context, pointer, 73U) == NULL;
}

typedef struct arena {
  arena_slot slots[2];
  int used[2];
  unsigned int allocations;
  unsigned int frees;
  int fail;
} arena;

static void *arena_malloc(void *context, size_t size) {
  arena *state = context;
  unsigned int index;

  ++state->allocations;
  if (state->fail || size > sizeof(state->slots[0].bytes))
    return NULL;
  for (index = 0U; index < 2U; ++index) {
    if (!state->used[index]) {
      state->used[index] = 1;
      return state->slots[index].bytes;
    }
  }
  return NULL;
}

static void arena_free(void *context, void *pointer) {
  arena *state = context;
  unsigned int index;

  for (index = 0U; index < 2U; ++index) {
    if (pointer == state->slots[index].bytes && state->used[index]) {
      state->used[index] = 0;
      ++state->frees;
      return;
    }
  }
  abort(); /* The callback must receive its original base, never the payload. */
}

static int exercise_arena(void) {
  arena state;
  lc_allocator allocator;
  unsigned char *pointer;
  unsigned char *resized;
  unsigned int index;
  unsigned int calls;
  unsigned char *stored;
  size_t stored_length;
  unsigned long stored_crc;
  char *descriptor;

  memset(&state, 0, sizeof(state));
  lc_allocator_init(&allocator);
  allocator.malloc_fn = arena_malloc;
  allocator.free_fn = arena_free;
  allocator.context = &state;
  pointer = lc_alloc_with_allocator(&allocator, 64U);
  if (pointer == NULL || !protected_header(pointer))
    return 1;
  for (index = 0U; index < 64U; ++index)
    pointer[index] = (unsigned char)index;
  state.fail = 1;
  if (lc_realloc_with_allocator(&allocator, pointer, 256U) != NULL)
    return 1;
  state.fail = 0;
  resized = lc_realloc_with_allocator(&allocator, pointer, 256U);
  if (resized == NULL || !protected_header(resized))
    return 1;
  pointer = resized;
  for (index = 0U; index < 64U; ++index)
    if (pointer[index] != (unsigned char)index)
      return 1;
  calls = state.allocations;
  if (lc_alloc_with_allocator(&allocator, (size_t)-1) != NULL ||
      lc_realloc_with_allocator(&allocator, pointer, (size_t)-1) != NULL ||
      state.allocations != calls)
    return 1;
  resized = lc_realloc_with_allocator(&allocator, pointer, 16U);
  if (resized == NULL || !protected_header(resized))
    return 1;
  pointer = resized;
  for (index = 0U; index < 16U; ++index)
    if (pointer[index] != (unsigned char)index)
      return 1;
  if (lc_realloc_with_allocator(&allocator, pointer, 0U) != NULL)
    return 1;
  if (state.used[0] || state.used[1] || state.frees != 3U)
    return 1;
  /* A disabled crypto object must still honor the output allocator. A raw
   * libc allocation cannot be released by this fixed-storage allocator. */
  if (lc_pouch_crypto_transform_memory(
          &allocator, NULL, NULL, (const unsigned char *)"payload", 7U, 0,
          &stored, &stored_length, &stored_crc, &descriptor, NULL) != LC_OK ||
      stored_length != 7U || descriptor != NULL ||
      memcmp(stored, "payload", 7U) != 0)
    return 1;
  lc_free_with_allocator(&allocator, stored);
  return state.used[0] || state.used[1] || state.frees != 4U;
}

static void *last_custom_pointer;
static size_t last_custom_size;

static void *custom_malloc(void *context, size_t size) {
  if (context != NULL)
    return NULL;
  last_custom_size = size;
  last_custom_pointer = malloc(size);
  return last_custom_pointer;
}

static void *custom_realloc(void *context, void *pointer, size_t size) {
  if (context != NULL)
    return NULL;
  last_custom_size = size;
  last_custom_pointer = realloc(pointer, size);
  return last_custom_pointer;
}

static void custom_free(void *context, void *pointer) {
  (void)context;
  free(pointer);
}

static int exercise_invalid_custom(void) {
  const char *endpoints[] = {"http://127.0.0.1:1"};
  lc_client_config config;
  lc_engine_client_config engine_config;
  lc_client *client;
  lc_engine_client *engine;
  lc_error error;
  lc_engine_error engine_error;
  unsigned int mask;

  for (mask = 1U; mask < 7U; ++mask) {
    if (mask == 3U) /* malloc/free without realloc is supported. */
      continue;
    client = NULL;
    engine = NULL;
    lc_error_init(&error);
    lc_engine_error_init(&engine_error);
    lc_client_config_init(&config);
    config.endpoints = endpoints;
    config.endpoint_count = 1U;
    config.disable_mtls = 1;
    config.allocator.malloc_fn = (mask & 1U) ? custom_malloc : NULL;
    config.allocator.free_fn = (mask & 2U) ? custom_free : NULL;
    config.allocator.realloc_fn = (mask & 4U) ? custom_realloc : NULL;
    last_custom_pointer = NULL;
    if (lc_client_open(&config, &client, &error) != LC_ERR_INVALID ||
        client != NULL || last_custom_pointer != NULL)
      return 1;
    memset(&engine_config, 0, sizeof(engine_config));
    engine_config.endpoints = endpoints;
    engine_config.endpoint_count = 1U;
    engine_config.disable_mtls = 1;
    engine_config.allocator.malloc_fn = config.allocator.malloc_fn;
    engine_config.allocator.realloc_fn = config.allocator.realloc_fn;
    engine_config.allocator.free_fn = config.allocator.free_fn;
    if (lc_engine_client_open(&engine_config, &engine, &engine_error) !=
            LC_ENGINE_ERROR_INVALID_ARGUMENT ||
        engine != NULL || last_custom_pointer != NULL)
      return 1;
    lc_error_cleanup(&error);
    lc_engine_error_cleanup(&engine_error);
  }
  return 0;
}

static void *exercise(void *context) {
  unsigned int index;
  void *pointer;
  void *resized;

  (void)context;
  for (index = 0U; index < 256U; ++index) {
    pointer = lc_builtin_malloc(17U);
    if (pointer == NULL)
      return (void *)(uintptr_t)1U;
    resized = lc_builtin_realloc(pointer, 33U);
    if (resized == NULL)
      return (void *)(uintptr_t)1U;
    pointer = resized;
    if (lc_builtin_realloc(pointer, (size_t)-1) != NULL)
      return (void *)(uintptr_t)1U;
    if (!protected_header(pointer))
      return (void *)(uintptr_t)1U;
    lc_builtin_free(pointer);
  }
  return NULL;
}

typedef struct handoff {
  void *pointers[256];
  unsigned int count;
  int *start;
} handoff;

static void *allocate_handoff(void *context) {
  handoff *state = context;
  unsigned int index;

  for (index = 0U; index < state->count; ++index) {
    state->pointers[index] = lc_builtin_malloc(37U);
    if (state->pointers[index] == NULL)
      return (void *)(uintptr_t)1U;
  }
  return NULL;
}

static void *release_shared_counter(void *context) {
  handoff *state = context;
  unsigned int index;
  void *resized;

  while (!__atomic_load_n(state->start, __ATOMIC_ACQUIRE))
    sched_yield();
  for (index = 0U; index < state->count; ++index) {
    resized = lc_builtin_realloc(state->pointers[index], 73U);
    if (resized == NULL)
      return (void *)(uintptr_t)1U;
    lc_builtin_free(resized);
  }
  return NULL;
}

static int exercise_shared_counter(void) {
  handoff states[4];
  pthread_t threads[4];
  unsigned int index;
  int start = 0;
  void *result;

  memset(states, 0, sizeof(states));
  for (index = 0U; index < 4U; ++index) {
    states[index].count = 256U;
    states[index].start = &start;
    /* Every header belongs to the main thread's counter, guaranteeing
     * contention when the four workers resize/free their independent blocks. */
    if (allocate_handoff(&states[index]) != NULL)
      return 1;
  }
  for (index = 0U; index < 4U; ++index)
    if (pthread_create(&threads[index], NULL, release_shared_counter,
                       &states[index]) != 0)
      return 1;
  __atomic_store_n(&start, 1, __ATOMIC_RELEASE);
  for (index = 0U; index < 4U; ++index)
    if (pthread_join(threads[index], &result) != 0 || result != NULL)
      return 1;
  return 0;
}

static int exercise_handoff(int leak) {
  handoff states[4];
  pthread_t threads[4];
  unsigned int index;
  unsigned int allocation;
  void *result;
  void *resized;
  pid_t child;
  int status;

  memset(states, 0, sizeof(states));
  for (index = 0U; index < 4U; ++index) {
    states[index].count = leak ? 1U : 256U;
    if (pthread_create(&threads[index], NULL, allocate_handoff,
                       &states[index]) != 0)
      return 1;
  }
  for (index = 0U; index < 4U; ++index) {
    if (pthread_join(threads[index], &result) != 0 || result != NULL)
      return 1;
  }
  if (leak == 1)
    return 0;
  if (leak == 2) {
    child = fork();
    if (child < 0)
      return 1;
    if (child == 0)
      exit(0); /* Report the four allocations inherited from terminated workers.
                */
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0)
      return 1;
  }
  /* Allocation and release need not occur on the same thread. Keep every
   * original pointer live until all allocating threads have terminated. */
  for (index = 0U; index < 4U; ++index) {
    for (allocation = 0U; allocation < states[index].count; ++allocation) {
      resized = lc_builtin_realloc(states[index].pointers[allocation], 73U);
      if (resized == NULL)
        return 1;
      lc_builtin_free(resized);
    }
  }
  return 0;
}

static int exercise_pouch(void) {
  char root[256];
  char template_path[] = "/tmp/liblockdc-allocator-exit-XXXXXX";
  char endpoint[384];
  const char *endpoints[1];
  lc_client_config config;
  lc_acquire_req request;
  lc_client *first;
  lc_client *second;
  lc_lease *lease;
  lc_pouch_state_shared_mutation_guard *guard;
  lc_error error;
  unsigned int index;

  if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                           "/tmp/liblockdc-allocator-exit-"))
    return 1;
  if (snprintf(endpoint, sizeof(endpoint),
               "pouch://%s/store?single_writer=false", root) < 0)
    return 1;
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  for (index = 0U; index < 8U; ++index) {
    if (lc_client_open(&config, &first, &error) != LC_OK ||
        lc_client_open(&config, &second, &error) != LC_OK)
      return 1;
    lc_acquire_req_init(&request);
    request.ns = "allocator-registry";
    request.key = "key";
    request.owner = "allocator-test";
    request.ttl_seconds = 30L;
    if (lc_acquire(second, &request, &lease, &error) != LC_OK)
      return 1;
    /* Closing a sibling must preserve this handle's active root authority. */
    if (lc_pouch_state_shared_mutation_enter(
            ((lc_client_handle *)second)->pouch, &guard, &error) != LC_OK)
      return 1;
    lc_client_close(first);
    lc_pouch_state_shared_mutation_leave(&guard);
    if (lc_lease_release(lease, NULL, &error) != LC_OK)
      return 1;
    lc_client_close(second);
  }
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, "/tmp/liblockdc-allocator-exit-");
  return 0;
}

int main(int argc, char **argv) {
  pthread_t threads[4];
  void *result;
  void *pointer;
  unsigned int index;
  int status;
  pid_t child;
  lc_attachment_info attachment;
  lc_attachment_list attachments;
  lc_tc_rm_list_res resources;
  lc_error error;
  int off;

  if (argc != 2)
    return 2;
  if (strcmp(argv[1], "unused-clean") == 0)
    return 0;
  lc_error_init(&error);
  off = strncmp(argv[1], "default-off-", 12U) == 0 ||
        strcmp(argv[1], "disabled-clean") == 0 ||
        strcmp(argv[1], "pouch-off-clean") == 0 ||
        strcmp(argv[1], "arena-off-clean") == 0 ||
        strcmp(argv[1], "invalid-custom-off-clean") == 0;
  if (lc_builtin_malloc != malloc || lc_builtin_realloc != realloc ||
      lc_builtin_free != free)
    return 1;
  if (strcmp(argv[1], "disabled-clean") == 0) {
    if (lc_allocator_tracking_configure(0, &error) != LC_OK ||
        lc_allocator_tracking_configure(1, &error) != LC_ERR_INVALID)
      return 1;
    lc_error_cleanup(&error);
  } else if (!off) {
    if (lc_allocator_tracking_configure(-1, &error) != LC_ERR_INVALID ||
        lc_allocator_tracking_configure(2, NULL) != LC_ERR_INVALID ||
        lc_allocator_tracking_configure(1, &error) != LC_OK ||
        lc_allocator_tracking_configure(0, &error) != LC_ERR_INVALID)
      return 1;
    lc_error_cleanup(&error);
    if (lc_builtin_malloc == malloc || lc_builtin_realloc == realloc ||
        lc_builtin_free == free)
      return 1;
  }
  if (off) {
    if (lc_builtin_malloc != malloc || lc_builtin_realloc != realloc ||
        lc_builtin_free != free)
      return 1;
    if (strcmp(argv[1], "arena-off-clean") == 0)
      return exercise_arena();
    if (strcmp(argv[1], "invalid-custom-off-clean") == 0)
      return exercise_invalid_custom();
    if (strcmp(argv[1], "pouch-off-clean") == 0)
      return exercise_pouch();
    pointer = lc_alloc_with_allocator(NULL, 37U);
    if (pointer == NULL)
      return 1;
    if (strcmp(argv[1], "default-off-leak") == 0)
      return 0;
    /* Disabled SDK allocations are ordinary libc pointers, with no prefix. */
    pointer = realloc(pointer, 73U);
    if (pointer == NULL)
      return 1;
    lc_free_with_allocator(NULL, pointer);
    return 0;
  }
  if (strcmp(argv[1], "arena-clean") == 0)
    return exercise_arena();
  if (strcmp(argv[1], "invalid-custom-clean") == 0)
    return exercise_invalid_custom();
  if (strcmp(argv[1], "alignment-clean") == 0)
    return exercise_alignment();
  if (strcmp(argv[1], "shared-counter-clean") == 0)
    return exercise_shared_counter();
  if (strcmp(argv[1], "delegated-leak") == 0)
    return exercise_delegated_leak();
  if (strcmp(argv[1], "fork-snapshot-leak") == 0)
    return exercise_handoff(2);
  if (strcmp(argv[1], "zero-leak") == 0)
    return lc_builtin_malloc(0U) == NULL ? 1 : 0;
  if (strcmp(argv[1], "raw-result-clean") == 0)
    goto raw_results;
  if (strcmp(argv[1], "handoff-clean") == 0)
    return exercise_handoff(0);
  if (strcmp(argv[1], "threaded-leak") == 0)
    return exercise_handoff(1);
  if (strcmp(argv[1], "pouch-clean") == 0)
    return exercise_pouch();
  if (strcmp(argv[1], "leak") == 0)
    return lc_builtin_malloc(37U) == NULL;
  if (strcmp(argv[1], "custom-leak") == 0 ||
      strcmp(argv[1], "custom-clean") == 0) {
    lc_allocator allocator;
    lc_allocator_init(&allocator);
    allocator.malloc_fn = custom_malloc;
    allocator.realloc_fn = custom_realloc;
    allocator.free_fn = custom_free;
    pointer = lc_alloc_with_allocator(&allocator, 37U);
    if (pointer == NULL || pointer != last_custom_pointer ||
        last_custom_size != 37U)
      return 1;
    if (strcmp(argv[1], "custom-leak") == 0)
      return 0;
    pointer = lc_realloc_with_allocator(&allocator, pointer, 73U);
    if (pointer == NULL || pointer != last_custom_pointer ||
        last_custom_size != 73U)
      return 1;
    lc_free_with_allocator(&allocator, pointer);
    return 0;
  }
  if (strcmp(argv[1], "raw-leak") == 0)
    return malloc(37U) == NULL;
  if (strcmp(argv[1], "failed-realloc-leak") == 0) {
    pointer = lc_builtin_malloc(37U);
    if (pointer == NULL)
      return 1;
    /* Valid arithmetic, but too large for the underlying allocator. This
     * exercises realloc failure, including re-poisoning the original header. */
    if (lc_builtin_realloc(pointer, (size_t)-1 - 1024U) != NULL ||
        !protected_header(pointer))
      return 1;
    return 0;
  }
  if (strcmp(argv[1], "resized-leak") == 0) {
    pointer = lc_builtin_malloc(37U);
    if (pointer == NULL)
      return 1;
    return lc_builtin_realloc(pointer, 73U) == NULL;
  }
  if (strcmp(argv[1], "clean") != 0)
    return 2;
  if (lc_builtin_malloc((size_t)-1) != NULL)
    return 1;
  pointer = lc_builtin_realloc(NULL, 9U);
  if (pointer == NULL)
    return 1;
  pointer = lc_builtin_realloc(pointer, 0U);
  lc_builtin_free(pointer);
  lc_builtin_free(NULL);
  for (index = 0U; index < 4U; ++index) {
    if (pthread_create(&threads[index], NULL, exercise, NULL) != 0)
      return 1;
  }
  child = fork();
  if (child < 0)
    return 1;
  if (child == 0)
    _exit(exercise(NULL) == NULL ? 0 : 1);
  if (waitpid(child, &status, 0) != child || !WIFEXITED(status) ||
      WEXITSTATUS(status))
    return 1;
  for (index = 0U; index < 4U; ++index) {
    if (pthread_join(threads[index], &result) != 0 || result != NULL)
      return 1;
  }
raw_results:
  memset(&attachment, 0, sizeof(attachment));
  attachment.id = malloc(37U);
  if (attachment.id == NULL)
    return 1;
  memset(&attachments, 0, sizeof(attachments));
  attachments.items = malloc(sizeof(*attachments.items));
  if (attachments.items == NULL)
    return 1;
  memset(attachments.items, 0, sizeof(*attachments.items));
  attachments.count = 1U;
  memset(&resources, 0, sizeof(resources));
  resources.backends = malloc(sizeof(*resources.backends));
  if (resources.backends == NULL)
    return 1;
  memset(resources.backends, 0, sizeof(*resources.backends));
  resources.backend_count = 1U;
  resources.backends[0].backend_hash = malloc(37U);
  resources.backends[0].endpoints.items = malloc(sizeof(char *));
  if (resources.backends[0].backend_hash == NULL ||
      resources.backends[0].endpoints.items == NULL)
    return 1;
  resources.backends[0].endpoints.items[0] = malloc(37U);
  if (resources.backends[0].endpoints.items[0] == NULL)
    return 1;
  resources.backends[0].endpoints.count = 1U;
  /* Public response buffers retain their raw allocation/cleanup contract. */
  lc_attachment_info_cleanup(&attachment);
  lc_attachment_list_cleanup(&attachments);
  lc_tc_rm_list_res_cleanup(&resources);
  return 0;
}
