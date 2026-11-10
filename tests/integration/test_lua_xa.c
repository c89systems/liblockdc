#include "lockdc_lua_internal.h"
#ifdef LOCKDC_LUA_COLLECTION_MAINTENANCE
#include "lc_collection_internal.h"
#endif
#include <lauxlib.h>
#include <lualib.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int allocated, freed;

/* The production converter backs collection positions and XA borrow identity.
 * Exercise its full unsigned range without manufacturing native transactions.
 */
static int integer_fields(lua_State *L) {
  lua_newtable(L);
  lcdc_set_u64_decimal(L, "zero", 0U);
  lcdc_set_u64_decimal(L, "above_exact", ((uint64_t)1U << 53U) + 1U);
  lcdc_set_u64_decimal(L, "maximum", (uint64_t)-1);
  return 1;
}

/* Drive the real shared maintenance pass explicitly; Lua scenarios need no
 * sleeps or shortened production worker cadence to observe lazy retention. */
#ifdef LOCKDC_LUA_COLLECTION_MAINTENANCE
static int maintain_collections(lua_State *L) {
  lc_client *client = NULL;
  lc_error error;
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result result = {0};
  int rc;
  lc_error_init(&error);
  options.ns = "journal";
  options.force = lua_toboolean(L, 2);
  rc = lc_lua_client_view(L, 1, &client, &error);
  if (rc == LC_OK && ((lc_client_handle *)client)->is_pouch)
    rc = lc_pouch_maintenance_run(((lc_client_handle *)client)->pouch, &options,
                                  &result, &error);
  else if (rc == LC_OK)
    rc = LC_ERR_INVALID;
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  lc_error_cleanup(&error);
  lua_pushboolean(L, rc == LC_OK);
  return 1;
}
#endif
typedef struct lua_heap_probe {
  lua_Alloc allocate;
  void *context;
  unsigned int failures;
  size_t limit;
} lua_heap_probe;

static void *probe_allocate(void *context, void *pointer, size_t old_size,
                            size_t new_size) {
  lua_heap_probe *probe = context;
  if (new_size != 0 && (pointer == NULL || new_size > old_size) &&
      probe->failures != 0) {
    --probe->failures;
    return NULL;
  }
  if (probe->limit != 0 && new_size > probe->limit &&
      (pointer == NULL || new_size > old_size))
    return NULL;
  return probe->allocate(probe->context, pointer, old_size, new_size);
}

static int heap_fail(lua_State *L) {
  void *context;
  (void)lua_getallocf(L, &context);
  lua_heap_probe *probe = context;
  lua_Integer failures = luaL_checkinteger(L, 1);
  if (failures < 0 || failures > 100)
    return luaL_error(L, "invalid test allocation failure count");
  probe->failures = (unsigned int)failures;
  return 0;
}

#ifdef LOCKDC_LUA_COLLECTION_MAINTENANCE
static int collection_result_oom(void *context, lc_collection *collection,
                                 int phase, lc_error *error) {
  lua_State *L = context;
  void *allocator_context;
  lua_heap_probe *probe;
  (void)collection;
  (void)error;
  if (phase == LC_COLLECTION_APPEND_PROJECTED) {
    (void)lua_getallocf(L, &allocator_context);
    probe = allocator_context;
    probe->failures = 2;
    lc_collection_append_test_hook = NULL;
    lc_collection_append_test_context = NULL;
  }
  return LC_OK;
}

static int edit_result_oom(void *context, lc_collection *collection, int phase,
                           lc_error *error) {
  if (phase != LC_COLLECTION_EDIT_PROJECTED)
    return LC_OK;
  lc_collection_edit_test_hook = NULL;
  lc_collection_edit_test_context = NULL;
  return collection_result_oom(context, collection,
                               LC_COLLECTION_APPEND_PROJECTED, error);
}
static int fail_collection_result(lua_State *L) {
  if (lua_toboolean(L, 1)) {
    lc_collection_edit_test_hook = edit_result_oom;
    lc_collection_edit_test_context = L;
    return 0;
  }
  lc_collection_append_test_hook = collection_result_oom;
  lc_collection_append_test_context = L;
  return 0;
}
#endif

static int heap_limit(lua_State *L) {
  void *context;
  (void)lua_getallocf(L, &context);
  lua_heap_probe *probe = context;
  lua_Integer limit = luaL_checkinteger(L, 1);
  if (limit < 0 || (uintmax_t)limit > (uintmax_t)(size_t)-1)
    return luaL_error(L, "invalid test allocation ceiling");
  probe->limit = (size_t)limit;
  return 0;
}
static int invalid_borrow(lua_State *L) {
  lc_xa_borrow *borrow = NULL;
  lc_error error;
  int rc;
  lc_error_init(&error);
  rc = lc_lua_xa_borrow_retain(L, -1, &borrow, &error);
  lc_error_cleanup(&error);
  lua_pushboolean(L, rc == LC_ERR_INVALID && borrow == NULL);
  return 1;
}
static int traceback(lua_State *L) {
  const char *message;
  if (lua_type(L, 1) == LUA_TTABLE)
    lua_getfield(L, 1, "message");
  else
    lua_pushvalue(L, 1);
  message = lua_tostring(L, -1);
  luaL_traceback(L, L, message != NULL ? message : "non-string Lua error", 1);
  return 1;
}
static int configure(lua_State *L, int index, void **out, lc_error *error) {
  (void)error;
  *out = malloc(8U);
  if (*out == NULL)
    return LC_ERR_NOMEM;
  ++allocated;
  /* Deliberately permit an __index exception after allocation: the public
   * interop contract promises protected cleanup of this partial result. */
  lua_getfield(L, index, "probe");
  lua_pop(L, 1);
  return LC_OK;
}
static void cleanup(void *configuration) {
  ++freed;
  free(configuration);
}
static int counters(lua_State *L) {
  lua_pushinteger(L, allocated);
  lua_pushinteger(L, freed);
  return 2;
}
static int wait_ready(lua_State *L) {
  struct pollfd fd;
  struct timespec now;
  lua_Integer deadline;
  int timeout = 1000;
  unsigned int events;
  lua_getfield(L, 1, "fd");
  fd.fd = (int)luaL_checkinteger(L, -1);
  lua_pop(L, 1);
  lua_getfield(L, 1, "events");
  events = (unsigned int)luaL_checkinteger(L, -1);
  lua_pop(L, 1);
  fd.events = (short)((events & 1U ? POLLIN : 0) | (events & 2U ? POLLOUT : 0));
  fd.revents = 0;
  lua_getfield(L, 1, "deadline_ms");
  deadline = luaL_checkinteger(L, -1);
  lua_pop(L, 1);
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    return luaL_error(L, "test clock failed");
  if (deadline != -1) {
    lua_Integer remaining =
        deadline - ((lua_Integer)now.tv_sec * 1000 + now.tv_nsec / 1000000);
    if (remaining < timeout)
      timeout = remaining <= 0 ? 0 : (int)remaining;
  }
  if (poll(&fd, 1U, timeout) < 0)
    return luaL_error(L, "test readiness poll failed");
  return 0;
}
int lc_test_lua_worker_stack(void);

int main(int argc, char **argv) {
  lua_State *L;
  lua_heap_probe heap = {0};
  lc_xa_driver driver;
  lc_error error;
  int rc;
  if (argc == 2 && strcmp(argv[1], "--stack") == 0)
    return lc_test_lua_worker_stack();
  if (argc != 3)
    return 2;
  L = luaL_newstate();
  if (L == NULL)
    return 2;
  heap.allocate = lua_getallocf(L, &heap.context);
  lua_setallocf(L, probe_allocate, &heap);
  luaL_openlibs(L);
  luaL_requiref(L, "lockdc.core", luaopen_lockdc_core, 0);
  lua_pop(L, 1);
  lc_error_init(&error);
  memset(&driver, 0, sizeof(driver));
  driver.size = sizeof(driver);
  driver.abi_version = LC_XA_DRIVER_ABI;
  /* ABI-compatible but incomplete: core must reject this table after the
   * facade successfully converts/releases configuration, before native work. */
  rc = lc_lua_xa_register_driver(L, "test-rejected", &driver, configure,
                                 cleanup, &error);
  if (rc != LC_OK) {
    lua_close(L);
    lc_error_cleanup(&error);
    return 2;
  }
  lc_error_cleanup(&error);
  lua_pushstring(L, argv[2]);
  lua_setglobal(L, "lockdc_test_root");
  lua_pushcfunction(L, wait_ready);
  lua_setglobal(L, "lockdc_test_wait");
  lua_pushcfunction(L, counters);
  lua_setglobal(L, "lockdc_test_counters");
  lua_pushcfunction(L, invalid_borrow);
  lua_setglobal(L, "lockdc_test_invalid_borrow");
  lua_pushcfunction(L, integer_fields);
  lua_setglobal(L, "lockdc_test_integer_fields");
#ifdef LOCKDC_LUA_COLLECTION_MAINTENANCE
  lua_pushcfunction(L, maintain_collections);
  lua_setglobal(L, "lockdc_test_maintain_collections");
  lua_pushcfunction(L, fail_collection_result);
  lua_setglobal(L, "lockdc_test_fail_collection_result");
#endif
  lua_pushcfunction(L, heap_fail);
  lua_setglobal(L, "lockdc_test_lua_heap_fail");
  lua_pushcfunction(L, heap_limit);
  lua_setglobal(L, "lockdc_test_lua_heap_limit");
  lua_pushcfunction(L, traceback);
  rc = luaL_loadfile(L, argv[1]);
  if (rc == LUA_OK)
    rc = lua_pcall(L, 0, LUA_MULTRET, 1);
  if (rc != LUA_OK)
    fprintf(stderr, "Lua XA test failed: %s\n", lua_tostring(L, -1));
  lua_close(L);
  return rc == LUA_OK && allocated == freed ? 0 : 1;
}
