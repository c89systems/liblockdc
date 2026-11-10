#define _GNU_SOURCE 1
#include "lockdc_lua_internal.h"
#include <lauxlib.h>
#include <lualib.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      abort();                                                                 \
    }                                                                          \
  } while (0)

typedef struct worker_state {
  lua_State *L;
  int migrated;
  int ready, done;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  uintptr_t markers[2];
} worker_state;

/* Each small trusted callback re-enters Lua through its native call boundary.
 * The runtime must reject deeper entry before consuming the worker's guard. */
static int recurse(lua_State *L) {
  lua_Integer depth = luaL_checkinteger(L, 1);
  if (depth == 0) {
    lua_pushinteger(L, 42);
    return 1;
  }
  lua_pushcfunction(L, recurse);
  lua_pushinteger(L, depth - 1);
  lua_call(L, 1, 1);
  return 1;
}

static void run(lua_State *L, const char *script) {
  int status = luaL_dostring(L, script);
  if (status != LUA_OK)
    fprintf(stderr, "Lua worker: %s\n", lua_tostring(L, -1));
  CHECK(status == LUA_OK);
  CHECK(lua_gettop(L) == 0);
}

static void *worker(void *context) {
  worker_state *state = context;
  lua_State *L = state->L;
  int migrated = state->migrated;
  unsigned char marker;
  char *nested;
  size_t i;
  int status;
  state->markers[migrated] = (uintptr_t)&marker;
  if (L == NULL) {
    L = luaL_newstate();
    CHECK(L != NULL);
    state->L = L;
    luaL_openlibs(L);
    luaL_requiref(L, "lockdc.core", luaopen_lockdc_core, 0);
    lua_pop(L, 1);
    lua_pushcfunction(L, recurse);
    lua_setglobal(L, "native_recurse");
  }
  run(L, "assert(native_recurse(4) == 42)");
  run(L, "local ok,e=pcall(native_recurse,4096); "
         "assert(not ok and type(e)=='string' and e:match('stack'))");
  run(L, "local ok,e=xpcall(function() return native_recurse(4096) end, "
         "function() return native_recurse(4096) end); "
         "assert(not ok and type(e)=='string')");
  /* A small generated input reaches parser nesting paths, rather than merely
   * exercising executable Lua recursion. Failure must remain protected. */
  nested = malloc(2064);
  CHECK(nested != NULL);
  memcpy(nested, "return ", 7);
  for (i = 0; i < 1024; ++i)
    nested[7 + i] = '(';
  nested[1031] = '1';
  for (i = 0; i < 1024; ++i)
    nested[1032 + i] = ')';
  status = luaL_loadbuffer(L, nested, 2056, "=worker-nesting");
  free(nested);
  CHECK(status != LUA_OK && lua_isstring(L, -1));
  lua_pop(L, 1);
  run(L, "local c=require('lockdc.core'); "
         "assert(type(c.version_string())=='string'); "
         "assert(#assert(c.xid_new())==20)");
  if (!migrated) {
    run(L, "co=coroutine.create(function() coroutine.yield('ready'); "
           "return native_recurse(4096) end); "
           "local ok,v=coroutine.resume(co); assert(ok and v=='ready')");
    /* Keep this worker alive while ownership moves. Otherwise pthreads/OS
     * mapping reuse can hide a stale cached stack bound on the next worker. */
    CHECK(pthread_mutex_lock(&state->mutex) == 0);
    state->ready = 1;
    CHECK(pthread_cond_signal(&state->condition) == 0);
    while (!state->done)
      CHECK(pthread_cond_wait(&state->condition, &state->mutex) == 0);
    CHECK(pthread_mutex_unlock(&state->mutex) == 0);
  } else {
    run(L, "local ok,e=coroutine.resume(co); "
           "assert(not ok and type(e)=='string' and e:match('stack')); "
           "co=nil; collectgarbage('collect'); "
           "assert(native_recurse(4)==42)");
    lua_close(L);
    state->L = NULL;
    CHECK(pthread_mutex_lock(&state->mutex) == 0);
    state->done = 1;
    CHECK(pthread_cond_signal(&state->condition) == 0);
    CHECK(pthread_mutex_unlock(&state->mutex) == 0);
  }
  return NULL;
}

int lc_test_lua_worker_stack(void) {
  worker_state state = {0};
  pthread_t first, second;
  lc_error error;
  lc_error_init(&error);
  CHECK(lc_allocator_tracking_configure(1, &error) == LC_OK);
  CHECK(pthread_mutex_init(&state.mutex, NULL) == 0);
  CHECK(pthread_cond_init(&state.condition, NULL) == 0);
  CHECK(pthread_create(&first, NULL, worker, &state) == 0);
  CHECK(pthread_mutex_lock(&state.mutex) == 0);
  while (!state.ready)
    CHECK(pthread_cond_wait(&state.condition, &state.mutex) == 0);
  CHECK(state.L != NULL);
  state.migrated = 1;
  CHECK(pthread_mutex_unlock(&state.mutex) == 0);
  CHECK(pthread_create(&second, NULL, worker, &state) == 0);
  CHECK(pthread_join(second, NULL) == 0);
  CHECK(pthread_join(first, NULL) == 0);
  CHECK(state.L == NULL);
  CHECK(state.markers[0] != state.markers[1]);
  CHECK(pthread_cond_destroy(&state.condition) == 0);
  CHECK(pthread_mutex_destroy(&state.mutex) == 0);
  lc_error_cleanup(&error);
  puts("Lua default-worker recursion, recovery and migration passed");
  return 0;
}
