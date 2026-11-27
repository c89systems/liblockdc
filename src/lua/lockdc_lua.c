#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <lauxlib.h>
#include <lua.h>

#include "lockdc_lua_internal.h"
#include <lc/lc.h>

#define LCDC_CLIENT_MT "lockdc.client"
#define LCDC_LEASE_MT "lockdc.lease"
#define LCDC_MESSAGE_MT "lockdc.message"
#define LCDC_OUTBOX_MT "lockdc.outbox"
#define LCDC_OUTBOX_DISPATCHER_MT "lockdc.outbox_dispatcher"
#define LCDC_OUTBOX_TXN_MT "lockdc.outbox_transaction"
#define LCDC_OUTBOX_PARTICIPANT_MT "lockdc.outbox_participant"
#define LCDC_OUTBOX_JOB_MT "lockdc.outbox_job"
#define LCDC_HISTORY_CONSUMER_MT "lockdc.history_consumer"

typedef struct lcdc_client_ud {
  lc_client *client;
  int streaming;
  int callback_active;
} lcdc_client_ud;

typedef struct lcdc_outbox_job_ud lcdc_outbox_job_ud;

typedef struct lcdc_lease_ud {
  lc_lease *lease;
  int owner_ref;
  int streaming;
  int borrowed;
  /* A stateful outbox checkpoint is borrowed from this job. The checkpoint
   * keeps the job reachable, while the job stores only a raw back-pointer, so
   * Lua's registry never contains a job/checkpoint reference cycle. */
  lcdc_outbox_job_ud *borrowed_job;
} lcdc_lease_ud;

typedef struct lcdc_message_ud {
  lc_message *message;
  /* A subscription-with-state callback borrows this lease from message.  The
   * delivery can consume message before the callback returns, so invalidate
   * the Lua lease wrapper together with the delivery wrapper. */
  lcdc_lease_ud *borrowed_state;
  int streaming;
  int borrowed;
} lcdc_message_ud;

typedef struct lcdc_outbox_ud {
  lc_outbox *outbox;
  int owner_ref;
  int streaming;
} lcdc_outbox_ud;

typedef struct lcdc_outbox_dispatcher_binding {
  lc_outbox_dispatcher *dispatcher;
  uint64_t binding_id;
  lc_client *client;
  lua_State *owner;
  int handler_mode;
  int raw_pull_mode;
  int consuming;
  int stopped;
  size_t wrapper_count;
  size_t wrapper_sequence;
  /* A pump/run frame pins this shared binding even when its handler closes
   * the wrapper that entered the frame. */
  size_t consumption_count;
  struct lcdc_outbox_dispatcher_binding *next;
} lcdc_outbox_dispatcher_binding;

typedef struct lcdc_outbox_dispatcher_ud {
  lc_outbox_dispatcher *dispatcher;
  int owner_ref;
  lcdc_outbox_dispatcher_binding *binding;
  int streaming;
} lcdc_outbox_dispatcher_ud;

typedef struct lcdc_outbox_txn_ud {
  lc_outbox_transaction *transaction;
  int owner_ref;
  int xa_owner_ref;
  void *xa_control;
  /* A participant source/sink can re-enter Lua. Keep terminal transaction
   * operations out of that frame because they release the participant leases
   * still owned by the native operation. */
  size_t streaming_count;
  int callback_scoped;
  unsigned long callback_staging_failed;
  int callback_duplicate_seen;
  int callback_duplicate_after_domain_participant;
  int callback_has_fresh_participant;
  int callback_has_domain_participant;
  int callback_duplicate_result_ref;
} lcdc_outbox_txn_ud;

typedef struct lcdc_outbox_participant_ud {
  lc_outbox_participant *participant;
  int owner_ref;
  /* The transaction userdata owns this wrapper.  It lets callback-scoped
   * transactions become rollback-only when a participant staging operation
   * reports a structured failure that Lua code elects not to raise. */
  lcdc_outbox_txn_ud *transaction_ud;
  int streaming;
} lcdc_outbox_participant_ud;

static void lcdc_outbox_txn_note_staging_failure(lcdc_outbox_txn_ud *ud);
static void lcdc_outbox_txn_note_duplicate(lua_State *L, lcdc_outbox_txn_ud *ud,
                                           int result_index);
static void lcdc_outbox_txn_clear_duplicate_result(lua_State *L,
                                                   lcdc_outbox_txn_ud *ud);

struct lcdc_outbox_job_ud {
  lc_outbox_job *job;
  /* The paired checkpoint retains this job while it is reachable. Keep only a
   * raw back-pointer here so that direction does not form a registry cycle. */
  lcdc_lease_ud *borrowed_state;
  int owner_ref;
  int handler_scoped;
  int payload_streaming;
  int terminal_operation;
  int terminal_ref;
};

typedef struct lcdc_history_consumer_ud {
  lc_history_consumer *consumer;
} lcdc_history_consumer_ud;

typedef struct lcdc_output {
  lc_sink *sink;
  int memory;
  size_t written;
} lcdc_output;

typedef struct lcdc_lua_source {
  lua_State *L;
  int read_ref;
  int reset_ref;
  int close_ref;
} lcdc_lua_source;

typedef struct lcdc_lua_sink {
  lc_sink pub;
  lua_State *L;
  int write_ref;
  int close_ref;
} lcdc_lua_sink;

typedef struct lcdc_acquire_for_update_handler {
  lua_State *L;
  int handler_ref;
} lcdc_acquire_for_update_handler;

typedef struct lcdc_query_keys_handler {
  lua_State *L;
  int begin_ref;
  int chunk_ref;
  int finish_ref;
} lcdc_query_keys_handler;

typedef struct lcdc_consumer_handler {
  lua_State *L;
  int handler_ref;
  int with_state;
} lcdc_consumer_handler;

typedef struct lcdc_watch_handler {
  lua_State *L;
  int handler_ref;
  int stopped;
} lcdc_watch_handler;

static size_t lcdc_lua_source_read(void *context, void *buffer, size_t count,
                                   lc_error *error);
static int lcdc_lua_source_reset(void *context, lc_error *error);
static void lcdc_lua_source_close(void *context);
static int lcdc_lua_sink_write(lc_sink *self, const void *bytes, size_t count,
                               lc_error *error);
static void lcdc_lua_sink_close(lc_sink *self);
static int lcdc_outbox_job_apply_terminal(lua_State *L, lcdc_outbox_job_ud *ud,
                                          int operation, int value_index,
                                          lc_error *error);

static pthread_mutex_t lcdc_outbox_dispatcher_bindings_mutex =
    PTHREAD_MUTEX_INITIALIZER;
static lcdc_outbox_dispatcher_binding *lcdc_outbox_dispatcher_bindings;
static uint64_t lcdc_outbox_dispatcher_next_binding_id = 1U;
static const char lcdc_outbox_dispatcher_handler_owners_key;
static const char lcdc_outbox_dispatcher_wrappers_key;

static lua_State *lcdc_lua_main_thread(lua_State *L) {
  lua_State *main_thread;

  lua_rawgeti(L, LUA_REGISTRYINDEX, LUA_RIDX_MAINTHREAD);
  main_thread = lua_tothread(L, -1);
  lua_pop(L, 1);
  return main_thread != NULL ? main_thread : L;
}

/* The registry owns this weak-value table, not its handler values.  The
 * handler table is instead retained by the dispatcher userdata that installed
 * it.  That makes a handler which closes over its dispatcher an ordinary Lua
 * cycle, rather than a registry-rooted leak. */
static void lcdc_push_outbox_dispatcher_handler_owners(lua_State *L) {
  lua_rawgetp(L, LUA_REGISTRYINDEX,
              (const void *)&lcdc_outbox_dispatcher_handler_owners_key);
  if (!lua_isnil(L, -1))
    return;
  lua_pop(L, 1);
  lua_newtable(L);
  lua_newtable(L);
  lua_pushstring(L, "v");
  lua_setfield(L, -2, "__mode");
  lua_setmetatable(L, -2);
  lua_pushvalue(L, -1);
  lua_rawsetp(L, LUA_REGISTRYINDEX,
              (const void *)&lcdc_outbox_dispatcher_handler_owners_key);
}

/* Track aliases weakly so installing a handler can give every live alias the
 * same userdata-owned map. The registry never roots a dispatcher cycle. */
static void lcdc_push_outbox_dispatcher_wrappers(lua_State *L) {
  lua_rawgetp(L, LUA_REGISTRYINDEX,
              (const void *)&lcdc_outbox_dispatcher_wrappers_key);
  if (!lua_isnil(L, -1))
    return;
  lua_pop(L, 1);
  lua_newtable(L);
  lua_pushvalue(L, -1);
  lua_rawsetp(L, LUA_REGISTRYINDEX,
              (const void *)&lcdc_outbox_dispatcher_wrappers_key);
}

static void lcdc_outbox_dispatcher_binding_register_wrapper(
    lua_State *L, lcdc_outbox_dispatcher_binding *binding,
    int dispatcher_index) {
  dispatcher_index = lua_absindex(L, dispatcher_index);
  lcdc_push_outbox_dispatcher_wrappers(L);
  lua_pushlightuserdata(L, binding->dispatcher);
  lua_rawget(L, -2);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    lua_newtable(L);
    lua_newtable(L);
    lua_pushstring(L, "v");
    lua_setfield(L, -2, "__mode");
    lua_setmetatable(L, -2);
    lua_pushlightuserdata(L, binding->dispatcher);
    lua_pushvalue(L, -2);
    lua_rawset(L, -4);
  }
  lua_pushvalue(L, dispatcher_index);
  lua_rawseti(L, -2, (lua_Integer)++binding->wrapper_sequence);
  lua_pop(L, 2);
}

static void
lcdc_outbox_dispatcher_clear_handlers(lua_State *L,
                                      lcdc_outbox_dispatcher_binding *binding) {
  if (binding == NULL)
    return;
  lcdc_push_outbox_dispatcher_handler_owners(L);
  lua_pushlightuserdata(L, binding->dispatcher);
  lua_pushnil(L);
  lua_rawset(L, -3);
  lua_pop(L, 1);
  lcdc_push_outbox_dispatcher_wrappers(L);
  lua_pushlightuserdata(L, binding->dispatcher);
  lua_pushnil(L);
  lua_rawset(L, -3);
  lua_pop(L, 1);
}

/* Push the handler map retained by this wrapper, falling back to the
 * collectible per-dispatcher map for an alias that predates handler binding. */
static int lcdc_outbox_dispatcher_push_handlers(lua_State *L,
                                                lcdc_outbox_dispatcher_ud *ud,
                                                int dispatcher_index) {
  lcdc_push_outbox_dispatcher_handler_owners(L);
  lua_pushlightuserdata(L, ud->dispatcher);
  lua_rawget(L, -2);
  lua_remove(L, -2);
  if (lua_istable(L, -1)) {
    lua_pushvalue(L, -1);
    lua_setiuservalue(L, dispatcher_index, 1);
    return 1;
  }
  lua_pop(L, 1);
  lua_getiuservalue(L, dispatcher_index, 1);
  return lua_istable(L, -1);
}

static void lcdc_outbox_dispatcher_set_handlers(lua_State *L,
                                                lcdc_outbox_dispatcher_ud *ud,
                                                int dispatcher_index,
                                                int handlers_index) {
  handlers_index = lua_absindex(L, handlers_index);
  lua_pushvalue(L, handlers_index);
  lua_setiuservalue(L, dispatcher_index, 1);
  lcdc_push_outbox_dispatcher_handler_owners(L);
  lua_pushlightuserdata(L, ud->dispatcher);
  lua_pushvalue(L, handlers_index);
  lua_rawset(L, -3);
  lua_pop(L, 1);
  if (ud->binding != NULL) {
    lcdc_push_outbox_dispatcher_wrappers(L);
    lua_pushlightuserdata(L, ud->binding->dispatcher);
    lua_rawget(L, -2);
    if (lua_istable(L, -1)) {
      lua_pushnil(L);
      while (lua_next(L, -2) != 0) {
        if (lua_isuserdata(L, -1)) {
          lua_pushvalue(L, handlers_index);
          lua_setiuservalue(L, -2, 1);
        }
        lua_pop(L, 1);
      }
    }
    lua_pop(L, 2);
  }
}

static int lcdc_push_error(lua_State *L, const lc_error *error) {
  lua_newtable(L);
  lua_pushinteger(L, (lua_Integer)(error != NULL ? error->code : 0));
  lua_setfield(L, -2, "code");
  lua_pushinteger(L, (lua_Integer)(error != NULL ? error->http_status : 0));
  lua_setfield(L, -2, "http_status");
  if (error != NULL && error->message != NULL) {
    lua_pushstring(L, error->message);
  } else {
    lua_pushstring(L, "lockdc error");
  }
  lua_setfield(L, -2, "message");
  if (error != NULL && error->detail != NULL) {
    lua_pushstring(L, error->detail);
    lua_setfield(L, -2, "detail");
  }
  if (error != NULL && error->server_code != NULL) {
    lua_pushstring(L, error->server_code);
    lua_setfield(L, -2, "server_code");
  }
  if (error != NULL && error->correlation_id != NULL) {
    lua_pushstring(L, error->correlation_id);
    lua_setfield(L, -2, "correlation_id");
  }
  return 1;
}

int lcdc_push_status_error(lua_State *L, int rc, const lc_error *error) {
  lua_pushnil(L);
  lcdc_push_error(L, error);
  lua_pushinteger(L, (lua_Integer)rc);
  return 3;
}

static lcdc_client_ud *lcdc_check_client(lua_State *L, int index) {
  lcdc_client_ud *ud;

  ud = (lcdc_client_ud *)luaL_checkudata(L, index, LCDC_CLIENT_MT);
  luaL_argcheck(L, ud != NULL && ud->client != NULL, index,
                "lockdc client is closed");
  luaL_argcheck(
      L, !ud->streaming, index,
      "lockdc client operation is not allowed while output is streaming");
  return ud;
}

int lc_lua_client_view(lua_State *L, int index, lc_client **out,
                       lc_error *error) {
  lcdc_client_ud *ud;
  if (out != NULL)
    *out = NULL;
  if (L == NULL || out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "client conversion requires Lua state and output", NULL,
                        NULL, NULL);
  ud = luaL_testudata(L, index, LCDC_CLIENT_MT);
  if (out == NULL || ud == NULL || ud->client == NULL || ud->streaming)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "expected a live, idle lockdc client userdata", NULL,
                        NULL, NULL);
  *out = ud->client;
  return LC_OK;
}

/* Request-table field access may invoke Lua metamethods. In particular, an
 * __index implementation can close this wrapper after its initial receiver
 * check, so every request parser must revalidate before passing the client to
 * the C API. */
static int lcdc_client_revalidate(lcdc_client_ud *ud, lc_client **out,
                                  lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->client == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "client was closed while preparing request", NULL, NULL,
                        NULL);
  }
  *out = ud->client;
  return LC_OK;
}

static int lcdc_lease_revalidate(lcdc_lease_ud *ud, lc_lease **out,
                                 lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->lease == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease was closed while preparing request", NULL, NULL,
                        NULL);
  }
  *out = ud->lease;
  return LC_OK;
}

static int lcdc_message_revalidate(lcdc_message_ud *ud, lc_message **out,
                                   lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->message == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message was closed while preparing request", NULL,
                        NULL, NULL);
  }
  *out = ud->message;
  return LC_OK;
}

static int lcdc_outbox_revalidate(lcdc_outbox_ud *ud, lc_outbox **out,
                                  lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->outbox == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox was closed while preparing input", NULL, NULL,
                        NULL);
  }
  *out = ud->outbox;
  return LC_OK;
}

static int lcdc_outbox_txn_revalidate(lcdc_outbox_txn_ud *ud,
                                      lc_outbox_transaction **out,
                                      lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->transaction == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox transaction was closed while preparing input",
                        NULL, NULL, NULL);
  }
  *out = ud->transaction;
  return LC_OK;
}

static int lcdc_outbox_participant_revalidate(lcdc_outbox_participant_ud *ud,
                                              lc_outbox_participant **out,
                                              lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->participant == NULL || ud->transaction_ud == NULL ||
      ud->transaction_ud->transaction == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant was closed while preparing input",
                        NULL, NULL, NULL);
  }
  *out = ud->participant;
  return LC_OK;
}

static int lcdc_outbox_dispatcher_revalidate(lcdc_outbox_dispatcher_ud *ud,
                                             lc_outbox_dispatcher **out,
                                             lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->dispatcher == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher was closed while preparing input",
                        NULL, NULL, NULL);
  }
  *out = ud->dispatcher;
  return LC_OK;
}

static int lcdc_outbox_job_revalidate(lcdc_outbox_job_ud *ud,
                                      lc_outbox_job **out, lc_error *error) {
  if (out != NULL)
    *out = NULL;
  if (ud == NULL || ud->job == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox job was closed while preparing input", NULL,
                        NULL, NULL);
  }
  *out = ud->job;
  return LC_OK;
}

static lcdc_lease_ud *lcdc_check_lease(lua_State *L, int index) {
  lcdc_lease_ud *ud;

  ud = (lcdc_lease_ud *)luaL_checkudata(L, index, LCDC_LEASE_MT);
  luaL_argcheck(L, ud != NULL && ud->lease != NULL, index,
                "lockdc lease is closed");
  luaL_argcheck(
      L, !ud->streaming, index,
      "lockdc lease operation is not allowed while output is streaming");
  return ud;
}

static lcdc_message_ud *lcdc_check_message(lua_State *L, int index) {
  lcdc_message_ud *ud;

  ud = (lcdc_message_ud *)luaL_checkudata(L, index, LCDC_MESSAGE_MT);
  luaL_argcheck(L, ud != NULL && ud->message != NULL, index,
                "lockdc message is closed");
  luaL_argcheck(
      L, !ud->streaming, index,
      "lockdc message operation is not allowed while output is streaming");
  return ud;
}

static lcdc_outbox_ud *lcdc_check_outbox(lua_State *L, int index) {
  lcdc_outbox_ud *ud =
      (lcdc_outbox_ud *)luaL_checkudata(L, index, LCDC_OUTBOX_MT);
  luaL_argcheck(L, ud != NULL && ud->outbox != NULL, index,
                "lockdc outbox is closed");
  luaL_argcheck(
      L, !ud->streaming, index,
      "lockdc outbox operation is not allowed while output is streaming");
  return ud;
}

static lcdc_outbox_txn_ud *lcdc_check_outbox_txn(lua_State *L, int index) {
  lcdc_outbox_txn_ud *ud =
      (lcdc_outbox_txn_ud *)luaL_checkudata(L, index, LCDC_OUTBOX_TXN_MT);
  luaL_argcheck(L, ud != NULL && ud->transaction != NULL, index,
                "lockdc outbox transaction is closed");
  luaL_argcheck(L, ud->streaming_count == 0U, index,
                "outbox transaction operation is not allowed while participant "
                "I/O is streaming");
  return ud;
}

static lcdc_outbox_dispatcher_ud *lcdc_check_outbox_dispatcher(lua_State *L,
                                                               int index) {
  lcdc_outbox_dispatcher_ud *ud = (lcdc_outbox_dispatcher_ud *)luaL_checkudata(
      L, index, LCDC_OUTBOX_DISPATCHER_MT);
  luaL_argcheck(L, ud != NULL && ud->dispatcher != NULL, index,
                "lockdc outbox dispatcher is closed");
  luaL_argcheck(L, !ud->streaming, index,
                "lockdc outbox dispatcher operation is not allowed while "
                "output is streaming");
  return ud;
}

static lcdc_outbox_participant_ud *lcdc_check_outbox_participant(lua_State *L,
                                                                 int index) {
  lcdc_outbox_participant_ud *ud =
      (lcdc_outbox_participant_ud *)luaL_checkudata(L, index,
                                                    LCDC_OUTBOX_PARTICIPANT_MT);
  luaL_argcheck(L, ud != NULL && ud->participant != NULL, index,
                "lockdc outbox participant is closed");
  luaL_argcheck(L, !ud->streaming, index,
                "lockdc outbox participant operation is not allowed while "
                "output is streaming");
  luaL_argcheck(L,
                ud->transaction_ud == NULL ||
                    ud->transaction_ud->streaming_count == 0U,
                index,
                "outbox transaction operation is not allowed while participant "
                "I/O is streaming");
  return ud;
}

static lcdc_outbox_job_ud *lcdc_check_outbox_job(lua_State *L, int index) {
  lcdc_outbox_job_ud *ud =
      (lcdc_outbox_job_ud *)luaL_checkudata(L, index, LCDC_OUTBOX_JOB_MT);
  luaL_argcheck(L, ud != NULL && ud->job != NULL, index,
                "lockdc outbox job is closed");
  return ud;
}

static lcdc_history_consumer_ud *lcdc_check_history_consumer(lua_State *L,
                                                             int index) {
  lcdc_history_consumer_ud *ud;

  ud = (lcdc_history_consumer_ud *)luaL_checkudata(L, index,
                                                   LCDC_HISTORY_CONSUMER_MT);
  luaL_argcheck(L, ud != NULL && ud->consumer != NULL, index,
                "lockdc history consumer is closed");
  return ud;
}

static void lcdc_set_string_field(lua_State *L, const char *name,
                                  const char *value) {
  if (value == NULL) {
    return;
  }
  lua_pushstring(L, value);
  lua_setfield(L, -2, name);
}

static void lcdc_set_integer_field(lua_State *L, const char *name, long value) {
  lua_pushinteger(L, (lua_Integer)value);
  lua_setfield(L, -2, name);
}

/* On Darwin LP64, Lua's signed long long has the same limits as long.
 * The SDK's LLONG constants use a suffix rejected in C89 mode. */
#if defined(__APPLE__) && defined(__LP64__) && LUA_INT_TYPE == LUA_INT_LONGLONG
#define LCDC_LUA_MAXINTEGER LONG_MAX
#define LCDC_LUA_MININTEGER LONG_MIN
#else
#define LCDC_LUA_MAXINTEGER LUA_MAXINTEGER
#define LCDC_LUA_MININTEGER LUA_MININTEGER
#endif

static int lcdc_set_size_field(lua_State *L, const char *name, size_t value,
                               lc_error *error) {
#if SIZE_MAX > LCDC_LUA_MAXINTEGER
  if ((uintmax_t)value > (uintmax_t)LCDC_LUA_MAXINTEGER) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox statistic exceeds Lua integer range", name,
                        NULL, NULL);
  }
#else
  (void)error;
#endif
  lua_pushinteger(L, (lua_Integer)value);
  lua_setfield(L, -2, name);
  return LC_OK;
}

static int lcdc_set_uint64_field(lua_State *L, const char *name, uint64_t value,
                                 lc_error *error) {
  if (value > (uint64_t)LCDC_LUA_MAXINTEGER) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox statistic exceeds Lua integer range", name,
                        NULL, NULL);
  }
  lua_pushinteger(L, (lua_Integer)value);
  lua_setfield(L, -2, name);
  return LC_OK;
}

static int lcdc_set_int64_field(lua_State *L, const char *name, int64_t value,
                                lc_error *error) {
  if (value < (int64_t)LCDC_LUA_MININTEGER ||
      value > (int64_t)LCDC_LUA_MAXINTEGER) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox value exceeds Lua integer range", name, NULL,
                        NULL);
  }
  lua_pushinteger(L, (lua_Integer)value);
  lua_setfield(L, -2, name);
  return LC_OK;
}

static int lcdc_set_unix_seconds_field(lua_State *L, const char *name,
                                       lc_unix_seconds value, lc_error *error) {
  return lcdc_set_int64_field(L, name, (int64_t)value, error);
}

void lcdc_set_version_field(lua_State *L, const char *name, lc_version value) {
  if (value < (lc_version)LCDC_LUA_MININTEGER ||
      value > (lc_version)LCDC_LUA_MAXINTEGER)
    luaL_error(L, "%s exceeds the Lua integer range", name);
  lua_pushinteger(L, (lua_Integer)value);
  lua_setfield(L, -2, name);
}

void lcdc_set_u64_decimal(lua_State *L, const char *key, uint64_t value) {
  char bytes[20];
  size_t offset = sizeof(bytes);
  do {
    bytes[--offset] = (char)('0' + value % 10U);
    value /= 10U;
  } while (value != 0U);
  lua_pushlstring(L, bytes + offset, sizeof(bytes) - offset);
  lua_setfield(L, -2, key);
}

void lcdc_set_state_retention(lua_State *L, lc_state_retention_support support,
                              int has_expiry, lc_unix_seconds expiry) {
  lcdc_set_string_field(L, "retention_support",
                        support == LC_STATE_RETENTION_SUPPORTED ? "supported"
                        : support == LC_STATE_RETENTION_UNSUPPORTED
                            ? "unsupported"
                            : "unknown");
  if (has_expiry)
    lcdc_set_version_field(L, "retention_expires_at_unix", (lc_version)expiry);
}

static void lcdc_set_bool_field(lua_State *L, const char *name, int value) {
  lua_pushboolean(L, value);
  lua_setfield(L, -2, name);
}

static int lcdc_opt_boolean_field(lua_State *L, int index, const char *name,
                                  int *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (!lua_isnil(L, -1)) {
      *out = lua_toboolean(L, -1);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

static long lcdc_check_long(lua_State *L, int index, const char *name) {
  lua_Integer value = luaL_checkinteger(L, index);
  long result = 0L;

  if (value < (lua_Integer)LONG_MIN || value > (lua_Integer)LONG_MAX)
    luaL_error(L, "%s must fit a C long", name);
  result = (long)value;
  return result;
}

static int64_t lcdc_check_int64(lua_State *L, int index, const char *name) {
  lua_Integer value = luaL_checkinteger(L, index);

  (void)name;
  return (int64_t)value;
}

static lc_version lcdc_check_version(lua_State *L, int index,
                                     const char *name) {
  lua_Integer value = luaL_checkinteger(L, index);

  (void)name;
  return (lc_version)value;
}

static int lcdc_opt_integer_field(lua_State *L, int index, const char *name,
                                  long *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (!lua_isnil(L, -1)) {
      *out = lcdc_check_long(L, -1, name);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

/* Parse before allocating mutation arrays or entering a streaming callback. */
static int lcdc_read_state_retention(lua_State *L, int index, int *selected,
                                     long *out) {
  lua_Integer value;
  int integer = 0;
  int valid;
  lc_error error;
  *selected = 0;
  if (!lua_istable(L, index))
    return 1;
  lua_getfield(L, index, "retention_seconds");
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return 1;
  }
  value = lua_tointegerx(L, -1, &integer);
  valid = lua_type(L, -1) == LUA_TNUMBER && integer && value >= 0 &&
          (uintmax_t)value <= (uintmax_t)LONG_MAX;
  lua_pop(L, 1);
  if (!valid) {
    lc_error_init(&error);
    lc_error_set(&error, LC_ERR_INVALID, 0L,
                 "retention_seconds must be a nonnegative integer "
                 "representable by a C long",
                 NULL, NULL, NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, &error);
    lc_error_cleanup(&error);
    return 0;
  }
  *selected = 1;
  *out = (long)value;
  return 1;
}

static int lcdc_opt_int64_field(lua_State *L, int index, const char *name,
                                int64_t *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (!lua_isnil(L, -1)) {
      *out = lcdc_check_int64(L, -1, name);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

static int lcdc_opt_version_field(lua_State *L, int index, const char *name,
                                  lc_version *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (!lua_isnil(L, -1)) {
      *out = lcdc_check_version(L, -1, name);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

static int lcdc_opt_size_field(lua_State *L, int index, const char *name,
                               size_t *out) {
  lua_Integer value;

  if (!lua_istable(L, index))
    return LC_ERR_INVALID;
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return 0;
  }
  value = luaL_checkinteger(L, -1);
  if (value < 0 || (uintmax_t)value > (uintmax_t)SIZE_MAX)
    luaL_error(L, "%s must be a non-negative size", name);
  *out = (size_t)value;
  lua_pop(L, 1);
  return 1;
}

static int lcdc_opt_int_field(lua_State *L, int index, const char *name,
                              int *out) {
  lua_Integer value;

  if (!lua_istable(L, index))
    return 0;
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return 0;
  }
  value = luaL_checkinteger(L, -1);
  if (value < (lua_Integer)INT_MIN || value > (lua_Integer)INT_MAX)
    luaL_error(L, "%s must fit a signed 32-bit integer", name);
  *out = (int)value;
  lua_pop(L, 1);
  return 1;
}

static uint64_t lcdc_check_uint64(lua_State *L, int index, const char *name) {
  lua_Integer value = luaL_checkinteger(L, index);

  if (value < 0) {
    luaL_error(L, "%s must be a non-negative 64-bit integer", name);
  }
  return (uint64_t)value;
}

static int lcdc_opt_uint64_field(lua_State *L, int index, const char *name,
                                 uint64_t *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (!lua_isnil(L, -1)) {
      *out = lcdc_check_uint64(L, -1, name);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

/* Lua integers are signed, so use explicit false for the C API's unsigned
 * unbounded sentinels. Keeping the field numeric otherwise avoids a second
 * configuration spelling and makes accidental true values actionable. */
static int lcdc_opt_retention_count_field(lua_State *L, int index,
                                          const char *name, size_t *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (lua_isboolean(L, -1)) {
      if (lua_toboolean(L, -1) != 0)
        luaL_error(L, "%s must be a non-negative size or false", name);
      *out = SIZE_MAX;
      lua_pop(L, 1);
      return 1;
    }
    if (!lua_isnil(L, -1)) {
      lua_Integer value = luaL_checkinteger(L, -1);

      if (value < 0 || (uintmax_t)value > (uintmax_t)SIZE_MAX)
        luaL_error(L, "%s must be a non-negative size or false", name);
      *out = (size_t)value;
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

static int lcdc_opt_dead_letter_bytes_field(lua_State *L, int index,
                                            const char *name, uint64_t *out) {
  if (lua_istable(L, index)) {
    lua_getfield(L, index, name);
    if (lua_isboolean(L, -1)) {
      if (lua_toboolean(L, -1) != 0)
        luaL_error(L, "%s must be a non-negative integer or false", name);
      *out = ~(uint64_t)0U;
      lua_pop(L, 1);
      return 1;
    }
    if (!lua_isnil(L, -1)) {
      *out = lcdc_check_uint64(L, -1, name);
      lua_pop(L, 1);
      return 1;
    }
    lua_pop(L, 1);
  }
  return 0;
}

static const char *lcdc_opt_string_field(lua_State *L, int index,
                                         const char *name) {
  const char *value;

  if (!lua_istable(L, index)) {
    return NULL;
  }
  if (strcmp(name, "namespace") == 0) {
    lua_pushliteral(L, "namespace_name");
    lua_rawget(L, index);
    if (!lua_isnil(L, -1)) {
      lua_pop(L, 1);
      luaL_error(L, "namespace_name is not supported; use namespace");
    }
    lua_pop(L, 1);
  }
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return NULL;
  }
  value = luaL_checkstring(L, -1);
  lua_pop(L, 1);
  return value;
}

static int lcdc_require_string_field(lua_State *L, int index, const char *name,
                                     const char **out) {
  if (!lua_istable(L, index)) {
    luaL_error(L, "expected request table");
  }
  if (strcmp(name, "namespace") == 0) {
    lua_pushliteral(L, "namespace_name");
    lua_rawget(L, index);
    if (!lua_isnil(L, -1)) {
      lua_pop(L, 1);
      luaL_error(L, "namespace_name is not supported; use namespace");
    }
    lua_pop(L, 1);
  }
  lua_getfield(L, index, name);
  if (out != NULL) {
    *out = luaL_checkstring(L, -1);
  } else {
    (void)luaL_checkstring(L, -1);
  }
  lua_pop(L, 1);
  return 1;
}

/* Copies a string field into a plain, caller-owned Lua table without ever
 * exposing an unrooted C string pointer. This is for request parsers that
 * retain pointers while resolving later metatable-backed fields. */
static void lcdc_normalize_string_field(lua_State *L, int source_index,
                                        int destination_index, const char *name,
                                        int required) {
  source_index = lua_absindex(L, source_index);
  destination_index = lua_absindex(L, destination_index);
  if (!lua_istable(L, source_index))
    luaL_error(L, "expected request table");
  if (strcmp(name, "namespace") == 0) {
    lua_pushliteral(L, "namespace_name");
    lua_rawget(L, source_index);
    if (!lua_isnil(L, -1)) {
      lua_pop(L, 1);
      luaL_error(L, "namespace_name is not supported; use namespace");
    }
    lua_pop(L, 1);
  }
  lua_getfield(L, source_index, name);
  if (lua_isnil(L, -1)) {
    if (required)
      (void)luaL_checkstring(L, -1);
    lua_pop(L, 1);
    return;
  }
  (void)luaL_checkstring(L, -1);
  /* Keep the source value on the stack until its duplicate is established in
   * the normalized table. `lua_setfield` may allocate and collect. */
  lua_pushvalue(L, -1);
  lua_setfield(L, destination_index, name);
  lua_pop(L, 1);
}

static const char *lcdc_normalized_string_value(lua_State *L, int index,
                                                const char *name) {
  const char *value;

  index = lua_absindex(L, index);
  lua_getfield(L, index, name);
  value = lua_tostring(L, -1);
  lua_pop(L, 1);
  return value;
}

static void lcdc_reject_field(lua_State *L, int index, const char *name,
                              const char *message) {
  lua_getfield(L, index, name);
  if (!lua_isnil(L, -1)) {
    lua_pop(L, 1);
    luaL_error(L, "%s", message);
  }
  lua_pop(L, 1);
}

static int lcdc_parse_string_array(lua_State *L, int index, const char *name,
                                   const char ***items_out, size_t *count_out) {
  const char **items;
  size_t count;
  size_t i;

  *items_out = NULL;
  *count_out = 0U;
  if (!lua_istable(L, index)) {
    return 1;
  }
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return 1;
  }
  luaL_checktype(L, -1, LUA_TTABLE);
  count = (size_t)lua_rawlen(L, -1);
  if (count == 0U) {
    lua_pop(L, 1);
    return 1;
  }
  items = (const char **)calloc(count, sizeof(*items));
  if (items == NULL) {
    lua_pop(L, 1);
    luaL_error(L, "out of memory");
  }
  for (i = 0U; i < count; ++i) {
    lua_rawgeti(L, -1, (lua_Integer)(i + 1U));
    items[i] = luaL_checkstring(L, -1);
    lua_pop(L, 1);
  }
  lua_pop(L, 1);
  *items_out = items;
  *count_out = count;
  return 1;
}

static void lcdc_free_string_array(const char ***items) {
  if (items != NULL && *items != NULL) {
    free((void *)*items);
    *items = NULL;
  }
}

typedef struct lcdc_lua_sink_write_args {
  lcdc_lua_sink *sink;
  const void *bytes;
  size_t count;
} lcdc_lua_sink_write_args;

/* String construction belongs inside the protected frame too: a Lua heap
 * failure must return through native stream cleanup and release its locks. */
static int lcdc_lua_sink_write_call(lua_State *L) {
  lcdc_lua_sink_write_args *args;
  args = lua_touserdata(L, 1);
  lua_rawgeti(L, LUA_REGISTRYINDEX, args->sink->write_ref);
  lua_pushlstring(L, (const char *)args->bytes, args->count);
  lua_call(L, 1, 2);
  return 2;
}

static int lcdc_lua_sink_write(lc_sink *self, const void *bytes, size_t count,
                               lc_error *error) {
  lcdc_lua_sink *sink;
  lua_State *L;
  const char *message;
  int failed;
  int status;
  lcdc_lua_sink_write_args args;

  sink = (lcdc_lua_sink *)self;
  if (sink == NULL || sink->L == NULL || sink->write_ref == LUA_NOREF) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "Lua sink is no longer available", NULL, NULL, NULL);
    return 0;
  }
  L = sink->L;
  if (!lua_checkstack(L, 2)) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to reserve Lua stream callback stack", NULL,
                       NULL, NULL);
    return 0;
  }
  args.sink = sink;
  args.bytes = bytes;
  args.count = count;
  lua_pushcfunction(L, lcdc_lua_sink_write_call);
  lua_pushlightuserdata(L, &args);
  status = lua_pcall(L, 1, 2, 0);
  if (status != LUA_OK) {
    message = lua_type(L, -1) == LUA_TSTRING ? lua_tostring(L, -1) : NULL;
    (void)lc_error_set(
        error, status == LUA_ERRMEM ? LC_ERR_NOMEM : LC_ERR_INVALID, 0L,
        message != NULL ? message : "Lua sink write failed", NULL, NULL, NULL);
    lua_pop(L, 1);
    return 0;
  }
  failed = (!lua_isnil(L, -1) && (lua_isnil(L, -2) || !lua_toboolean(L, -2)));
  if (failed) {
    message = lua_tostring(L, -1);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       message != NULL ? message : "Lua sink write failed",
                       NULL, NULL, NULL);
    lua_pop(L, 2);
    return 0;
  }
  if (lua_isboolean(L, -2) && !lua_toboolean(L, -2)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L, "Lua sink rejected chunk",
                       NULL, NULL, NULL);
    lua_pop(L, 2);
    return 0;
  }
  lua_pop(L, 2);
  return 1;
}

static void lcdc_lua_sink_close(lc_sink *self) {
  lcdc_lua_sink *sink;
  lua_State *L;

  sink = (lcdc_lua_sink *)self;
  if (sink == NULL)
    return;
  L = sink->L;
  if (L != NULL && sink->close_ref != LUA_NOREF) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, sink->close_ref);
    if (lua_pcall(L, 0, 0, 0) != 0)
      lua_pop(L, 1);
    luaL_unref(L, LUA_REGISTRYINDEX, sink->close_ref);
  }
  if (L != NULL && sink->write_ref != LUA_NOREF)
    luaL_unref(L, LUA_REGISTRYINDEX, sink->write_ref);
  free(sink);
}

int lcdc_sink_from_value(lua_State *L, int index, lc_sink **out,
                         lc_error *error) {
  const char *path;
  long fd;
  lcdc_lua_sink *sink;

  *out = NULL;
  if (index < 0)
    index = lua_gettop(L) + index + 1;
  if (lua_type(L, index) == LUA_TNUMBER) {
    fd = (long)lua_tointeger(L, index);
    return lc_sink_to_fd((int)fd, out, error);
  }
  if (lua_isstring(L, index)) {
    path = lua_tostring(L, index);
    return lc_sink_to_file(path, out, error);
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lua_getfield(L, index, "path");
  if (!lua_isnil(L, -1)) {
    path = luaL_checkstring(L, -1);
    lua_pop(L, 1);
    return lc_sink_to_file(path, out, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "fd");
  if (!lua_isnil(L, -1)) {
    fd = (long)luaL_checkinteger(L, -1);
    lua_pop(L, 1);
    return lc_sink_to_fd((int)fd, out, error);
  }
  lua_pop(L, 1);
  /* Resolve every user callback before allocating or retaining either one.
   * A table __index metamethod may throw; after a Lua longjmp there is no C
   * cleanup frame for a partially constructed sink. */
  lua_getfield(L, index, "write");
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "Lua sink requires path, fd, or write", NULL, NULL,
                        NULL);
  }
  luaL_checktype(L, -1, LUA_TFUNCTION);
  lua_getfield(L, index, "close");
  if (!lua_isnil(L, -1))
    luaL_checktype(L, -1, LUA_TFUNCTION);
  sink = (lcdc_lua_sink *)calloc(1U, sizeof(*sink));
  if (sink == NULL) {
    lua_pop(L, 2);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate Lua sink",
                        NULL, NULL, NULL);
  }
  sink->L = L;
  sink->write_ref = LUA_NOREF;
  sink->close_ref = LUA_NOREF;
  sink->pub.write = lcdc_lua_sink_write;
  sink->pub.close = lcdc_lua_sink_close;
  sink->pub.impl = sink;
  *out = &sink->pub;
  if (!lua_isnil(L, -1)) {
    sink->close_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  } else {
    lua_pop(L, 1);
  }
  sink->write_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  return LC_OK;
}

static int lcdc_init_output(lua_State *L, int index, lcdc_output *output,
                            lc_error *error) {
  output->sink = NULL;
  output->memory = 1;
  output->written = 0U;
  if (lua_isnoneornil(L, index))
    return lc_sink_to_memory(&output->sink, error);
  output->memory = 0;
  return lcdc_sink_from_value(L, index, &output->sink, error);
}

static void lcdc_push_output(lua_State *L, lcdc_output *output) {
  const void *bytes;
  size_t length;
  lc_error error;

  if (output->memory && output->sink != NULL) {
    lc_error_init(&error);
    if (lc_sink_memory_bytes(output->sink, &bytes, &length, &error) == LC_OK) {
      lua_pushlstring(L, (const char *)bytes, length);
    } else {
      lua_pushliteral(L, "");
    }
    lc_error_cleanup(&error);
  } else {
    lua_pushnil(L);
  }
  if (output->sink != NULL) {
    lc_sink_close(output->sink);
    output->sink = NULL;
  }
}

static size_t lcdc_lua_source_read(void *context, void *buffer, size_t count,
                                   lc_error *error) {
  lcdc_lua_source *source;
  lua_State *L;
  const char *chunk;
  const char *message;
  size_t length;
  int status;

  source = (lcdc_lua_source *)context;
  L = source->L;
  if (!lua_checkstack(L, 2)) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to reserve Lua source callback stack", NULL, NULL,
                 NULL);
    return 0U;
  }
  lua_rawgeti(L, LUA_REGISTRYINDEX, source->read_ref);
  lua_pushinteger(L, (lua_Integer)count);
  status = lua_pcall(L, 1, 2, 0);
  if (status != LUA_OK) {
    message = lua_type(L, -1) == LUA_TSTRING ? lua_tostring(L, -1) : NULL;
    lc_error_set(error, status == LUA_ERRMEM ? LC_ERR_NOMEM : LC_ERR_INVALID,
                 0L, message != NULL ? message : "Lua source read failed", NULL,
                 NULL, NULL);
    lua_pop(L, 1);
    return 0U;
  }
  if (lua_isnil(L, -2)) {
    if (!lua_isnil(L, -1)) {
      message = lua_tostring(L, -1);
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   message != NULL ? message : "Lua source read failed", NULL,
                   NULL, NULL);
    }
    lua_pop(L, 2);
    return 0U;
  }
  if (!lua_isstring(L, -2)) {
    lua_pop(L, 2);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "Lua source read must return a string, nil, or nil plus an "
                 "error message",
                 NULL, NULL, NULL);
    return 0U;
  }
  chunk = lua_tolstring(L, -2, &length);
  if (length > count) {
    lua_pop(L, 2);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "Lua source read returned more bytes than requested", NULL,
                 NULL, NULL);
    return 0U;
  }
  if (length != 0U) {
    memcpy(buffer, chunk, length);
  }
  lua_pop(L, 2);
  return length;
}

static int lcdc_lua_source_reset(void *context, lc_error *error) {
  lcdc_lua_source *source;
  lua_State *L;
  const char *message;

  source = (lcdc_lua_source *)context;
  if (source->reset_ref == LUA_NOREF) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "Lua source is not resettable", NULL, NULL, NULL);
  }
  L = source->L;
  lua_rawgeti(L, LUA_REGISTRYINDEX, source->reset_ref);
  if (lua_pcall(L, 0, 2, 0) != 0) {
    message = lua_tostring(L, -1);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 message != NULL ? message : "Lua source reset failed", NULL,
                 NULL, NULL);
    lua_pop(L, 1);
    return LC_ERR_INVALID;
  }
  if (lua_toboolean(L, -2) || (lua_isnil(L, -2) && lua_isnil(L, -1))) {
    lua_pop(L, 2);
    return LC_OK;
  }
  message = lua_tostring(L, -1);
  lc_error_set(error, LC_ERR_INVALID, 0L,
               message != NULL ? message : "Lua source reset failed", NULL,
               NULL, NULL);
  lua_pop(L, 2);
  return LC_ERR_INVALID;
}

static void lcdc_lua_source_close(void *context) {
  lcdc_lua_source *source;
  lua_State *L;

  source = (lcdc_lua_source *)context;
  if (source == NULL) {
    return;
  }
  L = source->L;
  if (source->close_ref != LUA_NOREF) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, source->close_ref);
    if (lua_pcall(L, 0, 0, 0) != 0) {
      lua_pop(L, 1);
    }
    luaL_unref(L, LUA_REGISTRYINDEX, source->close_ref);
  }
  if (source->reset_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, source->reset_ref);
  }
  if (source->read_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, source->read_ref);
  }
  free(source);
}

int lcdc_source_from_value(lua_State *L, int index, lc_source **out,
                           lc_error *error) {
  size_t len;
  const char *bytes;
  long fd;
  const char *path;
  lcdc_lua_source *lua_source;
  int rc;

  *out = NULL;
  if (index < 0) {
    index = lua_gettop(L) + index + 1;
  }
  if (lua_type(L, index) == LUA_TNUMBER) {
    fd = (long)lua_tointeger(L, index);
    return lc_source_from_fd((int)fd, out, error);
  }
  if (lua_isstring(L, index)) {
    bytes = lua_tolstring(L, index, &len);
    return lc_source_from_memory(bytes, len, out, error);
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lua_getfield(L, index, "bytes");
  if (!lua_isnil(L, -1)) {
    bytes = lua_tolstring(L, -1, &len);
    lua_pop(L, 1);
    return lc_source_from_memory(bytes, len, out, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "path");
  if (!lua_isnil(L, -1)) {
    path = luaL_checkstring(L, -1);
    lua_pop(L, 1);
    return lc_source_from_file(path, out, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "fd");
  if (!lua_isnil(L, -1)) {
    fd = (long)luaL_checkinteger(L, -1);
    lua_pop(L, 1);
    return lc_source_from_fd((int)fd, out, error);
  }
  lua_pop(L, 1);
  lua_getfield(L, index, "read");
  if (!lua_isnil(L, -1)) {
    /* Inspect all callback fields before retaining one. A later __index
     * failure must not leak a registry reference across Lua's longjmp. */
    luaL_checktype(L, -1, LUA_TFUNCTION);
    lua_getfield(L, index, "reset");
    if (!lua_isnil(L, -1))
      luaL_checktype(L, -1, LUA_TFUNCTION);
    lua_getfield(L, index, "close");
    if (!lua_isnil(L, -1))
      luaL_checktype(L, -1, LUA_TFUNCTION);
    lua_source = (lcdc_lua_source *)calloc(1U, sizeof(*lua_source));
    if (lua_source == NULL) {
      lua_pop(L, 3);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate Lua source", NULL, NULL, NULL);
    }
    lua_source->L = L;
    lua_source->reset_ref = LUA_NOREF;
    lua_source->close_ref = LUA_NOREF;
    lua_source->read_ref = LUA_NOREF;
    /* Publish ownership before registry growth can raise a Lua allocation
     * error. Protected callers can now release this partially filled adapter.
     */
    rc =
        lc_source_from_callbacks(lcdc_lua_source_read, lcdc_lua_source_reset,
                                 lcdc_lua_source_close, lua_source, out, error);
    if (rc != LC_OK) {
      lua_pop(L, 3);
      lcdc_lua_source_close(lua_source);
      return rc;
    }
    if (!lua_isnil(L, -1)) {
      lua_source->close_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
      lua_pop(L, 1);
    }
    if (!lua_isnil(L, -1)) {
      lua_source->reset_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
      lua_pop(L, 1);
    }
    lua_source->read_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    return LC_OK;
  }
  lua_pop(L, 1);
  (void)error;
  luaL_error(L, "expected bytes, path, or fd input");
  return LC_ERR_INVALID;
}

static void lcdc_push_lease_info(lua_State *L, lc_lease *lease) {
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", lease->ns);
  lcdc_set_string_field(L, "key", lease->key);
  lcdc_set_string_field(L, "owner", lease->owner);
  lcdc_set_string_field(L, "lease_id", lease->lease_id);
  lcdc_set_string_field(L, "txn_id", lease->txn_id);
  lcdc_set_integer_field(L, "fencing_token", lease->fencing_token);
  lcdc_set_version_field(L, "version", lease->version);
  lcdc_set_integer_field(L, "lease_expires_at_unix",
                         lease->lease_expires_at_unix);
  lcdc_set_string_field(L, "state_etag", lease->state_etag);
  lcdc_set_state_retention(L, lease->retention_support,
                           lease->has_retention_expires_at_unix,
                           lease->retention_expires_at_unix);
  lcdc_set_bool_field(L, "has_query_hidden", lease->has_query_hidden);
  lcdc_set_bool_field(L, "query_hidden", lease->query_hidden);
}

static void lcdc_push_message_info(lua_State *L, lc_message *message) {
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", message->ns);
  lcdc_set_string_field(L, "queue", message->queue);
  lcdc_set_string_field(L, "message_id", message->message_id);
  lcdc_set_integer_field(L, "attempts", message->attempts);
  lcdc_set_integer_field(L, "max_attempts", message->max_attempts);
  lcdc_set_integer_field(L, "failure_attempts", message->failure_attempts);
  lcdc_set_integer_field(L, "not_visible_until_unix",
                         message->not_visible_until_unix);
  lcdc_set_integer_field(L, "visibility_timeout_seconds",
                         message->visibility_timeout_seconds);
  lcdc_set_string_field(L, "payload_content_type",
                        message->payload_content_type);
  lcdc_set_string_field(L, "correlation_id", message->correlation_id);
  lcdc_set_string_field(L, "lease_id", message->lease_id);
  lcdc_set_integer_field(L, "lease_expires_at_unix",
                         message->lease_expires_at_unix);
  lcdc_set_integer_field(L, "fencing_token", message->fencing_token);
  lcdc_set_string_field(L, "txn_id", message->txn_id);
  lcdc_set_string_field(L, "meta_etag", message->meta_etag);
  lcdc_set_string_field(L, "next_cursor", message->next_cursor);
}

static int lcdc_push_client(lua_State *L, lc_client *client) {
  lcdc_client_ud *ud;

  ud = (lcdc_client_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->client = client;
  ud->streaming = 0;
  ud->callback_active = 0;
  luaL_getmetatable(L, LCDC_CLIENT_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_lease(lua_State *L, lc_lease *lease, int owner_index) {
  lcdc_lease_ud *ud;

  ud = (lcdc_lease_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->lease = lease;
  ud->owner_ref = LUA_NOREF;
  ud->streaming = 0;
  ud->borrowed = 0;
  ud->borrowed_job = NULL;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_LEASE_MT);
  lua_setmetatable(L, -2);
  return 1;
}

lc_lease **lcdc_xa_lease_shell(lua_State *L, int owner_index) {
  lcdc_lease_ud *ud;
  owner_index = lua_absindex(L, owner_index);
  ud = lua_newuserdatauv(L, sizeof(*ud), 1);
  memset(ud, 0, sizeof(*ud));
  ud->owner_ref = LUA_NOREF;
  luaL_setmetatable(L, LCDC_LEASE_MT);
  lua_pushvalue(L, owner_index);
  lua_setiuservalue(L, -2, 1);
  return &ud->lease;
}

static int lcdc_push_borrowed_lease_owned(lua_State *L, lc_lease *lease,
                                          int owner_index,
                                          lcdc_outbox_job_ud *borrowed_job) {
  lcdc_lease_ud *ud;

  if (lease == NULL) {
    lua_pushnil(L);
    return 1;
  }
  ud = (lcdc_lease_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->lease = lease;
  ud->owner_ref = LUA_NOREF;
  ud->streaming = 0;
  ud->borrowed = 1;
  ud->borrowed_job = borrowed_job;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_LEASE_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_borrowed_lease(lua_State *L, lc_lease *lease) {
  return lcdc_push_borrowed_lease_owned(L, lease, 0, NULL);
}

static int lcdc_push_message(lua_State *L, lc_message *message) {
  lcdc_message_ud *ud;

  ud = (lcdc_message_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->message = message;
  ud->borrowed_state = NULL;
  ud->streaming = 0;
  ud->borrowed = 0;
  luaL_getmetatable(L, LCDC_MESSAGE_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_borrowed_message(lua_State *L, lc_message *message) {
  lcdc_message_ud *ud;

  ud = (lcdc_message_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->message = message;
  ud->borrowed_state = NULL;
  ud->streaming = 0;
  ud->borrowed = 1;
  luaL_getmetatable(L, LCDC_MESSAGE_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_outbox(lua_State *L, lc_outbox *outbox, int owner_index) {
  lcdc_outbox_ud *ud = (lcdc_outbox_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->outbox = outbox;
  ud->owner_ref = LUA_NOREF;
  ud->streaming = 0;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_OUTBOX_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_outbox_dispatcher_binding_acquire(
    lua_State *L, lc_outbox_dispatcher *dispatcher, int client_index,
    lcdc_outbox_dispatcher_binding **out, lc_error *error) {
  lcdc_outbox_dispatcher_binding *binding;
  lua_State *owner;

  owner = lcdc_lua_main_thread(L);
  pthread_mutex_lock(&lcdc_outbox_dispatcher_bindings_mutex);
  for (binding = lcdc_outbox_dispatcher_bindings; binding != NULL;
       binding = binding->next) {
    if (binding->dispatcher != dispatcher)
      continue;
    if (binding->owner != owner) {
      pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "dispatcher already belongs to another Lua state",
                          NULL, NULL, NULL);
    }
    ++binding->wrapper_count;
    pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
    *out = binding;
    return LC_OK;
  }
  binding = (lcdc_outbox_dispatcher_binding *)calloc(1U, sizeof(*binding));
  if (binding == NULL) {
    pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate Lua dispatcher binding", NULL, NULL,
                        NULL);
  }
  binding->dispatcher = dispatcher;
  binding->binding_id = lcdc_outbox_dispatcher_next_binding_id++;
  if (lcdc_outbox_dispatcher_next_binding_id == 0U)
    lcdc_outbox_dispatcher_next_binding_id = 1U;
  binding->client =
      ((lcdc_client_ud *)luaL_checkudata(L, client_index, LCDC_CLIENT_MT))
          ->client;
  binding->owner = owner;
  binding->handler_mode = 0;
  binding->wrapper_count = 1U;
  binding->next = lcdc_outbox_dispatcher_bindings;
  lcdc_outbox_dispatcher_bindings = binding;
  pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
  *out = binding;
  return LC_OK;
}

/* Client close stops each native dispatcher before releasing its runtime, then
 * retires bindings that no live Lua wrapper still owns. */
static void
lcdc_outbox_dispatcher_bindings_note_client_stopped(lua_State *L,
                                                    int client_index) {
  lcdc_outbox_dispatcher_binding **link;
  lcdc_outbox_dispatcher_binding *binding;
  lcdc_outbox_dispatcher_binding *retired;
  lc_client *client;

  retired = NULL;
  client = ((lcdc_client_ud *)luaL_checkudata(L, client_index, LCDC_CLIENT_MT))
               ->client;
  pthread_mutex_lock(&lcdc_outbox_dispatcher_bindings_mutex);
  link = &lcdc_outbox_dispatcher_bindings;
  while (*link != NULL) {
    binding = *link;
    if (binding->owner != lcdc_lua_main_thread(L) ||
        binding->client != client) {
      link = &binding->next;
      continue;
    }
    binding->stopped = 1;
    if (binding->wrapper_count != 0U || binding->consumption_count != 0U) {
      link = &binding->next;
      continue;
    }
    *link = binding->next;
    binding->next = retired;
    retired = binding;
  }
  pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
  while (retired != NULL) {
    binding = retired;
    retired = binding->next;
    lcdc_outbox_dispatcher_clear_handlers(L, binding);
    free(binding);
  }
}

static void lcdc_outbox_dispatcher_binding_release(
    lua_State *L, lcdc_outbox_dispatcher_binding *binding) {
  lcdc_outbox_dispatcher_binding **link;

  if (binding == NULL)
    return;
  pthread_mutex_lock(&lcdc_outbox_dispatcher_bindings_mutex);
  if (binding->wrapper_count > 0U)
    --binding->wrapper_count;
  /* A running handler pins the shared binding independently of its wrapper:
   * handler code may close that wrapper before pump/run reaches cleanup. */
  if (binding->wrapper_count == 0U && binding->consumption_count == 0U) {
    link = &lcdc_outbox_dispatcher_bindings;
    while (*link != NULL && *link != binding)
      link = &(*link)->next;
    if (*link == binding)
      *link = binding->next;
  } else {
    binding = NULL;
  }
  pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
  if (binding != NULL) {
    lcdc_outbox_dispatcher_clear_handlers(L, binding);
    free(binding);
  }
}

static void lcdc_outbox_dispatcher_binding_note_stopped(
    lua_State *L, lcdc_outbox_dispatcher_binding *binding) {
  lcdc_outbox_dispatcher_binding **link;

  if (binding == NULL)
    return;
  pthread_mutex_lock(&lcdc_outbox_dispatcher_bindings_mutex);
  binding->stopped = 1;
  if (binding->wrapper_count == 0U && binding->consumption_count == 0U) {
    link = &lcdc_outbox_dispatcher_bindings;
    while (*link != NULL && *link != binding)
      link = &(*link)->next;
    if (*link == binding)
      *link = binding->next;
  } else {
    binding = NULL;
  }
  pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
  if (binding != NULL) {
    lcdc_outbox_dispatcher_clear_handlers(L, binding);
    free(binding);
  }
}

static int lcdc_push_outbox_dispatcher(lua_State *L,
                                       lc_outbox_dispatcher *dispatcher,
                                       int client_index, int owner_index,
                                       lc_error *error) {
  lcdc_outbox_dispatcher_ud *ud =
      (lcdc_outbox_dispatcher_ud *)lua_newuserdatauv(L, sizeof(*ud), 1);
  int rc;

  ud->dispatcher = NULL;
  ud->owner_ref = LUA_NOREF;
  ud->binding = NULL;
  ud->streaming = 0;
  rc = lcdc_outbox_dispatcher_binding_acquire(L, dispatcher, client_index,
                                              &ud->binding, error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    return rc;
  }
  ud->dispatcher = dispatcher;
  lcdc_outbox_dispatcher_binding_register_wrapper(L, ud->binding, -1);
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_OUTBOX_DISPATCHER_MT);
  lua_setmetatable(L, -2);
  return LC_OK;
}

static int lcdc_push_outbox_txn(lua_State *L,
                                lc_outbox_transaction *transaction,
                                int owner_index) {
  lcdc_outbox_txn_ud *ud =
      (lcdc_outbox_txn_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->transaction = transaction;
  ud->owner_ref = LUA_NOREF;
  ud->xa_owner_ref = LUA_NOREF;
  ud->xa_control = NULL;
  ud->streaming_count = 0U;
  ud->callback_scoped = 0;
  ud->callback_staging_failed = 0;
  ud->callback_duplicate_seen = 0;
  ud->callback_duplicate_after_domain_participant = 0;
  ud->callback_has_fresh_participant = 0;
  ud->callback_has_domain_participant = 0;
  ud->callback_duplicate_result_ref = LUA_NOREF;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_OUTBOX_TXN_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_outbox_participant(lua_State *L,
                                        lc_outbox_participant *participant,
                                        int owner_index,
                                        lcdc_outbox_txn_ud *transaction_ud) {
  lcdc_outbox_participant_ud *ud =
      (lcdc_outbox_participant_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->participant = participant;
  ud->owner_ref = LUA_NOREF;
  ud->transaction_ud = transaction_ud;
  ud->streaming = 0;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_OUTBOX_PARTICIPANT_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_outbox_job(lua_State *L, lc_outbox_job *job,
                                int owner_index) {
  lcdc_outbox_job_ud *ud =
      (lcdc_outbox_job_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->job = job;
  ud->borrowed_state = NULL;
  ud->owner_ref = LUA_NOREF;
  ud->handler_scoped = 0;
  ud->payload_streaming = 0;
  ud->terminal_operation = -1;
  ud->terminal_ref = LUA_NOREF;
  if (owner_index != 0) {
    lua_pushvalue(L, owner_index);
    ud->owner_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
  luaL_getmetatable(L, LCDC_OUTBOX_JOB_MT);
  lua_setmetatable(L, -2);
  /* A weak value cache returns the same checkpoint wrapper while the caller
   * retains it, but the wrapper is never kept alive solely by its job. The
   * wrapper itself owns the one-way registry reference back to this job. */
  lua_newtable(L);
  lua_newtable(L);
  lua_pushliteral(L, "__mode");
  lua_pushliteral(L, "v");
  lua_rawset(L, -3);
  lua_setmetatable(L, -2);
  lua_setiuservalue(L, -2, 1);
  return 1;
}

static int lcdc_push_history_consumer(lua_State *L,
                                      lc_history_consumer *consumer) {
  lcdc_history_consumer_ud *ud;

  ud = (lcdc_history_consumer_ud *)lua_newuserdata(L, sizeof(*ud));
  ud->consumer = consumer;
  luaL_getmetatable(L, LCDC_HISTORY_CONSUMER_MT);
  lua_setmetatable(L, -2);
  return 1;
}

static int lcdc_push_cloned_lease(lua_State *L, const lc_lease *lease) {
  lc_lease *lease_copy;

  if (lease == NULL) {
    lua_pushnil(L);
    return 1;
  }
  lease_copy = lc_lease_clone(lease);
  if (lease_copy == NULL) {
    return luaL_error(L, "failed to clone lease");
  }
  return lcdc_push_lease(L, lease_copy, 0);
}

static void lcdc_parse_lease_ref(lua_State *L, int index, lc_lease_ref *lease) {
  lcdc_lease_ud *lease_ud;

  lc_lease_ref_init(lease);
  if (luaL_testudata(L, index, LCDC_LEASE_MT) != NULL) {
    lease_ud = (lcdc_lease_ud *)luaL_checkudata(L, index, LCDC_LEASE_MT);
    luaL_argcheck(L, lease_ud->lease != NULL, index, "lease is closed");
    lease->ns = lease_ud->lease->ns;
    lease->key = lease_ud->lease->key;
    lease->lease_id = lease_ud->lease->lease_id;
    lease->txn_id = lease_ud->lease->txn_id;
    lease->fencing_token = lease_ud->lease->fencing_token;
    return;
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_require_string_field(L, index, "namespace", &lease->ns);
  lcdc_require_string_field(L, index, "key", &lease->key);
  lcdc_require_string_field(L, index, "lease_id", &lease->lease_id);
  lease->txn_id = lcdc_opt_string_field(L, index, "txn_id");
  lcdc_opt_integer_field(L, index, "fencing_token", &lease->fencing_token);
}

static void lcdc_parse_message_ref(lua_State *L, int index,
                                   lc_message_ref *message) {
  lcdc_message_ud *message_ud;

  lc_message_ref_init(message);
  if (luaL_testudata(L, index, LCDC_MESSAGE_MT) != NULL) {
    message_ud = (lcdc_message_ud *)luaL_checkudata(L, index, LCDC_MESSAGE_MT);
    luaL_argcheck(L, message_ud->message != NULL, index, "message is closed");
    message->ns = message_ud->message->ns;
    message->queue = message_ud->message->queue;
    message->message_id = message_ud->message->message_id;
    message->lease_id = message_ud->message->lease_id;
    message->txn_id = message_ud->message->txn_id;
    message->fencing_token = message_ud->message->fencing_token;
    message->meta_etag = message_ud->message->meta_etag;
    return;
  }
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_require_string_field(L, index, "namespace", &message->ns);
  lcdc_require_string_field(L, index, "queue", &message->queue);
  lcdc_require_string_field(L, index, "message_id", &message->message_id);
  lcdc_require_string_field(L, index, "lease_id", &message->lease_id);
  message->txn_id = lcdc_opt_string_field(L, index, "txn_id");
  lcdc_opt_integer_field(L, index, "fencing_token", &message->fencing_token);
  message->meta_etag = lcdc_opt_string_field(L, index, "meta_etag");
  message->state_etag = lcdc_opt_string_field(L, index, "state_etag");
  message->state_lease_id = lcdc_opt_string_field(L, index, "state_lease_id");
  lcdc_opt_integer_field(L, index, "state_fencing_token",
                         &message->state_fencing_token);
}

static void lcdc_parse_attachment_selector(lua_State *L, int index,
                                           lc_attachment_selector *selector) {
  lc_attachment_selector_init(selector);
  luaL_checktype(L, index, LUA_TTABLE);
  selector->id = lcdc_opt_string_field(L, index, "id");
  selector->name = lcdc_opt_string_field(L, index, "name");
}

static void lcdc_push_attachment_info(lua_State *L,
                                      const lc_attachment_info *info) {
  lua_newtable(L);
  lcdc_set_string_field(L, "id", info->id);
  lcdc_set_string_field(L, "name", info->name);
  lcdc_set_integer_field(L, "size", info->size);
  lcdc_set_string_field(L, "plaintext_sha256", info->plaintext_sha256);
  lcdc_set_string_field(L, "content_type", info->content_type);
  lcdc_set_integer_field(L, "created_at_unix", info->created_at_unix);
  lcdc_set_integer_field(L, "updated_at_unix", info->updated_at_unix);
}

static int lcdc_client_gc(lua_State *L) {
  lcdc_client_ud *ud;

  ud = (lcdc_client_ud *)luaL_checkudata(L, 1, LCDC_CLIENT_MT);
  if (ud->client != NULL) {
    lcdc_outbox_dispatcher_bindings_note_client_stopped(L, 1);
    lc_client_close(ud->client);
    ud->client = NULL;
  }
  return 0;
}

static int lcdc_lease_gc(lua_State *L) {
  lcdc_lease_ud *ud;

  ud = (lcdc_lease_ud *)luaL_checkudata(L, 1, LCDC_LEASE_MT);
  if (ud->borrowed_job != NULL && ud->borrowed_job->borrowed_state == ud) {
    ud->borrowed_job->borrowed_state = NULL;
  }
  ud->borrowed_job = NULL;
  if (ud->lease != NULL) {
    if (ud->owner_ref == LUA_NOREF && !ud->borrowed) {
      lc_lease_close(ud->lease);
    }
    ud->lease = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static void lcdc_message_invalidate_borrowed_state(lcdc_message_ud *ud) {
  if (ud->borrowed_state != NULL) {
    ud->borrowed_state->lease = NULL;
    ud->borrowed_state = NULL;
  }
}

static int lcdc_message_gc(lua_State *L) {
  lcdc_message_ud *ud;

  ud = (lcdc_message_ud *)luaL_checkudata(L, 1, LCDC_MESSAGE_MT);
  lcdc_message_invalidate_borrowed_state(ud);
  if (ud->message != NULL && !ud->borrowed) {
    lc_message_close(ud->message);
    ud->message = NULL;
  }
  return 0;
}

static int lcdc_outbox_gc(lua_State *L) {
  lcdc_outbox_ud *ud = (lcdc_outbox_ud *)luaL_checkudata(L, 1, LCDC_OUTBOX_MT);
  if (ud->outbox != NULL) {
    lc_outbox_close(ud->outbox);
    ud->outbox = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_outbox_dispatcher_gc(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = (lcdc_outbox_dispatcher_ud *)luaL_checkudata(
      L, 1, LCDC_OUTBOX_DISPATCHER_MT);
  if (ud->dispatcher != NULL) {
    lc_outbox_dispatcher_close(ud->dispatcher);
    ud->dispatcher = NULL;
  }
  /* A closed wrapper must not retain an adapter closure after its native
   * binding has been released. Other live aliases retain their own uservalue
   * and the shared registry owner until they close. */
  lua_pushnil(L);
  lua_setiuservalue(L, 1, 1);
  lcdc_outbox_dispatcher_binding_release(L, ud->binding);
  ud->binding = NULL;
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_outbox_txn_gc(lua_State *L) {
  lcdc_outbox_txn_ud *ud =
      (lcdc_outbox_txn_ud *)luaL_checkudata(L, 1, LCDC_OUTBOX_TXN_MT);
  if (ud->transaction != NULL) {
    lc_outbox_transaction_close(ud->transaction);
    ud->transaction = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  if (ud->callback_duplicate_result_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->callback_duplicate_result_ref);
    ud->callback_duplicate_result_ref = LUA_NOREF;
  }
  if (ud->xa_owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->xa_owner_ref);
    ud->xa_owner_ref = LUA_NOREF;
  }
  ud->xa_control = NULL;
  return 0;
}

static int lcdc_outbox_participant_gc(lua_State *L) {
  lcdc_outbox_participant_ud *ud =
      (lcdc_outbox_participant_ud *)luaL_checkudata(L, 1,
                                                    LCDC_OUTBOX_PARTICIPANT_MT);
  if (ud->participant != NULL) {
    lc_outbox_participant_close(ud->participant);
    ud->participant = NULL;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  ud->transaction_ud = NULL;
  return 0;
}

static void lcdc_outbox_job_invalidate_borrowed_state(lua_State *L,
                                                      lcdc_outbox_job_ud *ud) {
  if (ud->borrowed_state != NULL) {
    ud->borrowed_state->lease = NULL;
    ud->borrowed_state->borrowed_job = NULL;
    if (ud->borrowed_state->owner_ref != LUA_NOREF) {
      luaL_unref(L, LUA_REGISTRYINDEX, ud->borrowed_state->owner_ref);
      ud->borrowed_state->owner_ref = LUA_NOREF;
    }
    ud->borrowed_state = NULL;
  }
}

static int lcdc_outbox_job_gc(lua_State *L) {
  lcdc_outbox_job_ud *ud =
      (lcdc_outbox_job_ud *)luaL_checkudata(L, 1, LCDC_OUTBOX_JOB_MT);

  lcdc_outbox_job_invalidate_borrowed_state(L, ud);
  if (ud->job != NULL) {
    lc_outbox_job_close(ud->job);
    ud->job = NULL;
  }
  if (ud->terminal_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->terminal_ref);
    ud->terminal_ref = LUA_NOREF;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  return 0;
}

static int lcdc_history_consumer_gc(lua_State *L) {
  lcdc_history_consumer_ud *ud;

  ud = (lcdc_history_consumer_ud *)luaL_checkudata(L, 1,
                                                   LCDC_HISTORY_CONSUMER_MT);
  if (ud->consumer != NULL) {
    lc_history_consumer_close(ud->consumer);
    ud->consumer = NULL;
  }
  return 0;
}

static int lcdc_pouch_settings_check_keys(lua_State *L, int index,
                                          lc_error *error) {
  lua_pushnil(L);
  while (lua_next(L, index) != 0) {
    const char *name;

    if (lua_type(L, -2) != LUA_TSTRING) {
      lua_pop(L, 2);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch settings keys must be strings", NULL, NULL,
                          "pouch");
    }
    name = lua_tostring(L, -2);
    if (strcmp(name, "single_writer") != 0 &&
        strcmp(name, "durable_sync") != 0 &&
        strcmp(name, "fsync_batch_max_ops") != 0 &&
        strcmp(name, "segment_target_bytes") != 0 &&
        strcmp(name, "indexer_flush_docs") != 0 &&
        strcmp(name, "indexer_flush_interval_seconds") != 0 &&
        strcmp(name, "background_compaction") != 0 &&
        strcmp(name, "disable_compaction_throttling") != 0 &&
        strcmp(name, "terminal_reclaim_min_bytes") != 0 &&
        strcmp(name, "queue_watch") != 0 && strcmp(name, "query_engine") != 0 &&
        strcmp(name, "query_fallback_engine") != 0 &&
        strcmp(name, "query_indexing") != 0 &&
        strcmp(name, "crypto_key") != 0 &&
        strcmp(name, "crypto_key_file") != 0 &&
        strcmp(name, "crypto_generate_key_file") != 0 &&
        strcmp(name, "compression") != 0 &&
        strcmp(name, "disable_migration") != 0) {
      lua_pop(L, 2);
      return lc_error_set(error, LC_ERR_INVALID, 0L, "unknown pouch setting",
                          name, NULL, "pouch");
    }
    lua_pop(L, 1);
  }
  return LC_OK;
}

static int lcdc_pouch_setting_boolean(lua_State *L, int index, const char *name,
                                      int *out, int *present, lc_error *error) {
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    *present = 0;
    return LC_OK;
  }
  if (!lua_isboolean(L, -1)) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch setting must be a boolean", name, NULL, "pouch");
  }
  *out = lua_toboolean(L, -1) ? 1 : 0;
  *present = 1;
  lua_pop(L, 1);
  return LC_OK;
}

static int lcdc_pouch_setting_u64(lua_State *L, int index, const char *name,
                                  uint64_t *out, int *present,
                                  lc_error *error) {
  lua_Integer value;

  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    *present = 0;
    return LC_OK;
  }
  if (!lua_isinteger(L, -1)) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch setting must be a non-negative integer", name,
                        NULL, "pouch");
  }
  value = lua_tointeger(L, -1);
  if (value < 0) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch setting must be a non-negative integer", name,
                        NULL, "pouch");
  }
  *out = (uint64_t)value;
  *present = 1;
  lua_pop(L, 1);
  return LC_OK;
}

static int lcdc_pouch_setting_string(lua_State *L, int index, const char *name,
                                     int root_index, const char **out,
                                     int *present, lc_error *error) {
  index = lua_absindex(L, index);
  root_index = lua_absindex(L, root_index);
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    *present = 0;
    return LC_OK;
  }
  if (lua_type(L, -1) != LUA_TSTRING) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch setting must be a string", name, NULL, "pouch");
  }
  /* The source table may synthesize an otherwise unreferenced string through
   * __index.  Copy it into the plain root before any later field lookup can
   * allocate and collect that source value. */
  lua_pushvalue(L, -1);
  lua_setfield(L, root_index, name);
  lua_getfield(L, root_index, name);
  *out = lua_tostring(L, -1);
  lua_pop(L, 1);
  *present = 1;
  lua_pop(L, 1);
  return LC_OK;
}

static int lcdc_parse_pouch_settings(lua_State *L, int index,
                                     lc_pouch_settings *settings,
                                     lc_error *error) {
  const char *value;
  int root_index;
  int present;
  int rc;

  index = lua_absindex(L, index);
  lc_pouch_settings_init(settings);
  /* Keep selected string settings rooted until lc_client_open() has copied
   * them.  Metatable-backed fields are permitted, but their temporary strings
   * must never escape a Lua stack lifetime. */
  lua_newtable(L);
  root_index = lua_gettop(L);
  rc = lcdc_pouch_settings_check_keys(L, index, error);
  if (rc != LC_OK)
    return rc;
  rc = lcdc_pouch_setting_boolean(L, index, "single_writer",
                                  &settings->single_writer, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_SINGLE_WRITER;
  rc = lcdc_pouch_setting_boolean(L, index, "durable_sync",
                                  &settings->durable_sync, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_DURABLE_SYNC;
  rc = lcdc_pouch_setting_u64(L, index, "fsync_batch_max_ops",
                              &settings->fsync_batch_max_ops, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_FSYNC_BATCH_MAX_OPS;
  rc = lcdc_pouch_setting_u64(L, index, "segment_target_bytes",
                              &settings->segment_target_bytes, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_SEGMENT_TARGET_BYTES;
  rc = lcdc_pouch_setting_u64(L, index, "indexer_flush_docs",
                              &settings->indexer_flush_docs, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_INDEXER_FLUSH_DOCS;
  rc = lcdc_pouch_setting_u64(L, index, "indexer_flush_interval_seconds",
                              &settings->indexer_flush_interval_seconds,
                              &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_INDEXER_FLUSH_INTERVAL_SECONDS;
  rc = lcdc_pouch_setting_boolean(L, index, "background_compaction",
                                  &settings->background_compaction_enabled,
                                  &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_BACKGROUND_COMPACTION;
  rc = lcdc_pouch_setting_boolean(L, index, "disable_compaction_throttling",
                                  &settings->compaction_throttling_disabled,
                                  &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_DISABLE_COMPACTION_THROTTLING;
  rc = lcdc_pouch_setting_u64(L, index, "terminal_reclaim_min_bytes",
                              &settings->terminal_reclaim_min_bytes, &present,
                              error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_TERMINAL_RECLAIM_MIN_BYTES;
  rc = lcdc_pouch_setting_boolean(L, index, "queue_watch",
                                  &settings->queue_watch, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_QUEUE_WATCH;
  rc = lcdc_pouch_setting_string(L, index, "query_engine", root_index, &value,
                                 &present, error);
  if (rc != LC_OK)
    return rc;
  if (present) {
    settings->query_engine = value;
    settings->set_mask |= LC_POUCH_SETTING_QUERY_ENGINE;
  }
  rc = lcdc_pouch_setting_string(L, index, "query_fallback_engine", root_index,
                                 &value, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present) {
    settings->query_fallback_engine = value;
    settings->set_mask |= LC_POUCH_SETTING_QUERY_FALLBACK_ENGINE;
  }
  rc = lcdc_pouch_setting_boolean(L, index, "query_indexing",
                                  &settings->query_indexing_enabled, &present,
                                  error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_QUERY_INDEXING;
  rc = lcdc_pouch_setting_string(L, index, "crypto_key", root_index, &value,
                                 &present, error);
  if (rc != LC_OK)
    return rc;
  if (present) {
    settings->crypto_key = value;
    settings->set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
  }
  rc = lcdc_pouch_setting_string(L, index, "crypto_key_file", root_index,
                                 &value, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present) {
    settings->crypto_key_file = value;
    settings->set_mask |= LC_POUCH_SETTING_CRYPTO_KEY_FILE;
  }
  rc = lcdc_pouch_setting_boolean(L, index, "crypto_generate_key_file",
                                  &settings->crypto_generate_key_file, &present,
                                  error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_CRYPTO_GENERATE_KEY_FILE;
  rc = lcdc_pouch_setting_string(L, index, "compression", root_index, &value,
                                 &present, error);
  if (rc != LC_OK)
    return rc;
  if (present) {
    settings->compression = value;
    settings->set_mask |= LC_POUCH_SETTING_COMPRESSION;
  }
  rc =
      lcdc_pouch_setting_boolean(L, index, "disable_migration",
                                 &settings->disable_migration, &present, error);
  if (rc != LC_OK)
    return rc;
  if (present)
    settings->set_mask |= LC_POUCH_SETTING_DISABLE_MIGRATION;
  return LC_OK;
}

static int lcdc_open(lua_State *L) {
  lc_client_config config;
  lc_error error;
  lc_client *client;
  lc_source *client_bundle_source;
  lc_pouch_settings pouch_settings;
  size_t i;
  size_t endpoint_count;
  const char **endpoints;
  int rc;

  lc_client_config_init(&config);
  lc_error_init(&error);
  client = NULL;
  client_bundle_source = NULL;
  lc_pouch_settings_init(&pouch_settings);
  endpoints = NULL;
  endpoint_count = 0U;
  luaL_checktype(L, 1, LUA_TTABLE);
  lcdc_reject_field(L, 1, "client_bundle_path",
                    "lockdc.open uses client_bundle_source; "
                    "client_bundle_path is not supported");
  lcdc_reject_field(L, 1, "pouch_crypto_key",
                    "lockdc.open uses pouch.crypto_key; "
                    "pouch_crypto_key is not supported");
  lcdc_reject_field(L, 1, "pouch_crypto_key_file",
                    "lockdc.open uses pouch.crypto_key_file; "
                    "pouch_crypto_key_file is not supported");
  lcdc_reject_field(L, 1, "pouch_crypto_generate_key_file",
                    "lockdc.open uses pouch.crypto_generate_key_file; "
                    "pouch_crypto_generate_key_file is not supported");
  lcdc_reject_field(L, 1, "pouch_compression",
                    "lockdc.open uses pouch.compression; "
                    "pouch_compression is not supported");
  lua_getfield(L, 1, "endpoints");
  if (!lua_isnil(L, -1)) {
    luaL_checktype(L, -1, LUA_TTABLE);
    endpoint_count = (size_t)lua_rawlen(L, -1);
    if (endpoint_count != 0U) {
      endpoints = (const char **)calloc(endpoint_count, sizeof(*endpoints));
      if (endpoints == NULL) {
        lua_pop(L, 1);
        luaL_error(L, "out of memory");
      }
      for (i = 0U; i < endpoint_count; ++i) {
        lua_rawgeti(L, -1, (lua_Integer)(i + 1U));
        endpoints[i] = luaL_checkstring(L, -1);
        if ((strncmp(endpoints[i], "pouch://", 8U) == 0 ||
             strncmp(endpoints[i], "pouch+multifile://",
                     sizeof("pouch+multifile://") - 1U) == 0) &&
            strchr(endpoints[i], '?') != NULL) {
          lua_pop(L, 2);
          free(endpoints);
          lc_error_set(&error, LC_ERR_INVALID, 0L,
                       "Pouch endpoint options are not supported by Lua; "
                       "use the pouch settings table",
                       NULL, NULL, "endpoints");
          lcdc_push_status_error(L, LC_ERR_INVALID, &error);
          lc_error_cleanup(&error);
          return 3;
        }
        lua_pop(L, 1);
      }
      config.endpoints = endpoints;
      config.endpoint_count = endpoint_count;
    }
  }
  lua_pop(L, 1);
  config.unix_socket_path = lcdc_opt_string_field(L, 1, "unix_socket_path");
  lua_getfield(L, 1, "client_bundle_source");
  if (!lua_isnil(L, -1)) {
    rc = lcdc_source_from_value(L, -1, &client_bundle_source, &error);
    if (rc != LC_OK) {
      lua_pop(L, 1);
      free(endpoints);
      lcdc_push_status_error(L, rc, &error);
      lc_error_cleanup(&error);
      return 3;
    }
    config.client_bundle_source = client_bundle_source;
  }
  lua_pop(L, 1);
  config.default_namespace = lcdc_opt_string_field(L, 1, "default_namespace");
  lcdc_opt_integer_field(L, 1, "timeout_ms", &config.timeout_ms);
  lcdc_opt_boolean_field(L, 1, "disable_mtls", &config.disable_mtls);
  lcdc_opt_boolean_field(L, 1, "insecure_skip_verify",
                         &config.insecure_skip_verify);
  lcdc_opt_boolean_field(L, 1, "prefer_http_2", &config.prefer_http_2);
  lcdc_opt_boolean_field(L, 1, "disable_logger_sys_field",
                         &config.disable_logger_sys_field);
  lua_getfield(L, 1, "pouch");
  if (!lua_isnil(L, -1)) {
    if (!lua_istable(L, -1)) {
      lua_pop(L, 1);
      lc_source_close(client_bundle_source);
      free(endpoints);
      lc_error_set(&error, LC_ERR_INVALID, 0L, "pouch settings must be a table",
                   NULL, NULL, "pouch");
      lcdc_push_status_error(L, LC_ERR_INVALID, &error);
      lc_error_cleanup(&error);
      return 3;
    }
    rc = lcdc_parse_pouch_settings(L, lua_gettop(L), &pouch_settings, &error);
    if (rc != LC_OK) {
      lua_pop(L, 2);
      lc_source_close(client_bundle_source);
      free(endpoints);
      lcdc_push_status_error(L, rc, &error);
      lc_error_cleanup(&error);
      return 3;
    }
    /* Discard the user-controlled table but retain the normalized table left
     * above it by the parser until lc_client_open() copies its strings. */
    lua_remove(L, -2);
    config.pouch_settings = &pouch_settings;
  }
  {
    long limit;

    limit = 0L;
    if (lcdc_opt_integer_field(L, 1, "http_json_response_limit_bytes",
                               &limit)) {
      config.http_json_response_limit_bytes = (size_t)limit;
    }
  }
  rc = lc_client_open(&config, &client, &error);
  lua_pop(L, 1);
  lc_source_close(client_bundle_source);
  free(endpoints);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_client(L, client);
}

static int lcdc_allocator_tracking_configure(lua_State *L) {
  lc_error error;
  int enabled;
  int rc;

  if (lua_type(L, 1) == LUA_TBOOLEAN)
    enabled = lua_toboolean(L, 1);
  else if (lua_type(L, 1) == LUA_TNUMBER &&
           (lua_tonumber(L, 1) == 0 || lua_tonumber(L, 1) == 1))
    enabled = (int)lua_tonumber(L, 1);
  else
    return luaL_argerror(L, 1, "expected a boolean or 0/1");
  lc_error_init(&error);
  rc = lc_allocator_tracking_configure(enabled, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  return 1;
}

static int lcdc_version_string(lua_State *L) {
  lua_pushstring(L, lc_version_string());
  return 1;
}

static int lcdc_xid_new(lua_State *L) {
  char xid[LC_XID_STRING_SIZE];
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_xid_new(xid, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushstring(L, xid);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_pouch_crypto_generate_key(lua_State *L) {
  char *key_string;
  lc_error error;
  int rc;

  key_string = NULL;
  lc_error_init(&error);
  rc = lc_pouch_crypto_generate_key_string(&key_string, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushstring(L, key_string);
  lc_pouch_crypto_key_string_free(key_string);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_pouch_crypto_default_key_file(lua_State *L) {
  char *path;
  lc_error error;
  int rc;

  path = NULL;
  lc_error_init(&error);
  rc = lc_pouch_crypto_default_key_file(&path, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushstring(L, path);
  lc_pouch_crypto_key_string_free(path);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_pouch_crypto_generate_key_file(lua_State *L) {
  const char *path;
  char *key_string;
  lc_error error;
  int overwrite;
  int rc;

  path = luaL_checkstring(L, 1);
  overwrite = lua_toboolean(L, 2);
  key_string = NULL;
  lc_error_init(&error);
  rc = lc_pouch_crypto_generate_key_file(path, overwrite, &key_string, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushstring(L, key_string);
  lc_pouch_crypto_key_string_free(key_string);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_info(lua_State *L) {
  lcdc_client_ud *ud;

  ud = lcdc_check_client(L, 1);
  lua_newtable(L);
  lcdc_set_string_field(L, "default_namespace", ud->client->default_namespace);
  return 1;
}

static int lcdc_client_close(lua_State *L) {
  lcdc_client_ud *ud;

  ud = lcdc_check_client(L, 1);
  luaL_argcheck(L, !ud->callback_active, 1,
                "lockdc client cannot close during a native callback");
  return lcdc_client_gc(L);
}

static int lcdc_client_acquire(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_acquire_req req;
  lc_error error;
  lc_lease *lease;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_acquire_req_init(&req);
  lc_error_init(&error);
  lease = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  req.ns = lcdc_opt_string_field(L, 2, "namespace");
  lcdc_require_string_field(L, 2, "key", &req.key);
  lcdc_require_string_field(L, 2, "owner", &req.owner);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  lcdc_opt_integer_field(L, 2, "block_seconds", &req.block_seconds);
  lcdc_opt_boolean_field(L, 2, "if_not_exists", &req.if_not_exists);
  req.txn_id = lcdc_opt_string_field(L, 2, "txn_id");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_acquire(client, &req, &lease, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_lease(L, lease, 0);
}

static const char *lcdc_lua_error_message(lua_State *L, int index) {
  const char *message;

  if (index < 0) {
    index = lua_gettop(L) + index + 1;
  }
  if (lua_istable(L, index)) {
    /* Handler-supplied error objects are untrusted Lua values. Raw access
     * keeps an __index metamethod from longjmping past callback cleanup. */
    lua_pushliteral(L, "message");
    lua_rawget(L, index);
    message = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (message != NULL) {
      return message;
    }
  }
  return lua_tostring(L, index);
}

static int lcdc_lua_error_to_lc_error(lua_State *L, int index,
                                      lc_error *error) {
  const char *message;
  const char *detail;
  const char *server_code;
  const char *correlation_id;
  long http_status;
  int code;
  int rc;

  if (index < 0) {
    index = lua_gettop(L) + index + 1;
  }
  if (!lua_istable(L, index)) {
    message = lcdc_lua_error_message(L, index);
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        message != NULL ? message : "Lua acquire_for_update handler failed",
        NULL, NULL, NULL);
  }

  code = LC_ERR_INVALID;
  http_status = 0L;

  lua_pushliteral(L, "code");
  lua_rawget(L, index);
  if (lua_isnumber(L, -1)) {
    code = (int)lua_tointeger(L, -1);
  }
  lua_pop(L, 1);
  if (code == LC_OK) {
    code = LC_ERR_INVALID;
  }

  lua_pushliteral(L, "http_status");
  lua_rawget(L, index);
  if (lua_isnumber(L, -1)) {
    http_status = (long)lua_tointeger(L, -1);
  }
  lua_pop(L, 1);

  lua_pushliteral(L, "message");
  lua_rawget(L, index);
  message = lua_tostring(L, -1);
  lua_pushliteral(L, "detail");
  lua_rawget(L, index);
  detail = lua_tostring(L, -1);
  lua_pushliteral(L, "server_code");
  lua_rawget(L, index);
  server_code = lua_tostring(L, -1);
  lua_pushliteral(L, "correlation_id");
  lua_rawget(L, index);
  correlation_id = lua_tostring(L, -1);

  rc = lc_error_set(error, code, http_status,
                    message != NULL ? message
                                    : "Lua acquire_for_update handler failed",
                    detail, server_code, correlation_id);
  lua_pop(L, 4);
  return rc;
}

static int lcdc_acquire_for_update_handler_call(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  lcdc_acquire_for_update_handler *handler;
  lua_State *L;
  lc_sink *sink;
  const void *bytes;
  const char *message;
  size_t length;
  size_t written;
  lcdc_lease_ud *lease_ud;
  int rc;

  handler = (lcdc_acquire_for_update_handler *)context;
  L = handler->L;
  sink = NULL;
  lua_rawgeti(L, LUA_REGISTRYINDEX, handler->handler_ref);
  lua_newtable(L);
  if (lcdc_push_borrowed_lease(L, update->lease) != 1) {
    lua_pop(L, 2);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to create Lua acquire_for_update lease", NULL,
                        NULL, NULL);
  }
  lease_ud = (lcdc_lease_ud *)luaL_checkudata(L, -1, LCDC_LEASE_MT);
  lua_setfield(L, -2, "lease");
  if (update->state.reader != NULL) {
    rc = lc_sink_to_memory(&sink, error);
    if (rc != LC_OK) {
      lua_pop(L, 2);
      lease_ud->lease = NULL;
      return rc;
    }
    rc = lc_copy(update->state.reader, sink, &written, error);
    (void)written;
    if (rc != LC_OK) {
      lc_sink_close(sink);
      lua_pop(L, 2);
      lease_ud->lease = NULL;
      return rc;
    }
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    if (rc != LC_OK) {
      lc_sink_close(sink);
      lua_pop(L, 2);
      lease_ud->lease = NULL;
      return rc;
    }
    lua_pushlstring(L, (const char *)bytes, length);
    lc_sink_close(sink);
    sink = NULL;
  } else {
    lua_pushnil(L);
  }
  lua_setfield(L, -2, "state");
  lua_newtable(L);
  lcdc_set_bool_field(L, "has_state", update->state.has_state);
  lcdc_set_bool_field(L, "no_content", !update->state.has_state);
  lcdc_set_string_field(L, "content_type", update->state.content_type);
  lcdc_set_string_field(L, "etag", update->state.etag);
  lcdc_set_version_field(L, "version", update->state.version);
  lcdc_set_integer_field(L, "fencing_token", update->state.fencing_token);
  lcdc_set_string_field(L, "correlation_id", update->state.correlation_id);
  lcdc_set_state_retention(L, update->state.retention_support,
                           update->state.has_retention_expires_at_unix,
                           update->state.retention_expires_at_unix);
  lua_setfield(L, -2, "state_meta");

  if (lua_pcall(L, 1, 2, 0) != 0) {
    message = lcdc_lua_error_message(L, -1);
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      message != NULL ? message
                                      : "Lua acquire_for_update handler failed",
                      NULL, NULL, NULL);
    lua_pop(L, 1);
    lease_ud->lease = NULL;
    return rc;
  }
  if ((!lua_isnil(L, -1)) || (lua_isboolean(L, -2) && !lua_toboolean(L, -2)) ||
      lua_isstring(L, -2)) {
    rc = lcdc_lua_error_to_lc_error(L, !lua_isnil(L, -1) ? -1 : -2, error);
    lua_pop(L, 2);
    lease_ud->lease = NULL;
    return rc;
  }
  lua_pop(L, 2);
  lease_ud->lease = NULL;
  return LC_OK;
}

static int lcdc_client_acquire_for_update(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_acquire_req req;
  lc_error error;
  lcdc_acquire_for_update_handler handler;
  int rc;
  int callback_active;

  ud = lcdc_check_client(L, 1);
  client = NULL;
  lc_acquire_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  luaL_checktype(L, 3, LUA_TFUNCTION);
  req.ns = lcdc_opt_string_field(L, 2, "namespace");
  lcdc_require_string_field(L, 2, "key", &req.key);
  lcdc_require_string_field(L, 2, "owner", &req.owner);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  lcdc_opt_integer_field(L, 2, "block_seconds", &req.block_seconds);
  lcdc_opt_boolean_field(L, 2, "if_not_exists", &req.if_not_exists);
  req.txn_id = lcdc_opt_string_field(L, 2, "txn_id");
  handler.L = L;
  lua_pushvalue(L, 3);
  handler.handler_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK) {
    callback_active = ud->callback_active;
    ud->callback_active = 1;
    rc = lc_acquire_for_update(
        client, &req, lcdc_acquire_for_update_handler_call, &handler, &error);
    ud->callback_active = callback_active;
  }
  luaL_unref(L, LUA_REGISTRYINDEX, handler.handler_ref);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_describe(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_describe_req req;
  lc_describe_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_describe_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  req.ns = lcdc_opt_string_field(L, 2, "namespace");
  lcdc_require_string_field(L, 2, "key", &req.key);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_describe(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", res.ns);
  lcdc_set_string_field(L, "key", res.key);
  lcdc_set_string_field(L, "owner", res.owner);
  lcdc_set_version_field(L, "version", res.version);
  lcdc_set_string_field(L, "lease_id", res.lease_id);
  lcdc_set_integer_field(L, "lease_expires_at_unix", res.lease_expires_at_unix);
  lcdc_set_integer_field(L, "fencing_token", res.fencing_token);
  lcdc_set_string_field(L, "txn_id", res.txn_id);
  lcdc_set_string_field(L, "state_etag", res.state_etag);
  lcdc_set_string_field(L, "public_state_etag", res.public_state_etag);
  lcdc_set_bool_field(L, "has_query_hidden", res.has_query_hidden);
  lcdc_set_bool_field(L, "query_hidden", res.query_hidden);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lcdc_set_state_retention(L, res.retention_support,
                           res.has_retention_expires_at_unix,
                           res.retention_expires_at_unix);
  lc_describe_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_get(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lcdc_output output;
  lc_get_opts opts;
  lc_get_res res;
  lc_error error;
  const char *key;
  const char *ns;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_get_opts_init(&opts);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "key", &key);
  ns = lcdc_opt_string_field(L, 2, "namespace");
  lcdc_opt_boolean_field(L, 2, "public_read", &opts.public_read);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->streaming = 1;
  rc = ns == NULL ? lc_get(client, key, &opts, output.sink, &res, &error)
                  : lc_get_in_namespace(client, ns, key, &opts, output.sink,
                                        &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_set_bool_field(L, "no_content", res.no_content);
  lcdc_set_string_field(L, "content_type", res.content_type);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_version_field(L, "version", res.version);
  lcdc_set_integer_field(L, "fencing_token", res.fencing_token);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lcdc_set_state_retention(L, res.retention_support,
                           res.has_retention_expires_at_unix,
                           res.retention_expires_at_unix);
  lc_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_client_update(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_update_req req;
  lc_source *src;
  lc_update_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  client = NULL;
  lc_update_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &req.has_retention_seconds,
                                 &req.retention_seconds)) {
    return 3;
  }
  lcdc_parse_lease_ref(L, 2, &req.lease);
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_version_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK) {
    ud->streaming = 1;
    rc = lc_update(client, &req, src, &res, &error);
    lc_source_close(src);
    ud->streaming = 0;
  } else {
    lc_source_close(src);
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_version_field(L, "new_version", res.new_version);
  lcdc_set_string_field(L, "new_state_etag", res.new_state_etag);
  lcdc_set_integer_field(L, "bytes", res.bytes);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lcdc_set_state_retention(L, res.retention_support,
                           res.has_retention_expires_at_unix,
                           res.retention_expires_at_unix);
  lc_update_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_mutate(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_mutate_op req;
  lc_mutate_res res;
  lc_error error;
  const char **mutations;
  size_t mutation_count;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_mutate_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  mutations = NULL;
  mutation_count = 0U;
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &req.has_retention_seconds,
                                 &req.retention_seconds)) {
    return 3;
  }
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  req.mutations = mutations;
  req.mutation_count = mutation_count;
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_version_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_mutate(client, &req, &res, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_version_field(L, "new_version", res.new_version);
  lcdc_set_string_field(L, "new_state_etag", res.new_state_etag);
  lcdc_set_integer_field(L, "bytes", res.bytes);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lcdc_set_state_retention(L, res.retention_support,
                           res.has_retention_expires_at_unix,
                           res.retention_expires_at_unix);
  lc_mutate_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_metadata(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_metadata_op req;
  lc_metadata_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_metadata_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &req.has_retention_seconds,
                                 &req.retention_seconds)) {
    return 3;
  }
  lcdc_parse_lease_ref(L, 2, &req.lease);
  if (lcdc_opt_boolean_field(L, 2, "query_hidden", &req.query_hidden)) {
    req.has_query_hidden = 1;
  }
  if (lcdc_opt_version_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_metadata(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", res.ns);
  lcdc_set_string_field(L, "key", res.key);
  lcdc_set_version_field(L, "version", res.version);
  lcdc_set_bool_field(L, "has_query_hidden", res.has_query_hidden);
  lcdc_set_bool_field(L, "query_hidden", res.query_hidden);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lcdc_set_state_retention(L, res.retention_support,
                           res.has_retention_expires_at_unix,
                           res.retention_expires_at_unix);
  lc_metadata_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_remove(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_remove_op req;
  lc_remove_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_remove_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_version_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_remove(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "removed", res.removed);
  lcdc_set_version_field(L, "new_version", res.new_version);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_remove_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_keepalive(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_keepalive_op req;
  lc_keepalive_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_keepalive_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_keepalive(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_integer_field(L, "lease_expires_at_unix", res.lease_expires_at_unix);
  lcdc_set_version_field(L, "version", res.version);
  lcdc_set_string_field(L, "state_etag", res.state_etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_keepalive_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_release(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_release_op req;
  lc_release_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_release_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_opt_boolean_field(L, 2, "rollback", &req.rollback);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_release(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "released", res.released);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_release_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_attach(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_attach_op req;
  lc_attach_res res;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  client = NULL;
  lc_attach_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_require_string_field(L, 2, "name", &req.name);
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  if (lcdc_opt_integer_field(L, 2, "max_bytes", &req.max_bytes)) {
    req.has_max_bytes = 1;
  }
  lcdc_opt_boolean_field(L, 2, "prevent_overwrite", &req.prevent_overwrite);
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK) {
    ud->streaming = 1;
    rc = lc_attach(client, &req, src, &res, &error);
    lc_source_close(src);
    ud->streaming = 0;
  } else {
    lc_source_close(src);
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_bool_field(L, "noop", res.noop);
  lcdc_set_version_field(L, "version", res.version);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attach_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_list_attachments(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_attachment_list_req req;
  lc_attachment_list res;
  lc_error error;
  size_t i;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_list_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lcdc_opt_boolean_field(L, 2, "public_read", &req.public_read);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_list_attachments(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lua_createtable(L, (int)res.count, 0);
  for (i = 0U; i < res.count; ++i) {
    lcdc_push_attachment_info(L, &res.items[i]);
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
  }
  lua_setfield(L, -2, "items");
  lc_attachment_list_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_get_attachment(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_attachment_get_op req;
  lc_attachment_get_res res;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_get_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &req.selector);
  lua_pop(L, 1);
  lcdc_opt_boolean_field(L, 2, "public_read", &req.public_read);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->streaming = 1;
  rc = lc_get_attachment(client, &req, output.sink, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attachment_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_client_delete_attachment(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_attachment_delete_op req;
  lc_error error;
  int deleted;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_delete_op_init(&req);
  lc_error_init(&error);
  deleted = 0;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &req.selector);
  lua_pop(L, 1);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_delete_attachment(client, &req, &deleted, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, deleted);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_delete_all_attachments(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_attachment_delete_all_op req;
  lc_error error;
  int deleted_count;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_attachment_delete_all_op_init(&req);
  lc_error_init(&error);
  deleted_count = 0;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_lease_ref(L, 2, &req.lease);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_delete_all_attachments(client, &req, &deleted_count, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushinteger(L, (lua_Integer)deleted_count);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_queue_stats(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_queue_stats_req req;
  lc_queue_stats_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_queue_stats_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  req.ns = lcdc_opt_string_field(L, 2, "namespace");
  lcdc_require_string_field(L, 2, "queue", &req.queue);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_queue_stats(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", res.ns);
  lcdc_set_string_field(L, "queue", res.queue);
  lcdc_set_integer_field(L, "waiting_consumers", res.waiting_consumers);
  lcdc_set_integer_field(L, "pending_candidates", res.pending_candidates);
  lcdc_set_integer_field(L, "total_consumers", res.total_consumers);
  lcdc_set_bool_field(L, "has_active_watcher", res.has_active_watcher);
  lcdc_set_bool_field(L, "available", res.available);
  lcdc_set_string_field(L, "head_message_id", res.head_message_id);
  lcdc_set_integer_field(L, "head_enqueued_at_unix", res.head_enqueued_at_unix);
  lcdc_set_integer_field(L, "head_not_visible_until_unix",
                         res.head_not_visible_until_unix);
  lcdc_set_integer_field(L, "head_age_seconds", res.head_age_seconds);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_queue_stats_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static lc_nack_intent lcdc_parse_nack_intent(lua_State *L, int index,
                                             const char *name) {
  const char *value;

  value = lcdc_opt_string_field(L, index, name);
  if (value == NULL) {
    return LC_NACK_INTENT_UNSPECIFIED;
  }
  if (strcmp(value, "failure") == 0) {
    return LC_NACK_INTENT_FAILURE;
  }
  if (strcmp(value, "defer") == 0) {
    return LC_NACK_INTENT_DEFER;
  }
  luaL_error(L, "invalid nack intent '%s'", value);
  return LC_NACK_INTENT_UNSPECIFIED;
}

static int lcdc_client_queue_ack(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_ack_op req;
  lc_ack_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  lc_message_ref_init(&req.message);
  lcdc_parse_message_ref(L, 2, &req.message);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_queue_ack(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "acked", res.acked);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_ack_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_queue_nack(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_nack_op req;
  lc_nack_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_nack_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_message_ref(L, 2, &req.message);
  lcdc_opt_integer_field(L, 2, "delay_seconds", &req.delay_seconds);
  req.intent = lcdc_parse_nack_intent(L, 2, "intent");
  req.last_error_json = lcdc_opt_string_field(L, 2, "last_error_json");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_queue_nack(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "requeued", res.requeued);
  lcdc_set_string_field(L, "meta_etag", res.meta_etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_nack_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_queue_extend(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_extend_op req;
  lc_extend_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_extend_op_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_message_ref(L, 2, &req.message);
  lcdc_opt_integer_field(L, 2, "extend_by_seconds", &req.extend_by_seconds);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_queue_extend(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_integer_field(L, "lease_expires_at_unix", res.lease_expires_at_unix);
  lcdc_set_integer_field(L, "visibility_timeout_seconds",
                         res.visibility_timeout_seconds);
  lcdc_set_string_field(L, "meta_etag", res.meta_etag);
  lcdc_set_integer_field(L, "state_lease_expires_at_unix",
                         res.state_lease_expires_at_unix);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_extend_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static void lcdc_parse_query_req(lua_State *L, int index, lc_query_req *req);

static int lcdc_client_query(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_query_req req;
  lc_query_res res;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  /* Reserve the optional destination slot before adding internal roots: an
   * omitted third argument must remain distinct from our stack-owned table. */
  if (lua_isnone(L, 3))
    lua_pushnil(L);
  lcdc_parse_query_req(L, 2, &req);
  if ((req.selector_lql == NULL || req.selector_lql[0] == '\0') &&
      (req.selector_json == NULL || req.selector_json[0] == '\0')) {
    return luaL_error(L, "query requires selector_lql or selector_json");
  }
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lua_pop(L, 1);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->streaming = 1;
  rc = lc_query(client, &req, output.sink, &res, &error);
  lua_pop(L, 1);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_set_string_field(L, "cursor", res.cursor);
  lcdc_set_string_field(L, "return_mode", res.return_mode);
  rc = lcdc_set_uint64_field(L, "index_seq", res.index_seq, &error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    lcdc_push_status_error(L, rc, &error);
    lc_query_res_cleanup(&res);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_set_string_field(L, "metadata_json", res.metadata_json);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_query_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_query_keys_call(lua_State *L, int function_ref, int argc,
                                lc_error *error) {
  const char *message;

  if (function_ref == LUA_NOREF) {
    lua_pop(L, argc);
    return 1;
  }
  lua_rawgeti(L, LUA_REGISTRYINDEX, function_ref);
  lua_insert(L, -1 - argc);
  if (lua_pcall(L, argc, 0, 0) != 0) {
    message = lua_tostring(L, -1);
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 message != NULL ? message : "Lua query_keys handler failed",
                 NULL, NULL, NULL);
    lua_pop(L, 1);
    return 0;
  }
  return 1;
}

static int lcdc_query_keys_begin(void *context, lc_error *error) {
  lcdc_query_keys_handler *handler = (lcdc_query_keys_handler *)context;

  return lcdc_query_keys_call(handler->L, handler->begin_ref, 0, error);
}

static int lcdc_query_keys_chunk(void *context, const char *bytes, size_t len,
                                 lc_error *error) {
  lcdc_query_keys_handler *handler = (lcdc_query_keys_handler *)context;

  lua_pushlstring(handler->L, bytes, len);
  return lcdc_query_keys_call(handler->L, handler->chunk_ref, 1, error);
}

static int lcdc_query_keys_finish(void *context, lc_error *error) {
  lcdc_query_keys_handler *handler = (lcdc_query_keys_handler *)context;

  return lcdc_query_keys_call(handler->L, handler->finish_ref, 0, error);
}

static void lcdc_query_keys_handler_init(lua_State *L, int index,
                                         lcdc_query_keys_handler *handler) {
  int type;

  handler->L = L;
  handler->begin_ref = LUA_NOREF;
  handler->chunk_ref = LUA_NOREF;
  handler->finish_ref = LUA_NOREF;
  type = lua_type(L, index);
  if (type == LUA_TFUNCTION) {
    lua_pushvalue(L, index);
    handler->chunk_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    return;
  }
  index = lua_absindex(L, index);
  /* Metamethod-backed field lookup may raise a Lua error.  Keep every value
   * on the Lua stack until all three lookups and type checks have succeeded:
   * luaL_error unwinds that stack, whereas a registry reference would survive
   * the longjmp and retain a callback graph indefinitely. */
  luaL_checktype(L, index, LUA_TTABLE);
  lua_getfield(L, index, "begin");
  lua_getfield(L, index, "chunk");
  lua_getfield(L, index, "finish");
  if (!lua_isnil(L, -3))
    luaL_checktype(L, -3, LUA_TFUNCTION);
  luaL_checktype(L, -2, LUA_TFUNCTION);
  if (!lua_isnil(L, -1)) {
    luaL_checktype(L, -1, LUA_TFUNCTION);
  }
  if (!lua_isnil(L, -1)) {
    handler->finish_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  } else {
    lua_pop(L, 1);
  }
  handler->chunk_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  if (!lua_isnil(L, -1)) {
    handler->begin_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  } else {
    lua_pop(L, 1);
  }
}

static void lcdc_query_keys_handler_cleanup(lcdc_query_keys_handler *handler) {
  if (handler->begin_ref != LUA_NOREF) {
    luaL_unref(handler->L, LUA_REGISTRYINDEX, handler->begin_ref);
  }
  if (handler->chunk_ref != LUA_NOREF) {
    luaL_unref(handler->L, LUA_REGISTRYINDEX, handler->chunk_ref);
  }
  if (handler->finish_ref != LUA_NOREF) {
    luaL_unref(handler->L, LUA_REGISTRYINDEX, handler->finish_ref);
  }
  handler->begin_ref = LUA_NOREF;
  handler->chunk_ref = LUA_NOREF;
  handler->finish_ref = LUA_NOREF;
}

/* Leaves a plain Lua table on the stack which owns all request strings until
 * the caller has returned from native code and every Lua callback it invokes.
 */
static void lcdc_parse_query_req(lua_State *L, int index, lc_query_req *req) {
  index = lua_absindex(L, index);
  lc_query_req_init(req);
  lua_createtable(L, 0, 8);
  lcdc_normalize_string_field(L, index, -1, "namespace", 0);
  lcdc_normalize_string_field(L, index, -1, "selector_lql", 0);
  lcdc_normalize_string_field(L, index, -1, "selector_json", 0);
  lcdc_normalize_string_field(L, index, -1, "cursor", 0);
  lcdc_normalize_string_field(L, index, -1, "fields_json", 0);
  lcdc_normalize_string_field(L, index, -1, "return_mode", 0);
  lcdc_normalize_string_field(L, index, -1, "engine", 0);
  lcdc_normalize_string_field(L, index, -1, "refresh", 0);
  lcdc_opt_integer_field(L, index, "limit", &req->limit);
  req->ns = lcdc_normalized_string_value(L, -1, "namespace");
  req->selector_lql = lcdc_normalized_string_value(L, -1, "selector_lql");
  req->selector_json = lcdc_normalized_string_value(L, -1, "selector_json");
  req->cursor = lcdc_normalized_string_value(L, -1, "cursor");
  req->fields_json = lcdc_normalized_string_value(L, -1, "fields_json");
  req->return_mode = lcdc_normalized_string_value(L, -1, "return_mode");
  req->engine = lcdc_normalized_string_value(L, -1, "engine");
  req->refresh = lcdc_normalized_string_value(L, -1, "refresh");
}

static int lcdc_client_query_keys(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_query_req req;
  lc_query_key_handler stream_handler;
  lcdc_query_keys_handler handler;
  lc_query_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  client = NULL;
  lc_query_req_init(&req);
  memset(&stream_handler, 0, sizeof(stream_handler));
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_query_req(L, 2, &req);
  lcdc_query_keys_handler_init(L, 3, &handler);
  stream_handler.begin = lcdc_query_keys_begin;
  stream_handler.chunk = lcdc_query_keys_chunk;
  stream_handler.end = lcdc_query_keys_finish;
  /* Request and handler table lookups may execute Lua metamethods, including
   * client:close(). Re-read the userdata only after those callbacks have
   * returned; retaining a pointer captured before them can dereference a
   * freed client allocation. */
  client = ud->client;
  if (client == NULL) {
    lcdc_query_keys_handler_cleanup(&handler);
    lua_pop(L, 1);
    (void)lc_error_set(&error, LC_ERR_INVALID, 0L,
                       "client was closed while preparing query_keys", NULL,
                       NULL, NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_query_keys(client, &req, &stream_handler, &handler, &res, &error);
  lcdc_query_keys_handler_cleanup(&handler);
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_query_res_cleanup(&res);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "cursor", res.cursor);
  lcdc_set_string_field(L, "return_mode", res.return_mode);
  rc = lcdc_set_uint64_field(L, "index_seq", res.index_seq, &error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    lcdc_push_status_error(L, rc, &error);
    lc_query_res_cleanup(&res);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_set_string_field(L, "metadata_json", res.metadata_json);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_query_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_get_namespace_config(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_namespace_config_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "namespace", &req.ns);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_get_namespace_config(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", res.ns);
  lcdc_set_string_field(L, "preferred_engine", res.preferred_engine);
  lcdc_set_string_field(L, "fallback_engine", res.fallback_engine);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_namespace_config_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_update_namespace_config(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_namespace_config_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "namespace", &req.ns);
  req.preferred_engine = lcdc_opt_string_field(L, 2, "preferred_engine");
  req.fallback_engine = lcdc_opt_string_field(L, 2, "fallback_engine");
  req.if_etag = lcdc_opt_string_field(L, 2, "if_etag");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_update_namespace_config(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", res.ns);
  lcdc_set_string_field(L, "preferred_engine", res.preferred_engine);
  lcdc_set_string_field(L, "fallback_engine", res.fallback_engine);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_namespace_config_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_flush_index(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_index_flush_req req;
  lc_index_flush_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_index_flush_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "namespace", &req.ns);
  req.mode = lcdc_opt_string_field(L, 2, "mode");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_flush_index(client, &req, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", res.ns);
  lcdc_set_string_field(L, "mode", res.mode);
  lcdc_set_string_field(L, "flush_id", res.flush_id);
  lcdc_set_bool_field(L, "accepted", res.accepted);
  lcdc_set_bool_field(L, "flushed", res.flushed);
  lcdc_set_bool_field(L, "pending", res.pending);
  rc = lcdc_set_uint64_field(L, "index_seq", res.index_seq, &error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    lcdc_push_status_error(L, rc, &error);
    lc_index_flush_res_cleanup(&res);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_index_flush_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static void lcdc_free_txn_participants(lc_txn_participant **participants) {
  if (participants != NULL && *participants != NULL) {
    free(*participants);
    *participants = NULL;
  }
}

static void lcdc_parse_txn_participants(lua_State *L, int index,
                                        lc_txn_participant **out,
                                        size_t *count_out) {
  lc_txn_participant *participants;
  int normalized_index;
  int participants_index;
  size_t count;
  size_t i;

  *out = NULL;
  *count_out = 0U;
  lua_getfield(L, index, "participants");
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return;
  }
  luaL_checktype(L, -1, LUA_TTABLE);
  count = (size_t)lua_rawlen(L, -1);
  if (count == 0U) {
    lua_pop(L, 1);
    return;
  }
  participants_index = lua_absindex(L, -1);
  /* Normalize all metatable-backed fields before allocating C-owned storage.
   * Lua argument errors use longjmp, so a second lookup after calloc() could
   * otherwise bypass ordinary cleanup. The private, plain Lua table also keeps
   * the string values alive until the native request completes. */
  lua_createtable(L, (int)count, 0);
  normalized_index = lua_absindex(L, -1);
  for (i = 0U; i < count; ++i) {
    lua_rawgeti(L, participants_index, (lua_Integer)(i + 1U));
    luaL_checktype(L, -1, LUA_TTABLE);
    lua_createtable(L, 0, 3);
    lcdc_normalize_string_field(L, -2, -1, "namespace", 1);
    lcdc_normalize_string_field(L, -2, -1, "key", 1);
    lcdc_normalize_string_field(L, -2, -1, "backend_hash", 0);
    lua_rawseti(L, normalized_index, (lua_Integer)(i + 1U));
    lua_pop(L, 1);
  }
  participants = (lc_txn_participant *)calloc(count, sizeof(*participants));
  if (participants == NULL) {
    luaL_error(L, "out of memory");
  }
  for (i = 0U; i < count; ++i) {
    lua_rawgeti(L, normalized_index, (lua_Integer)(i + 1U));
    lua_getfield(L, -1, "namespace");
    participants[i].ns = lua_tostring(L, -1);
    lua_pop(L, 1);
    lua_getfield(L, -1, "key");
    participants[i].key = lua_tostring(L, -1);
    lua_pop(L, 1);
    lua_getfield(L, -1, "backend_hash");
    participants[i].backend_hash = lua_tostring(L, -1);
    lua_pop(L, 1);
    lua_pop(L, 1);
  }
  /* Keep the normalized table on the stack. The decision parser retains it in
   * its request root so pointers in the C participant array stay valid until
   * the native operation has completed. */
  lua_remove(L, participants_index);
  *out = participants;
  *count_out = count;
}

static int lcdc_parse_txn_decision_req(lua_State *L, int index,
                                       lc_txn_decision_req *req,
                                       lc_txn_participant **participants,
                                       lc_error *error) {
  int root_index;

  index = lua_absindex(L, index);
  lc_txn_decision_req_init(req);
  /* Metamethod-backed request fields can return a string with no other Lua
   * owner. Normalize every pointer-bearing value into this private request
   * root before a later lookup can collect it. The caller pops the root only
   * after the native decision call has finished. */
  lua_createtable(L, 0, 3);
  root_index = lua_absindex(L, -1);
  lua_getfield(L, index, "txn_id");
  if (!lcdc_xa_text(L, -1, 0)) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "txn_id must be a string without NUL bytes", NULL, NULL,
                        NULL);
  }
  lua_setfield(L, root_index, "txn_id");
  lcdc_opt_int64_field(L, index, "expires_at_unix", &req->expires_at_unix);
  lcdc_opt_uint64_field(L, index, "tc_term", &req->tc_term);
  lcdc_normalize_string_field(L, index, root_index, "target_backend_hash", 0);
  lcdc_parse_txn_participants(L, index, participants, &req->participant_count);
  if (*participants != NULL)
    lua_setfield(L, root_index, "participants");
  lua_getfield(L, root_index, "txn_id");
  req->txn_id = lua_tostring(L, -1);
  lua_pop(L, 1);
  lua_getfield(L, root_index, "target_backend_hash");
  req->target_backend_hash = lua_tostring(L, -1);
  lua_pop(L, 1);
  req->participants = *participants;
  return LC_OK;
}

static int lcdc_push_txn_decision_res(lua_State *L,
                                      const lc_txn_decision_res *res,
                                      lc_error *error) {
  lua_newtable(L);
  lcdc_set_string_field(L, "txn_id", res->txn_id);
  lcdc_set_string_field(L, "state", res->state);
  lcdc_set_string_field(L, "correlation_id", res->correlation_id);
  (void)error;
  return LC_OK;
}

typedef struct lcdc_txn_decision_scope {
  lc_txn_decision_res result;
  lc_txn_replay_res replay;
  lc_error error;
} lcdc_txn_decision_scope;
static int lcdc_txn_decision_gc(lua_State *L) {
  lcdc_txn_decision_scope *scope =
      luaL_testudata(L, 1, "lockdc.txn_decision_scope");
  if (scope != NULL) {
    lc_txn_decision_res_cleanup(&scope->result);
    lc_txn_replay_res_cleanup(&scope->replay);
    lc_error_cleanup(&scope->error);
  }
  return 0;
}
static lcdc_txn_decision_scope *lcdc_new_txn_decision_scope(lua_State *L) {
  lcdc_txn_decision_scope *scope = lua_newuserdatauv(L, sizeof(*scope), 0);
  memset(scope, 0, sizeof(*scope));
  lc_error_init(&scope->error);
  luaL_newmetatable(L, "lockdc.txn_decision_scope");
  lua_pushcfunction(L, lcdc_txn_decision_gc);
  lua_setfield(L, -2, "__gc");
  lua_setmetatable(L, -2);
  return scope;
}

static int lcdc_client_txn_replay(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_txn_replay_req req;
  lcdc_txn_decision_scope *scope;
  int rc;
  int count = 1;

  ud = lcdc_check_client(L, 1);
  lc_txn_replay_req_init(&req);
  scope = lcdc_new_txn_decision_scope(L);
  luaL_checktype(L, 2, LUA_TTABLE);
  /* Keep the exact Lua string rooted until the native replay returns. */
  lua_getfield(L, 2, "txn_id");
  if (!lcdc_xa_text(L, -1, 0))
    rc = lc_error_set(&scope->error, LC_ERR_INVALID, 0L,
                      "txn_id must be a string without NUL bytes", NULL, NULL,
                      NULL);
  else {
    req.txn_id = lua_tostring(L, -1);
    rc = lcdc_client_revalidate(ud, &client, &scope->error);
  }
  if (rc == LC_OK)
    rc = lc_txn_replay(client, &req, &scope->replay, &scope->error);
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &scope->error);
    count = scope->replay.txn_id != NULL ? 4 : 3;
  }
  if (rc == LC_OK || count == 4) {
    lua_newtable(L);
    lcdc_set_string_field(L, "txn_id", scope->replay.txn_id);
    lcdc_set_string_field(L, "state", scope->replay.state);
    lcdc_set_string_field(L, "correlation_id", scope->replay.correlation_id);
  }
  lc_txn_replay_res_cleanup(&scope->replay);
  lc_error_cleanup(&scope->error);
  return count;
}

static int lcdc_client_txn_decision(lua_State *L, int operation) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_txn_decision_req req;
  lcdc_txn_decision_scope *scope;
  lc_txn_participant *participants;
  int rc;
  int count;

  ud = lcdc_check_client(L, 1);
  scope = lcdc_new_txn_decision_scope(L);
  participants = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  rc = lcdc_parse_txn_decision_req(L, 2, &req, &participants, &scope->error);
  if (rc == LC_OK)
    rc = lcdc_client_revalidate(ud, &client, &scope->error);
  if (rc == LC_OK && operation == 0) {
    rc = lc_txn_prepare(client, &req, &scope->result, &scope->error);
  } else if (rc == LC_OK && operation == 1) {
    rc = lc_txn_commit(client, &req, &scope->result, &scope->error);
  } else if (rc == LC_OK) {
    rc = lc_txn_rollback(client, &req, &scope->result, &scope->error);
  }
  lcdc_free_txn_participants(&participants);
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &scope->error);
    count = 3;
    /* Preserve C's durable decision on pending application, without treating
     * an uncompleted transaction as a Lua success. The fourth value is the
     * actual result snapshot, not a reconstructed or replayed decision. */
    if (scope->result.txn_id != NULL) {
      lcdc_push_txn_decision_res(L, &scope->result, &scope->error);
      count = 4;
    }
    lc_txn_decision_res_cleanup(&scope->result);
    lc_error_cleanup(&scope->error);
    return count;
  }
  lcdc_push_txn_decision_res(L, &scope->result, &scope->error);
  lc_txn_decision_res_cleanup(&scope->result);
  lc_error_cleanup(&scope->error);
  return 1;
}

static int lcdc_client_txn_prepare(lua_State *L) {
  return lcdc_client_txn_decision(L, 0);
}

static int lcdc_client_txn_commit(lua_State *L) {
  return lcdc_client_txn_decision(L, 1);
}

static int lcdc_client_txn_rollback(lua_State *L) {
  return lcdc_client_txn_decision(L, 2);
}

static int lcdc_push_tc_lease_res(lua_State *L, const char *success_field,
                                  int success, const char *leader_id,
                                  const char *leader_endpoint, uint64_t term,
                                  lc_unix_seconds expires_at_unix,
                                  const char *correlation_id, lc_error *error) {
  int rc;

  lua_newtable(L);
  lcdc_set_bool_field(L, success_field, success);
  lcdc_set_string_field(L, "leader_id", leader_id);
  lcdc_set_string_field(L, "leader_endpoint", leader_endpoint);
  rc = lcdc_set_uint64_field(L, "term", term, error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    return rc;
  }
  rc =
      lcdc_set_unix_seconds_field(L, "expires_at_unix", expires_at_unix, error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    return rc;
  }
  lcdc_set_string_field(L, "correlation_id", correlation_id);
  return LC_OK;
}

static int lcdc_client_tc_lease_acquire(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_tc_lease_acquire_req req;
  lc_tc_lease_acquire_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_tc_lease_acquire_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lua_createtable(L, 0, 2);
  lcdc_normalize_string_field(L, 2, -1, "candidate_id", 1);
  lcdc_normalize_string_field(L, 2, -1, "candidate_endpoint", 1);
  lcdc_opt_uint64_field(L, 2, "term", &req.term);
  lcdc_opt_integer_field(L, 2, "ttl_ms", &req.ttl_ms);
  req.candidate_id = lcdc_normalized_string_value(L, -1, "candidate_id");
  req.candidate_endpoint =
      lcdc_normalized_string_value(L, -1, "candidate_endpoint");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_tc_lease_acquire(client, &req, &res, &error);
  lua_pop(L, 1);
  if (rc == LC_OK) {
    rc = lcdc_push_tc_lease_res(
        L, "granted", res.granted, res.leader_id, res.leader_endpoint, res.term,
        res.expires_at_unix, res.correlation_id, &error);
  }
  lc_tc_lease_acquire_res_cleanup(&res);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_tc_lease_renew(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_tc_lease_renew_req req;
  lc_tc_lease_renew_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_tc_lease_renew_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lua_createtable(L, 0, 1);
  lcdc_normalize_string_field(L, 2, -1, "leader_id", 1);
  lcdc_opt_uint64_field(L, 2, "term", &req.term);
  lcdc_opt_integer_field(L, 2, "ttl_ms", &req.ttl_ms);
  req.leader_id = lcdc_normalized_string_value(L, -1, "leader_id");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_tc_lease_renew(client, &req, &res, &error);
  lua_pop(L, 1);
  if (rc == LC_OK) {
    rc = lcdc_push_tc_lease_res(
        L, "renewed", res.renewed, res.leader_id, res.leader_endpoint, res.term,
        res.expires_at_unix, res.correlation_id, &error);
  }
  lc_tc_lease_renew_res_cleanup(&res);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_tc_lease_release(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_tc_lease_release_req req;
  lc_tc_lease_release_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_tc_lease_release_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lua_createtable(L, 0, 1);
  lcdc_normalize_string_field(L, 2, -1, "leader_id", 1);
  lcdc_opt_uint64_field(L, 2, "term", &req.term);
  req.leader_id = lcdc_normalized_string_value(L, -1, "leader_id");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_tc_lease_release(client, &req, &res, &error);
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_bool_field(L, "released", res.released);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_tc_lease_release_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_tc_leader(lua_State *L) {
  lcdc_client_ud *ud;
  lc_tc_leader_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  rc = lc_tc_leader(ud->client, &res, &error);
  if (rc == LC_OK) {
    lua_newtable(L);
    lcdc_set_string_field(L, "leader_id", res.leader_id);
    lcdc_set_string_field(L, "leader_endpoint", res.leader_endpoint);
    rc = lcdc_set_uint64_field(L, "term", res.term, &error);
    if (rc == LC_OK) {
      rc = lcdc_set_unix_seconds_field(L, "expires_at_unix",
                                       res.expires_at_unix, &error);
    }
    if (rc == LC_OK) {
      lcdc_set_string_field(L, "correlation_id", res.correlation_id);
    } else {
      lua_pop(L, 1);
    }
  }
  lc_tc_leader_res_cleanup(&res);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return 1;
}

static void lcdc_push_string_list(lua_State *L, const lc_string_list *list) {
  size_t i;

  lua_createtable(L, (int)list->count, 0);
  for (i = 0U; i < list->count; ++i) {
    lua_pushstring(L, list->items[i]);
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
  }
}

static int lcdc_push_tc_cluster_res(lua_State *L, const lc_tc_cluster_res *res,
                                    lc_error *error) {
  int rc;

  lua_newtable(L);
  lcdc_push_string_list(L, &res->endpoints);
  lua_setfield(L, -2, "endpoints");
  rc = lcdc_set_unix_seconds_field(L, "updated_at_unix", res->updated_at_unix,
                                   error);
  if (rc == LC_OK)
    rc = lcdc_set_unix_seconds_field(L, "expires_at_unix", res->expires_at_unix,
                                     error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    return rc;
  }
  lcdc_set_string_field(L, "correlation_id", res->correlation_id);
  return LC_OK;
}

static int lcdc_client_tc_cluster(lua_State *L, int operation) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_tc_cluster_announce_req req;
  lc_tc_cluster_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_tc_cluster_announce_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  if (operation == 0) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_require_string_field(L, 2, "self_endpoint", &req.self_endpoint);
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK && operation == 0) {
    rc = lc_tc_cluster_announce(client, &req, &res, &error);
  } else if (rc == LC_OK && operation == 1) {
    rc = lc_tc_cluster_leave(client, &res, &error);
  } else if (rc == LC_OK) {
    rc = lc_tc_cluster_list(client, &res, &error);
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_tc_cluster_res_cleanup(&res);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_push_tc_cluster_res(L, &res, &error);
  lc_tc_cluster_res_cleanup(&res);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_tc_cluster_announce(lua_State *L) {
  return lcdc_client_tc_cluster(L, 0);
}

static int lcdc_client_tc_cluster_leave(lua_State *L) {
  return lcdc_client_tc_cluster(L, 1);
}

static int lcdc_client_tc_cluster_list(lua_State *L) {
  return lcdc_client_tc_cluster(L, 2);
}

static int lcdc_push_tc_rm_res(lua_State *L, const lc_tc_rm_res *res,
                               lc_error *error) {
  int rc;

  lua_newtable(L);
  lcdc_set_string_field(L, "backend_hash", res->backend_hash);
  lcdc_push_string_list(L, &res->endpoints);
  lua_setfield(L, -2, "endpoints");
  rc = lcdc_set_unix_seconds_field(L, "updated_at_unix", res->updated_at_unix,
                                   error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    return rc;
  }
  lcdc_set_string_field(L, "correlation_id", res->correlation_id);
  return LC_OK;
}

static int lcdc_client_tc_rm(lua_State *L, int operation) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_tc_rm_register_req register_req;
  lc_tc_rm_unregister_req unregister_req;
  lc_tc_rm_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_tc_rm_register_req_init(&register_req);
  lc_tc_rm_unregister_req_init(&unregister_req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (operation == 0) {
    lcdc_require_string_field(L, 2, "backend_hash", &register_req.backend_hash);
    lcdc_require_string_field(L, 2, "endpoint", &register_req.endpoint);
  } else {
    lcdc_require_string_field(L, 2, "backend_hash",
                              &unregister_req.backend_hash);
    lcdc_require_string_field(L, 2, "endpoint", &unregister_req.endpoint);
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK && operation == 0) {
    rc = lc_tc_rm_register(client, &register_req, &res, &error);
  } else if (rc == LC_OK) {
    rc = lc_tc_rm_unregister(client, &unregister_req, &res, &error);
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_tc_rm_res_cleanup(&res);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_push_tc_rm_res(L, &res, &error);
  lc_tc_rm_res_cleanup(&res);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_tc_rm_register(lua_State *L) {
  return lcdc_client_tc_rm(L, 0);
}

static int lcdc_client_tc_rm_unregister(lua_State *L) {
  return lcdc_client_tc_rm(L, 1);
}

static int lcdc_client_tc_rm_list(lua_State *L) {
  lcdc_client_ud *ud;
  lc_tc_rm_list_res res;
  lc_error error;
  size_t i;
  int rc;

  ud = lcdc_check_client(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  rc = lc_tc_rm_list(ud->client, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lua_createtable(L, (int)res.backend_count, 0);
  for (i = 0U; i < res.backend_count; ++i) {
    lua_newtable(L);
    lcdc_set_string_field(L, "backend_hash", res.backends[i].backend_hash);
    lcdc_push_string_list(L, &res.backends[i].endpoints);
    lua_setfield(L, -2, "endpoints");
    rc = lcdc_set_unix_seconds_field(L, "updated_at_unix",
                                     res.backends[i].updated_at_unix, &error);
    if (rc != LC_OK) {
      lua_pop(L, 2);
      lc_tc_rm_list_res_cleanup(&res);
      lcdc_push_status_error(L, rc, &error);
      lc_error_cleanup(&error);
      return 3;
    }
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
  }
  lua_setfield(L, -2, "backends");
  rc = lcdc_set_unix_seconds_field(L, "updated_at_unix", res.updated_at_unix,
                                   &error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    lc_tc_rm_list_res_cleanup(&res);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_tc_rm_list_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_enqueue(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_enqueue_req req;
  lc_enqueue_res res;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  client = NULL;
  lc_enqueue_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  req.ns = lcdc_opt_string_field(L, 2, "namespace");
  lcdc_require_string_field(L, 2, "queue", &req.queue);
  lcdc_opt_integer_field(L, 2, "delay_seconds", &req.delay_seconds);
  lcdc_opt_integer_field(L, 2, "visibility_timeout_seconds",
                         &req.visibility_timeout_seconds);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  lcdc_opt_int_field(L, 2, "max_attempts", &req.max_attempts);
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK) {
    ud->streaming = 1;
    rc = lc_enqueue(client, &req, src, &res, &error);
    lc_source_close(src);
    ud->streaming = 0;
  } else {
    lc_source_close(src);
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", res.ns);
  lcdc_set_string_field(L, "queue", res.queue);
  lcdc_set_string_field(L, "message_id", res.message_id);
  lcdc_set_integer_field(L, "attempts", res.attempts);
  lcdc_set_integer_field(L, "max_attempts", res.max_attempts);
  lcdc_set_integer_field(L, "failure_attempts", res.failure_attempts);
  lcdc_set_integer_field(L, "not_visible_until_unix",
                         res.not_visible_until_unix);
  lcdc_set_integer_field(L, "visibility_timeout_seconds",
                         res.visibility_timeout_seconds);
  lcdc_set_integer_field(L, "payload_bytes", res.payload_bytes);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_enqueue_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static void lcdc_parse_dequeue_req(lua_State *L, int index,
                                   lc_dequeue_req *req) {
  long page_size;

  index = lua_absindex(L, index);
  lc_dequeue_req_init(req);
  /* The caller retains this root through dequeue callbacks: the source table
   * may be mutated or collected by application code during delivery. */
  lua_createtable(L, 0, 5);
  lcdc_normalize_string_field(L, index, -1, "namespace", 0);
  lcdc_normalize_string_field(L, index, -1, "queue", 1);
  lcdc_normalize_string_field(L, index, -1, "owner", 0);
  lcdc_normalize_string_field(L, index, -1, "txn_id", 0);
  lcdc_normalize_string_field(L, index, -1, "start_after", 0);
  lcdc_opt_integer_field(L, index, "visibility_timeout_seconds",
                         &req->visibility_timeout_seconds);
  lcdc_opt_integer_field(L, index, "wait_seconds", &req->wait_seconds);
  page_size = 0L;
  if (lcdc_opt_integer_field(L, index, "page_size", &page_size)) {
    req->page_size = (int)page_size;
  }
  req->ns = lcdc_normalized_string_value(L, -1, "namespace");
  req->queue = lcdc_normalized_string_value(L, -1, "queue");
  req->owner = lcdc_normalized_string_value(L, -1, "owner");
  req->txn_id = lcdc_normalized_string_value(L, -1, "txn_id");
  req->start_after = lcdc_normalized_string_value(L, -1, "start_after");
}

static int lcdc_client_dequeue_common(lua_State *L, int with_state) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_dequeue_req req;
  lc_message *message;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_error_init(&error);
  message = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_dequeue_req(L, 2, &req);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK && with_state) {
    rc = lc_dequeue_with_state(client, &req, &message, &error);
  } else if (rc == LC_OK) {
    rc = lc_dequeue(client, &req, &message, &error);
  }
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_message(L, message);
}

static int lcdc_client_dequeue(lua_State *L) {
  return lcdc_client_dequeue_common(L, 0);
}

static int lcdc_client_dequeue_with_state(lua_State *L) {
  return lcdc_client_dequeue_common(L, 1);
}

static int lcdc_lua_consumer_handle(void *context, lc_message *message,
                                    lc_error *error) {
  lcdc_consumer_handler *handler;
  lcdc_message_ud *message_ud;
  lcdc_lease_ud *state_ud;
  lc_lease *state;
  const char *message_text;
  int top;
  int failed;
  int message_ref;
  int state_ref;

  handler = (lcdc_consumer_handler *)context;
  top = lua_gettop(handler->L);
  message_ud = NULL;
  state_ud = NULL;
  message_ref = LUA_NOREF;
  state_ref = LUA_NOREF;
  lua_rawgeti(handler->L, LUA_REGISTRYINDEX, handler->handler_ref);
  lcdc_push_borrowed_message(handler->L, message);
  message_ud =
      (lcdc_message_ud *)luaL_checkudata(handler->L, -1, LCDC_MESSAGE_MT);
  lua_pushvalue(handler->L, -1);
  message_ref = luaL_ref(handler->L, LUA_REGISTRYINDEX);
  if (handler->with_state) {
    state = lc_message_state(message);
    lcdc_push_borrowed_lease(handler->L, state);
    if (state != NULL) {
      state_ud =
          (lcdc_lease_ud *)luaL_checkudata(handler->L, -1, LCDC_LEASE_MT);
      message_ud->borrowed_state = state_ud;
      lua_pushvalue(handler->L, -1);
      state_ref = luaL_ref(handler->L, LUA_REGISTRYINDEX);
    }
  }
  if (lua_pcall(handler->L, handler->with_state ? 2 : 1, 2, 0) != 0) {
    message_text = lua_tostring(handler->L, -1);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       message_text != NULL ? message_text
                                            : "Lua subscribe handler failed",
                       NULL, NULL, NULL);
    lua_settop(handler->L, top);
    message_ud->message = NULL;
    lcdc_message_invalidate_borrowed_state(message_ud);
    luaL_unref(handler->L, LUA_REGISTRYINDEX, message_ref);
    if (state_ref != LUA_NOREF)
      luaL_unref(handler->L, LUA_REGISTRYINDEX, state_ref);
    return LC_ERR_INVALID;
  }
  failed = (lua_isboolean(handler->L, -2) && !lua_toboolean(handler->L, -2)) ||
           (lua_isnil(handler->L, -2) && !lua_isnil(handler->L, -1));
  if (failed) {
    message_text = lcdc_lua_error_message(handler->L, -1);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       message_text != NULL ? message_text
                                            : "Lua subscribe handler failed",
                       NULL, NULL, NULL);
  }
  lua_settop(handler->L, top);
  message_ud->message = NULL;
  lcdc_message_invalidate_borrowed_state(message_ud);
  luaL_unref(handler->L, LUA_REGISTRYINDEX, message_ref);
  if (state_ref != LUA_NOREF)
    luaL_unref(handler->L, LUA_REGISTRYINDEX, state_ref);
  return failed ? LC_ERR_INVALID : LC_OK;
}

static int lcdc_client_subscribe_common(lua_State *L, int with_state) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_consumer consumer;
  lc_dequeue_req req;
  lcdc_consumer_handler handler;
  lc_error error;
  int callback_active;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_consumer_init(&consumer);
  lc_dequeue_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  luaL_checktype(L, 3, LUA_TFUNCTION);
  lcdc_parse_dequeue_req(L, 2, &req);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  handler.L = L;
  handler.with_state = with_state;
  lua_pushvalue(L, 3);
  handler.handler_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  consumer.handle = lcdc_lua_consumer_handle;
  consumer.context = &handler;
  callback_active = ud->callback_active;
  ud->callback_active = 1;
  if (with_state) {
    rc = lc_subscribe_with_state(client, &req, &consumer, &error);
  } else {
    rc = lc_subscribe(client, &req, &consumer, &error);
  }
  ud->callback_active = callback_active;
  luaL_unref(L, LUA_REGISTRYINDEX, handler.handler_ref);
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_subscribe(lua_State *L) {
  return lcdc_client_subscribe_common(L, 0);
}

static int lcdc_client_subscribe_with_state(lua_State *L) {
  return lcdc_client_subscribe_common(L, 1);
}

static int lcdc_lua_watch_handle(void *context, const lc_watch_event *event,
                                 lc_error *error) {
  lcdc_watch_handler *handler;
  const char *message;
  int top;
  int failed;
  int stop;

  handler = (lcdc_watch_handler *)context;
  top = lua_gettop(handler->L);
  lua_rawgeti(handler->L, LUA_REGISTRYINDEX, handler->handler_ref);
  lua_newtable(handler->L);
  lcdc_set_string_field(handler->L, "namespace", event->ns);
  lcdc_set_string_field(handler->L, "queue", event->queue);
  lcdc_set_bool_field(handler->L, "available", event->available);
  lcdc_set_string_field(handler->L, "head_message_id", event->head_message_id);
  lcdc_set_integer_field(handler->L, "changed_at_unix", event->changed_at_unix);
  lcdc_set_string_field(handler->L, "correlation_id", event->correlation_id);
  if (lua_pcall(handler->L, 1, 2, 0) != 0) {
    message = lua_tostring(handler->L, -1);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       message != NULL ? message : "Lua queue watch failed",
                       NULL, NULL, NULL);
    lua_settop(handler->L, top);
    return 0;
  }
  failed = !lua_isnil(handler->L, -1) &&
           (lua_isnil(handler->L, -2) ||
            (lua_isboolean(handler->L, -2) && !lua_toboolean(handler->L, -2)));
  stop = lua_isboolean(handler->L, -2) && !lua_toboolean(handler->L, -2) &&
         lua_isnil(handler->L, -1);
  if (failed) {
    message = lcdc_lua_error_message(handler->L, -1);
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       message != NULL ? message : "Lua queue watch failed",
                       NULL, NULL, NULL);
  }
  if (stop) {
    handler->stopped = 1;
  }
  lua_settop(handler->L, top);
  return !failed && !stop;
}

static int lcdc_client_watch_queue(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client;
  lc_watch_queue_req req;
  lc_watch_handler watch;
  lcdc_watch_handler handler;
  lc_error error;
  int rc;

  ud = lcdc_check_client(L, 1);
  lc_watch_queue_req_init(&req);
  lc_watch_handler_init(&watch);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  luaL_checktype(L, 3, LUA_TFUNCTION);
  /* A watch invokes Lua before it stops using this request on later polls.
   * Keep private copies rather than borrowing strings from the callback's
   * mutable request table. */
  lua_createtable(L, 0, 2);
  lcdc_normalize_string_field(L, 2, -1, "namespace", 0);
  lcdc_normalize_string_field(L, 2, -1, "queue", 0);
  req.ns = lcdc_normalized_string_value(L, -1, "namespace");
  req.queue = lcdc_normalized_string_value(L, -1, "queue");
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  handler.L = L;
  handler.stopped = 0;
  lua_pushvalue(L, 3);
  handler.handler_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  watch.handle = lcdc_lua_watch_handle;
  watch.context = &handler;
  ud->streaming = 1;
  rc = lc_watch_queue(client, &req, &watch, &error);
  ud->streaming = 0;
  luaL_unref(L, LUA_REGISTRYINDEX, handler.handler_ref);
  lua_pop(L, 1);
  if (handler.stopped) {
    lua_pushboolean(L, 1);
    lc_error_cleanup(&error);
    return 1;
  }
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_client_dequeue_batch(lua_State *L) {
  lcdc_client_ud *ud;
  lc_client *client = NULL;
  lc_dequeue_req req;
  lc_dequeue_batch_res res;
  lc_error error;
  size_t i;
  int rc;

  ud = lcdc_check_client(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_dequeue_req(L, 2, &req);
  rc = lcdc_client_revalidate(ud, &client, &error);
  if (rc == LC_OK)
    rc = lc_dequeue_batch(client, &req, &res, &error);
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_createtable(L, (int)res.count, 0);
  for (i = 0U; i < res.count; ++i) {
    lcdc_push_message(L, res.messages[i]);
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
    res.messages[i] = NULL;
  }
  lc_dequeue_batch_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_info(lua_State *L) {
  lcdc_lease_ud *ud;

  ud = lcdc_check_lease(L, 1);
  lcdc_push_lease_info(L, ud->lease);
  return 1;
}

static int lcdc_lease_close(lua_State *L) {
  (void)lcdc_check_lease(L, 1);
  return lcdc_lease_gc(L);
}

static int lcdc_lease_describe(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_error_init(&error);
  rc = lc_lease_describe(ud->lease, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_get(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lcdc_output output;
  lc_get_opts opts;
  lc_get_res res;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_get_opts_init(&opts);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_boolean_field(L, 2, "public_read", &opts.public_read);
  }
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->streaming = 1;
  rc = lc_lease_get(lease, output.sink, &opts, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_set_bool_field(L, "no_content", res.no_content);
  lcdc_set_string_field(L, "content_type", res.content_type);
  lcdc_set_string_field(L, "etag", res.etag);
  lcdc_set_version_field(L, "version", res.version);
  lcdc_set_integer_field(L, "fencing_token", res.fencing_token);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lcdc_set_state_retention(L, res.retention_support,
                           res.has_retention_expires_at_unix,
                           res.retention_expires_at_unix);
  lc_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_lease_update(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_update_opts opts;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_update_opts_init(&opts);
  lc_error_init(&error);
  src = NULL;
  if (!lua_isnoneornil(L, 3)) {
    luaL_checktype(L, 3, LUA_TTABLE);
    if (!lcdc_read_state_retention(L, 3, &opts.has_retention_seconds,
                                   &opts.retention_seconds)) {
      return 3;
    }
    opts.if_state_etag = lcdc_opt_string_field(L, 3, "if_state_etag");
    if (lcdc_opt_version_field(L, 3, "if_version", &opts.if_version)) {
      opts.has_if_version = 1;
    }
    opts.content_type = lcdc_opt_string_field(L, 3, "content_type");
  }
  rc = lcdc_source_from_value(L, 2, &src, &error);
  if (rc == LC_OK)
    rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc != LC_OK) {
    lc_source_close(src);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  /* Source callbacks may re-enter Lua. Keep the lease active until source
   * cleanup has also returned, because its close callback can re-enter just
   * as its read callback can. */
  ud->streaming = 1;
  rc = lc_lease_update(lease, src, &opts, &error);
  lc_source_close(src);
  ud->streaming = 0;
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_mutate(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_mutate_req req;
  lc_error error;
  const char **mutations;
  size_t mutation_count;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_mutate_req_init(&req);
  lc_error_init(&error);
  mutations = NULL;
  mutation_count = 0U;
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &req.has_retention_seconds,
                                 &req.retention_seconds)) {
    return 3;
  }
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  req.mutations = mutations;
  req.mutation_count = mutation_count;
  req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_version_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc == LC_OK)
    rc = lc_lease_mutate(lease, &req, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_mutate_local(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_mutate_local_req req;
  lc_error error;
  const char **mutations;
  size_t mutation_count;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_mutate_local_req_init(&req);
  lc_error_init(&error);
  mutations = NULL;
  mutation_count = 0U;
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &req.update.has_retention_seconds,
                                 &req.update.retention_seconds)) {
    return 3;
  }
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  req.mutations = mutations;
  req.mutation_count = mutation_count;
  lcdc_opt_boolean_field(L, 2, "disable_fetched_cas", &req.disable_fetched_cas);
  req.file_value_base_dir = lcdc_opt_string_field(L, 2, "file_value_base_dir");
  req.update.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_version_field(L, 2, "if_version", &req.update.if_version)) {
    req.update.has_if_version = 1;
  }
  req.update.content_type = lcdc_opt_string_field(L, 2, "content_type");
  rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc == LC_OK)
    rc = lc_lease_mutate_local(lease, &req, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_metadata(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_metadata_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_metadata_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &req.has_retention_seconds,
                                 &req.retention_seconds)) {
    return 3;
  }
  if (lcdc_opt_boolean_field(L, 2, "query_hidden", &req.query_hidden)) {
    req.has_query_hidden = 1;
  }
  if (lcdc_opt_version_field(L, 2, "if_version", &req.if_version)) {
    req.has_if_version = 1;
  }
  rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc == LC_OK)
    rc = lc_lease_metadata(lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_remove(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_remove_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_remove_req_init(&req);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    req.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
    if (lcdc_opt_version_field(L, 2, "if_version", &req.if_version)) {
      req.has_if_version = 1;
    }
  }
  rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc == LC_OK)
    rc = lc_lease_remove(lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_keepalive(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_keepalive_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_keepalive_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &req.ttl_seconds);
  rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc == LC_OK)
    rc = lc_lease_keepalive(lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_lease_info(L, ud->lease);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_release(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_release_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_release_req_init(&req);
  lc_error_init(&error);
  if (ud->borrowed) {
    rc = lc_error_set(&error, LC_ERR_INVALID, 0L,
                      "a subscription state lease is owned by its delivery",
                      NULL, NULL, NULL);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_boolean_field(L, 2, "rollback", &req.rollback);
  }
  rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc == LC_OK)
    rc = lc_lease_release(lease, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->lease = NULL;
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_attach(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_attach_req req;
  lc_attach_res res;
  lc_source *src;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_attach_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  src = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "name", &req.name);
  req.content_type = lcdc_opt_string_field(L, 2, "content_type");
  if (lcdc_opt_integer_field(L, 2, "max_bytes", &req.max_bytes)) {
    req.has_max_bytes = 1;
  }
  lcdc_opt_boolean_field(L, 2, "prevent_overwrite", &req.prevent_overwrite);
  rc = lcdc_source_from_value(L, 3, &src, &error);
  if (rc == LC_OK)
    rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc != LC_OK) {
    lc_source_close(src);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  /* See lcdc_lease_update(): source cleanup is still part of the active lease
   * operation. */
  ud->streaming = 1;
  rc = lc_lease_attach(lease, &req, src, &res, &error);
  lc_source_close(src);
  ud->streaming = 0;
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_bool_field(L, "noop", res.noop);
  lcdc_set_version_field(L, "version", res.version);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attach_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_list_attachments(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_attachment_list res;
  lc_error error;
  size_t i;
  int rc;

  ud = lcdc_check_lease(L, 1);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  rc = lc_lease_list_attachments(ud->lease, &res, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lua_createtable(L, (int)res.count, 0);
  for (i = 0U; i < res.count; ++i) {
    lcdc_push_attachment_info(L, &res.items[i]);
    lua_rawseti(L, -2, (lua_Integer)(i + 1U));
  }
  lua_setfield(L, -2, "items");
  lc_attachment_list_cleanup(&res);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_get_attachment(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_attachment_get_req req;
  lc_attachment_get_res res;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_attachment_get_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &req.selector);
  lua_pop(L, 1);
  lcdc_opt_boolean_field(L, 2, "public_read", &req.public_read);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->streaming = 1;
  rc = lc_lease_get_attachment(lease, &req, output.sink, &res, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_push_attachment_info(L, &res.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_string_field(L, "correlation_id", res.correlation_id);
  lc_attachment_get_res_cleanup(&res);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_lease_delete_attachment(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_lease *lease = NULL;
  lc_attachment_selector selector;
  lc_error error;
  int deleted;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_error_init(&error);
  deleted = 0;
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_attachment_selector(L, 2, &selector);
  rc = lcdc_lease_revalidate(ud, &lease, &error);
  if (rc == LC_OK)
    rc = lc_lease_delete_attachment(lease, &selector, &deleted, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, deleted);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_lease_delete_all_attachments(lua_State *L) {
  lcdc_lease_ud *ud;
  lc_error error;
  int deleted_count;
  int rc;

  ud = lcdc_check_lease(L, 1);
  lc_error_init(&error);
  deleted_count = 0;
  rc = lc_lease_delete_all_attachments(ud->lease, &deleted_count, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushinteger(L, (lua_Integer)deleted_count);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_info(lua_State *L) {
  lcdc_message_ud *ud;

  ud = lcdc_check_message(L, 1);
  lcdc_push_message_info(L, ud->message);
  return 1;
}

static int lcdc_message_close(lua_State *L) {
  (void)lcdc_check_message(L, 1);
  return lcdc_message_gc(L);
}

static int lcdc_message_ack(lua_State *L) {
  lcdc_message_ud *ud;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_error_init(&error);
  rc = lc_message_ack(ud->message, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_message_invalidate_borrowed_state(ud);
  ud->message = NULL;
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_nack(lua_State *L) {
  lcdc_message_ud *ud;
  lc_message *message = NULL;
  lc_nack_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_nack_req_init(&req);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_integer_field(L, 2, "delay_seconds", &req.delay_seconds);
    req.intent = lcdc_parse_nack_intent(L, 2, "intent");
    req.last_error_json = lcdc_opt_string_field(L, 2, "last_error_json");
  }
  rc = lcdc_message_revalidate(ud, &message, &error);
  if (rc == LC_OK)
    rc = lc_message_nack(message, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_message_invalidate_borrowed_state(ud);
  ud->message = NULL;
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_extend(lua_State *L) {
  lcdc_message_ud *ud;
  lc_message *message = NULL;
  lc_extend_req req;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_extend_req_init(&req);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_opt_integer_field(L, 2, "extend_by_seconds", &req.extend_by_seconds);
  rc = lcdc_message_revalidate(ud, &message, &error);
  if (rc == LC_OK)
    rc = lc_message_extend(message, &req, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_message_info(L, ud->message);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_state(lua_State *L) {
  lcdc_message_ud *ud;
  lc_lease *lease;

  ud = lcdc_check_message(L, 1);
  lease = lc_message_state(ud->message);
  return lcdc_push_cloned_lease(L, lease);
}

static int lcdc_message_rewind_payload(lua_State *L) {
  lcdc_message_ud *ud;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_error_init(&error);
  rc = lc_message_rewind_payload(ud->message, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_message_payload(lua_State *L) {
  lcdc_message_ud *ud;
  lc_message *message = NULL;
  lcdc_output output;
  lc_error error;
  int rc;

  ud = lcdc_check_message(L, 1);
  lc_error_init(&error);
  rc = lcdc_init_output(L, 2, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_message_revalidate(ud, &message, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->streaming = 1;
  rc = lc_message_write_payload(message, output.sink, &output.written, &error);
  if (rc != LC_OK) {
    if (output.sink != NULL) {
      lc_sink_close(output.sink);
    }
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_pushinteger(L, (lua_Integer)output.written);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_parse_outbox_entry(lua_State *L, int index,
                                   lc_outbox_entry *entry) {
  int root_index;

  lc_outbox_entry_init(entry);
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_reject_field(L, index, "headers",
                    "outbox entry uses headers_json; headers is not supported");
  lua_newtable(L);
  root_index = lua_gettop(L);
  lcdc_normalize_string_field(L, index, root_index, "operation_id", 1);
  lcdc_normalize_string_field(L, index, root_index, "effect_id", 1);
  lcdc_normalize_string_field(L, index, root_index, "effect_key", 1);
  lcdc_normalize_string_field(L, index, root_index, "payload_digest", 1);
  lcdc_normalize_string_field(L, index, root_index, "causation_id", 0);
  lcdc_normalize_string_field(L, index, root_index, "kind", 1);
  lcdc_normalize_string_field(L, index, root_index, "schema_version", 0);
  lcdc_normalize_string_field(L, index, root_index, "destination", 1);
  lcdc_normalize_string_field(L, index, root_index, "content_type", 0);
  lcdc_normalize_string_field(L, index, root_index, "headers_json", 0);
  lcdc_normalize_string_field(L, index, root_index, "trace_context", 0);
  entry->operation_id =
      lcdc_normalized_string_value(L, root_index, "operation_id");
  entry->effect_id = lcdc_normalized_string_value(L, root_index, "effect_id");
  entry->effect_key = lcdc_normalized_string_value(L, root_index, "effect_key");
  entry->payload_digest =
      lcdc_normalized_string_value(L, root_index, "payload_digest");
  entry->causation_id =
      lcdc_normalized_string_value(L, root_index, "causation_id");
  entry->kind = lcdc_normalized_string_value(L, root_index, "kind");
  entry->schema_version =
      lcdc_normalized_string_value(L, root_index, "schema_version");
  entry->destination =
      lcdc_normalized_string_value(L, root_index, "destination");
  entry->content_type =
      lcdc_normalized_string_value(L, root_index, "content_type");
  entry->headers_json =
      lcdc_normalized_string_value(L, root_index, "headers_json");
  entry->trace_context =
      lcdc_normalized_string_value(L, root_index, "trace_context");
  return root_index;
}

static int lcdc_parse_command_identity(lua_State *L, int index,
                                       lc_command_identity *identity) {
  int root_index;

  lc_command_identity_init(identity);
  luaL_checktype(L, index, LUA_TTABLE);
  lua_newtable(L);
  root_index = lua_gettop(L);
  lcdc_normalize_string_field(L, index, root_index, "scope", 1);
  lcdc_normalize_string_field(L, index, root_index, "command_type", 1);
  lcdc_normalize_string_field(L, index, root_index, "idempotency_key", 1);
  identity->scope = lcdc_normalized_string_value(L, root_index, "scope");
  identity->command_type =
      lcdc_normalized_string_value(L, root_index, "command_type");
  identity->idempotency_key =
      lcdc_normalized_string_value(L, root_index, "idempotency_key");
  return root_index;
}

static int lcdc_parse_command_request(lua_State *L, int index,
                                      lc_command_request *request) {
  int root_index;

  lc_command_request_init(request);
  luaL_checktype(L, index, LUA_TTABLE);
  lcdc_opt_boolean_field(L, index, "generate_idempotency_key",
                         &request->generate_idempotency_key);
  lua_newtable(L);
  root_index = lua_gettop(L);
  lcdc_normalize_string_field(L, index, root_index, "scope", 1);
  lcdc_normalize_string_field(L, index, root_index, "command_type", 1);
  lcdc_normalize_string_field(L, index, root_index, "idempotency_key",
                              !request->generate_idempotency_key);
  lcdc_normalize_string_field(L, index, root_index, "request_digest", 1);
  lcdc_normalize_string_field(L, index, root_index, "operation_id", 0);
  request->identity.scope =
      lcdc_normalized_string_value(L, root_index, "scope");
  request->identity.command_type =
      lcdc_normalized_string_value(L, root_index, "command_type");
  request->identity.idempotency_key =
      lcdc_normalized_string_value(L, root_index, "idempotency_key");
  request->request_digest =
      lcdc_normalized_string_value(L, root_index, "request_digest");
  request->operation_id =
      lcdc_normalized_string_value(L, root_index, "operation_id");
  return root_index;
}

static int lcdc_parse_inbox_message(lua_State *L, int index,
                                    lc_inbox_message *message) {
  int root_index;

  lc_inbox_message_init(message);
  luaL_checktype(L, index, LUA_TTABLE);
  lua_newtable(L);
  root_index = lua_gettop(L);
  lcdc_normalize_string_field(L, index, root_index, "consumer_id", 1);
  lcdc_normalize_string_field(L, index, root_index, "source_kind", 1);
  lcdc_normalize_string_field(L, index, root_index, "source_id", 1);
  lcdc_normalize_string_field(L, index, root_index, "message_id", 1);
  lcdc_normalize_string_field(L, index, root_index, "payload_digest", 0);
  lcdc_normalize_string_field(L, index, root_index, "operation_id", 0);
  message->consumer_id =
      lcdc_normalized_string_value(L, root_index, "consumer_id");
  message->source_kind =
      lcdc_normalized_string_value(L, root_index, "source_kind");
  message->source_id = lcdc_normalized_string_value(L, root_index, "source_id");
  message->message_id =
      lcdc_normalized_string_value(L, root_index, "message_id");
  message->payload_digest =
      lcdc_normalized_string_value(L, root_index, "payload_digest");
  message->operation_id =
      lcdc_normalized_string_value(L, root_index, "operation_id");
  return root_index;
}

static int
lcdc_parse_outbox_participant_request(lua_State *L, int index,
                                      lc_outbox_participant_request *request) {
  int root_index;

  lc_outbox_participant_request_init(request);
  luaL_checktype(L, index, LUA_TTABLE);
  lua_newtable(L);
  root_index = lua_gettop(L);
  lcdc_normalize_string_field(L, index, root_index, "namespace", 0);
  lcdc_normalize_string_field(L, index, root_index, "key", 1);
  lcdc_normalize_string_field(L, index, root_index, "owner", 0);
  request->acquire.ns =
      lcdc_normalized_string_value(L, root_index, "namespace");
  request->acquire.key = lcdc_normalized_string_value(L, root_index, "key");
  request->acquire.owner = lcdc_normalized_string_value(L, root_index, "owner");
  lcdc_opt_integer_field(L, index, "ttl_seconds",
                         &request->acquire.ttl_seconds);
  lcdc_opt_integer_field(L, index, "block_seconds",
                         &request->acquire.block_seconds);
  lcdc_opt_boolean_field(L, index, "if_not_exists",
                         &request->acquire.if_not_exists);
  return root_index;
}

static void lcdc_push_command_receipt(lua_State *L,
                                      const lc_command_receipt *receipt) {
  lua_newtable(L);
  lcdc_set_integer_field(L, "state", receipt->state);
  lcdc_set_bool_field(L, "duplicate", receipt->duplicate);
  lcdc_set_string_field(L, "command_id", receipt->command_id);
  lcdc_set_string_field(L, "scope", receipt->scope);
  lcdc_set_string_field(L, "command_type", receipt->command_type);
  lcdc_set_string_field(L, "idempotency_key", receipt->idempotency_key);
  lcdc_set_string_field(L, "operation_id", receipt->operation_id);
  lcdc_set_string_field(L, "result_code", receipt->result_code);
  lcdc_set_string_field(L, "result_reference", receipt->result_reference);
  lcdc_set_string_field(L, "failure_code", receipt->failure_code);
  lcdc_set_string_field(L, "failure_message", receipt->failure_message);
  lcdc_set_bool_field(L, "has_result_body", receipt->has_result_body);
}

static void lcdc_push_outbox_receipt(lua_State *L,
                                     const lc_outbox_receipt *receipt) {
  lua_newtable(L);
  lcdc_set_string_field(L, "outbox_key", receipt->outbox_key);
  lcdc_set_string_field(L, "effect_key", receipt->effect_key);
  lcdc_set_bool_field(L, "duplicate", receipt->duplicate);
}

static void lcdc_push_inbox_result(lua_State *L,
                                   const lc_inbox_accept_result *result) {
  lua_newtable(L);
  lcdc_set_bool_field(L, "accepted", result->accepted);
  lcdc_set_bool_field(L, "duplicate", result->duplicate);
}

static int lcdc_push_outbox_participant_info(
    lua_State *L, const lc_outbox_participant *participant, lc_error *error) {
  lua_newtable(L);
  lcdc_set_string_field(L, "namespace", participant->ns);
  lcdc_set_string_field(L, "key", participant->key);
  lcdc_set_string_field(L, "txn_id", participant->txn_id);
  lcdc_set_integer_field(L, "fencing_token", participant->fencing_token);
  if (lcdc_set_int64_field(L, "version", participant->version, error) !=
      LC_OK) {
    lua_pop(L, 1);
    return LC_ERR_INVALID;
  }
  lcdc_set_string_field(L, "state_etag", participant->state_etag);
  lcdc_set_state_retention(L, participant->retention_support,
                           participant->has_retention_expires_at_unix,
                           participant->retention_expires_at_unix);
  return LC_OK;
}

static int lcdc_push_outbox_job_info(lua_State *L, const lc_outbox_job *job,
                                     lc_error *error) {
  lua_newtable(L);
  lcdc_set_string_field(L, "outbox_key", job->outbox_key);
  lcdc_set_string_field(L, "operation_id", job->operation_id);
  lcdc_set_string_field(L, "effect_id", job->effect_id);
  lcdc_set_string_field(L, "effect_key", job->effect_key);
  lcdc_set_string_field(L, "message_id", job->message_id);
  lcdc_set_string_field(L, "command_id", job->command_id);
  lcdc_set_string_field(L, "causation_id", job->causation_id);
  lcdc_set_string_field(L, "kind", job->kind);
  lcdc_set_string_field(L, "schema_version", job->schema_version);
  lcdc_set_string_field(L, "destination", job->destination);
  lcdc_set_string_field(L, "content_type", job->content_type);
  lcdc_set_string_field(L, "headers_json", job->headers_json);
  lcdc_set_string_field(L, "trace_context", job->trace_context);
  lcdc_set_integer_field(L, "attempt", job->attempt);
  lcdc_set_integer_field(L, "max_attempts", job->max_attempts);
  if (lcdc_set_int64_field(L, "lease_expires_at_unix",
                           job->lease_expires_at_unix, error) != LC_OK) {
    lua_pop(L, 1);
    return LC_ERR_INVALID;
  }
  return LC_OK;
}

static int lcdc_return_outbox_participant_info(
    lua_State *L, const lc_outbox_participant *participant, lc_error *error) {
  int rc = lcdc_push_outbox_participant_info(L, participant, error);

  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, error);
    return 3;
  }
  return 1;
}

/* Lua C functions return result counts, not lc_status values. Keep the
 * transaction callback rollback-only decision at the C-status boundary. */
static int lcdc_return_outbox_participant_info_for_transaction(
    lua_State *L, lcdc_outbox_participant_ud *ud, lc_error *error) {
  int rc;

  rc = lcdc_push_outbox_participant_info(L, ud->participant, error);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, error);
    return 3;
  }
  return 1;
}

static int lcdc_return_outbox_job_info(lua_State *L, const lc_outbox_job *job,
                                       lc_error *error) {
  int rc = lcdc_push_outbox_job_info(L, job, error);

  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, error);
    return 3;
  }
  return 1;
}

static int lcdc_push_outbox_stats(lua_State *L, const lc_outbox_stats *stats,
                                  lc_error *error) {
  lua_newtable(L);
  lcdc_set_bool_field(L, "running", stats->running);
  if (lcdc_set_size_field(L, "pending_candidates", stats->pending_candidates,
                          error) != LC_OK ||
      lcdc_set_size_field(L, "delayed_wakes", stats->delayed_wakes, error) !=
          LC_OK ||
      lcdc_set_size_field(L, "waiting_consumers", stats->waiting_consumers,
                          error) != LC_OK ||
      lcdc_set_uint64_field(L, "direct_notifications",
                            stats->direct_notifications, error) != LC_OK ||
      lcdc_set_uint64_field(L, "notification_overflows",
                            stats->notification_overflows, error) != LC_OK ||
      lcdc_set_uint64_field(L, "recovery_queries", stats->recovery_queries,
                            error) != LC_OK ||
      lcdc_set_uint64_field(L, "recovered_claims", stats->recovered_claims,
                            error) != LC_OK ||
      lcdc_set_uint64_field(L, "claim_losses", stats->claim_losses, error) !=
          LC_OK ||
      lcdc_set_uint64_field(L, "payload_open_failures",
                            stats->payload_open_failures, error) != LC_OK ||
      lcdc_set_uint64_field(L, "dead_letter_reclaims",
                            stats->dead_letter_reclaims, error) != LC_OK ||
      lcdc_set_uint64_field(L, "terminal_reclaims", stats->terminal_reclaims,
                            error) != LC_OK ||
      lcdc_set_uint64_field(L, "terminal_reclaim_failures",
                            stats->terminal_reclaim_failures, error) != LC_OK ||
      lcdc_set_uint64_field(L, "dead_letter_reclaim_failures",
                            stats->dead_letter_reclaim_failures,
                            error) != LC_OK) {
    lua_pop(L, 1);
    return LC_ERR_INVALID;
  }
  lcdc_set_string_field(L, "last_error", stats->last_error);
  return LC_OK;
}

static int lcdc_client_new_outbox(lua_State *L) {
  lcdc_client_ud *client_ud = lcdc_check_client(L, 1);
  lc_client *client = NULL;
  lc_outbox_config config;
  lc_outbox *outbox = NULL;
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_error error;
  int has_dispatcher_options;
  int root_index;
  int rc;

  lc_outbox_config_init(&config);
  lc_error_init(&error);
  luaL_checktype(L, 2, LUA_TTABLE);
  has_dispatcher_options = !lua_isnoneornil(L, 3);
  /* Request-table access can synthesize strings and run collection. Root the
   * normalized strings until the native constructor has copied them. */
  lua_createtable(L, 0, 2);
  root_index = lua_gettop(L);
  lcdc_normalize_string_field(L, 2, root_index, "namespace", 0);
  lcdc_normalize_string_field(L, 2, root_index, "owner", 0);
  config.ns = lcdc_normalized_string_value(L, root_index, "namespace");
  config.owner = lcdc_normalized_string_value(L, root_index, "owner");
  lcdc_opt_integer_field(L, 2, "transaction_ttl_seconds",
                         &config.transaction_ttl_seconds);
  lcdc_opt_integer_field(L, 2, "claim_ttl_seconds", &config.claim_ttl_seconds);
  lcdc_opt_int_field(L, 2, "max_attempts", &config.max_attempts);
  lcdc_opt_integer_field(L, 2, "retry_initial_delay_seconds",
                         &config.retry_initial_delay_seconds);
  lcdc_opt_integer_field(L, 2, "retry_max_delay_seconds",
                         &config.retry_max_delay_seconds);
  lcdc_opt_integer_field(L, 2, "host_retry_delay_max_seconds",
                         &config.host_retry_delay_max_seconds);
  lcdc_opt_integer_field(L, 2, "recovery_interval_seconds",
                         &config.recovery_interval_seconds);
  lcdc_opt_integer_field(L, 2, "shutdown_timeout_ms",
                         &config.shutdown_timeout_ms);
  lcdc_opt_boolean_field(L, 2, "replay_dead_letters_on_startup",
                         &config.replay_dead_letters_on_startup);
  lcdc_opt_integer_field(L, 2, "dead_letter_retention_seconds",
                         &config.dead_letter_retention_seconds);
  (void)lcdc_opt_retention_count_field(L, 2, "dead_letter_max_count",
                                       &config.dead_letter_max_count);
  lcdc_opt_integer_field(L, 2, "completed_retention_seconds",
                         &config.completed_retention_seconds);
  (void)lcdc_opt_retention_count_field(L, 2, "completed_max_count",
                                       &config.completed_max_count);
  lcdc_opt_integer_field(L, 2, "inbox_retention_seconds",
                         &config.inbox_retention_seconds);
  (void)lcdc_opt_retention_count_field(L, 2, "inbox_max_count",
                                       &config.inbox_max_count);
  lcdc_opt_integer_field(L, 2, "command_retention_seconds",
                         &config.command_retention_seconds);
  (void)lcdc_opt_retention_count_field(L, 2, "command_max_count",
                                       &config.command_max_count);
  (void)lcdc_opt_dead_letter_bytes_field(L, 2, "dead_letter_max_bytes",
                                         &config.dead_letter_max_bytes);
  (void)lcdc_opt_size_field(L, 2, "notification_capacity",
                            &config.notification_capacity);
  if (has_dispatcher_options) {
    lcdc_outbox_dispatcher_ud *dispatcher_ud;

    luaL_checktype(L, 3, LUA_TTABLE);
    lua_getfield(L, 3, "dispatcher");
    if (!lua_isnil(L, -1)) {
      dispatcher_ud = lcdc_check_outbox_dispatcher(L, -1);
      dispatcher = dispatcher_ud->dispatcher;
    }
    lua_pop(L, 1);
  }
  rc = lcdc_client_revalidate(client_ud, &client, &error);
  if (rc == LC_OK) {
    rc = dispatcher == NULL
             ? lc_client_new_outbox(client, &config, &outbox, &error)
             : lc_client_new_outbox_with_dispatcher(client, &config, dispatcher,
                                                    &outbox, &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_outbox(L, outbox, 1);
}

static int lcdc_push_history_consumer_position(
    lua_State *L, const lc_history_consumer_position *position,
    lc_error *error) {
  lua_newtable(L);
  if (lcdc_set_uint64_field(L, "acknowledged_index_seq",
                            position->acknowledged_index_seq, error) != LC_OK ||
      lcdc_set_uint64_field(L, "current_index_seq", position->current_index_seq,
                            error) != LC_OK) {
    lua_pop(L, 1);
    return LC_ERR_INVALID;
  }
  return LC_OK;
}

static int lcdc_client_new_history_consumer(lua_State *L) {
  lcdc_client_ud *client_ud;
  lc_client *client;
  lc_history_consumer_config config;
  lc_history_consumer *consumer;
  lc_error error;
  int has_initial_acknowledged_index_seq;
  int start_at_current;
  int rc;

  client_ud = lcdc_check_client(L, 1);
  client = NULL;
  lc_history_consumer_config_init(&config);
  lc_error_init(&error);
  consumer = NULL;
  luaL_checktype(L, 2, LUA_TTABLE);
  /* Metatable-backed fields may collect while later config is parsed. Keep
   * plain private values alive until the native constructor has copied them. */
  lua_createtable(L, 0, 2);
  lcdc_normalize_string_field(L, 2, -1, "namespace", 0);
  lcdc_normalize_string_field(L, 2, -1, "consumer_id", 1);
  has_initial_acknowledged_index_seq = 0;
  lua_getfield(L, 2, "initial_acknowledged_index_seq");
  if (!lua_isnil(L, -1)) {
    config.initial_acknowledged_index_seq =
        lcdc_check_uint64(L, -1, "initial_acknowledged_index_seq");
    has_initial_acknowledged_index_seq = 1;
  }
  lua_pop(L, 1);
  start_at_current = 0;
  (void)lcdc_opt_boolean_field(L, 2, "start_at_current", &start_at_current);
  if (start_at_current && has_initial_acknowledged_index_seq) {
    lua_pop(L, 1);
    lc_error_set(&error, LC_ERR_INVALID, 0L,
                 "start_at_current and initial_acknowledged_index_seq cannot "
                 "both be supplied",
                 NULL, NULL, NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (start_at_current) {
    config.initial_acknowledged_index_seq =
        LC_HISTORY_CONSUMER_START_AT_CURRENT;
  }
  config.ns = lcdc_normalized_string_value(L, -1, "namespace");
  config.consumer_id = lcdc_normalized_string_value(L, -1, "consumer_id");
  rc = lcdc_client_revalidate(client_ud, &client, &error);
  if (rc == LC_OK) {
    rc = lc_client_new_history_consumer(client, &config, &consumer, &error);
  }
  lua_pop(L, 1);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_history_consumer(L, consumer);
}

static int lcdc_history_consumer_position(lua_State *L) {
  lcdc_history_consumer_ud *ud;
  lc_history_consumer_position position;
  lc_error error;
  int rc;

  ud = lcdc_check_history_consumer(L, 1);
  memset(&position, 0, sizeof(position));
  lc_error_init(&error);
  rc = ud->consumer->position(ud->consumer, &position, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_push_history_consumer_position(L, &position, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_history_consumer_advance(lua_State *L) {
  lcdc_history_consumer_ud *ud;
  lc_history_consumer_position position;
  lc_index_seq acknowledged_index_seq;
  lc_error error;
  int rc;

  ud = lcdc_check_history_consumer(L, 1);
  acknowledged_index_seq =
      (lc_index_seq)lcdc_check_uint64(L, 2, "acknowledged_index_seq");
  memset(&position, 0, sizeof(position));
  lc_error_init(&error);
  rc = ud->consumer->advance(ud->consumer, acknowledged_index_seq, &position,
                             &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_push_history_consumer_position(L, &position, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_history_consumer_unregister(lua_State *L) {
  lcdc_history_consumer_ud *ud;
  lc_error error;
  int rc;

  ud = lcdc_check_history_consumer(L, 1);
  lc_error_init(&error);
  rc = ud->consumer->unregister(ud->consumer, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_history_consumer_close(lua_State *L) {
  return lcdc_history_consumer_gc(L);
}

static int lcdc_outbox_close(lua_State *L) {
  (void)lcdc_check_outbox(L, 1);
  return lcdc_outbox_gc(L);
}

static int lcdc_outbox_append(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox *outbox;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_transaction *transaction = NULL;
  lc_source *payload = NULL;
  lc_error error;
  int root_index;
  int rc;

  root_index = lcdc_parse_outbox_entry(L, 2, &entry);
  lc_outbox_receipt_init(&receipt);
  lc_error_init(&error);
  outbox = NULL;
  rc = lcdc_source_from_value(L, 3, &payload, &error);
  if (rc == LC_OK) {
    rc = lcdc_outbox_revalidate(ud, &outbox, &error);
  }
  if (rc == LC_OK) {
    ud->streaming = 1;
    rc = lc_outbox_append(outbox, &entry, payload, &transaction, &receipt,
                          &error);
    lc_source_close(payload);
    ud->streaming = 0;
  } else {
    lc_source_close(payload);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lc_outbox_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL) {
    lcdc_push_outbox_txn(L, transaction, 1);
  } else {
    lua_pushnil(L);
  }
  lcdc_push_outbox_receipt(L, &receipt);
  lc_outbox_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_accept_inbox(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox *outbox = NULL;
  lc_inbox_message message;
  lc_inbox_accept_result result;
  lc_outbox_transaction *transaction = NULL;
  lc_error error;
  int root_index;
  int rc;

  memset(&result, 0, sizeof(result));
  root_index = lcdc_parse_inbox_message(L, 2, &message);
  lc_error_init(&error);
  rc = lcdc_outbox_revalidate(ud, &outbox, &error);
  if (rc == LC_OK) {
    rc =
        lc_outbox_accept_inbox(outbox, &message, &transaction, &result, &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL) {
    lcdc_push_outbox_txn(L, transaction, 1);
  } else {
    lua_pushnil(L);
  }
  lcdc_push_inbox_result(L, &result);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_accept_command(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox *outbox = NULL;
  lc_command_request request;
  lc_command_receipt receipt;
  lc_outbox_transaction *transaction = NULL;
  lc_error error;
  int root_index;
  int rc;

  root_index = lcdc_parse_command_request(L, 2, &request);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lcdc_outbox_revalidate(ud, &outbox, &error);
  if (rc == LC_OK) {
    rc = lc_outbox_accept_command(outbox, &request, &transaction, &receipt,
                                  &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL)
    lcdc_push_outbox_txn(L, transaction, 1);
  else
    lua_pushnil(L);
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_get_command_receipt(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox *outbox = NULL;
  lc_command_identity identity;
  lc_command_receipt receipt;
  lc_error error;
  int root_index;
  int rc;

  root_index = lcdc_parse_command_identity(L, 2, &identity);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lcdc_outbox_revalidate(ud, &outbox, &error);
  if (rc == LC_OK)
    rc = lc_outbox_get_command_receipt(outbox, &identity, &receipt, &error);
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_get_command_receipt_by_id(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_command_receipt receipt;
  lc_error error;
  int rc;

  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lc_outbox_get_command_receipt_by_id(ud->outbox, luaL_checkstring(L, 2),
                                           &receipt, &error);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_wait_command(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_command_receipt receipt;
  lc_error error;
  long timeout_ms;
  int rc;

  timeout_ms = lcdc_check_long(L, 3, "outbox command wait timeout");
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lc_outbox_wait_command(ud->outbox, luaL_checkstring(L, 2), timeout_ms,
                              &receipt, &error);
  if (rc != LC_OK) {
    if (rc != LC_ERR_TIMEOUT) {
      lc_command_receipt_cleanup(&receipt);
      lcdc_push_status_error(L, rc, &error);
      lc_error_cleanup(&error);
      return 3;
    }
    lcdc_push_command_receipt(L, &receipt);
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_error(L, &error);
    lc_error_cleanup(&error);
    return 2;
  }
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_write_command_result(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox *outbox = NULL;
  lc_command_identity identity;
  lcdc_output output;
  lc_error error;
  size_t written = 0U;
  int root_index;
  int rc;

  /* The parser roots its normalized identity. Reserve an omitted destination
   * before that root is pushed so a buffered read is not mistaken for a sink.
   */
  if (lua_isnone(L, 3))
    lua_pushnil(L);
  root_index = lcdc_parse_command_identity(L, 2, &identity);
  lc_error_init(&error);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_outbox_revalidate(ud, &outbox, &error);
  ud->streaming = rc == LC_OK;
  if (rc == LC_OK) {
    rc = lc_outbox_write_command_result(outbox, &identity, output.sink,
                                        &written, &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_pushinteger(L, (lua_Integer)written);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_resume_command(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox *outbox = NULL;
  lc_command_identity identity;
  lc_command_receipt receipt;
  lc_outbox_transaction *transaction = NULL;
  lc_error error;
  int root_index;
  int rc;

  root_index = lcdc_parse_command_identity(L, 2, &identity);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lcdc_outbox_revalidate(ud, &outbox, &error);
  if (rc == LC_OK) {
    rc = lc_outbox_resume_command(outbox, &identity, &transaction, &receipt,
                                  &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL)
    lcdc_push_outbox_txn(L, transaction, 1);
  else
    lua_pushnil(L);
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_resume_command_by_id(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_command_receipt receipt;
  lc_outbox_transaction *transaction = NULL;
  lc_error error;
  int rc;

  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lc_outbox_resume_command_by_id(ud->outbox, luaL_checkstring(L, 2),
                                      &transaction, &receipt, &error);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction != NULL)
    lcdc_push_outbox_txn(L, transaction, 1);
  else
    lua_pushnil(L);
  lcdc_push_command_receipt(L, &receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_begin(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox_transaction *transaction = NULL;
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_outbox_begin(ud->outbox, &transaction, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  return lcdc_push_outbox_txn(L, transaction, 1);
}

static int
lcdc_push_outbox_commit_result(lua_State *L,
                               const lc_outbox_commit_result *result) {
  size_t index;

  lua_newtable(L);
  lua_newtable(L);
  for (index = 0U; index < result->outbox_receipt_count; ++index) {
    lcdc_push_outbox_receipt(L, &result->outbox_receipts[index]);
    lua_rawseti(L, -2, (lua_Integer)index + 1);
  }
  lua_setfield(L, -2, "outbox_receipts");
  return 1;
}

static int lcdc_outbox_transaction(lua_State *L) {
  lcdc_outbox_ud *outbox_ud = lcdc_check_outbox(L, 1);
  lc_outbox_transaction *transaction = NULL;
  lc_outbox_commit_result result;
  lc_error error;
  lcdc_outbox_txn_ud *transaction_ud;
  int transaction_index;
  int rc;

  luaL_checktype(L, 2, LUA_TFUNCTION);
  lc_error_init(&error);
  rc = lc_outbox_begin(outbox_ud->outbox, &transaction, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_outbox_txn(L, transaction, 1);
  transaction_index = lua_gettop(L);
  transaction_ud = (lcdc_outbox_txn_ud *)lua_touserdata(L, transaction_index);
  transaction_ud->callback_scoped = 1;
  lua_pushvalue(L, 2);
  lua_pushvalue(L, transaction_index);
  if (lua_pcall(L, 1, LUA_MULTRET, 0) != 0) {
    lc_error rollback_error;

    transaction_ud->callback_scoped = 0;
    lc_error_init(&rollback_error);
    (void)lc_outbox_transaction_rollback(transaction, &rollback_error);
    lc_error_cleanup(&rollback_error);
    lc_outbox_transaction_close(transaction);
    transaction_ud->transaction = NULL;
    lcdc_outbox_txn_clear_duplicate_result(L, transaction_ud);
    lua_remove(L, transaction_index);
    return lua_error(L);
  }
  transaction_ud->callback_scoped = 0;
  if (transaction_ud->callback_staging_failed) {
    lc_error rollback_error;

    lc_error_init(&rollback_error);
    (void)lc_outbox_transaction_rollback(transaction, &rollback_error);
    lc_error_cleanup(&rollback_error);
    lc_outbox_transaction_close(transaction);
    transaction_ud->transaction = NULL;
    lcdc_outbox_txn_clear_duplicate_result(L, transaction_ud);
    lua_settop(L, transaction_index);
    lua_remove(L, transaction_index);
    (void)lc_error_set(
        &error, LC_ERR_INVALID, 0L,
        "outbox callback ignored a failed staging operation; transaction "
        "was rolled back",
        NULL, NULL, NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (transaction_ud->callback_duplicate_seen &&
      (!transaction_ud->callback_has_fresh_participant ||
       transaction_ud->callback_duplicate_after_domain_participant)) {
    /* A duplicate first record owns no local xid. A duplicate found after a
     * domain participant has already caused the C core to roll that xid back.
     * In either case the callback's durable duplicate result is the outcome;
     * do not invent a second terminal decision. */
    lc_outbox_transaction_close(transaction);
    transaction_ud->transaction = NULL;
    lua_settop(L, transaction_index);
    lua_remove(L, transaction_index);
    if (transaction_ud->callback_duplicate_result_ref != LUA_NOREF) {
      lua_rawgeti(L, LUA_REGISTRYINDEX,
                  transaction_ud->callback_duplicate_result_ref);
      lcdc_outbox_txn_clear_duplicate_result(L, transaction_ud);
      lc_error_cleanup(&error);
      return 1;
    }
    (void)lc_error_set(&error, LC_ERR_PROTOCOL, 0L,
                       "scoped outbox transaction lost its duplicate result",
                       NULL, NULL, NULL);
    lcdc_push_status_error(L, LC_ERR_PROTOCOL, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_outbox_commit_result_init(&result);
  rc = lc_outbox_transaction_commit(transaction, &result, &error);
  lc_outbox_transaction_close(transaction);
  transaction_ud->transaction = NULL;
  lcdc_outbox_txn_clear_duplicate_result(L, transaction_ud);
  /* Callback values are deliberately not part of the transaction contract:
   * the only successful value is the durable commit result.  Clearing them
   * before pushing it preserves the conventional Lua result, error shape even
   * when a callback explicitly returns nil or incidental values. */
  lua_settop(L, 2);
  if (rc != LC_OK) {
    lc_outbox_commit_result_cleanup(&result);
    lua_settop(L, 2);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_outbox_commit_result(L, &result);
  lc_outbox_commit_result_cleanup(&result);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher(lua_State *L) {
  lcdc_outbox_ud *ud = lcdc_check_outbox(L, 1);
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_outbox_dispatcher_get_or_start(ud->outbox, &dispatcher, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_rawgeti(L, LUA_REGISTRYINDEX, ud->owner_ref);
  rc = lcdc_push_outbox_dispatcher(L, dispatcher, lua_gettop(L), 1, &error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    lc_outbox_dispatcher_close(dispatcher);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_remove(L, -2);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_bind_handlers(lua_State *L,
                                                lcdc_outbox_dispatcher_ud *ud,
                                                int options_index,
                                                lc_error *error) {
  lcdc_outbox_dispatcher_binding *binding = ud->binding;
  size_t handler_count;
  int matches;

  luaL_checktype(L, options_index, LUA_TTABLE);
  if (binding == NULL || binding->owner != lcdc_lua_main_thread(L)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher already belongs to another Lua state", NULL,
                        NULL, NULL);
  }
  if (binding->raw_pull_mode) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher is bound to raw pull mode", NULL, NULL,
                        NULL);
  }
  lua_getfield(L, options_index, "handlers");
  if (!lua_istable(L, -1)) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher handlers table is required", NULL, NULL,
                        NULL);
  }
  /* `lua_getfield` may have run an options metatable that closed this last
   * wrapper. The userdata remains valid, but its binding can already have
   * been released, so do not dereference the pre-lookup pointer. */
  if (ud->dispatcher == NULL || ud->binding == NULL || ud->binding != binding ||
      binding->owner != lcdc_lua_main_thread(L)) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher was closed while preparing handlers", NULL,
                        NULL, NULL);
  }
  handler_count = 0U;
  lua_pushnil(L);
  while (lua_next(L, -2) != 0) {
    size_t kind_length;
    const char *kind;

    if (lua_type(L, -2) != LUA_TSTRING) {
      lua_pop(L, 3);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "dispatcher handlers require non-empty kind keys "
                          "and function values",
                          NULL, NULL, NULL);
    }
    kind = lua_tolstring(L, -2, &kind_length);
    if (kind == NULL || kind_length == 0U ||
        memchr(kind, '\0', kind_length) != NULL || !lua_isfunction(L, -1)) {
      lua_pop(L, 3);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "dispatcher handlers require non-empty kind keys "
                          "and function values",
                          NULL, NULL, NULL);
    }
    ++handler_count;
    lua_pop(L, 1);
  }
  if (handler_count == 0U) {
    lua_pop(L, 1);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher handlers table must not be empty", NULL,
                        NULL, NULL);
  }
  if (!binding->handler_mode) {
    binding->handler_mode = 1;
    lcdc_outbox_dispatcher_set_handlers(L, ud, 1, -1);
    /* `lockdc` constructs this plain options table for its public façade.
     * Mark it only after the immutable native binding exists, allowing that
     * layer to retain its adapter map without treating rejected options as an
     * activation.  This is deliberately private to the two Lua layers. */
    /* This acknowledgement must not invoke a caller's __newindex hook: handler
     * binding is already live, and a Lua longjmp here would bypass its normal
     * dispatcher cleanup. */
    lua_pushstring(L, "_lockdc_facade_handlers_bound");
    lua_pushboolean(L, 1);
    lua_rawset(L, options_index);
    lua_pop(L, 1);
    return LC_OK;
  }
  if (!lcdc_outbox_dispatcher_push_handlers(L, ud, 1)) {
    lua_pop(L, 2);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher handler map is no longer available", NULL,
                        NULL, NULL);
  }
  matches = lua_rawequal(L, -1, -2);
  lua_pop(L, 2);
  if (!matches) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher handlers cannot change after activation",
                        NULL, NULL, NULL);
  }
  lua_pushstring(L, "_lockdc_facade_handlers_bound");
  lua_pushboolean(L, 1);
  lua_rawset(L, options_index);
  return LC_OK;
}

static int
lcdc_outbox_dispatcher_begin_consumption(lcdc_outbox_dispatcher_ud *ud,
                                         lcdc_outbox_dispatcher_binding **out,
                                         lc_error *error) {
  lcdc_outbox_dispatcher_binding *binding;

  if (out != NULL)
    *out = NULL;
  binding = ud->binding;
  if (binding == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher binding is unavailable", NULL, NULL, NULL);
  }
  pthread_mutex_lock(&lcdc_outbox_dispatcher_bindings_mutex);
  if (binding->consuming) {
    pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher cannot consume recursively from a handler",
                        NULL, NULL, NULL);
  }
  binding->consuming = 1;
  ++binding->consumption_count;
  pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
  if (out != NULL)
    *out = binding;
  return LC_OK;
}

static void lcdc_outbox_dispatcher_end_consumption(
    lua_State *L, lcdc_outbox_dispatcher_binding *binding) {
  lcdc_outbox_dispatcher_binding **link;

  if (binding == NULL)
    return;
  pthread_mutex_lock(&lcdc_outbox_dispatcher_bindings_mutex);
  binding->consuming = 0;
  if (binding->consumption_count > 0U)
    --binding->consumption_count;
  if (binding->wrapper_count == 0U && binding->consumption_count == 0U) {
    link = &lcdc_outbox_dispatcher_bindings;
    while (*link != NULL && *link != binding)
      link = &(*link)->next;
    if (*link == binding)
      *link = binding->next;
  } else {
    binding = NULL;
  }
  pthread_mutex_unlock(&lcdc_outbox_dispatcher_bindings_mutex);
  if (binding != NULL) {
    lcdc_outbox_dispatcher_clear_handlers(L, binding);
    free(binding);
  }
}

static void lcdc_outbox_job_consumed(lua_State *L, lcdc_outbox_job_ud *ud) {
  lcdc_outbox_job_invalidate_borrowed_state(L, ud);
  ud->job = NULL;
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
}

/* A handler-owned job is never transferable to Lua code.  Release every
 * native and registry resource before unwinding a failed handler path, so a
 * dispatcher stop cannot depend on a future Lua garbage-collection cycle. */
static void lcdc_outbox_job_discard(lua_State *L, lcdc_outbox_job_ud *ud) {
  if (ud == NULL)
    return;
  lcdc_outbox_job_invalidate_borrowed_state(L, ud);
  if (ud->job != NULL) {
    lc_outbox_job_close(ud->job);
    ud->job = NULL;
  }
  if (ud->terminal_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->terminal_ref);
    ud->terminal_ref = LUA_NOREF;
  }
  if (ud->owner_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
    ud->owner_ref = LUA_NOREF;
  }
}

static int lcdc_outbox_dispatcher_retry_handler_failure(
    lua_State *L, lcdc_outbox_job_ud *job_ud, const char *diagnostic,
    lc_error *error) {
  char bounded_diagnostic[LC_OUTBOX_MAX_DIAGNOSTIC_BYTES + 1U];
  lc_outbox_retry retry;
  size_t diagnostic_length;
  int rc;

  if (job_ud == NULL || job_ud->job == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "Lua outbox handler invalidated its claimed job", NULL,
                        NULL, NULL);
  }
  lc_outbox_retry_init(&retry);
  /* A Lua error message is unbounded, while the durable retry diagnostic is
   * deliberately capped. Preserve as much context as fits so handler failure
   * cannot turn into an abandoned active claim merely because the exception
   * was verbose. */
  if (diagnostic == NULL) {
    retry.diagnostic = "Lua outbox handler failed";
  } else {
    for (diagnostic_length = 0U;
         diagnostic_length <= LC_OUTBOX_MAX_DIAGNOSTIC_BYTES;
         ++diagnostic_length) {
      if (diagnostic[diagnostic_length] == '\0')
        break;
    }
    if (diagnostic_length <= LC_OUTBOX_MAX_DIAGNOSTIC_BYTES) {
      retry.diagnostic = diagnostic;
    } else {
      memcpy(bounded_diagnostic, diagnostic, LC_OUTBOX_MAX_DIAGNOSTIC_BYTES);
      bounded_diagnostic[LC_OUTBOX_MAX_DIAGNOSTIC_BYTES] = '\0';
      retry.diagnostic = bounded_diagnostic;
    }
  }
  rc = lc_outbox_job_retry(job_ud->job, &retry, error);
  if (rc == LC_OK)
    lcdc_outbox_job_consumed(L, job_ud);
  return rc;
}

typedef struct lcdc_deferred_terminal_apply {
  lcdc_outbox_job_ud *job_ud;
  int operation;
  lc_error *error;
  int rc;
} lcdc_deferred_terminal_apply;

/* Deferred handler outcomes are deliberately interpreted under lua_pcall().
 * A handler can hand us an ordinary table that becomes invalid only while its
 * fields are read (for example through a metatable); that must follow the
 * handler-failure retry path instead of longjmping past dispatcher cleanup. */
static int lcdc_outbox_job_apply_terminal_protected(lua_State *L) {
  lcdc_deferred_terminal_apply *apply =
      (lcdc_deferred_terminal_apply *)lua_touserdata(L, lua_upvalueindex(1));

  if (apply == NULL || apply->job_ud == NULL || apply->error == NULL)
    return luaL_error(L, "outbox deferred outcome context is unavailable");
  apply->rc = lcdc_outbox_job_apply_terminal(L, apply->job_ud, apply->operation,
                                             1, apply->error);
  return 0;
}

static int lcdc_outbox_dispatcher_handle_one(
    lua_State *L, lcdc_outbox_dispatcher_ud *dispatcher_ud, long timeout_ms,
    int with_state, int *handled, lc_error *error) {
  lc_outbox_job *job;
  lcdc_outbox_job_ud *job_ud;
  const char *diagnostic;
  int job_index;
  int rc;

  *handled = 0;
  job = NULL;
  rc = with_state ? lc_outbox_dispatcher_next_with_state(
                        dispatcher_ud->dispatcher, timeout_ms, &job, error)
                  : lc_outbox_dispatcher_next(dispatcher_ud->dispatcher,
                                              timeout_ms, &job, error);
  if (rc != LC_OK || job == NULL)
    return rc;
  if (!lcdc_outbox_dispatcher_push_handlers(L, dispatcher_ud, 1)) {
    lua_pop(L, 1);
    lc_outbox_job_close(job);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher handler map is no longer available", NULL,
                        NULL, NULL);
  }
  /* Handler maps are data, not an extension point. A raw lookup keeps a
   * hostile __index metamethod from longjmping past the claimed-job cleanup. */
  lua_pushstring(L, job->kind == NULL ? "" : job->kind);
  lua_rawget(L, -2);
  lua_remove(L, -2);
  if (!lua_isfunction(L, -1)) {
    lua_pop(L, 1);
    lc_outbox_job_close(job);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher has no handler for outbox kind", NULL, NULL,
                        NULL);
  }
  lcdc_push_outbox_job(L, job, 1);
  /* Keep one userdata reference below the function call: terminal handling
   * after the callback still needs its native handle, and Lua may otherwise
   * collect the argument immediately when lua_pcall returns. */
  lua_pushvalue(L, -1);
  lua_insert(L, -3);
  job_index = lua_gettop(L) - 2;
  job_ud = (lcdc_outbox_job_ud *)lua_touserdata(L, job_index);
  job_ud->handler_scoped = 1;
  if (lua_pcall(L, 1, 0, 0) != 0) {
    diagnostic = lua_tostring(L, -1);
    job_ud->handler_scoped = 0;
    rc = lcdc_outbox_dispatcher_retry_handler_failure(
        L, job_ud,
        diagnostic == NULL ? "Lua outbox handler failed" : diagnostic, error);
    lua_pop(L, 1);
    goto cleanup;
  }
  job_ud->handler_scoped = 0;
  if (job_ud->terminal_operation < 0) {
    rc = lcdc_outbox_dispatcher_retry_handler_failure(
        L, job_ud, "Lua outbox handler returned without an outcome", error);
  } else if (job_ud->job == NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "Lua outbox handler invalidated its claimed job", NULL,
                      NULL, NULL);
  } else {
    lcdc_deferred_terminal_apply apply;

    memset(&apply, 0, sizeof(apply));
    apply.job_ud = job_ud;
    apply.operation = job_ud->terminal_operation;
    apply.error = error;
    lua_pushlightuserdata(L, &apply);
    lua_pushcclosure(L, lcdc_outbox_job_apply_terminal_protected, 1);
    lua_rawgeti(L, LUA_REGISTRYINDEX, job_ud->terminal_ref);
    /* Outcome fields may invoke a metatable.  They remain inside the handler
     * ownership boundary until decoding and terminal application finish, so a
     * reentrant close cannot free the native claim under the C call. */
    job_ud->handler_scoped = 1;
    if (lua_pcall(L, 1, 0, 0) != 0) {
      diagnostic = lua_tostring(L, -1);
      job_ud->handler_scoped = 0;
      rc = lcdc_outbox_dispatcher_retry_handler_failure(
          L, job_ud,
          diagnostic == NULL ? "Lua outbox handler selected an invalid outcome"
                             : diagnostic,
          error);
      lua_pop(L, 1);
    } else {
      job_ud->handler_scoped = 0;
      rc = apply.rc;
    }
  }
cleanup:
  /* Success consumes the native job.  Any failed terminal apply or fallback
   * retry leaves a live claim, which must be closed before the handler frame
   * is released.  This also drops a deferred value that may reference the job
   * itself. */
  lcdc_outbox_job_discard(L, job_ud);
  lua_remove(L, job_index);
  if (rc == LC_OK)
    *handled = 1;
  return rc;
}

static int lcdc_outbox_dispatcher_pump(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  long max_jobs;
  long timeout_ms;
  int with_state;
  /* A handler-map validation failure performs no pull, so the observable
   * completed-job count is zero.  Initialize explicitly for optimized
   * toolchains that cannot infer the guarded loop assignment below. */
  long index = 0L;
  int handled;
  int consuming;
  lcdc_outbox_dispatcher_binding *consuming_binding;
  lc_error error;
  int rc;

  luaL_checktype(L, 2, LUA_TTABLE);
  max_jobs = 1L;
  timeout_ms = 0L;
  with_state = 0;
  lcdc_opt_integer_field(L, 2, "max_jobs", &max_jobs);
  lcdc_opt_integer_field(L, 2, "timeout_ms", &timeout_ms);
  lcdc_opt_boolean_field(L, 2, "with_state", &with_state);
  if (max_jobs < 1L || max_jobs > 1024L || timeout_ms < 0L) {
    return luaL_error(L, "dispatcher pump limits are invalid");
  }
  lc_error_init(&error);
  consuming = 0;
  consuming_binding = NULL;
  /* Handler-map lookup is allowed to run Lua metamethods. Do it before
   * consuming is marked so a Lua longjmp cannot strand the shared binding. */
  rc = lcdc_outbox_dispatcher_bind_handlers(L, ud, 2, &error);
  if (rc == LC_OK) {
    rc = lcdc_outbox_dispatcher_begin_consumption(ud, &consuming_binding,
                                                  &error);
    consuming = rc == LC_OK;
  }
  if (rc == LC_OK) {
    for (index = 0L; index < max_jobs; ++index) {
      handled = 0;
      rc = lcdc_outbox_dispatcher_handle_one(
          L, ud, index == 0L ? timeout_ms : 0L, with_state, &handled, &error);
      if (rc != LC_OK || !handled)
        break;
    }
  }
  if (consuming)
    lcdc_outbox_dispatcher_end_consumption(L, consuming_binding);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushinteger(L, (lua_Integer)index);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_run(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  lc_error error;
  int handled;
  int consuming;
  lcdc_outbox_dispatcher_binding *consuming_binding;
  int with_state;
  int rc;

  luaL_checktype(L, 2, LUA_TTABLE);
  lc_error_init(&error);
  consuming = 0;
  consuming_binding = NULL;
  with_state = 0;
  lcdc_opt_boolean_field(L, 2, "with_state", &with_state);
  /* See pump(): binding validates Lua-owned handler data before it enters the
   * consumption scope whose cleanup must always run. */
  rc = lcdc_outbox_dispatcher_bind_handlers(L, ud, 2, &error);
  if (rc == LC_OK) {
    rc = lcdc_outbox_dispatcher_begin_consumption(ud, &consuming_binding,
                                                  &error);
    consuming = rc == LC_OK;
  }
  while (rc == LC_OK) {
    handled = 0;
    rc = lcdc_outbox_dispatcher_handle_one(L, ud, -1L, with_state, &handled,
                                           &error);
    if (rc == LC_OK && !handled)
      continue;
  }
  if (consuming)
    lcdc_outbox_dispatcher_end_consumption(L, consuming_binding);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_next_common(lua_State *L, int with_state) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  lc_outbox_job *job = NULL;
  lc_error error;
  long timeout_ms = -1L;
  int rc;

  if (ud->binding != NULL && ud->binding->handler_mode)
    return luaL_error(L, "dispatcher is bound to Lua handler mode");
  if (!lua_isnoneornil(L, 2))
    timeout_ms = lcdc_check_long(L, 2, "outbox dispatcher next timeout");
  if (ud->binding == NULL || ud->binding->owner != lcdc_lua_main_thread(L))
    return luaL_error(L, "dispatcher already belongs to another Lua state");
  ud->binding->raw_pull_mode = 1;
  lc_error_init(&error);
  rc =
      with_state
          ? lc_outbox_dispatcher_next_with_state(ud->dispatcher, timeout_ms,
                                                 &job, &error)
          : lc_outbox_dispatcher_next(ud->dispatcher, timeout_ms, &job, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_error_cleanup(&error);
  if (job == NULL) {
    lua_pushnil(L);
    return 1;
  }
  return lcdc_push_outbox_job(L, job, 1);
}

static int lcdc_outbox_dispatcher_next(lua_State *L) {
  return lcdc_outbox_dispatcher_next_common(L, 0);
}

static int lcdc_outbox_dispatcher_next_with_state(lua_State *L) {
  return lcdc_outbox_dispatcher_next_common(L, 1);
}

/* Private facade identity. It lets the Lua layer share handler adapters among
 * aliases without retaining an adapter after the native binding retires. */
static int lcdc_outbox_dispatcher_binding_id(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);

  if (ud->binding == NULL)
    return luaL_error(L, "outbox dispatcher is closed");
  lua_pushinteger(L, (lua_Integer)ud->binding->binding_id);
  return 1;
}

static int lcdc_outbox_dispatcher_stats(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  lc_outbox_stats stats;
  lc_error error;
  int rc;

  lc_outbox_stats_init(&stats);
  lc_error_init(&error);
  rc = lc_outbox_dispatcher_get_stats(ud->dispatcher, &stats, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_push_outbox_stats(L, &stats, &error);
  if (rc != LC_OK) {
    lc_outbox_stats_cleanup(&stats);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lc_outbox_stats_cleanup(&stats);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_reconcile(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_outbox_dispatcher_reconcile(ud->dispatcher, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_replay_dead_letter(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  const char *outbox_key = luaL_checkstring(L, 2);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_outbox_dispatcher_replay_dead_letter(ud->dispatcher, outbox_key,
                                               &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_delete_dead_letter(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  const char *outbox_key = luaL_checkstring(L, 2);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_outbox_dispatcher_delete_dead_letter(ud->dispatcher, outbox_key,
                                               &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_export_dead_letters(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  lc_outbox_dispatcher *dispatcher = NULL;
  lc_dead_letter_export_opts options;
  lc_dead_letter_export_res result;
  lcdc_output output;
  lc_error error;
  const char *format;
  long limit;
  int rc;

  lc_dead_letter_export_opts_init(&options);
  lc_dead_letter_export_res_init(&result);
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    format = lcdc_opt_string_field(L, 2, "format");
    if (format != NULL) {
      if (strcmp(format, "json") == 0)
        options.format = LC_DEAD_LETTER_EXPORT_JSON;
      else if (strcmp(format, "jsonl") == 0)
        options.format = LC_DEAD_LETTER_EXPORT_JSONL;
      else
        return luaL_error(L, "dead-letter export format must be json or jsonl");
    }
    limit = 0L;
    if (lcdc_opt_integer_field(L, 2, "limit", &limit)) {
      if (limit < 0L)
        return luaL_error(L, "dead-letter export limit must be non-negative");
      options.limit = (size_t)limit;
    }
  }
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_outbox_dispatcher_revalidate(ud, &dispatcher, &error);
  ud->streaming = rc == LC_OK;
  if (rc == LC_OK) {
    rc = lc_outbox_dispatcher_export_dead_letters(dispatcher, &options,
                                                  output.sink, &result, &error);
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_set_integer_field(L, "exported", (long)result.exported);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_dispatcher_notify_outbox_key(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  const char *key = luaL_checkstring(L, 2);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lc_outbox_dispatcher_notify_outbox_key(ud->dispatcher, key, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_stop(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  lc_error error;
  long deadline_ms = -1L;
  int rc;

  if (!lua_isnoneornil(L, 2))
    deadline_ms = lcdc_check_long(L, 2, "outbox dispatcher stop deadline");
  lc_error_init(&error);
  if (ud->binding != NULL && ud->binding->consuming) {
    (void)lc_error_set(
        &error, LC_ERR_INVALID, 0L,
        "outbox dispatcher stop is not allowed inside its handler", NULL, NULL,
        NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_outbox_dispatcher_stop(ud->dispatcher, deadline_ms, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_outbox_dispatcher_binding_note_stopped(L, ud->binding);
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_wait(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);
  lc_error error;
  long deadline_ms = -1L;
  int rc;

  if (!lua_isnoneornil(L, 2))
    deadline_ms = lcdc_check_long(L, 2, "outbox dispatcher wait deadline");
  lc_error_init(&error);
  if (ud->binding != NULL && ud->binding->consuming) {
    (void)lc_error_set(
        &error, LC_ERR_INVALID, 0L,
        "outbox dispatcher wait is not allowed inside its handler", NULL, NULL,
        NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lc_outbox_dispatcher_wait(ud->dispatcher, deadline_ms, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_outbox_dispatcher_binding_note_stopped(L, ud->binding);
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_dispatcher_close(lua_State *L) {
  lcdc_outbox_dispatcher_ud *ud = lcdc_check_outbox_dispatcher(L, 1);

  if (ud->binding != NULL && ud->binding->consuming) {
    return luaL_error(
        L, "outbox dispatcher close is not allowed inside its handler");
  }
  return lcdc_outbox_dispatcher_gc(L);
}

static int lcdc_outbox_txn_close(lua_State *L) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);

  if (ud->callback_scoped) {
    return luaL_error(
        L, "outbox transaction close is not allowed inside its callback");
  }
  return lcdc_outbox_txn_gc(L);
}

static void lcdc_outbox_txn_note_staging_failure(lcdc_outbox_txn_ud *ud) {
  if (ud != NULL && (ud->callback_scoped || ud->xa_control != NULL)) {
    if (ud->callback_staging_failed != ULONG_MAX)
      ++ud->callback_staging_failed;
    lcdc_xa_staging_begin(ud->xa_control);
  }
}

/* Lua argument failures longjmp and may be caught by the callback with pcall.
 * Arm the callback as rollback-only before parsing a state-changing operation;
 * only a fully successful operation removes its own guard. Nested failures
 * leave an outstanding guard even when an outer operation succeeds. */
static void lcdc_outbox_txn_begin_staging(lcdc_outbox_txn_ud *ud) {
  if (ud != NULL && (ud->callback_scoped || ud->xa_control != NULL)) {
    if (ud->callback_staging_failed != ULONG_MAX)
      ++ud->callback_staging_failed;
    lcdc_xa_staging_begin(ud->xa_control);
  }
}

static void lcdc_outbox_txn_finish_staging(lcdc_outbox_txn_ud *ud) {
  if (ud != NULL && (ud->callback_scoped || ud->xa_control != NULL)) {
    if (ud->callback_staging_failed != 0UL &&
        ud->callback_staging_failed != ULONG_MAX)
      --ud->callback_staging_failed;
    lcdc_xa_staging_succeeded(ud->xa_control);
  }
}

static int lcdc_outbox_txn_id(lua_State *L) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lua_pushstring(L, lc_outbox_transaction_id(ud->transaction));
  return 1;
}
static int lcdc_outbox_txn_bind_xa(lua_State *L) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_error *error = lcdc_xa_error_scope(L, 2);
  lc_xa_transaction *tx = NULL;
  int reference, rc;
  rc = lcdc_xa_transaction_view(L, 2, &tx, error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, error);
    lc_error_cleanup(error);
    return 3;
  }
  lua_pushvalue(L, 2);
  reference = luaL_ref(L, LUA_REGISTRYINDEX);
  rc = lc_outbox_transaction_bind_xa(ud->transaction, tx, error);
  if (rc != LC_OK) {
    luaL_unref(L, LUA_REGISTRYINDEX, reference);
    lcdc_push_status_error(L, rc, error);
    lc_error_cleanup(error);
    return 3;
  }
  if (ud->xa_owner_ref != LUA_NOREF)
    luaL_unref(L, LUA_REGISTRYINDEX, ud->xa_owner_ref);
  ud->xa_owner_ref = reference;
  ud->xa_control = lcdc_xa_transaction_control(L, 2);
  lc_error_cleanup(error);
  lua_pushboolean(L, 1);
  return 1;
}
static int lcdc_outbox_txn_xa_terminal(lua_State *L, int rollback) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_error *error = lcdc_xa_error_scope(L, 2);
  lc_xa_coordinator_operation **out;
  lua_Integer deadline = 0;
  int rc;
  if (!lua_isnil(L, 2) && !lua_isinteger(L, 2)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "XA deadline must be an exact Lua integer", NULL, NULL, NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, error);
    lc_error_cleanup(error);
    return 3;
  }
  if (!lua_isnil(L, 2))
    deadline = lua_tointeger(L, 2);
  if (!rollback && (ud->callback_staging_failed ||
                    lcdc_xa_transaction_failed(ud->xa_control))) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "XA outbox is rollback-only after a failed staged operation",
                 NULL, NULL, NULL);
    lcdc_push_status_error(L, LC_ERR_INVALID, error);
    lc_error_cleanup(error);
    return 3;
  }
  out = lcdc_xa_operation_shell(L, 1);
  rc = rollback
           ? lc_outbox_transaction_rollback_start(ud->transaction,
                                                  (int64_t)deadline, out, error)
           : lc_outbox_transaction_commit_start(ud->transaction,
                                                (int64_t)deadline, out, error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, error);
    lc_error_cleanup(error);
    return 3;
  }
  lc_error_cleanup(error);
  return 1;
}
static int lcdc_outbox_txn_commit_start(lua_State *L) {
  return lcdc_outbox_txn_xa_terminal(L, 0);
}
static int lcdc_outbox_txn_rollback_start(lua_State *L) {
  return lcdc_outbox_txn_xa_terminal(L, 1);
}

static void lcdc_outbox_txn_note_duplicate(lua_State *L, lcdc_outbox_txn_ud *ud,
                                           int result_index) {
  ud->callback_duplicate_seen = 1;
  if (ud->callback_has_domain_participant)
    ud->callback_duplicate_after_domain_participant = 1;
  if (ud->callback_scoped && ud->callback_duplicate_result_ref == LUA_NOREF) {
    lua_pushvalue(L, result_index);
    ud->callback_duplicate_result_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  }
}

static void lcdc_outbox_txn_clear_duplicate_result(lua_State *L,
                                                   lcdc_outbox_txn_ud *ud) {
  if (ud->callback_duplicate_result_ref != LUA_NOREF) {
    luaL_unref(L, LUA_REGISTRYINDEX, ud->callback_duplicate_result_ref);
    ud->callback_duplicate_result_ref = LUA_NOREF;
  }
}

static int lcdc_outbox_txn_acquire(lua_State *L) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_outbox_transaction *transaction = NULL;
  lc_outbox_participant_request request;
  lc_outbox_participant *participant = NULL;
  lc_error error;
  int root_index;
  int rc;

  lcdc_outbox_txn_begin_staging(ud);
  root_index = lcdc_parse_outbox_participant_request(L, 2, &request);
  lc_error_init(&error);
  rc = lcdc_outbox_txn_revalidate(ud, &transaction, &error);
  if (rc == LC_OK) {
    rc = lc_outbox_transaction_acquire(transaction, &request, &participant,
                                       &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  ud->callback_has_fresh_participant = 1;
  ud->callback_has_domain_participant = 1;
  lc_error_cleanup(&error);
  rc = lcdc_push_outbox_participant(L, participant, 1, ud);
  lcdc_outbox_txn_finish_staging(ud);
  return rc;
}

static int lcdc_outbox_txn_append(lua_State *L) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_outbox_transaction *transaction;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_source *payload = NULL;
  lc_error error;
  int root_index;
  int rc;

  lcdc_outbox_txn_begin_staging(ud);
  root_index = lcdc_parse_outbox_entry(L, 2, &entry);
  lc_outbox_receipt_init(&receipt);
  lc_error_init(&error);
  transaction = NULL;
  rc = lcdc_source_from_value(L, 3, &payload, &error);
  if (rc == LC_OK) {
    rc = lcdc_outbox_txn_revalidate(ud, &transaction, &error);
  }
  if (rc == LC_OK) {
    ++ud->streaming_count;
    rc = lc_outbox_transaction_append(transaction, &entry, payload, &receipt,
                                      &error);
    lc_source_close(payload);
    --ud->streaming_count;
  } else {
    lc_source_close(payload);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud);
    lc_outbox_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (receipt.duplicate) {
    lcdc_push_outbox_receipt(L, &receipt);
    lcdc_outbox_txn_note_duplicate(L, ud, -1);
  } else {
    ud->callback_has_fresh_participant = 1;
    lcdc_push_outbox_receipt(L, &receipt);
  }
  lc_outbox_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  lcdc_outbox_txn_finish_staging(ud);
  return 1;
}

static int lcdc_outbox_txn_accept_command(lua_State *L) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_outbox_transaction *transaction = NULL;
  lc_command_request request;
  lc_command_receipt receipt;
  lc_error error;
  int root_index;
  int rc;

  lcdc_outbox_txn_begin_staging(ud);
  root_index = lcdc_parse_command_request(L, 2, &request);
  lc_command_receipt_init(&receipt);
  lc_error_init(&error);
  rc = lcdc_outbox_txn_revalidate(ud, &transaction, &error);
  if (rc == LC_OK) {
    rc = lc_outbox_transaction_accept_command(transaction, &request, &receipt,
                                              &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud);
    lc_command_receipt_cleanup(&receipt);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (receipt.duplicate) {
    lcdc_push_command_receipt(L, &receipt);
    lcdc_outbox_txn_note_duplicate(L, ud, -1);
  } else {
    ud->callback_has_fresh_participant = 1;
    lcdc_push_command_receipt(L, &receipt);
  }
  lc_command_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
  lcdc_outbox_txn_finish_staging(ud);
  return 1;
}

static int lcdc_outbox_txn_accept_inbox(lua_State *L) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_outbox_transaction *transaction = NULL;
  lc_inbox_message message;
  lc_inbox_accept_result result;
  lc_error error;
  int root_index;
  int rc;

  lcdc_outbox_txn_begin_staging(ud);
  memset(&result, 0, sizeof(result));
  root_index = lcdc_parse_inbox_message(L, 2, &message);
  lc_error_init(&error);
  rc = lcdc_outbox_txn_revalidate(ud, &transaction, &error);
  if (rc == LC_OK) {
    rc = lc_outbox_transaction_accept_inbox(transaction, &message, &result,
                                            &error);
  }
  lua_remove(L, root_index);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (result.duplicate) {
    lcdc_push_inbox_result(L, &result);
    lcdc_outbox_txn_note_duplicate(L, ud, -1);
  } else {
    ud->callback_has_fresh_participant = 1;
    lcdc_push_inbox_result(L, &result);
  }
  lc_error_cleanup(&error);
  lcdc_outbox_txn_finish_staging(ud);
  return 1;
}

static int lcdc_outbox_txn_terminal_command(lua_State *L, int failed) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_outbox_transaction *transaction;
  lc_command_result result;
  lc_source *body = NULL;
  lc_error error;
  int rc;

  lcdc_outbox_txn_begin_staging(ud);
  transaction = NULL;
  lc_command_result_init(&result);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (failed) {
    lcdc_require_string_field(L, 2, "failure_code", &result.failure_code);
    result.failure_message = lcdc_opt_string_field(L, 2, "failure_message");
  } else {
    lcdc_require_string_field(L, 2, "result_code", &result.result_code);
    result.result_reference = lcdc_opt_string_field(L, 2, "result_reference");
    result.content_type = lcdc_opt_string_field(L, 2, "content_type");
    lua_getfield(L, 2, "body");
    if (!lua_isnil(L, -1)) {
      lc_error_init(&error);
      rc = lcdc_source_from_value(L, -1, &body, &error);
      if (rc != LC_OK) {
        lua_pop(L, 1);
        lcdc_outbox_txn_note_staging_failure(ud);
        lcdc_push_status_error(L, rc, &error);
        lc_error_cleanup(&error);
        return 3;
      }
    }
    lua_pop(L, 1);
    result.body = body;
  }
  lc_error_init(&error);
  rc = lcdc_outbox_txn_revalidate(ud, &transaction, &error);
  if (rc == LC_OK && body != NULL)
    ++ud->streaming_count;
  if (rc == LC_OK) {
    rc = failed
             ? lc_outbox_transaction_fail_command(transaction, &result, &error)
             : lc_outbox_transaction_complete_command(transaction, &result,
                                                      &error);
  }
  lc_source_close(body);
  if (transaction != NULL && body != NULL)
    --ud->streaming_count;
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  lcdc_outbox_txn_finish_staging(ud);
  return 1;
}

static int lcdc_outbox_txn_complete_command(lua_State *L) {
  return lcdc_outbox_txn_terminal_command(L, 0);
}

static int lcdc_outbox_txn_fail_command(lua_State *L) {
  return lcdc_outbox_txn_terminal_command(L, 1);
}

static int lcdc_outbox_txn_terminal(lua_State *L, int rollback) {
  lcdc_outbox_txn_ud *ud = lcdc_check_outbox_txn(L, 1);
  lc_error error;
  lc_outbox_commit_result commit_result;
  int rc;

  if (ud->callback_scoped) {
    return luaL_error(
        L, "outbox transaction terminal decisions are not allowed inside its "
           "callback");
  }
  lc_error_init(&error);
  lc_outbox_commit_result_init(&commit_result);
  rc = rollback ? lc_outbox_transaction_rollback(ud->transaction, &error)
                : lc_outbox_transaction_commit(ud->transaction, &commit_result,
                                               &error);
  if (rc != LC_OK) {
    lc_outbox_commit_result_cleanup(&commit_result);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  if (rollback) {
    lua_pushboolean(L, 1);
  } else {
    lcdc_push_outbox_commit_result(L, &commit_result);
  }
  lc_outbox_commit_result_cleanup(&commit_result);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_txn_commit(lua_State *L) {
  return lcdc_outbox_txn_terminal(L, 0);
}

static int lcdc_outbox_txn_rollback(lua_State *L) {
  return lcdc_outbox_txn_terminal(L, 1);
}

static int lcdc_outbox_participant_close(lua_State *L) {
  (void)lcdc_check_outbox_participant(L, 1);
  return lcdc_outbox_participant_gc(L);
}

static int lcdc_outbox_participant_info(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lcdc_return_outbox_participant_info(L, ud->participant, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_describe(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = ud->participant->describe(ud->participant, &error);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_participant_info_for_transaction(L, ud, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_get(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lcdc_output output;
  lc_get_opts opts;
  lc_get_res result;
  lc_error error;
  int rc;

  lc_get_opts_init(&opts);
  memset(&result, 0, sizeof(result));
  lc_error_init(&error);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    lcdc_opt_boolean_field(L, 2, "public_read", &opts.public_read);
  }
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  ud->streaming = rc == LC_OK;
  if (rc == LC_OK) {
    ++ud->transaction_ud->streaming_count;
    rc = participant->get(participant, output.sink, &opts, &result, &error);
    --ud->transaction_ud->streaming_count;
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    ud->streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_get_res_cleanup(&result);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_set_bool_field(L, "no_content", result.no_content);
  lcdc_set_string_field(L, "content_type", result.content_type);
  lcdc_set_string_field(L, "etag", result.etag);
  rc = lcdc_set_int64_field(L, "version", result.version, &error);
  if (rc != LC_OK) {
    lua_pop(L, 2);
    lcdc_push_status_error(L, rc, &error);
    lc_get_res_cleanup(&result);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_set_integer_field(L, "fencing_token", result.fencing_token);
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lcdc_set_state_retention(L, result.retention_support,
                           result.has_retention_expires_at_unix,
                           result.retention_expires_at_unix);
  lc_get_res_cleanup(&result);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_participant_update(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant;
  lc_update_opts opts;
  lc_source *source = NULL;
  lc_error error;
  int rc;

  lc_update_opts_init(&opts);
  lc_error_init(&error);
  participant = NULL;
  if (!lua_isnoneornil(L, 3)) {
    luaL_checktype(L, 3, LUA_TTABLE);
    if (!lcdc_read_state_retention(L, 3, &opts.has_retention_seconds,
                                   &opts.retention_seconds)) {
      lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
      return 3;
    }
    opts.if_state_etag = lcdc_opt_string_field(L, 3, "if_state_etag");
    if (lcdc_opt_version_field(L, 3, "if_version", &opts.if_version)) {
      opts.has_if_version = 1;
    }
    opts.content_type = lcdc_opt_string_field(L, 3, "content_type");
  }
  rc = lcdc_source_from_value(L, 2, &source, &error);
  if (rc == LC_OK) {
    rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  }
  if (rc == LC_OK) {
    ud->streaming = 1;
    ++ud->transaction_ud->streaming_count;
    rc = participant->update(participant, source, &opts, &error);
    lc_source_close(source);
    --ud->transaction_ud->streaming_count;
    ud->streaming = 0;
  } else {
    lc_source_close(source);
  }
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_participant_info_for_transaction(L, ud, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_mutate(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lc_mutate_req request;
  lc_error error;
  const char **mutations = NULL;
  size_t mutation_count = 0U;
  int rc;

  lc_mutate_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &request.has_retention_seconds,
                                 &request.retention_seconds)) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    return 3;
  }
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  request.mutations = mutations;
  request.mutation_count = mutation_count;
  request.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_version_field(L, 2, "if_version", &request.if_version)) {
    request.has_if_version = 1;
  }
  lc_error_init(&error);
  rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  if (rc == LC_OK)
    rc = participant->mutate(participant, &request, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_participant_info_for_transaction(L, ud, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_mutate_local(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lc_mutate_local_req request;
  lc_error error;
  const char **mutations = NULL;
  size_t mutation_count = 0U;
  int rc;

  lc_mutate_local_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &request.update.has_retention_seconds,
                                 &request.update.retention_seconds)) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    return 3;
  }
  lcdc_parse_string_array(L, 2, "mutations", &mutations, &mutation_count);
  request.mutations = mutations;
  request.mutation_count = mutation_count;
  lcdc_opt_boolean_field(L, 2, "disable_fetched_cas",
                         &request.disable_fetched_cas);
  request.file_value_base_dir =
      lcdc_opt_string_field(L, 2, "file_value_base_dir");
  request.update.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
  if (lcdc_opt_version_field(L, 2, "if_version", &request.update.if_version)) {
    request.update.has_if_version = 1;
  }
  request.update.content_type = lcdc_opt_string_field(L, 2, "content_type");
  lc_error_init(&error);
  rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  if (rc == LC_OK)
    rc = participant->mutate_local(participant, &request, &error);
  lcdc_free_string_array(&mutations);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_participant_info_for_transaction(L, ud, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_metadata(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lc_metadata_req request;
  lc_error error;
  int rc;

  lc_metadata_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  if (!lcdc_read_state_retention(L, 2, &request.has_retention_seconds,
                                 &request.retention_seconds)) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    return 3;
  }
  if (lcdc_opt_boolean_field(L, 2, "query_hidden", &request.query_hidden)) {
    request.has_query_hidden = 1;
  }
  if (lcdc_opt_version_field(L, 2, "if_version", &request.if_version)) {
    request.has_if_version = 1;
  }
  lc_error_init(&error);
  rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  if (rc == LC_OK)
    rc = participant->metadata(participant, &request, &error);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_participant_info_for_transaction(L, ud, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_remove(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lc_remove_req request;
  lc_error error;
  int rc;

  lc_remove_req_init(&request);
  if (!lua_isnoneornil(L, 2)) {
    luaL_checktype(L, 2, LUA_TTABLE);
    request.if_state_etag = lcdc_opt_string_field(L, 2, "if_state_etag");
    if (lcdc_opt_version_field(L, 2, "if_version", &request.if_version)) {
      request.has_if_version = 1;
    }
  }
  lc_error_init(&error);
  rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  if (rc == LC_OK)
    rc = participant->remove(participant, &request, &error);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_participant_info_for_transaction(L, ud, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_keepalive(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lc_keepalive_req request;
  lc_error error;
  int rc;

  lc_keepalive_req_init(&request);
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_opt_integer_field(L, 2, "ttl_seconds", &request.ttl_seconds);
  lc_error_init(&error);
  rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  if (rc == LC_OK)
    rc = participant->keepalive(participant, &request, &error);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_participant_info_for_transaction(L, ud, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_participant_attach(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant;
  lc_attach_req request;
  lc_attach_res result;
  lc_source *source = NULL;
  lc_error error;
  int rc;

  lc_attach_req_init(&request);
  memset(&result, 0, sizeof(result));
  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_require_string_field(L, 2, "name", &request.name);
  request.content_type = lcdc_opt_string_field(L, 2, "content_type");
  if (lcdc_opt_integer_field(L, 2, "max_bytes", &request.max_bytes)) {
    request.has_max_bytes = 1;
  }
  lcdc_opt_boolean_field(L, 2, "prevent_overwrite", &request.prevent_overwrite);
  lc_error_init(&error);
  participant = NULL;
  rc = lcdc_source_from_value(L, 3, &source, &error);
  if (rc == LC_OK) {
    rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  }
  if (rc == LC_OK) {
    ud->streaming = 1;
    ++ud->transaction_ud->streaming_count;
    rc = participant->attach(participant, &request, source, &result, &error);
    lc_source_close(source);
    --ud->transaction_ud->streaming_count;
    ud->streaming = 0;
  } else {
    lc_source_close(source);
  }
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lc_attach_res_cleanup(&result);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_push_attachment_info(L, &result.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_bool_field(L, "noop", result.noop);
  rc = lcdc_set_int64_field(L, "version", result.version, &error);
  if (rc != LC_OK) {
    lua_pop(L, 1);
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lc_attach_res_cleanup(&result);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lc_attach_res_cleanup(&result);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_participant_get_attachment(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lc_attachment_get_req request;
  lc_attachment_get_res result;
  lcdc_output output;
  lc_error error;
  int rc;

  lc_attachment_get_req_init(&request);
  memset(&result, 0, sizeof(result));
  luaL_checktype(L, 2, LUA_TTABLE);
  lua_getfield(L, 2, "selector");
  lcdc_parse_attachment_selector(L, -1, &request.selector);
  lua_pop(L, 1);
  lcdc_opt_boolean_field(L, 2, "public_read", &request.public_read);
  lc_error_init(&error);
  rc = lcdc_init_output(L, 3, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  ud->streaming = rc == LC_OK;
  if (rc == LC_OK) {
    ++ud->transaction_ud->streaming_count;
    rc = participant->get_attachment(participant, &request, output.sink,
                                     &result, &error);
    --ud->transaction_ud->streaming_count;
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    ud->streaming = 0;
    lc_attachment_get_res_cleanup(&result);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->streaming = 0;
  lua_newtable(L);
  lcdc_push_attachment_info(L, &result.attachment);
  lua_setfield(L, -2, "attachment");
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lc_attachment_get_res_cleanup(&result);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_participant_list_attachments(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_attachment_list result;
  lc_error error;
  size_t index;
  int rc;

  memset(&result, 0, sizeof(result));
  lc_error_init(&error);
  rc = ud->participant->list_attachments(ud->participant, &result, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_newtable(L);
  lcdc_set_string_field(L, "correlation_id", result.correlation_id);
  lua_createtable(L, (int)result.count, 0);
  for (index = 0U; index < result.count; ++index) {
    lcdc_push_attachment_info(L, &result.items[index]);
    lua_rawseti(L, -2, (lua_Integer)(index + 1U));
  }
  lua_setfield(L, -2, "items");
  lc_attachment_list_cleanup(&result);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_participant_delete_attachment(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_outbox_participant *participant = NULL;
  lc_attachment_selector selector;
  lc_error error;
  int deleted = 0;
  int rc;

  luaL_checktype(L, 2, LUA_TTABLE);
  lcdc_parse_attachment_selector(L, 2, &selector);
  lc_error_init(&error);
  rc = lcdc_outbox_participant_revalidate(ud, &participant, &error);
  if (rc == LC_OK) {
    rc = participant->delete_attachment(participant, &selector, &deleted,
                                        &error);
  }
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, deleted);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_participant_delete_all_attachments(lua_State *L) {
  lcdc_outbox_participant_ud *ud = lcdc_check_outbox_participant(L, 1);
  lc_error error;
  int deleted_count = 0;
  int rc;

  lc_error_init(&error);
  rc = ud->participant->delete_all_attachments(ud->participant, &deleted_count,
                                               &error);
  if (rc != LC_OK) {
    lcdc_outbox_txn_note_staging_failure(ud->transaction_ud);
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushinteger(L, (lua_Integer)deleted_count);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_job_close(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);

  if (ud->payload_streaming)
    return luaL_error(
        L, "outbox job close is not allowed while payload is streaming");
  if (ud->borrowed_state != NULL && ud->borrowed_state->streaming)
    return luaL_error(
        L, "outbox job close is not allowed while checkpoint is streaming");
  if (ud->handler_scoped) {
    return luaL_error(
        L, "outbox job close is not allowed inside its dispatcher handler");
  }
  return lcdc_outbox_job_gc(L);
}

static int lcdc_outbox_job_state(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lc_lease *lease;

  if (ud->job == NULL)
    return luaL_error(L, "outbox job is closed");
  lease = lc_outbox_job_state(ud->job);
  if (lease == NULL) {
    lua_pushnil(L);
    return 1;
  }
  if (ud->borrowed_state != NULL && ud->borrowed_state->lease != NULL) {
    lua_getiuservalue(L, 1, 1);
    lua_getfield(L, -1, "state");
    lua_remove(L, -2);
    if (luaL_testudata(L, -1, LCDC_LEASE_MT) != NULL)
      return 1;
    lua_pop(L, 1);
  }
  lcdc_outbox_job_invalidate_borrowed_state(L, ud);
  if (lcdc_push_borrowed_lease_owned(L, lease, 1, ud) != 1) {
    return luaL_error(L, "failed to create Lua outbox checkpoint lease");
  }
  ud->borrowed_state = (lcdc_lease_ud *)luaL_checkudata(L, -1, LCDC_LEASE_MT);
  lua_getiuservalue(L, 1, 1);
  lua_pushvalue(L, -2);
  lua_setfield(L, -2, "state");
  lua_pop(L, 1);
  return 1;
}

static int lcdc_outbox_job_info(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lc_error error;
  int rc;

  lc_error_init(&error);
  rc = lcdc_return_outbox_job_info(L, ud->job, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_job_write_payload(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lc_outbox_job *job = NULL;
  lcdc_output output;
  lc_error error;
  int rc;

  if (ud->payload_streaming)
    return luaL_error(L, "outbox job payload cannot be streamed recursively");
  lc_error_init(&error);
  rc = lcdc_init_output(L, 2, &output, &error);
  if (rc == LC_OK)
    rc = lcdc_outbox_job_revalidate(ud, &job, &error);
  ud->payload_streaming = rc == LC_OK;
  if (rc == LC_OK) {
    rc = lc_outbox_job_write_payload(job, output.sink, &output.written, &error);
  }
  if (rc != LC_OK) {
    if (output.sink != NULL)
      lc_sink_close(output.sink);
    ud->payload_streaming = 0;
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lcdc_push_output(L, &output);
  ud->payload_streaming = 0;
  lua_pushinteger(L, (lua_Integer)output.written);
  lc_error_cleanup(&error);
  return 2;
}

static int lcdc_outbox_job_renew(lua_State *L) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lc_error error;
  long ttl_seconds = lcdc_check_long(L, 2, "outbox renewal ttl");
  int rc;

  if (ud->payload_streaming)
    return luaL_error(
        L, "outbox job renew is not allowed while payload is streaming");
  if (ud->borrowed_state != NULL && ud->borrowed_state->streaming)
    return luaL_error(L, "outbox job renew is not allowed while checkpoint is "
                         "streaming");
  lc_error_init(&error);
  rc = lc_outbox_job_renew(ud->job, ttl_seconds, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  rc = lcdc_return_outbox_job_info(L, ud->job, &error);
  lc_error_cleanup(&error);
  return rc;
}

static int lcdc_outbox_job_apply_terminal(lua_State *L, lcdc_outbox_job_ud *ud,
                                          int operation, int value_index,
                                          lc_error *error) {
  lc_outbox_completion completion;
  lc_outbox_job *job = NULL;
  int rc;

  lc_outbox_completion_init(&completion);
  if (operation == 0) {
    if (value_index != 0 && !lua_isnoneornil(L, value_index)) {
      luaL_checktype(L, value_index, LUA_TTABLE);
      completion.delivery_reference =
          lcdc_opt_string_field(L, value_index, "delivery_reference");
      completion.response_digest =
          lcdc_opt_string_field(L, value_index, "response_digest");
    }
    rc = lcdc_outbox_job_revalidate(ud, &job, error);
    if (rc == LC_OK)
      rc = lc_outbox_job_complete(job, &completion, error);
  } else if (operation == 1) {
    lc_outbox_retry retry;

    lc_outbox_retry_init(&retry);
    if (value_index != 0 && !lua_isnoneornil(L, value_index)) {
      luaL_checktype(L, value_index, LUA_TTABLE);
      lcdc_opt_integer_field(L, value_index, "delay_seconds",
                             &retry.delay_seconds);
      retry.diagnostic = lcdc_opt_string_field(L, value_index, "diagnostic");
    }
    rc = lcdc_outbox_job_revalidate(ud, &job, error);
    if (rc == LC_OK)
      rc = lc_outbox_job_retry(job, &retry, error);
  } else {
    const char *diagnostic = value_index == 0 || lua_isnoneornil(L, value_index)
                                 ? NULL
                                 : luaL_checkstring(L, value_index);

    rc = lcdc_outbox_job_revalidate(ud, &job, error);
    if (rc == LC_OK)
      rc = lc_outbox_job_dead_letter(job, diagnostic, error);
  }
  /* The C terminal methods consume successful jobs.  Retain the userdata for
   * Lua identity and GC safety, but release its native owner reference now so
   * neither an explicit close nor collection can touch the consumed handle. */
  if (rc == LC_OK) {
    lcdc_outbox_job_invalidate_borrowed_state(L, ud);
    ud->job = NULL;
    if (ud->owner_ref != LUA_NOREF) {
      luaL_unref(L, LUA_REGISTRYINDEX, ud->owner_ref);
      ud->owner_ref = LUA_NOREF;
    }
  }
  return rc;
}

static int lcdc_outbox_job_terminal(lua_State *L, int operation) {
  lcdc_outbox_job_ud *ud = lcdc_check_outbox_job(L, 1);
  lc_error error;
  int rc;

  if (ud->payload_streaming)
    return luaL_error(L, "outbox job terminal operation is not allowed while "
                         "payload is streaming");
  if (ud->borrowed_state != NULL && ud->borrowed_state->streaming)
    return luaL_error(L, "outbox job terminal operation is not allowed while "
                         "checkpoint is streaming");
  if (ud->handler_scoped) {
    if (ud->terminal_operation >= 0)
      return luaL_error(L, "outbox handler selected more than one outcome");
    if (operation != 2 && !lua_isnoneornil(L, 2))
      luaL_checktype(L, 2, LUA_TTABLE);
    if (operation == 2 && !lua_isnoneornil(L, 2))
      (void)luaL_checkstring(L, 2);
    ud->terminal_operation = operation;
    if (lua_isnoneornil(L, 2))
      lua_pushnil(L);
    else
      lua_pushvalue(L, 2);
    ud->terminal_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    lua_pushboolean(L, 1);
    return 1;
  }
  lc_error_init(&error);
  rc = lcdc_outbox_job_apply_terminal(L, ud, operation, 2, &error);
  if (rc != LC_OK) {
    lcdc_push_status_error(L, rc, &error);
    lc_error_cleanup(&error);
    return 3;
  }
  lua_pushboolean(L, 1);
  lc_error_cleanup(&error);
  return 1;
}

static int lcdc_outbox_job_complete(lua_State *L) {
  return lcdc_outbox_job_terminal(L, 0);
}

static int lcdc_outbox_job_retry(lua_State *L) {
  return lcdc_outbox_job_terminal(L, 1);
}

static int lcdc_outbox_job_dead_letter(lua_State *L) {
  return lcdc_outbox_job_terminal(L, 2);
}

static const luaL_Reg lcdc_client_methods[] = {
    {"create_collection", lcdc_collection_create},
    {"open_collection", lcdc_collection_open},
    {"list_collections", lcdc_collection_list},
    {"collection_capabilities", lcdc_collection_capabilities},
    {"xa_store", lcdc_xa_store_open},
    {"xa_coordinator", lcdc_xa_open},
    {"info", lcdc_client_info},
    {"close", lcdc_client_close},
    {"new_outbox", lcdc_client_new_outbox},
    {"new_history_consumer", lcdc_client_new_history_consumer},
    {"acquire", lcdc_client_acquire},
    {"acquire_for_update", lcdc_client_acquire_for_update},
    {"describe", lcdc_client_describe},
    {"get", lcdc_client_get},
    {"update", lcdc_client_update},
    {"mutate", lcdc_client_mutate},
    {"metadata", lcdc_client_metadata},
    {"remove", lcdc_client_remove},
    {"keepalive", lcdc_client_keepalive},
    {"release", lcdc_client_release},
    {"attach", lcdc_client_attach},
    {"list_attachments", lcdc_client_list_attachments},
    {"get_attachment", lcdc_client_get_attachment},
    {"delete_attachment", lcdc_client_delete_attachment},
    {"delete_all_attachments", lcdc_client_delete_all_attachments},
    {"queue_stats", lcdc_client_queue_stats},
    {"queue_ack", lcdc_client_queue_ack},
    {"queue_nack", lcdc_client_queue_nack},
    {"queue_extend", lcdc_client_queue_extend},
    {"query", lcdc_client_query},
    {"query_keys", lcdc_client_query_keys},
    {"get_namespace_config", lcdc_client_get_namespace_config},
    {"update_namespace_config", lcdc_client_update_namespace_config},
    {"flush_index", lcdc_client_flush_index},
    {"txn_replay", lcdc_client_txn_replay},
    {"txn_prepare", lcdc_client_txn_prepare},
    {"txn_commit", lcdc_client_txn_commit},
    {"txn_rollback", lcdc_client_txn_rollback},
    {"tc_lease_acquire", lcdc_client_tc_lease_acquire},
    {"tc_lease_renew", lcdc_client_tc_lease_renew},
    {"tc_lease_release", lcdc_client_tc_lease_release},
    {"tc_leader", lcdc_client_tc_leader},
    {"tc_cluster_announce", lcdc_client_tc_cluster_announce},
    {"tc_cluster_leave", lcdc_client_tc_cluster_leave},
    {"tc_cluster_list", lcdc_client_tc_cluster_list},
    {"tc_rm_register", lcdc_client_tc_rm_register},
    {"tc_rm_unregister", lcdc_client_tc_rm_unregister},
    {"tc_rm_list", lcdc_client_tc_rm_list},
    {"enqueue", lcdc_client_enqueue},
    {"dequeue", lcdc_client_dequeue},
    {"dequeue_batch", lcdc_client_dequeue_batch},
    {"dequeue_with_state", lcdc_client_dequeue_with_state},
    {"subscribe", lcdc_client_subscribe},
    {"subscribe_with_state", lcdc_client_subscribe_with_state},
    {"watch_queue", lcdc_client_watch_queue},
    {NULL, NULL}};

static const luaL_Reg lcdc_lease_methods[] = {
    {"info", lcdc_lease_info},
    {"close", lcdc_lease_close},
    {"describe", lcdc_lease_describe},
    {"get", lcdc_lease_get},
    {"update", lcdc_lease_update},
    {"mutate", lcdc_lease_mutate},
    {"mutate_local", lcdc_lease_mutate_local},
    {"metadata", lcdc_lease_metadata},
    {"remove", lcdc_lease_remove},
    {"keepalive", lcdc_lease_keepalive},
    {"release", lcdc_lease_release},
    {"attach", lcdc_lease_attach},
    {"list_attachments", lcdc_lease_list_attachments},
    {"get_attachment", lcdc_lease_get_attachment},
    {"delete_attachment", lcdc_lease_delete_attachment},
    {"delete_all_attachments", lcdc_lease_delete_all_attachments},
    {NULL, NULL}};

static const luaL_Reg lcdc_message_methods[] = {
    {"info", lcdc_message_info},
    {"close", lcdc_message_close},
    {"ack", lcdc_message_ack},
    {"nack", lcdc_message_nack},
    {"extend", lcdc_message_extend},
    {"state", lcdc_message_state},
    {"rewind_payload", lcdc_message_rewind_payload},
    {"payload", lcdc_message_payload},
    {NULL, NULL}};

static const luaL_Reg lcdc_outbox_methods[] = {
    {"close", lcdc_outbox_close},
    {"begin", lcdc_outbox_begin},
    {"transaction", lcdc_outbox_transaction},
    {"dispatcher", lcdc_outbox_dispatcher},
    {"accept_command", lcdc_outbox_accept_command},
    {"get_command_receipt", lcdc_outbox_get_command_receipt},
    {"get_command_receipt_by_id", lcdc_outbox_get_command_receipt_by_id},
    {"wait_command", lcdc_outbox_wait_command},
    {"write_command_result", lcdc_outbox_write_command_result},
    {"resume_command", lcdc_outbox_resume_command},
    {"resume_command_by_id", lcdc_outbox_resume_command_by_id},
    {"append", lcdc_outbox_append},
    {"accept_inbox", lcdc_outbox_accept_inbox},
    {NULL, NULL}};

static const luaL_Reg lcdc_outbox_dispatcher_methods[] = {
    {"close", lcdc_outbox_dispatcher_close},
    {"next", lcdc_outbox_dispatcher_next},
    {"next_with_state", lcdc_outbox_dispatcher_next_with_state},
    {"_binding_id", lcdc_outbox_dispatcher_binding_id},
    {"pump", lcdc_outbox_dispatcher_pump},
    {"run", lcdc_outbox_dispatcher_run},
    {"notify_outbox_key", lcdc_outbox_dispatcher_notify_outbox_key},
    {"stats", lcdc_outbox_dispatcher_stats},
    {"reconcile", lcdc_outbox_dispatcher_reconcile},
    {"replay_dead_letter", lcdc_outbox_dispatcher_replay_dead_letter},
    {"delete_dead_letter", lcdc_outbox_dispatcher_delete_dead_letter},
    {"export_dead_letters", lcdc_outbox_dispatcher_export_dead_letters},
    {"stop", lcdc_outbox_dispatcher_stop},
    {"wait", lcdc_outbox_dispatcher_wait},
    {NULL, NULL}};

static const luaL_Reg lcdc_outbox_txn_methods[] = {
    {"id", lcdc_outbox_txn_id},
    {"bind_xa", lcdc_outbox_txn_bind_xa},
    {"commit_start", lcdc_outbox_txn_commit_start},
    {"rollback_start", lcdc_outbox_txn_rollback_start},
    {"close", lcdc_outbox_txn_close},
    {"accept_command", lcdc_outbox_txn_accept_command},
    {"accept_inbox", lcdc_outbox_txn_accept_inbox},
    {"acquire", lcdc_outbox_txn_acquire},
    {"append", lcdc_outbox_txn_append},
    {"complete_command", lcdc_outbox_txn_complete_command},
    {"fail_command", lcdc_outbox_txn_fail_command},
    {"commit", lcdc_outbox_txn_commit},
    {"rollback", lcdc_outbox_txn_rollback},
    {NULL, NULL}};

static const luaL_Reg lcdc_outbox_participant_methods[] = {
    {"close", lcdc_outbox_participant_close},
    {"info", lcdc_outbox_participant_info},
    {"describe", lcdc_outbox_participant_describe},
    {"get", lcdc_outbox_participant_get},
    {"update", lcdc_outbox_participant_update},
    {"mutate", lcdc_outbox_participant_mutate},
    {"mutate_local", lcdc_outbox_participant_mutate_local},
    {"metadata", lcdc_outbox_participant_metadata},
    {"remove", lcdc_outbox_participant_remove},
    {"keepalive", lcdc_outbox_participant_keepalive},
    {"attach", lcdc_outbox_participant_attach},
    {"list_attachments", lcdc_outbox_participant_list_attachments},
    {"get_attachment", lcdc_outbox_participant_get_attachment},
    {"delete_attachment", lcdc_outbox_participant_delete_attachment},
    {"delete_all_attachments", lcdc_outbox_participant_delete_all_attachments},
    {NULL, NULL}};

static const luaL_Reg lcdc_outbox_job_methods[] = {
    {"close", lcdc_outbox_job_close},
    {"info", lcdc_outbox_job_info},
    {"write_payload", lcdc_outbox_job_write_payload},
    {"state", lcdc_outbox_job_state},
    {"renew", lcdc_outbox_job_renew},
    {"complete", lcdc_outbox_job_complete},
    {"retry", lcdc_outbox_job_retry},
    {"dead_letter", lcdc_outbox_job_dead_letter},
    {NULL, NULL}};

static const luaL_Reg lcdc_history_consumer_methods[] = {
    {"position", lcdc_history_consumer_position},
    {"advance", lcdc_history_consumer_advance},
    {"unregister", lcdc_history_consumer_unregister},
    {"close", lcdc_history_consumer_close},
    {NULL, NULL}};

static void lcdc_create_metatable(lua_State *L, const char *name,
                                  const luaL_Reg *methods, lua_CFunction gc) {
  luaL_newmetatable(L, name);
  lua_pushcfunction(L, gc);
  lua_setfield(L, -2, "__gc");
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_setfuncs(L, methods, 0);
  lua_pop(L, 1);
}

int luaopen_lockdc_core(lua_State *L) {
  static const luaL_Reg module_functions[] = {
      {"open", lcdc_open},
      {"allocator_tracking_configure", lcdc_allocator_tracking_configure},
      {"version_string", lcdc_version_string},
      {"xid_new", lcdc_xid_new},
      {"pouch_crypto_generate_key", lcdc_pouch_crypto_generate_key},
      {"pouch_crypto_default_key_file", lcdc_pouch_crypto_default_key_file},
      {"pouch_crypto_generate_key_file", lcdc_pouch_crypto_generate_key_file},
      {NULL, NULL}};

  lcdc_xa_install(L);
  lcdc_collections_install(L);
  lcdc_create_metatable(L, LCDC_CLIENT_MT, lcdc_client_methods, lcdc_client_gc);
  lcdc_create_metatable(L, LCDC_LEASE_MT, lcdc_lease_methods, lcdc_lease_gc);
  lcdc_create_metatable(L, LCDC_MESSAGE_MT, lcdc_message_methods,
                        lcdc_message_gc);
  lcdc_create_metatable(L, LCDC_OUTBOX_MT, lcdc_outbox_methods, lcdc_outbox_gc);
  lcdc_create_metatable(L, LCDC_OUTBOX_DISPATCHER_MT,
                        lcdc_outbox_dispatcher_methods,
                        lcdc_outbox_dispatcher_gc);
  lcdc_create_metatable(L, LCDC_OUTBOX_TXN_MT, lcdc_outbox_txn_methods,
                        lcdc_outbox_txn_gc);
  lcdc_create_metatable(L, LCDC_OUTBOX_PARTICIPANT_MT,
                        lcdc_outbox_participant_methods,
                        lcdc_outbox_participant_gc);
  lcdc_create_metatable(L, LCDC_OUTBOX_JOB_MT, lcdc_outbox_job_methods,
                        lcdc_outbox_job_gc);
  lcdc_create_metatable(L, LCDC_HISTORY_CONSUMER_MT,
                        lcdc_history_consumer_methods,
                        lcdc_history_consumer_gc);
  lua_newtable(L);
  luaL_setfuncs(L, module_functions, 0);
  lcdc_xa_constants(L);
  lua_pushinteger(L, LC_OK);
  lua_setfield(L, -2, "OK");
  lua_pushinteger(L, LC_ERR_INVALID);
  lua_setfield(L, -2, "ERR_INVALID");
  lua_pushinteger(L, LC_ERR_NOMEM);
  lua_setfield(L, -2, "ERR_NOMEM");
  lua_pushinteger(L, LC_ERR_TRANSPORT);
  lua_setfield(L, -2, "ERR_TRANSPORT");
  lua_pushinteger(L, LC_ERR_PROTOCOL);
  lua_setfield(L, -2, "ERR_PROTOCOL");
  lua_pushinteger(L, LC_ERR_SERVER);
  lua_setfield(L, -2, "ERR_SERVER");
  lua_pushinteger(L, LC_ERR_TIMEOUT);
  lua_setfield(L, -2, "ERR_TIMEOUT");
  lua_pushinteger(L, LC_ERR_PENDING);
  lua_setfield(L, -2, "ERR_PENDING");
  lua_pushinteger(L, LC_COMMAND_PENDING);
  lua_setfield(L, -2, "COMMAND_PENDING");
  lua_pushinteger(L, LC_COMMAND_COMPLETED);
  lua_setfield(L, -2, "COMMAND_COMPLETED");
  lua_pushinteger(L, LC_COMMAND_FAILED);
  lua_setfield(L, -2, "COMMAND_FAILED");
  lua_pushinteger(L, LC_NACK_INTENT_FAILURE);
  lua_setfield(L, -2, "NACK_FAILURE");
  lua_pushinteger(L, LC_NACK_INTENT_DEFER);
  lua_setfield(L, -2, "NACK_DEFER");
  return 1;
}
