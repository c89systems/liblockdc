#include "lockdc_lua_internal.h"
#include <lauxlib.h>
#include <string.h>

#define STORE_MT "lockdc.xa_store"
#define RESOURCE_MT "lockdc.xa_resource"
#define BRANCH_MT "lockdc.xa_branch"
#define OP_MT "lockdc.xa_resource_operation"
#define BORROW_MT "lockdc.xa_borrow"
#define bad lcdc_xa_bad
#define failure lcdc_xa_failure
#define scope lcdc_xa_error_scope
#define shell lcdc_xa_shell

typedef struct store_ud {
  lc_xa_store *handle;
  int configuring;
} store_ud;
typedef struct resource_ud {
  lc_xa_resource *handle;
  lc_xa_coordinator *coordinator;
} resource_ud;
typedef struct branch_ud {
  lc_xa_branch *handle;
} branch_ud;
typedef struct operation_ud {
  lc_xa_operation *handle;
  /* The rooted receiver owns this copy even if a Lua allocation re-enters
   * close(). Keep the bounded inventory off the native worker stack. */
  lc_xa_result snapshot;
} operation_ud;
typedef struct borrow_ud {
  lc_xa_borrow *handle;
} borrow_ud;

static void text(lua_State *L, const char *key, const char *value) {
  lua_pushstring(L, value);
  lua_setfield(L, -2, key);
}
static void number(lua_State *L, const char *key, lua_Integer value) {
  lua_pushinteger(L, value);
  lua_setfield(L, -2, key);
}
static void record_push(lua_State *L, const lc_xa_branch_record *record) {
  lc_xa_branch_record snapshot = *record;
  record = &snapshot;
  lua_newtable(L);
  text(L, "txn_id", record->txn_id);
  text(L, "resource_id", record->resource_id);
  text(L, "branch_id", record->branch_id);
  text(L, "gid", record->gid);
  text(L, "full_xid", record->full_xid);
  text(L, "system_id", record->system_id);
  text(L, "database_oid", record->database_oid);
  text(L, "role_oid", record->role_oid);
  number(L, "phase", record->phase);
  number(L, "evidence", record->evidence);
  number(L, "completed_at_unix", (lua_Integer)record->completed_at_unix);
  lua_pushboolean(L, record->prepared_observed);
  lua_setfield(L, -2, "prepared_observed");
}
static int record_text(lua_State *L, const char *key, char *out, size_t size) {
  size_t length;
  const char *value;
  lua_getfield(L, 2, key);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return 1;
  }
  if (lua_type(L, -1) != LUA_TSTRING) {
    lua_pop(L, 1);
    return 0;
  }
  value = lua_tolstring(L, -1, &length);
  if (length >= size || memchr(value, 0, length) != NULL) {
    lua_pop(L, 1);
    return 0;
  }
  memcpy(out, value, length + 1U);
  lua_pop(L, 1);
  return 1;
}
static int record_read(lua_State *L, lc_xa_branch_record *record) {
  lua_Integer value;
  memset(record, 0, sizeof(*record));
  if (lua_type(L, 2) != LUA_TTABLE)
    return 0;
#define FIELD(name)                                                            \
  if (!record_text(L, #name, record->name, sizeof(record->name)))              \
  return 0
  FIELD(txn_id);
  FIELD(resource_id);
  FIELD(branch_id);
  FIELD(gid);
  FIELD(full_xid);
  FIELD(system_id);
  FIELD(database_oid);
  FIELD(role_oid);
#undef FIELD
  lua_getfield(L, 2, "phase");
  if (!lua_isnil(L, -1)) {
    if (!lua_isinteger(L, -1))
      return 0;
    value = lua_tointeger(L, -1);
    if (value < LC_XA_ENROLLED || value > LC_XA_ABORTED)
      return 0;
    record->phase = (unsigned char)value;
  }
  lua_pop(L, 1);
  lua_getfield(L, 2, "evidence");
  if (!lua_isnil(L, -1)) {
    if (!lua_isinteger(L, -1))
      return 0;
    value = lua_tointeger(L, -1);
    if (value < LC_XA_EVIDENCE_NONE || value > LC_XA_EVIDENCE_RECONCILED)
      return 0;
    record->evidence = (unsigned char)value;
  }
  lua_pop(L, 1);
  lua_getfield(L, 2, "prepared_observed");
  if (!lua_isnil(L, -1) && lua_type(L, -1) != LUA_TBOOLEAN)
    return 0;
  record->prepared_observed = (unsigned char)lua_toboolean(L, -1);
  lua_pop(L, 1);
  lua_getfield(L, 2, "completed_at_unix");
  if (!lua_isnil(L, -1)) {
    if (!lua_isinteger(L, -1))
      return 0;
    record->completed_at_unix = (lc_unix_seconds)lua_tointeger(L, -1);
  }
  lua_pop(L, 1);
  return 1;
}
int lcdc_xa_text(lua_State *L, int index, int optional) {
  size_t length;
  const char *value;
  if (optional && lua_isnoneornil(L, index))
    return 1;
  if (lua_type(L, index) != LUA_TSTRING)
    return 0;
  value = lua_tolstring(L, index, &length);
  return memchr(value, 0, length) == NULL;
}
#define valid_text lcdc_xa_text
static int boolean(lua_State *L, int index, int *out) {
  if (lua_type(L, index) != LUA_TBOOLEAN)
    return 0;
  *out = lua_toboolean(L, index);
  return 1;
}
static int success(lua_State *L, int rc, lc_error *error) {
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  lua_pushboolean(L, 1);
  return 1;
}

int lcdc_xa_store_open(lua_State *L) {
  lc_client *client;
  store_ud *ud;
  lc_error *error = scope(L, 1);
  int rc = lc_lua_client_view(L, 1, &client, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  ud = shell(L, sizeof(*ud), STORE_MT, 1);
  rc = lc_xa_store_open(client, &ud->handle, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int store_close(lua_State *L) {
  store_ud *ud = luaL_testudata(L, 1, STORE_MT);
  if (ud == NULL || ud->configuring)
    return bad(L, "XA store is parsing configuration");
  if (ud->handle != NULL)
    lc_xa_store_close(ud->handle);
  ud->handle = NULL;
  lua_pushboolean(L, 1);
  return 1;
}
static int store_gc(lua_State *L) {
  store_ud *ud = luaL_testudata(L, 1, STORE_MT);
  if (ud != NULL && !ud->configuring && ud->handle != NULL) {
    lc_xa_store_close(ud->handle);
    ud->handle = NULL;
  }
  return 0;
}
static int store_retain(lua_State *L) {
  store_ud *ud = luaL_testudata(L, 1, STORE_MT), *copy;
  lc_error *error = scope(L, 1);
  int rc;
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA store is closed");
  copy = shell(L, sizeof(*copy), STORE_MT, 1);
  rc = lc_xa_store_retain(ud->handle, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  copy->handle = ud->handle;
  lc_error_cleanup(error);
  return 1;
}
static int store_write(lua_State *L, int kind) {
  store_ud *ud = luaL_testudata(L, 1, STORE_MT);
  lc_xa_branch_record record;
  lc_error *error = scope(L, 2);
  int rc;
  if (ud == NULL || ud->handle == NULL || !record_read(L, &record))
    return bad(L, "XA store requires a live receiver and valid branch record");
  /* __index callbacks may close the receiver while fields are copied. */
  if (ud->handle == NULL)
    return bad(L, "XA store closed while parsing a record");
  rc = kind == 0   ? lc_xa_store_register_blocking(ud->handle, &record, error)
       : kind == 1 ? lc_xa_store_enroll_blocking(ud->handle, &record, error)
                   : lc_xa_store_progress_blocking(ud->handle, &record, error);
  return success(L, rc, error);
}
static int store_register(lua_State *L) { return store_write(L, 0); }
static int store_enroll(lua_State *L) { return store_write(L, 1); }
static int store_progress(lua_State *L) { return store_write(L, 2); }
static int store_read(lua_State *L) {
  store_ud *ud = luaL_testudata(L, 1, STORE_MT);
  lc_xa_branch_record record;
  int prepared, rc;
  lc_error *error = scope(L, 3);
  if (ud == NULL || ud->handle == NULL || !valid_text(L, 2, 0) ||
      !valid_text(L, 3, 0))
    return bad(L, "XA journal read requires transaction and branch IDs");
  rc = lc_xa_store_read_blocking(ud->handle, lua_tostring(L, 2),
                                 lua_tostring(L, 3), &record, &prepared, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  record_push(L, &record);
  lua_pushboolean(L, prepared);
  return 2;
}
static int resource_open(lua_State *L) {
  store_ud *store = luaL_testudata(L, 1, STORE_MT);
  resource_ud *resource;
  int index, count;
  lc_error *error = scope(L, 3);
  if (store == NULL || store->handle == NULL || store->configuring)
    return bad(L, "XA resource open requires a live journal");
  resource = shell(L, sizeof(*resource), RESOURCE_MT, 1);
  index = lua_gettop(L);
  count = lcdc_xa_resource_open_provider(L, store->handle, &resource->handle,
                                         &store->configuring, error);
  if (count != 0)
    return count;
  lc_error_cleanup(error);
  lua_pushvalue(L, index);
  return 1;
}
int lcdc_xa_resource_push(lua_State *L, lc_xa_resource *resource,
                          lc_xa_coordinator *coordinator, int owner) {
  resource_ud *ud;
  lc_error *error = scope(L, 2);
  int rc;
  ud = shell(L, sizeof(*ud), RESOURCE_MT, owner);
  rc = lc_xa_coordinator_retain(coordinator, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  ud->handle = resource;
  ud->coordinator = coordinator;
  lc_error_cleanup(error);
  return 1;
}
static int resource_close(lua_State *L) {
  resource_ud *ud = luaL_testudata(L, 1, RESOURCE_MT);
  lc_error *error = scope(L, 1);
  int rc = LC_OK;
  if (ud == NULL)
    return bad(L, "invalid XA resource receiver");
  if (ud->handle != NULL) {
    rc = ud->coordinator != NULL
             ? lc_xa_coordinator_close_blocking(ud->coordinator, error)
             : lc_xa_resource_close_blocking(ud->handle, error);
    if (rc == LC_OK) {
      ud->handle = NULL;
      ud->coordinator = NULL;
    }
  }
  return success(L, rc, error);
}
static int resource_gc(lua_State *L) {
  resource_ud *ud = luaL_testudata(L, 1, RESOURCE_MT);
  if (ud != NULL && ud->handle != NULL) {
    if (ud->coordinator != NULL)
      (void)lc_xa_coordinator_close_blocking(ud->coordinator, NULL);
    else
      (void)lc_xa_resource_close_blocking(ud->handle, NULL);
    ud->handle = NULL;
    ud->coordinator = NULL;
  }
  return 0;
}
static int resource_identity(lua_State *L) {
  resource_ud *ud = luaL_testudata(L, 1, RESOURCE_MT);
  const lc_xa_branch_record *record = ud != NULL && ud->handle != NULL
                                          ? lc_xa_resource_identity(ud->handle)
                                          : NULL;
  if (record == NULL)
    return bad(L, "XA resource is closed or belongs to another owner");
  record_push(L, record);
  return 1;
}
static int resource_start(lua_State *L, int kind) {
  resource_ud *ud = luaL_testudata(L, 1, RESOURCE_MT);
  operation_ud *op;
  int commit = 0, rc,
      arguments = kind == 0   ? 2
                  : kind == 1 ? 3
                  : kind == 2 ? 5
                              : 4;
  int64_t deadline;
  lua_Integer limit = 0;
  lc_error *error = scope(L, arguments);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA resource is closed");
  if ((kind == 1 || kind == 2 || kind == 3) && !valid_text(L, 2, 0))
    return bad(L, "XA transaction ID must be a string without NUL bytes");
  if ((kind == 2 || kind == 3) && !valid_text(L, 3, 0))
    return bad(L, "XA branch ID must be a string without NUL bytes");
  if (kind == 2 && !boolean(L, 4, &commit))
    return bad(L, "XA commit direction must be boolean");
  if (kind == 4) {
    if (!valid_text(L, 2, 1) ||
        (!lua_isnoneornil(L, 3) && !lua_isinteger(L, 3)))
      return bad(L,
                 "XA inventory requires an optional cursor and integer limit");
    if (!lua_isnoneornil(L, 3))
      limit = lua_tointeger(L, 3);
    if (limit < 0 || limit > 64)
      return bad(L, "XA inventory limit must be between 0 and 64");
  }
  rc = lcdc_xa_deadline(L, arguments, &deadline, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  op = shell(L, sizeof(*op), OP_MT, 1);
  if (kind == 0)
    rc =
        lc_xa_resource_register_start(ud->handle, deadline, &op->handle, error);
  else if (kind == 1)
    rc = lc_xa_resource_branch_start(ud->handle, lua_tostring(L, 2), deadline,
                                     &op->handle, error);
  else if (kind == 2)
    rc = lc_xa_resource_recover_start(ud->handle, lua_tostring(L, 2),
                                      lua_tostring(L, 3), commit, deadline,
                                      &op->handle, error);
  else if (kind == 3)
    rc = lc_xa_resource_status_start(ud->handle, lua_tostring(L, 2),
                                     lua_tostring(L, 3), deadline, &op->handle,
                                     error);
  else
    rc = lc_xa_resource_inventory_start(ud->handle, lua_tostring(L, 2),
                                        (size_t)limit, deadline, &op->handle,
                                        error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int resource_register(lua_State *L) { return resource_start(L, 0); }
static int resource_branch(lua_State *L) { return resource_start(L, 1); }
static int resource_recover(lua_State *L) { return resource_start(L, 2); }
static int resource_status(lua_State *L) { return resource_start(L, 3); }
static int resource_inventory(lua_State *L) { return resource_start(L, 4); }

static int branch_close(lua_State *L) {
  branch_ud *ud = luaL_testudata(L, 1, BRANCH_MT);
  lc_error *error = scope(L, 1);
  int rc = LC_OK;
  if (ud == NULL)
    return bad(L, "invalid XA branch receiver");
  if (ud->handle != NULL) {
    rc = lc_xa_branch_close(ud->handle, error);
    if (rc == LC_OK)
      ud->handle = NULL;
  }
  return success(L, rc, error);
}
static int branch_gc(lua_State *L) {
  branch_ud *ud = luaL_testudata(L, 1, BRANCH_MT);
  if (ud != NULL && ud->handle != NULL) {
    (void)lc_xa_branch_close(ud->handle, NULL);
    ud->handle = NULL;
  }
  return 0;
}
static int branch_record(lua_State *L) {
  branch_ud *ud = luaL_testudata(L, 1, BRANCH_MT);
  const lc_xa_branch_record *record = ud != NULL && ud->handle != NULL
                                          ? lc_xa_branch_record_get(ud->handle)
                                          : NULL;
  if (record == NULL)
    return bad(L, "XA branch is closed or belongs to another owner");
  record_push(L, record);
  return 1;
}
static int branch_start(lua_State *L, int resolve) {
  branch_ud *ud = luaL_testudata(L, 1, BRANCH_MT);
  operation_ud *op;
  int commit = 0, rc;
  int64_t deadline;
  lc_error *error = scope(L, resolve ? 3 : 2);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA branch is closed");
  if (resolve && !boolean(L, 2, &commit))
    return bad(L, "XA commit direction must be boolean");
  rc = lcdc_xa_deadline(L, resolve ? 3 : 2, &deadline, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  op = shell(L, sizeof(*op), OP_MT, 1);
  rc = resolve ? lc_xa_branch_resolve_start(ud->handle, commit, deadline,
                                            &op->handle, error)
               : lc_xa_branch_prepare_start(ud->handle, deadline, &op->handle,
                                            error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int branch_prepare(lua_State *L) { return branch_start(L, 0); }
static int branch_resolve(lua_State *L) { return branch_start(L, 1); }
static int branch_borrow(lua_State *L) {
  branch_ud *ud = luaL_testudata(L, 1, BRANCH_MT);
  borrow_ud *borrow;
  lc_error *error = scope(L, 1);
  int rc;
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA branch is closed");
  borrow = shell(L, sizeof(*borrow), BORROW_MT, 1);
  rc = lc_xa_branch_borrow(ud->handle, &borrow->handle, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static int operation_gc(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  if (ud != NULL && ud->handle != NULL) {
    (void)lc_xa_operation_close_blocking(ud->handle, NULL);
    ud->handle = NULL;
  }
  return 0;
}
static int operation_close(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_error *error = scope(L, 1);
  int rc = LC_OK;
  if (ud == NULL)
    return bad(L, "invalid XA operation receiver");
  if (ud->handle != NULL) {
    rc = lc_xa_operation_close_blocking(ud->handle, error);
    if (rc == LC_OK)
      ud->handle = NULL;
  }
  return success(L, rc, error);
}
static int operation_advance(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_error *error = scope(L, 1);
  int rc;
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  rc = lc_xa_operation_advance(ud->handle, error);
  if (rc == LC_ERR_PENDING && lc_xa_operation_result(ud->handle) == NULL) {
    lc_error_cleanup(error);
    lua_pushboolean(L, 0);
    return 1;
  }
  return success(L, rc, error);
}
static int operation_wait(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_xa_wait wait;
  lc_error *error = scope(L, 1);
  int rc;
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  rc = lc_xa_operation_wait(ud->handle, &wait, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  lua_newtable(L);
  number(L, "fd", wait.fd);
  number(L, "events", wait.events);
  number(L, "deadline_ms", (lua_Integer)wait.deadline_ms);
  return 1;
}
static int operation_result(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  const lc_xa_result *result;
  lc_xa_wait wait;
  lc_error *error = scope(L, 1);
  int rc;
  size_t i;
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  result = lc_xa_operation_result(ud->handle);
  if (result == NULL) {
    rc = lc_xa_operation_wait(ud->handle, &wait, error);
    if (rc != LC_OK)
      return failure(L, rc, error);
  }
  lc_error_cleanup(error);
  if (result != NULL) {
    ud->snapshot = *result;
    result = &ud->snapshot;
    if (result->row_count > 64U)
      return bad(L, "XA inventory exceeds its bounded result capacity");
  }
  lua_newtable(L);
  lua_pushboolean(L, result != NULL);
  lua_setfield(L, -2, "done");
  if (result == NULL)
    return 1;
  record_push(L, &result->record);
  lua_setfield(L, -2, "record");
  number(L, "native_outcome", result->native_outcome);
  text(L, "sqlstate", result->sqlstate);
  number(L, "row_count", (lua_Integer)result->row_count);
  lua_newtable(L);
  for (i = 0; i < result->row_count; ++i) {
    lua_newtable(L);
    text(L, "gid", result->rows[i].gid);
    text(L, "xid", result->rows[i].xid);
    text(L, "database_oid", result->rows[i].database_oid);
    text(L, "role_oid", result->rows[i].role_oid);
    lua_rawseti(L, -2, (lua_Integer)i + 1);
  }
  lua_setfield(L, -2, "rows");
  return 1;
}
static int operation_cancel(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  lc_error *error = scope(L, 1);
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  return success(L, lc_xa_operation_cancel(ud->handle, error), error);
}
static int operation_take_branch(lua_State *L) {
  operation_ud *ud = luaL_testudata(L, 1, OP_MT);
  branch_ud *branch;
  lc_error *error = scope(L, 1);
  int rc;
  if (ud == NULL || ud->handle == NULL)
    return bad(L, "XA operation is closed");
  branch = shell(L, sizeof(*branch), BRANCH_MT, 1);
  rc = lc_xa_operation_take_branch(ud->handle, &branch->handle, error);
  if (rc != LC_OK)
    return failure(L, rc, error);
  lc_error_cleanup(error);
  return 1;
}
static void metatable(lua_State *L, const char *name, const luaL_Reg *methods,
                      lua_CFunction gc) {
  luaL_newmetatable(L, name);
  lua_pushcfunction(L, gc);
  lua_setfield(L, -2, "__gc");
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_setfuncs(L, methods, 0);
  lua_pop(L, 1);
}
void lcdc_xa_resource_install(lua_State *L) {
  static const luaL_Reg store[] = {{"resource", resource_open},
                                   {"retain", store_retain},
                                   {"register_blocking", store_register},
                                   {"enroll_blocking", store_enroll},
                                   {"progress_blocking", store_progress},
                                   {"read_blocking", store_read},
                                   {"close", store_close},
                                   {NULL, NULL}};
  static const luaL_Reg resource[] = {{"identity", resource_identity},
                                      {"register_start", resource_register},
                                      {"branch_start", resource_branch},
                                      {"recover_start", resource_recover},
                                      {"status_start", resource_status},
                                      {"inventory_start", resource_inventory},
                                      {"close_blocking", resource_close},
                                      {NULL, NULL}};
  static const luaL_Reg branch[] = {
      {"record", branch_record},         {"prepare_start", branch_prepare},
      {"resolve_start", branch_resolve}, {"borrow", branch_borrow},
      {"close", branch_close},           {NULL, NULL}};
  static const luaL_Reg operation[] = {{"advance", operation_advance},
                                       {"wait", operation_wait},
                                       {"result", operation_result},
                                       {"cancel", operation_cancel},
                                       {"take_branch", operation_take_branch},
                                       {"close_blocking", operation_close},
                                       {NULL, NULL}};
  metatable(L, STORE_MT, store, store_gc);
  metatable(L, RESOURCE_MT, resource, resource_gc);
  metatable(L, BRANCH_MT, branch, branch_gc);
  metatable(L, OP_MT, operation, operation_gc);
}
void lcdc_xa_constants(lua_State *L) {
#define CONSTANT(name) number(L, #name + 3, name)
  CONSTANT(LC_XA_ENROLLED);
  CONSTANT(LC_XA_PREPARE_SENT);
  CONSTANT(LC_XA_PREPARED);
  CONSTANT(LC_XA_COMMIT_SENT);
  CONSTANT(LC_XA_ROLLBACK_SENT);
  CONSTANT(LC_XA_COMMITTED);
  CONSTANT(LC_XA_ABORTED);
  CONSTANT(LC_XA_EVIDENCE_NONE);
  CONSTANT(LC_XA_EVIDENCE_ACK);
  CONSTANT(LC_XA_EVIDENCE_NATIVE);
  CONSTANT(LC_XA_EVIDENCE_RECONCILED);
  CONSTANT(LC_XA_OUTCOME_UNKNOWN);
  CONSTANT(LC_XA_OUTCOME_COMMITTED);
  CONSTANT(LC_XA_OUTCOME_ABORTED);
  CONSTANT(LC_XA_OUTCOME_IN_PROGRESS);
  CONSTANT(LC_XA_OUTCOME_DISCARDED);
  CONSTANT(LC_XA_RESOURCE_POSTGRES);
  CONSTANT(LC_XA_DECISION_UNDECIDED);
  CONSTANT(LC_XA_DECISION_COMMIT);
  CONSTANT(LC_XA_DECISION_ABORT);
#undef CONSTANT
}
