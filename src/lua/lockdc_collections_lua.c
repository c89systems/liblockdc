#include "lockdc_lua_internal.h"

#include <lauxlib.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#define COLLECTION_MT "lockdc.collection"
#define CALL_MT "lockdc.collection_call"
#define WRITER_MT "lockdc.collection_writer"

enum collection_operation {
  CREATE,
  OPEN,
  LIST,
  CAPABILITIES,
  DESCRIBE,
  METADATA,
  ENTRY_METADATA,
  APPEND,
  APPEND_STAGED,
  APPEND_STATUS,
  READ,
  DELETE_ENTRY,
  DROP,
  APPEND_BEGIN,
  ACQUIRE,
  TAKEOVER,
  KEEPALIVE,
  RELEASE,
  HEAD,
  EDIT,
  EDIT_STATUS,
  WRITER_WRITE,
  WRITER_COMMIT,
  WRITER_ABORT
};

typedef struct collection_ud {
  lc_collection *handle;
  lc_collection *closing;
  int active;
} collection_ud;

typedef struct writer_ud {
  lc_collection_writer *handle;
  lc_collection_writer *closing;
  int active;
} writer_ud;

/* This userdata owns every temporary native allocation before another Lua
 * operation can raise. Its owner uservalue also pins the receiver across GC,
 * callbacks, explicit close, and protected result/error construction. */
/* Only edit/head calls pay for bounded recovery/range storage. Ordinary
 * append/read userdata retains its small allocation and existing lifecycle. */
typedef struct collection_edit_data {
  lc_collection_head_res head;
  lc_collection_edit_req edit;
  lc_collection_edit_res edited;
  lc_collection_edit_status_res edit_status;
  lc_collection_edit_range ranges[LC_COLLECTION_EDIT_MAX_RANGES];
} collection_edit_data;

typedef struct collection_call {
  lua_State *L;
  int operation;
  int metadata_ref;
  int cancel_ref;
  int visitor_ref;
  int callback_failed;
  collection_ud *receiver;
  writer_ud *writer_receiver;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  lc_collection_read_res read;
  collection_edit_data *extra;
  const lc_collection_read_metadata *frame;
  const lc_collection_info *info;
} collection_call;

static int invalid(collection_call *call, const char *message) {
  return lc_error_set(&call->error, LC_ERR_INVALID, 0L, message, NULL, NULL,
                      NULL);
}

static void release_call(collection_call *call) {
  lc_source *source = call->source;
  lc_sink *sink = call->sink;
  call->source = NULL;
  call->sink = NULL;
  lc_source_close(source);
  lc_sink_close(sink);
  if (call->receiver != NULL) {
    lc_collection_close(call->receiver->closing);
    call->receiver->closing = NULL;
    call->receiver->active = 0;
    call->receiver = NULL;
  }
  if (call->writer_receiver != NULL) {
    lc_collection_writer_close(call->writer_receiver->closing);
    call->writer_receiver->closing = NULL;
    call->writer_receiver->active = 0;
    call->writer_receiver = NULL;
  }
  luaL_unref(call->L, LUA_REGISTRYINDEX, call->metadata_ref);
  luaL_unref(call->L, LUA_REGISTRYINDEX, call->cancel_ref);
  luaL_unref(call->L, LUA_REGISTRYINDEX, call->visitor_ref);
  call->metadata_ref = call->cancel_ref = call->visitor_ref = LUA_NOREF;
  lc_collection_read_res_cleanup(&call->read);
  lc_error_cleanup(&call->error);
}

static int call_gc(lua_State *L) {
  collection_call *call = luaL_testudata(L, 1, CALL_MT);
  if (call != NULL) {
    call->L = L;
    release_call(call);
  }
  return 0;
}

static int collection_close(lua_State *L) {
  collection_ud *ud = luaL_testudata(L, 1, COLLECTION_MT);
  if (ud != NULL && ud->handle != NULL) {
    lc_collection *handle = ud->handle;
    ud->handle = NULL;
    if (ud->active)
      ud->closing = handle;
    else
      lc_collection_close(handle);
  }
  return 0;
}

static int writer_close(lua_State *L) {
  writer_ud *ud = luaL_testudata(L, 1, WRITER_MT);
  if (ud != NULL && ud->handle != NULL) {
    lc_collection_writer *handle = ud->handle;
    ud->handle = NULL;
    if (ud->active)
      ud->closing = handle;
    else
      lc_collection_writer_close(handle);
  }
  return 0;
}

static void set_text(lua_State *L, const char *key, const char *value) {
  if (value != NULL) {
    lua_pushstring(L, value);
    lua_setfield(L, -2, key);
  }
}

static void set_bool(lua_State *L, const char *key, int value) {
  lua_pushboolean(L, value);
  lua_setfield(L, -2, key);
}

static void push_lease(lua_State *L, const lc_collection_lease_info *lease) {
  lua_newtable(L);
  set_bool(L, "active", lease->active);
  set_text(L, "owner", lease->owner);
  set_text(L, "lease_id", lease->lease_id);
  lcdc_set_u64_decimal(L, "fencing_token", lease->fencing_token);
  lcdc_set_version_field(L, "expires_at_unix",
                         (lc_version)lease->expires_at_unix);
  set_bool(L, "private_read", lease->private_read);
}

static void push_info(lua_State *L, const lc_collection_info *info) {
  lua_newtable(L);
  set_text(L, "namespace", info->ns);
  set_text(L, "name", info->name);
  set_text(L, "collection_id", info->collection_id);
  lcdc_set_u64_decimal(L, "upper_position", info->upper_position);
  lcdc_set_u64_decimal(L, "live_count", info->live_count);
  lcdc_set_u64_decimal(L, "max_entries", info->max_entries);
  set_bool(L, "query_indexing", info->query_indexing);
  push_lease(L, &info->lease);
  lua_setfield(L, -2, "lease");
  lcdc_set_state_retention(L, info->retention_support,
                           info->has_retention_expires_at_unix,
                           info->retention_expires_at_unix);
}

static void push_entry(lua_State *L, const lc_collection_append_res *entry) {
  lua_newtable(L);
  lcdc_set_u64_decimal(L, "position", entry->position);
  lcdc_set_u64_decimal(L, "bytes", entry->bytes);
  set_text(L, "etag", entry->etag);
  set_bool(L, "duplicate", entry->duplicate);
  set_bool(L, "deleted", entry->deleted);
  lcdc_set_state_retention(L, entry->retention_support,
                           entry->has_retention_expires_at_unix,
                           entry->retention_expires_at_unix);
  if (entry->has_collection_retention_expires_at_unix)
    lcdc_set_version_field(
        L, "collection_retention_expires_at_unix",
        (lc_version)entry->collection_retention_expires_at_unix);
}

static void push_head(lua_State *L, const lc_collection_head_res *head) {
  lua_newtable(L);
  set_text(L, "collection_id", head->collection_id);
  set_text(L, "etag", head->etag);
  lcdc_set_u64_decimal(L, "upper_position", head->upper_position);
  lcdc_set_u64_decimal(L, "live_count", head->live_count);
  lcdc_set_u64_decimal(L, "recovery_revision", head->recovery_revision);
  lcdc_set_u64_decimal(L, "recovery_anchor", head->recovery_anchor);
  set_bool(L, "recovery_anchor_available", head->recovery_anchor_available);
  set_text(L, "recovery", head->recovery);
}
static void push_edit(lua_State *L, const lc_collection_edit_res *result) {
  lua_newtable(L);
  set_text(L, "head_etag", result->head_etag);
  set_bool(L, "published", result->published);
  lcdc_set_u64_decimal(L, "upper_position", result->upper_position);
  lcdc_set_u64_decimal(L, "live_count", result->live_count);
  lcdc_set_u64_decimal(L, "deleted_count", result->deleted_count);
  set_bool(L, "duplicate", result->duplicate);
  if (result->has_replacement) {
    push_entry(L, &result->replacement);
    lua_setfield(L, -2, "replacement");
  }
}
static void push_read(lua_State *L, const lc_collection_read_res *read) {
  lua_newtable(L);
  lcdc_set_u64_decimal(L, "upper_position", read->upper_position);
  lcdc_set_u64_decimal(L, "completed_records", read->completed_records);
  lcdc_set_u64_decimal(L, "last_examined_position",
                       read->last_examined_position);
  set_bool(L, "complete", read->complete);
  set_text(L, "stop_reason",
           read->stop_reason == LC_COLLECTION_READ_CANCELLED ? "cancelled"
           : read->stop_reason == LC_COLLECTION_READ_ERROR   ? "error"
                                                             : "finished");
  set_text(L, "resume_cursor", read->resume_cursor);
  set_text(L, "checkpoint_cursor", read->checkpoint_cursor);
}

static int failure(lua_State *L, collection_call *call, int rc) {
  int results = lcdc_push_status_error(L, rc, &call->error);
  if (call->operation == EDIT && call->extra->edited.published) {
    int error_index = lua_gettop(L) - results + 2;
    if (lua_istable(L, error_index)) {
      lua_pushboolean(L, 1);
      lua_setfield(L, error_index, "uncertain");
      push_edit(L, &call->extra->edited);
      lua_setfield(L, error_index, "result");
    }
  }
  if (call->operation == EDIT && call->error.server_code &&
      strcmp(call->error.server_code, "collection_changed") == 0) {
    int error_index = lua_gettop(L) - results + 2;
    if (lua_istable(L, error_index)) {
      push_edit(L, &call->extra->edited);
      lua_setfield(L, error_index, "current_head");
    }
  }
  if (call->operation == READ) {
    call->read.stop_reason = LC_COLLECTION_READ_ERROR;
    push_read(L, &call->read);
    ++results;
  }
  return results;
}

static int options(collection_call *call, int index, const char *const *keys,
                   int required) {
  lua_State *L = call->L;
  int rc = LC_OK;
  if (lua_isnoneornil(L, index) && !required)
    return LC_OK;
  if (!lua_istable(L, index))
    return invalid(call, "collection options must be a table");
  lua_pushnil(L);
  while (lua_next(L, index) != 0) {
    const char *key = NULL;
    size_t length, i;
    if (lua_type(L, -2) == LUA_TSTRING)
      key = lua_tolstring(L, -2, &length);
    if (key != NULL && strlen(key) == length) {
      for (i = 0U; keys[i] != NULL && strcmp(keys[i], key) != 0; ++i)
        ;
      if (keys[i] == NULL)
        rc = invalid(call, "unknown collection option");
    } else
      rc = invalid(call, "collection option names must be strings");
    lua_pop(L, 1);
    if (rc != LC_OK) {
      lua_pop(L, 1);
      break;
    }
  }
  return rc;
}

static int text(collection_call *call, int index, int optional,
                const char **out) {
  size_t length;
  lua_State *L = call->L;
  *out = NULL;
  if (optional && lua_isnoneornil(L, index))
    return LC_OK;
  if (lua_type(L, index) != LUA_TSTRING)
    return invalid(call, "collection text options must be strings");
  *out = lua_tolstring(L, index, &length);
  if (strlen(*out) != length)
    return invalid(call, "collection text options cannot contain NUL");
  return LC_OK;
}

static int text_field(collection_call *call, int index, const char *key,
                      int optional, const char **out) {
  int rc;
  if (lua_isnoneornil(call->L, index)) {
    *out = NULL;
    return optional ? LC_OK
                    : invalid(call, "required collection option missing");
  }
  lua_getfield(call->L, index, key);
  rc = text(call, -1, optional, out);
  /* A metatable can produce a temporary string. Keep it on this protected
   * worker's stack until the native operation finishes borrowing its bytes. */
  if (rc != LC_OK)
    lua_pop(call->L, 1);
  return rc;
}

static int retention_field(collection_call *call, int index, const char *name,
                           int *selected, long *seconds) {
  lua_State *L = call->L;
  lua_Integer value;
  int exact;
  *selected = 0;
  *seconds = 0;
  if (lua_isnoneornil(L, index))
    return LC_OK;
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return LC_OK;
  }
  value = lua_tointegerx(L, -1, &exact);
  if (lua_type(L, -1) != LUA_TNUMBER || !exact || value < 0 ||
      (uintmax_t)value > (uintmax_t)LONG_MAX) {
    lua_pop(L, 1);
    return invalid(call, "retention duration must be a nonnegative integer "
                         "representable by a C long");
  }
  lua_pop(L, 1);
  *selected = 1;
  *seconds = (long)value;
  return LC_OK;
}

static int position(collection_call *call, int index, uint64_t *out) {
  const char *bytes;
  size_t length, i;
  uint64_t value = 0U;
  if (lua_type(call->L, index) != LUA_TSTRING)
    return invalid(
        call, "collection position must be a canonical uint64 decimal string");
  bytes = lua_tolstring(call->L, index, &length);
  if (length == 0U || (length > 1U && bytes[0] == '0'))
    return invalid(call, "collection position is not canonical");
  for (i = 0U; i < length; ++i) {
    unsigned int digit = (unsigned char)bytes[i] - (unsigned int)'0';
    if (digit > 9U || value > ((uint64_t)-1 - digit) / 10U)
      return invalid(call, "collection position exceeds uint64 range");
    value = value * 10U + digit;
  }
  *out = value;
  return LC_OK;
}

static int position_field(collection_call *call, int index, const char *name,
                          int *selected, uint64_t *out) {
  int rc;
  *selected = 0;
  if (lua_isnoneornil(call->L, index))
    return LC_OK;
  lua_getfield(call->L, index, name);
  if (lua_isnil(call->L, -1)) {
    lua_pop(call->L, 1);
    return LC_OK;
  }
  rc = position(call, -1, out);
  lua_pop(call->L, 1);
  if (rc == LC_OK)
    *selected = 1;
  return rc;
}

static int function_field(collection_call *call, int index, const char *name,
                          int *reference) {
  lua_State *L = call->L;
  if (lua_isnoneornil(L, index))
    return LC_OK;
  lua_getfield(L, index, name);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    return LC_OK;
  }
  if (!lua_isfunction(L, -1)) {
    lua_pop(L, 1);
    return invalid(call, "collection callback must be a function");
  }
  *reference = luaL_ref(L, LUA_REGISTRYINDEX);
  return LC_OK;
}

/* Includes metadata construction in the protected frame, not only user code.
 * Native record/description pointers remain borrowed for this callback. */
static int callback_call(lua_State *L) {
  collection_call *call = lua_touserdata(L, 1);
  if (call->info != NULL) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, call->visitor_ref);
    push_info(L, call->info);
  } else {
    const lc_collection_read_metadata *frame = call->frame;
    lua_rawgeti(L, LUA_REGISTRYINDEX, call->metadata_ref);
    lua_newtable(L);
    set_text(L, "kind",
             frame->kind == LC_COLLECTION_READ_METADATA_ENTRY ? "entry"
                                                              : "progress");
    lcdc_set_u64_decimal(L, "last_examined_position",
                         frame->last_examined_position);
    set_text(L, "resume_cursor", frame->resume_cursor);
    if (frame->kind == LC_COLLECTION_READ_METADATA_ENTRY) {
      lcdc_set_u64_decimal(L, "position", frame->position);
      lcdc_set_u64_decimal(L, "bytes", frame->bytes);
      set_text(L, "append_id", frame->append_id);
      set_text(L, "etag", frame->etag);
      lcdc_set_state_retention(L, frame->retention_support,
                               frame->has_retention_expires_at_unix,
                               frame->retention_expires_at_unix);
    }
  }
  lua_call(L, 1, 2);
  return 2;
}

static int callback(collection_call *call, lc_error *error) {
  lua_State *L = call->L;
  int status, rc;
  const char *message;
  if (!lua_checkstack(L, 2))
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to reserve collection callback stack", NULL,
                        NULL, NULL);
  lua_pushcfunction(L, callback_call);
  lua_pushlightuserdata(L, call);
  status = lua_pcall(L, 1, 2, 0);
  if (status != LUA_OK) {
    message = lua_type(L, -1) == LUA_TSTRING ? lua_tostring(L, -1) : NULL;
    rc = lc_error_set(error,
                      status == LUA_ERRMEM ? LC_ERR_NOMEM : LC_ERR_INVALID, 0L,
                      message != NULL ? message : "collection callback failed",
                      NULL, NULL, NULL);
    lua_pop(L, 1);
    return rc;
  }
  rc = LC_OK;
  if (!((lua_isnil(L, -2) && lua_isnil(L, -1)) ||
        (lua_isboolean(L, -2) && lua_toboolean(L, -2)))) {
    message = lua_type(L, -1) == LUA_TSTRING ? lua_tostring(L, -1) : NULL;
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      message != NULL ? message
                                      : "collection callback rejected progress",
                      NULL, NULL, NULL);
  }
  lua_pop(L, 2);
  return rc;
}

static int on_metadata(void *context, const lc_collection_read_metadata *frame,
                       lc_error *error) {
  collection_call *call = context;
  call->frame = frame;
  return callback(call, error);
}

static int on_collection(void *context, const lc_collection_info *info,
                         lc_error *error) {
  collection_call *call = context;
  call->info = info;
  return callback(call, error);
}

static int cancelled(void *context) {
  collection_call *call = context;
  lua_State *L = call->L;
  int status, stop;
  const char *message;
  if (!lua_checkstack(L, 1)) {
    call->callback_failed = lc_error_set(
        &call->error, LC_ERR_NOMEM, 0L,
        "failed to reserve collection cancellation stack", NULL, NULL, NULL);
    return 1;
  }
  lua_rawgeti(L, LUA_REGISTRYINDEX, call->cancel_ref);
  status = lua_pcall(L, 0, 1, 0);
  if (status != LUA_OK) {
    message = lua_type(L, -1) == LUA_TSTRING ? lua_tostring(L, -1) : NULL;
    call->callback_failed = lc_error_set(
        &call->error, status == LUA_ERRMEM ? LC_ERR_NOMEM : LC_ERR_INVALID, 0L,
        message != NULL ? message : "collection cancellation callback failed",
        NULL, NULL, NULL);
    stop = 1;
  } else
    stop = lua_toboolean(L, -1);
  lua_pop(L, 1);
  return stop;
}

static int lease_options(collection_call *call, int index,
                         lc_collection_lease_req *req, int acquisition) {
  static const char *const keys[] = {"owner", "ttl_seconds", "private_read",
                                     NULL};
  static const char *const renewal[] = {"ttl_seconds", NULL};
  lua_State *L = call->L;
  int rc, selected;
  index = lua_absindex(L, index);
  lc_collection_lease_req_init(req);
  rc = options(call, index, acquisition ? keys : renewal, 1);
  if (rc == LC_OK && acquisition)
    rc = text_field(call, index, "owner", 0, &req->owner);
  if (rc == LC_OK)
    rc = retention_field(call, index, "ttl_seconds", &selected,
                         &req->ttl_seconds);
  if (rc == LC_OK && !selected)
    req->ttl_seconds = 30;
  if (rc == LC_OK && acquisition) {
    lua_getfield(L, index, "private_read");
    if (!lua_isnil(L, -1) && !lua_isboolean(L, -1))
      rc = invalid(call, "private_read must be a boolean");
    req->private_read = lua_toboolean(L, -1);
    lua_pop(L, 1);
  }
  return rc;
}

static int worker(lua_State *L) {
  static const char *const config_keys[] = {
      "namespace", "name", "retention_seconds", "max_entries", "lease", NULL};
  static const char *const metadata_keys[] = {"retention_seconds",
                                              "max_entries", NULL};
  static const char *const append_keys[] = {
      "append_id", "retention_seconds", "collection_retention_seconds", NULL};
  static const char *const read_keys[] = {
      "selector_lql", "cursor",    "after",         "through", "first_n",
      "on_metadata",  "cancelled", "expected_head", NULL};
  static const char *const edit_keys[] = {
      "operation_id", "expected_head",   "ranges", "replacement",
      "recovery",     "recovery_anchor", NULL};
  static const char *const range_keys[] = {"after", "through", NULL};
  static const char *const list_keys[] = {"namespace", NULL};
  collection_call *call = lua_touserdata(L, 4);
  lc_client *client = NULL;
  lc_collection *handle;
  lc_collection_config config;
  lc_collection_metadata_req change;
  lc_collection_append_req append;
  lc_collection_append_res entry;
  lc_collection_append_status_res status;
  lc_collection_delete_res deleted;
  lc_collection_read_opts read;
  lc_collection_capabilities capabilities;
  lc_collection_info info;
  lc_collection_lease_req lease;
  lc_collection_lease_info ownership;
  collection_ud *ud;
  writer_ud *writing;
  const char *value;
  uint64_t seq;
  int rc = LC_OK;
  if (call->operation >= WRITER_WRITE) {
    const char *bytes = NULL;
    size_t count = 0;
    writing = luaL_testudata(L, 1, WRITER_MT);
    if (writing == NULL || writing->handle == NULL || writing->active)
      return failure(L, call,
                     invalid(call, "expected a live, idle collection writer"));
    if (call->operation == WRITER_WRITE) {
      if (lua_type(L, 2) != LUA_TSTRING)
        return failure(
            L, call,
            invalid(call, "collection writer.write requires a byte string"));
      bytes = lua_tolstring(L, 2, &count);
    }
    call->writer_receiver = writing;
    writing->active = 1;
    if (call->operation == WRITER_WRITE)
      rc = lc_collection_writer_write(writing->handle, bytes, count,
                                      &call->error);
    else if (call->operation == WRITER_COMMIT)
      rc = lc_collection_writer_commit(writing->handle, &entry, &call->error);
    else
      rc = lc_collection_writer_abort(writing->handle, &call->error);
    if (rc != LC_OK)
      return failure(L, call, rc);
    if (call->operation == WRITER_COMMIT)
      push_entry(L, &entry);
    else
      lua_pushboolean(L, 1);
    return 1;
  }
  if (call->operation <= CAPABILITIES) {
    rc = lc_lua_client_view(L, 1, &client, &call->error);
    if (rc != LC_OK)
      return failure(L, call, rc);
    if (call->operation == CREATE || call->operation == OPEN) {
      lc_collection_config_init(&config);
      rc = options(call, 2, config_keys, 1);
      if (rc == LC_OK)
        rc = text_field(call, 2, "namespace", 1, &config.ns);
      if (rc == LC_OK)
        rc = text_field(call, 2, "name", 0, &config.name);
      if (rc == LC_OK)
        rc = retention_field(call, 2, "retention_seconds",
                             &config.has_retention_seconds,
                             &config.retention_seconds);
      if (rc == LC_OK)
        rc = position_field(call, 2, "max_entries", &config.has_max_entries,
                            &config.max_entries);
      if (rc == LC_OK) {
        lua_getfield(L, 2, "lease");
        if (!lua_isnil(L, -1)) {
          rc = lease_options(call, lua_gettop(L), &lease, 1);
          config.lease = &lease;
        }
      }
      if (rc == LC_OK)
        rc = lc_lua_client_view(L, 1, &client, &call->error);
      if (rc != LC_OK)
        return failure(L, call, rc);
      ud = lua_newuserdatauv(L, sizeof(*ud), 1);
      memset(ud, 0, sizeof(*ud));
      luaL_setmetatable(L, COLLECTION_MT);
      lua_pushvalue(L, 1);
      lua_setiuservalue(L, -2, 1);
      rc = lc_lua_client_view(L, 1, &client, &call->error);
      if (rc != LC_OK)
        return failure(L, call, rc);
      rc = call->operation == CREATE
               ? lc_client_create_collection(client, &config, &ud->handle,
                                             &call->error)
               : lc_client_open_collection(client, &config, &ud->handle,
                                           &call->error);
      return rc == LC_OK ? 1 : failure(L, call, rc);
    }
    if (call->operation == LIST) {
      rc = options(call, 2, list_keys, 0);
      if (rc == LC_OK)
        rc = text_field(call, 2, "namespace", 1, &value);
      if (rc == LC_OK && !lua_isfunction(L, 3))
        rc = invalid(call, "list_collections requires a visitor function");
      if (rc == LC_OK) {
        lua_pushvalue(L, 3);
        call->visitor_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        rc = lc_lua_client_view(L, 1, &client, &call->error);
      }
      if (rc == LC_OK)
        rc = lc_client_list_collections(client, value, on_collection, call,
                                        &call->error);
      if (rc != LC_OK)
        return failure(L, call, rc);
      lua_pushboolean(L, 1);
      return 1;
    }
    rc = lc_client_collection_capabilities(client, &capabilities, &call->error);
    if (rc != LC_OK)
      return failure(L, call, rc);
    lua_newtable(L);
    set_bool(L, "collections", capabilities.collections);
    set_bool(L, "lql_filter", capabilities.lql_filter);
    set_bool(L, "resume", capabilities.resume);
    set_bool(L, "checkpoint", capabilities.checkpoint);
    set_bool(L, "ranges", capabilities.ranges);
    set_bool(L, "first_n", capabilities.first_n);
    set_bool(L, "last_n", capabilities.last_n);
    set_bool(L, "position_selection", capabilities.position_selection);
    set_bool(L, "count_retention", capabilities.count_retention);
    set_bool(L, "append_collection_retention",
             capabilities.append_collection_retention);
    set_bool(L, "append_writer", capabilities.append_writer);
    set_bool(L, "append_staged", capabilities.append_staged);
    set_bool(L, "leases", capabilities.leases);
    set_bool(L, "lease_takeover", capabilities.lease_takeover);
    set_bool(L, "private_reads", capabilities.private_reads);
    set_bool(L, "atomic_edit", capabilities.atomic_edit);
    return 1;
  }
  ud = luaL_testudata(L, 1, COLLECTION_MT);
  if (ud == NULL || ud->handle == NULL || ud->active)
    return failure(L, call,
                   invalid(call, "expected a live, idle collection receiver"));
  if (call->operation == ACQUIRE || call->operation == TAKEOVER ||
      call->operation == KEEPALIVE) {
    rc = lease_options(call, 2, &lease, call->operation != KEEPALIVE);
    if (rc == LC_OK && call->operation == TAKEOVER)
      rc = text(call, 3, 0, &value);
  }
  if (call->operation == METADATA || call->operation == ENTRY_METADATA) {
    int index = call->operation == METADATA ? 2 : 3;
    lc_collection_metadata_req_init(&change);
    rc = options(call, index, metadata_keys, 1);
    if (rc == LC_OK)
      rc = retention_field(call, index, "retention_seconds",
                           &change.has_retention_seconds,
                           &change.retention_seconds);
    if (rc == LC_OK)
      rc = position_field(call, index, "max_entries", &change.has_max_entries,
                          &change.max_entries);
    if (rc == LC_OK && call->operation == ENTRY_METADATA)
      rc = position(call, 2, &seq);
  } else if (call->operation == APPEND || call->operation == APPEND_STAGED ||
             call->operation == APPEND_BEGIN) {
    lc_collection_append_req_init(&append);
    rc = options(call, 2, append_keys, 1);
    if (rc == LC_OK)
      rc = text_field(call, 2, "append_id", 0, &append.append_id);
    if (rc == LC_OK)
      rc = retention_field(call, 2, "retention_seconds",
                           &append.has_retention_seconds,
                           &append.retention_seconds);
    if (rc == LC_OK)
      rc = retention_field(call, 2, "collection_retention_seconds",
                           &append.has_collection_retention_seconds,
                           &append.collection_retention_seconds);
    if (rc == LC_OK &&
        (call->operation == APPEND || call->operation == APPEND_STAGED))
      rc = lcdc_source_from_value(L, 3, &call->source, &call->error);
  } else if (call->operation == EDIT) {
    lc_collection_edit_req *req = &call->extra->edit;
    int index, selected;
    size_t i, count;
    lc_collection_edit_req_init(req);
    rc = options(call, 2, edit_keys, 1);
    if (rc == LC_OK)
      rc = text_field(call, 2, "operation_id", 0, &req->operation_id);
    if (rc == LC_OK)
      rc = text_field(call, 2, "expected_head", 0, &req->expected_head);
    if (rc == LC_OK)
      rc = text_field(call, 2, "recovery", 1, &req->recovery);
    req->has_recovery = req->recovery != NULL;
    if (rc == LC_OK) {
      lua_getfield(L, 2, "recovery_anchor");
      if (lua_type(L, -1) == LUA_TSTRING && lua_rawlen(L, -1) == 11 &&
          strcmp(lua_tostring(L, -1), "replacement") == 0)
        /* musl's UINT64_MAX literal is not C89-compatible on 32-bit targets. */
        req->recovery_anchor = (uint64_t)-1;
      else if (!lua_isnil(L, -1))
        rc = position(call, -1, &req->recovery_anchor);
      lua_pop(L, 1);
    }
    if (rc == LC_OK) {
      lua_getfield(L, 2, "ranges");
      index = lua_gettop(L);
      if (!lua_isnil(L, index) && !lua_istable(L, index))
        rc = invalid(call, "edit ranges must be a dense array");
      count = lua_istable(L, index) ? lua_rawlen(L, index) : 0;
      if (count > LC_COLLECTION_EDIT_MAX_RANGES)
        rc = invalid(call, "too many edit ranges");
      if (rc == LC_OK && lua_istable(L, index)) {
        lua_pushnil(L);
        while (lua_next(L, index) != 0) {
          int exact;
          lua_Integer key = lua_tointegerx(L, -2, &exact);
          if (!exact || lua_type(L, -2) != LUA_TNUMBER || key < 1 ||
              (lua_Unsigned)key > count)
            rc = invalid(call, "edit ranges must be a dense array");
          lua_pop(L, 1);
        }
      }
      req->range_count = count;
      req->ranges = call->extra->ranges;
      for (i = 0; rc == LC_OK && i < count; ++i) {
        int item;
        lua_rawgeti(L, index, (lua_Integer)i + 1);
        item = lua_gettop(L);
        rc = options(call, item, range_keys, 1);
        if (rc == LC_OK)
          rc = position_field(call, item, "after", &selected,
                              &call->extra->ranges[i].after);
        if (rc == LC_OK && !selected)
          rc = invalid(call, "edit range requires after");
        if (rc == LC_OK)
          rc = position_field(call, item, "through", &selected,
                              &call->extra->ranges[i].through);
        if (rc == LC_OK && !selected)
          rc = invalid(call, "edit range requires through");
        lua_pop(L, 1);
      }
    }
    if (rc == LC_OK) {
      lua_getfield(L, 2, "replacement");
      index = lua_gettop(L);
      if (!lua_isnil(L, index)) {
        lc_collection_append_req_init(&append);
        req->replacement = &append;
        rc = options(call, index, append_keys, 1);
        if (rc == LC_OK)
          rc = text_field(call, index, "append_id", 0, &append.append_id);
        if (rc == LC_OK)
          rc = retention_field(call, index, "retention_seconds",
                               &append.has_retention_seconds,
                               &append.retention_seconds);
        if (rc == LC_OK)
          rc = retention_field(call, index, "collection_retention_seconds",
                               &append.has_collection_retention_seconds,
                               &append.collection_retention_seconds);
        if (rc == LC_OK)
          rc = lcdc_source_from_value(L, 3, &call->source, &call->error);
      } else if (!lua_isnoneornil(L, 3))
        rc = invalid(call, "edit source requires replacement options");
    }
  } else if (call->operation == APPEND_STATUS || call->operation == EDIT_STATUS)
    rc = text(call, 2, 0, &value);
  else if (call->operation == DELETE_ENTRY)
    rc = position(call, 2, &seq);
  else if (call->operation == READ) {
    lc_collection_read_opts_init(&read);
    rc = options(call, 2, read_keys, 0);
    if (rc == LC_OK)
      rc = text_field(call, 2, "expected_head", 1, &read.expected_head);
    if (rc == LC_OK)
      rc = text_field(call, 2, "selector_lql", 1, &read.selector_lql);
    if (rc == LC_OK)
      rc = text_field(call, 2, "cursor", 1, &read.cursor);
    if (rc == LC_OK)
      rc = position_field(call, 2, "after", &read.has_after, &read.after);
    if (rc == LC_OK)
      rc = position_field(call, 2, "through", &read.has_through, &read.through);
    if (rc == LC_OK)
      rc = position_field(call, 2, "first_n", &read.has_first_n, &read.first_n);
    if (rc == LC_OK)
      rc = function_field(call, 2, "on_metadata", &call->metadata_ref);
    if (rc == LC_OK)
      rc = function_field(call, 2, "cancelled", &call->cancel_ref);
    if (rc == LC_OK && lua_isnoneornil(L, 3))
      rc = invalid(call, "collection read requires a streaming sink");
    if (rc == LC_OK)
      rc = lcdc_sink_from_value(L, 3, &call->sink, &call->error);
    read.on_metadata = call->metadata_ref == LUA_NOREF ? NULL : on_metadata;
    read.metadata_context = call;
    read.cancelled = call->cancel_ref == LUA_NOREF ? NULL : cancelled;
    read.cancel_context = call;
  }
  /* Option/source/sink accessors may close the receiver. Revalidate after
   * every such conversion, before giving the native library a pointer. */
  if (rc == LC_OK && (ud->handle == NULL || ud->active))
    rc = invalid(call,
                 "collection closed or became busy during option conversion");
  if (rc != LC_OK)
    return failure(L, call, rc);
  handle = ud->handle;
  call->receiver = ud;
  ud->active = 1;
  switch (call->operation) {
  case ACQUIRE:
  case TAKEOVER: {
    collection_ud *owned = lua_newuserdatauv(L, sizeof(*owned), 1);
    memset(owned, 0, sizeof(*owned));
    luaL_setmetatable(L, COLLECTION_MT);
    lua_pushvalue(L, 1);
    lua_setiuservalue(L, -2, 1);
    if (ud->handle == NULL)
      rc = invalid(call, "collection closed during lease receiver allocation");
    else if (call->operation == ACQUIRE)
      rc = lc_collection_acquire(handle, &lease, &owned->handle, &call->error);
    else
      rc = lc_collection_takeover(handle, &lease, value, &owned->handle,
                                  &call->error);
    return rc == LC_OK ? 1 : failure(L, call, rc);
  }
  case KEEPALIVE:
    rc = lc_collection_keepalive(handle, lease.ttl_seconds, &ownership,
                                 &call->error);
    break;
  case RELEASE:
    rc = lc_collection_release(handle, &call->error);
    if (rc == LC_OK)
      ud->handle = NULL;
    break;
  case HEAD:
    rc = lc_collection_head(handle, &call->extra->head, &call->error);
    break;
  case EDIT:
    rc = lc_collection_edit(handle, &call->extra->edit, call->source,
                            &call->extra->edited, &call->error);
    break;
  case EDIT_STATUS:
    rc = lc_collection_edit_status(handle, value, &call->extra->edit_status,
                                   &call->error);
    break;
  case DESCRIBE:
    rc = lc_collection_describe(handle, &info, &call->error);
    break;
  case METADATA:
    rc = lc_collection_metadata(handle, &change, &info, &call->error);
    break;
  case ENTRY_METADATA:
    rc = lc_collection_entry_metadata(handle, seq, &change, &entry,
                                      &call->error);
    break;
  case APPEND:
    rc = lc_collection_append(handle, &append, call->source, &entry,
                              &call->error);
    break;
  case APPEND_STAGED:
    rc = lc_collection_append_staged(handle, &append, call->source, &entry,
                                     &call->error);
    break;
  case APPEND_BEGIN:
    writing = lua_newuserdatauv(L, sizeof(*writing), 1);
    memset(writing, 0, sizeof(*writing));
    luaL_setmetatable(L, WRITER_MT);
    lua_pushvalue(L, 1);
    lua_setiuservalue(L, -2, 1);
    if (ud->handle == NULL)
      rc = invalid(call, "collection closed during writer allocation");
    else
      rc = lc_collection_append_begin(ud->handle, &append, &writing->handle,
                                      &call->error);
    return rc == LC_OK ? 1 : failure(L, call, rc);
  case APPEND_STATUS:
    rc = lc_collection_append_status(handle, value, &status, &call->error);
    break;
  case READ:
    rc = lc_collection_read(handle, &read, call->sink, &call->read,
                            &call->error);
    if (call->callback_failed != 0)
      rc = call->callback_failed;
    break;
  case DELETE_ENTRY:
    rc = lc_collection_delete_entry(handle, seq, &deleted, &call->error);
    break;
  case DROP:
    rc = lc_collection_drop(handle, &call->error);
    break;
  default:
    rc = invalid(call, "unknown collection operation");
    break;
  }
  if (rc != LC_OK)
    return failure(L, call, rc);
  if (call->operation == HEAD)
    push_head(L, &call->extra->head);
  else if (call->operation == EDIT)
    push_edit(L, &call->extra->edited);
  else if (call->operation == EDIT_STATUS) {
    lua_newtable(L);
    set_bool(L, "found", call->extra->edit_status.found);
    if (call->extra->edit_status.found) {
      push_edit(L, &call->extra->edit_status.result);
      lua_setfield(L, -2, "result");
    }
  } else if (call->operation == KEEPALIVE)
    push_lease(L, &ownership);
  else if (call->operation == DESCRIBE || call->operation == METADATA)
    push_info(L, &info);
  else if (call->operation == APPEND || call->operation == APPEND_STAGED ||
           call->operation == ENTRY_METADATA)
    push_entry(L, &entry);
  else if (call->operation == APPEND_STATUS) {
    lua_newtable(L);
    set_text(L, "state",
             status.state == LC_COLLECTION_APPEND_COMMITTED ? "committed"
             : status.state == LC_COLLECTION_APPEND_DELETED ? "deleted"
                                                            : "absent");
    if (status.state != LC_COLLECTION_APPEND_ABSENT) {
      push_entry(L, &status.entry);
      lua_setfield(L, -2, "entry");
    }
  } else if (call->operation == READ) {
    lua_pushnil(L);
    push_read(L, &call->read);
    return 2;
  } else if (call->operation == DELETE_ENTRY) {
    lua_newtable(L);
    lcdc_set_u64_decimal(L, "position", deleted.position);
    set_bool(L, "deleted", deleted.deleted);
    set_bool(L, "already_deleted", deleted.already_deleted);
  } else
    lua_pushboolean(L, 1);
  return 1;
}

static int dispatch(lua_State *L, int operation) {
  collection_call *call;
  const char *message;
  int status, count;
  lua_settop(L, 3);
  call = lua_newuserdatauv(
      L,
      sizeof(*call) +
          ((operation == HEAD || operation == EDIT || operation == EDIT_STATUS)
               ? sizeof(collection_edit_data)
               : 0),
      2);
  memset(call, 0, sizeof(*call));
  if (operation == HEAD || operation == EDIT || operation == EDIT_STATUS) {
    call->extra = (collection_edit_data *)(call + 1);
    memset(call->extra, 0, sizeof(*call->extra));
  }
  call->L = L;
  call->operation = operation;
  call->metadata_ref = call->cancel_ref = call->visitor_ref = LUA_NOREF;
  lc_error_init(&call->error);
  lc_collection_read_res_init(&call->read);
  luaL_setmetatable(L, CALL_MT);
  lua_pushvalue(L, 1);
  lua_setiuservalue(L, 4, 1);
  lua_pushthread(L);
  lua_setiuservalue(L, 4, 2);
  lua_pushcfunction(L, worker);
  lua_pushvalue(L, 1);
  lua_pushvalue(L, 2);
  lua_pushvalue(L, 3);
  lua_pushvalue(L, 4);
  status = lua_pcall(L, 4, LUA_MULTRET, 0);
  if (status != LUA_OK) {
    message = lua_type(L, -1) == LUA_TSTRING ? lua_tostring(L, -1) : NULL;
    lc_error_set(&call->error,
                 status == LUA_ERRMEM ? LC_ERR_NOMEM : LC_ERR_INVALID, 0L,
                 message != NULL ? message : "Lua collection operation failed",
                 NULL, NULL, NULL);
    lua_pop(L, 1);
    count = failure(L, call, call->error.code);
  } else
    count = lua_gettop(L) - 4;
  release_call(call);
  return count;
}

#define COLLECTION_METHOD(name, operation)                                     \
  static int name(lua_State *L) { return dispatch(L, operation); }
COLLECTION_METHOD(describe, DESCRIBE)
COLLECTION_METHOD(acquire, ACQUIRE)
COLLECTION_METHOD(takeover, TAKEOVER)
COLLECTION_METHOD(keepalive, KEEPALIVE)
COLLECTION_METHOD(release, RELEASE)
COLLECTION_METHOD(metadata, METADATA)
COLLECTION_METHOD(entry_metadata, ENTRY_METADATA)
COLLECTION_METHOD(append, APPEND)
COLLECTION_METHOD(append_staged, APPEND_STAGED)
COLLECTION_METHOD(append_begin, APPEND_BEGIN)
COLLECTION_METHOD(writer_write, WRITER_WRITE)
COLLECTION_METHOD(writer_commit, WRITER_COMMIT)
COLLECTION_METHOD(writer_abort, WRITER_ABORT)
COLLECTION_METHOD(append_status, APPEND_STATUS)
COLLECTION_METHOD(head, HEAD)
COLLECTION_METHOD(edit, EDIT)
COLLECTION_METHOD(edit_status, EDIT_STATUS)
COLLECTION_METHOD(read_entries, READ)
COLLECTION_METHOD(delete_entry, DELETE_ENTRY)
COLLECTION_METHOD(drop, DROP)

int lcdc_collection_create(lua_State *L) { return dispatch(L, CREATE); }
int lcdc_collection_open(lua_State *L) { return dispatch(L, OPEN); }
int lcdc_collection_list(lua_State *L) { return dispatch(L, LIST); }
int lcdc_collection_capabilities(lua_State *L) {
  return dispatch(L, CAPABILITIES);
}

void lcdc_collections_install(lua_State *L) {
  static const luaL_Reg methods[] = {{"head", head},
                                     {"edit", edit},
                                     {"edit_status", edit_status},
                                     {"describe", describe},
                                     {"acquire", acquire},
                                     {"takeover", takeover},
                                     {"keepalive", keepalive},
                                     {"release", release},
                                     {"metadata", metadata},
                                     {"entry_metadata", entry_metadata},
                                     {"append", append},
                                     {"append_begin", append_begin},
                                     {"append_staged", append_staged},
                                     {"append_status", append_status},
                                     {"read", read_entries},
                                     {"delete_entry", delete_entry},
                                     {"drop", drop},
                                     {"close", collection_close},
                                     {NULL, NULL}};
  static const luaL_Reg writer_methods[] = {{"write", writer_write},
                                            {"commit", writer_commit},
                                            {"abort", writer_abort},
                                            {"close", writer_close},
                                            {NULL, NULL}};
  luaL_newmetatable(L, CALL_MT);
  lua_pushcfunction(L, call_gc);
  lua_setfield(L, -2, "__gc");
  lua_pop(L, 1);
  luaL_newmetatable(L, COLLECTION_MT);
  lua_pushcfunction(L, collection_close);
  lua_setfield(L, -2, "__gc");
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_setfuncs(L, methods, 0);
  lua_pop(L, 1);
  luaL_newmetatable(L, WRITER_MT);
  lua_pushcfunction(L, writer_close);
  lua_setfield(L, -2, "__gc");
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_setfuncs(L, writer_methods, 0);
  lua_pop(L, 1);
}
