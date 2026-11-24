if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/src/lua/lockdc_lua.c" lua_binding)

foreach(required_snippet
    "static long lcdc_check_long(lua_State *L, int index, const char *name)"
    "value < (lua_Integer)LONG_MIN || value > (lua_Integer)LONG_MAX"
    "luaL_error(L, \"%s must fit a C long\", name)"
    "static lc_version lcdc_check_version(lua_State *L, int index,"
    "static int lcdc_opt_version_field(lua_State *L, int index, const char *name,"
    "lcdc_opt_version_field(L, 2, \"if_version\", &request.if_version)"
    "void lcdc_set_version_field(lua_State *L, const char *name,"
    "value < (lc_version)LCDC_LUA_MININTEGER ||"
    "value > (lc_version)LCDC_LUA_MAXINTEGER"
    "luaL_error(L, \"%s exceeds the Lua integer range\", name)"
    "timeout_ms = lcdc_check_long(L, 2, \"outbox dispatcher next timeout\")"
    "timeout_ms = lcdc_check_long(L, 3, \"outbox command wait timeout\")"
    "long ttl_seconds = lcdc_check_long(L, 2, \"outbox renewal ttl\")"
    "static int lcdc_set_size_field(lua_State *L, const char *name, size_t value,"
    "static int lcdc_set_uint64_field(lua_State *L, const char *name,"
    "static int lcdc_set_int64_field(lua_State *L, const char *name, int64_t value,"
    "outbox statistic exceeds Lua integer range"
    "outbox value exceeds Lua integer range"
    "lcdc_set_uint64_field(L, \"direct_notifications\","
    "stats->direct_notifications, error)"
    "lcdc_set_uint64_field(L, \"index_seq\", res.index_seq, &error)"
    "static int lcdc_set_unix_seconds_field(lua_State *L, const char *name,"
    "lcdc_set_unix_seconds_field(L, \"expires_at_unix\", expires_at_unix,"
    "lcdc_set_unix_seconds_field(L, \"updated_at_unix\", res->updated_at_unix,"
    "static int lcdc_client_flush_index(lua_State *L)"
    "lcdc_set_int64_field(L, \"version\", participant->version, error)"
    "lcdc_set_int64_field(L, \"version\", result.version, &error)"
    "lcdc_set_int64_field(L, \"lease_expires_at_unix\","
    "job->lease_expires_at_unix, error)")
    string(FIND "${lua_binding}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua outbox numeric conversion contract is missing: ${required_snippet}")
    endif()
endforeach()

foreach(forbidden_snippet
    "(long)stats->pending_notifications"
    "(long)stats->ready_jobs"
    "(long)stats->direct_notifications"
    "(long)stats->notification_overflows"
    "(long)stats->recovery_queries"
    "(long)stats->recovered_claims"
    "(long)stats->claim_losses"
    "(long)stats->payload_open_failures"
    "lcdc_set_integer_field(L, \"version\", participant->version)"
    "lcdc_set_integer_field(L, \"lease_expires_at_unix\",
                         job->lease_expires_at_unix)"
    "lcdc_set_integer_field(L, \"expires_at_unix\", expires_at_unix)"
    "lcdc_set_integer_field(L, \"updated_at_unix\", res->updated_at_unix)")
    string(FIND "${lua_binding}" "${forbidden_snippet}" snippet_index)
    if(NOT snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua outbox statistics must not narrow through C long: ${forbidden_snippet}")
    endif()
endforeach()

string(FIND "${lua_binding}"
    "static int lcdc_outbox_participant_get(lua_State *L)" participant_get_start)
string(FIND "${lua_binding}"
    "static int lcdc_outbox_participant_update(lua_State *L)" participant_get_end)
if(participant_get_start EQUAL -1 OR participant_get_end EQUAL -1 OR
   participant_get_end LESS participant_get_start)
    message(FATAL_ERROR "Lua outbox participant get binding is missing")
endif()

string(FIND "${lua_binding}"
    "static int lcdc_client_query_keys(lua_State *L)" query_keys_start)
string(FIND "${lua_binding}"
    "static int lcdc_client_get_namespace_config(lua_State *L)" query_keys_end)
if(query_keys_start EQUAL -1 OR query_keys_end EQUAL -1 OR
   query_keys_end LESS query_keys_start)
    message(FATAL_ERROR "Lua query-key binding is missing")
endif()

string(FIND "${lua_binding}"
    "static int lcdc_client_flush_index(lua_State *L)" flush_index_start)
string(FIND "${lua_binding}"
    "static void lcdc_free_txn_participants" flush_index_end)
if(flush_index_start EQUAL -1 OR flush_index_end EQUAL -1 OR
   flush_index_end LESS flush_index_start)
    message(FATAL_ERROR "Lua index-flush binding is missing")
endif()
math(EXPR flush_index_length "${flush_index_end} - ${flush_index_start}")
string(SUBSTRING "${lua_binding}" ${flush_index_start} ${flush_index_length}
    flush_index_binding)
string(FIND "${flush_index_binding}"
    "lcdc_set_uint64_field(L, \"index_seq\", res.index_seq, &error)"
    flush_index_wide_index)
if(flush_index_wide_index EQUAL -1)
    message(FATAL_ERROR
        "Lua index-flush results must preserve 64-bit index sequences")
endif()
string(FIND "${flush_index_binding}"
    "lcdc_set_uinteger_field(L, \"index_seq\", res.index_seq)"
    flush_index_narrow_index)
if(NOT flush_index_narrow_index EQUAL -1)
    message(FATAL_ERROR
        "Lua index-flush results must not narrow index sequences through C long")
endif()
math(EXPR query_keys_length "${query_keys_end} - ${query_keys_start}")
string(SUBSTRING "${lua_binding}" ${query_keys_start} ${query_keys_length}
    query_keys_binding)
string(FIND "${query_keys_binding}"
    "lcdc_set_uint64_field(L, \"index_seq\", res.index_seq, &error)"
    query_keys_wide_index)
if(query_keys_wide_index EQUAL -1)
    message(FATAL_ERROR
        "Lua query-key results must preserve 64-bit index sequences")
endif()
string(FIND "${query_keys_binding}"
    "lcdc_set_uinteger_field(L, \"index_seq\", res.index_seq)"
    query_keys_narrow_index)
if(NOT query_keys_narrow_index EQUAL -1)
    message(FATAL_ERROR
        "Lua query-key results must not narrow index sequences through C long")
endif()
math(EXPR participant_get_length "${participant_get_end} - ${participant_get_start}")
string(SUBSTRING "${lua_binding}" ${participant_get_start}
    ${participant_get_length} participant_get_binding)
string(FIND "${participant_get_binding}"
    "lcdc_set_integer_field(L, \"version\", result.version)"
    participant_get_narrowing_index)
if(NOT participant_get_narrowing_index EQUAL -1)
    message(FATAL_ERROR
        "Lua outbox participant get must not narrow version through C long")
endif()
string(FIND "${participant_get_binding}"
    "lcdc_set_int64_field(L, \"version\", result.version, &error)"
    participant_get_wide_index)
if(participant_get_wide_index EQUAL -1)
    message(FATAL_ERROR
        "Lua outbox participant get must preserve 64-bit versions")
endif()

string(FIND "${lua_binding}"
    "static int lcdc_outbox_participant_attach(lua_State *L)"
    participant_attach_start)
string(FIND "${lua_binding}"
    "static int lcdc_outbox_participant_get_attachment(lua_State *L)"
    participant_attach_end)
if(participant_attach_start EQUAL -1 OR participant_attach_end EQUAL -1 OR
   participant_attach_end LESS participant_attach_start)
    message(FATAL_ERROR "Lua outbox participant attach binding is missing")
endif()
math(EXPR participant_attach_length
    "${participant_attach_end} - ${participant_attach_start}")
string(SUBSTRING "${lua_binding}" ${participant_attach_start}
    ${participant_attach_length} participant_attach_binding)
string(FIND "${participant_attach_binding}"
    "lcdc_set_int64_field(L, \"version\", result.version, &error)"
    participant_attach_wide_index)
if(participant_attach_wide_index EQUAL -1)
    message(FATAL_ERROR
        "Lua outbox participant attach must preserve 64-bit versions")
endif()
string(FIND "${participant_attach_binding}"
    "lcdc_set_integer_field(L, \"version\", result.version)"
    participant_attach_narrowing_index)
if(NOT participant_attach_narrowing_index EQUAL -1)
    message(FATAL_ERROR
        "Lua outbox participant attach must not narrow version through C long")
endif()
