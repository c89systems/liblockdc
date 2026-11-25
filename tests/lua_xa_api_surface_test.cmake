cmake_minimum_required(VERSION 3.20)

if(NOT LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

# Keep the public C contract as the independent inventory. New public XA
# functions must gain a Lua binding or an explicit language-lifetime mapping.
# Runtime journal and real PostgreSQL tests prove the mapped workflows.
set(binding_sources "")
foreach(source lockdc_xa_lua.c lockdc_xa_resource_lua.c lockdc_xa_provider_lua.c)
    file(READ "${LOCKDC_ROOT}/src/lua/${source}" source_text)
    string(APPEND binding_sources "${source_text}")
endforeach()
set(language_lifetimes
    lc_xa_borrow_open lc_xa_transaction_retain)
# PostgreSQL's public native primitives delegate through the generic driver;
# SQL connection pointers are obtained only through the public checked C bridge.
set(native_aliases
    lc_postgres_open=lc_xa_resource_open
    lc_postgres_driver=lc_lua_postgres_register
    lc_postgres_register_start=lc_xa_resource_register_start
    lc_postgres_branch_start=lc_xa_resource_branch_start
    lc_postgres_prepare_start=lc_xa_branch_prepare_start
    lc_postgres_resolve_start=lc_xa_branch_resolve_start
    lc_postgres_recover_start=lc_xa_resource_recover_start
    lc_postgres_status_start=lc_xa_resource_status_start
    lc_postgres_inventory_start=lc_xa_resource_inventory_start
    lc_postgres_operation_advance=lc_xa_operation_advance
    lc_postgres_operation_wait=lc_xa_operation_wait
    lc_postgres_operation_result=lc_xa_operation_result
    lc_postgres_operation_cancel=lc_xa_operation_cancel
    lc_postgres_operation_close_blocking=lc_xa_operation_close_blocking
    lc_postgres_branch_close=lc_xa_branch_close
    lc_postgres_close_blocking=lc_xa_resource_close_blocking
    lc_postgres_branch_borrow=lc_xa_branch_borrow
    lc_postgres_branch_borrow_generic=lc_xa_branch_borrow
    lc_postgres_branch_record=lc_xa_branch_record_get
    lc_sql_borrow_postgres=lc_lua_xa_borrow_retain
    lc_sql_borrow_release=lc_xa_borrow_release)
file(READ "${LOCKDC_ROOT}/src/lua/lockdc_postgres_lua.c" postgres_binding)
string(APPEND binding_sources "${postgres_binding}")
foreach(alias IN LISTS native_aliases)
    string(REPLACE "=" ";" pair "${alias}")
    list(GET pair 0 native)
    list(GET pair 1 generic)
    set(alias_${native} "${generic}")
endforeach()
foreach(header include/lc/lc.h include/lc/postgres.h)
    file(READ "${LOCKDC_ROOT}/${header}" declarations)
    string(REGEX MATCHALL "lc_(xa|postgres|sql)_[a-z_]+[ \t\r\n]*\\(" functions "${declarations}")
    foreach(function IN LISTS functions)
        string(REGEX REPLACE "[ \t\r\n]*\\($" "" name "${function}")
        if(name IN_LIST language_lifetimes)
            continue()
        endif()
        if(DEFINED alias_${name})
            set(name "${alias_${name}}")
        endif()
        string(FIND "${binding_sources}" "${name}(" found)
        if(found EQUAL -1)
            message(FATAL_ERROR "Public C XA operation has no Lua binding or mapping: ${function}")
        endif()
    endforeach()
    string(REGEX MATCHALL "LC_(XA|POSTGRES)_[A-Z_]+[ \t]*=" constants "${declarations}")
    foreach(constant IN LISTS constants)
        string(REGEX REPLACE "[ \t]*=$" "" name "${constant}")
        if(name MATCHES "^LC_POSTGRES_")
            set(export "OUTCOME(${name})")
        else()
            set(export "CONSTANT(${name})")
        endif()
        string(FIND "${binding_sources}" "${export}" found)
        if(found EQUAL -1)
            message(FATAL_ERROR "Public C XA constant has no Lua export: ${name}")
        endif()
    endforeach()
    string(REGEX REPLACE "/\\*([^*]|\\*+[^*/])*\\*+/" "" clean_declarations "${declarations}")
    foreach(struct lc_xa_branch_record lc_xa_connection_view lc_xa_wait lc_xa_result
        lc_xa_inventory_row lc_xa_coordinator_result lc_xa_coordinator_config
        lc_postgres_config lc_postgres_result lc_postgres_inventory_row lc_postgres_wait)
        string(REGEX MATCH "typedef struct ${struct} *\\{([^}]*)\\}" definition "${clean_declarations}")
        if(NOT definition)
            continue()
        endif()
        string(REGEX MATCHALL "[a-z_]+ *(\\[[^]]+\\])? *;" fields "${CMAKE_MATCH_1}")
        foreach(field IN LISTS fields)
            string(REGEX MATCH "^[a-z_]+" name "${field}")
            if(NOT name)
                continue()
            endif()
            if(name STREQUAL "native_connection" OR name STREQUAL "owner_thread" OR name STREQUAL "branch")
                # Opaque pointers stay behind C interop; owned branch transfer
                # is operation:take_branch(), never a duplicated result shell.
                continue()
            endif()
            string(FIND "${binding_sources}" "\"${name}\"" found)
            if(found EQUAL -1)
                message(FATAL_ERROR "Public C XA field has no Lua representation: ${struct}.${name}")
            endif()
        endforeach()
    endforeach()
endforeach()
