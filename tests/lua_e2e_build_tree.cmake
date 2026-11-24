# Included by the common E2E launcher. The scenario scripts run with the
# CMake-built lockdc module and the pinned lonejson binding source.
set(fixture "${LOCKDC_LUA_FIXTURE_ROOT}/native")
set(lonejson_source "${fixture}/lonejson-0.50.0")
if(LOCKDC_LUA_PREPARE_FIXTURE)
    include("${CMAKE_CURRENT_LIST_DIR}/acquire_lonejson_lua_rock.cmake")
    lockdc_acquire_lonejson_lua_rock(rock)
    lockdc_import_cache_value(CMAKE_C_COMPILER)
    lockdc_import_cache_value(CMAKE_SYSROOT)
    lockdc_import_cache_value(LOCKDC_EXTERNAL_ROOT)
    file(REMOVE_RECURSE "${fixture}")
    file(MAKE_DIRECTORY "${fixture}/lonejson")
    file(ARCHIVE_EXTRACT INPUT "${rock}" DESTINATION "${fixture}")
    file(ARCHIVE_EXTRACT INPUT "${fixture}/lonejson-lua-0.50.0.tar.gz" DESTINATION "${fixture}")

    set(lonejson_sdk "${LOCKDC_EXTERNAL_ROOT}/lonejson/install")
    set(lua_headers "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/include")
    set(link_args -shared)
    if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
        set(link_args -bundle -Wl,-undefined,dynamic_lookup)
    endif()
    if(CMAKE_SYSROOT)
        list(APPEND link_args "--sysroot=${CMAKE_SYSROOT}")
    endif()
    execute_process(COMMAND "${CMAKE_C_COMPILER}" ${link_args} -fPIC
        "-I${lonejson_sdk}/include" "-I${lua_headers}"
        "${lonejson_source}/src/lua/lonejson_lua.c"
        "-L${lonejson_sdk}/lib" "-Wl,-rpath,${lonejson_sdk}/lib" -llonejson
        -o "${fixture}/lonejson/core.so"
        COMMAND_ERROR_IS_FATAL ANY)
endif()

string(REPLACE "|" ";" scenario_env "${LOCKDC_LUA_TEST_ENV}")
set(lua_args "${LOCKDC_LUA_TEST_SCRIPT}")
if("$ENV{LOCKDC_TEST_ALLOCATOR_TRACKING}" STREQUAL "1")
    set(lua_args -e [=[
assert(require("lockdc").allocator_tracking_configure(true))
dofile(os.getenv("LOCKDC_TRACKED_LUA_SCRIPT"))
collectgarbage("collect")
]=])
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env ${scenario_env}
    "LUA_PATH=${LOCKDC_ROOT}/lua/?.lua;${LOCKDC_ROOT}/lua/?/init.lua;${lonejson_source}/lua/?/init.lua"
    "LUA_CPATH=${LOCKDC_BINARY_DIR}/lua/?.so;${fixture}/?.so"
    "LOCKDC_LUA_SMOKE_ROOT=${fixture}/collection-writer.pouch"
    "LOCKDC_TRACKED_LUA_SCRIPT=${LOCKDC_LUA_TEST_SCRIPT}"
    "ASAN_OPTIONS=detect_leaks=0"
    "${LOCKDC_LUA_BIN}" ${lua_args}
    RESULT_VARIABLE result OUTPUT_VARIABLE stdout ERROR_VARIABLE stderr)
if(NOT result EQUAL 0 OR stdout MATCHES "liblockdc allocator:" OR stderr MATCHES "liblockdc allocator:")
    message(FATAL_ERROR "Lua build-tree run failed for ${LOCKDC_TEST_NAME}\n${stdout}\n${stderr}")
endif()
