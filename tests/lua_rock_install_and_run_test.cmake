if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

# CTest prepares a fresh shared rock once per E2E invocation. Each scenario
# then runs in its own Lua process, with its own queue/key/Pouch fixture.
if(DEFINED LOCKDC_LUA_FIXTURE_ROOT AND NOT LOCKDC_LUA_FIXTURE_ROOT STREQUAL ""
   AND NOT LOCKDC_LUA_PREPARE_FIXTURE)
    find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks REQUIRED)
    string(REPLACE "|" ";" scenario_env "${LOCKDC_LUA_TEST_ENV}")
    execute_process(COMMAND "${CMAKE_COMMAND}" -E env ${scenario_env}
        "LOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}"
        "LOCKDC_LUAROCKS_BIN=${LOCKDC_LUAROCKS_BIN}"
        "ASAN_OPTIONS=detect_leaks=0"
        "LOCKDC_LUAROCKS_WORKDIR=${LOCKDC_ROOT}"
        bash "${LOCKDC_ROOT}/scripts/validate_luarocks.sh" --run
        "${LOCKDC_LUA_FIXTURE_ROOT}/tree" "${LOCKDC_LUA_FIXTURE_ROOT}/prefix"
        unused "${LOCKDC_LUA_TEST_SCRIPT}"
        RESULT_VARIABLE run_result OUTPUT_VARIABLE run_stdout ERROR_VARIABLE run_stderr)
    if(NOT run_result EQUAL 0 OR run_stdout MATCHES "liblockdc allocator:" OR
       run_stderr MATCHES "liblockdc allocator:")
        message(FATAL_ERROR "LuaRocks validation failed for ${LOCKDC_TEST_NAME}\n${run_stdout}\n${run_stderr}")
    endif()
    return()
endif()

function(lockdc_import_cache_value var_name)
    if(DEFINED ${var_name} AND NOT "${${var_name}}" STREQUAL "")
        return()
    endif()

    file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" cache_line
        REGEX "^${var_name}(:[^=]+)?="
        LIMIT_COUNT 1)
    if(cache_line)
        string(FIND "${cache_line}" "=" cache_value_offset)
        if(cache_value_offset LESS 0)
            message(FATAL_ERROR "malformed cache value for ${var_name}: ${cache_line}")
        endif()
        math(EXPR cache_value_offset "${cache_value_offset} + 1")
        string(SUBSTRING "${cache_line}" ${cache_value_offset} -1 cache_value)
        set(${var_name} "${cache_value}" PARENT_SCOPE)
    endif()
endfunction()

lockdc_import_cache_value(CMAKE_C_COMPILER)
lockdc_import_cache_value(CMAKE_C_FLAGS)
lockdc_import_cache_value(CMAKE_C_FLAGS_DEBUG)
lockdc_import_cache_value(CMAKE_BUILD_TYPE)
lockdc_import_cache_value(CMAKE_SYSROOT)
lockdc_import_cache_value(LOCKDC_EXTERNAL_ROOT)
include("${LOCKDC_ROOT}/tests/bootlin_runtime_test_support.cmake")

if(NOT DEFINED LOCKDC_TEST_NAME OR LOCKDC_TEST_NAME STREQUAL "")
    message(FATAL_ERROR "LOCKDC_TEST_NAME is required")
endif()

if(NOT DEFINED LOCKDC_LUA_TEST_SCRIPT OR LOCKDC_LUA_TEST_SCRIPT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_LUA_TEST_SCRIPT is required")
endif()

if(NOT DEFINED CMAKE_C_COMPILER OR CMAKE_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "CMAKE_C_COMPILER is required for LuaRocks validation")
endif()

if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()

set(LOCKDC_RUN_LUAROCKS_VALIDATION ON)
if(NOT DEFINED LOCKDC_RUN_LUA_SMOKE)
    set(LOCKDC_RUN_LUA_SMOKE ON)
endif()

if(DEFINED LOCKDC_TARGET_ID AND NOT LOCKDC_TARGET_ID STREQUAL "")
    execute_process(
        COMMAND uname -m
        OUTPUT_VARIABLE lockdc_host_arch_raw
        RESULT_VARIABLE lockdc_host_arch_result
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(lockdc_host_arch_result EQUAL 0)
        string(TOLOWER "${lockdc_host_arch_raw}" lockdc_host_arch)
        if(lockdc_host_arch STREQUAL "amd64")
            set(lockdc_host_arch "x86_64")
        elseif(lockdc_host_arch STREQUAL "arm64")
            set(lockdc_host_arch "aarch64")
        elseif(lockdc_host_arch MATCHES "^armv[0-9]+l$")
            set(lockdc_host_arch "armhf")
        endif()
    endif()

    string(REGEX MATCH "^[^-]+" lockdc_target_arch "${LOCKDC_TARGET_ID}")
    if(DEFINED lockdc_host_arch AND NOT lockdc_host_arch STREQUAL "" AND NOT lockdc_target_arch STREQUAL lockdc_host_arch)
        set(LOCKDC_RUN_LUA_SMOKE OFF)
        set(LOCKDC_RUN_LUAROCKS_VALIDATION OFF)
    endif()
endif()

find_program(LOCKDC_BASH_BIN NAMES bash)
find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks)

if(NOT LOCKDC_RUN_LUAROCKS_VALIDATION)
    message(STATUS "Skipping LuaRocks validation for ${LOCKDC_TEST_NAME}: target ${LOCKDC_TARGET_ID} is not runnable on this host")
    return()
endif()

if(NOT LOCKDC_BASH_BIN)
    message(FATAL_ERROR "bash is required for LuaRocks validation")
endif()
if(NOT DEFINED LOCKDC_LUA_BIN OR LOCKDC_LUA_BIN STREQUAL "")
    message(FATAL_ERROR "a project-built Bootlin Lua runner is required for LuaRocks validation")
endif()
if(NOT LOCKDC_LUAROCKS_BIN)
    message(FATAL_ERROR "luarocks is required for LuaRocks validation")
endif()
if(NOT CMAKE_C_COMPILER)
    message(FATAL_ERROR "CMAKE_C_COMPILER is required for LuaRocks validation")
endif()

if(NOT DEFINED LOCKDC_SDK_PREFIX OR LOCKDC_SDK_PREFIX STREQUAL "")
    if(DEFINED LOCKDC_LUA_FIXTURE_ROOT AND NOT LOCKDC_LUA_FIXTURE_ROOT STREQUAL "")
        set(LOCKDC_SDK_PREFIX "${LOCKDC_LUA_FIXTURE_ROOT}/prefix")
    else()
        set(LOCKDC_SDK_PREFIX "${LOCKDC_BINARY_DIR}/lua-rock-tests/${LOCKDC_TEST_NAME}/prefix")
    endif()
    file(REMOVE_RECURSE "${LOCKDC_SDK_PREFIX}")
    # The base rock consumes only the core SDK. Optional provider components
    # may be configured without being built by the focused E2E targets.
    foreach(lockdc_sdk_component runtime dev)
        execute_process(
            COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}"
                --prefix "${LOCKDC_SDK_PREFIX}" --component "${lockdc_sdk_component}"
            RESULT_VARIABLE install_result
            OUTPUT_VARIABLE install_stdout
            ERROR_VARIABLE install_stderr
        )
        if(NOT install_result EQUAL 0)
            message(FATAL_ERROR
                "failed to stage ${lockdc_sdk_component} SDK component for LuaRocks test ${LOCKDC_TEST_NAME}\n"
                "stdout:\n${install_stdout}\n"
                "stderr:\n${install_stderr}")
        endif()
    endforeach()
endif()

if(DEFINED LOCKDC_ROCKSPEC_PATH AND NOT LOCKDC_ROCKSPEC_PATH STREQUAL "")
    set(LOCKDC_LUA_PACKAGE_PATH "${LOCKDC_ROCKSPEC_PATH}")
elseif(DEFINED LOCKDC_ROCK_PATH AND NOT LOCKDC_ROCK_PATH STREQUAL "")
    set(LOCKDC_LUA_PACKAGE_PATH "${LOCKDC_ROCK_PATH}")
else()
    file(GLOB generated_rockspec "${LOCKDC_BINARY_DIR}/lockdc-*-1.rockspec")
    list(LENGTH generated_rockspec generated_rockspec_count)
    if(NOT generated_rockspec_count EQUAL 1)
        message(FATAL_ERROR "expected one generated lockdc rockspec in ${LOCKDC_BINARY_DIR}")
    endif()
    list(GET generated_rockspec 0 LOCKDC_LUA_PACKAGE_PATH)
endif()

set(lonejson_cache_dir "${LOCKDC_BINARY_DIR}/lua-rock-cache")
set(lonejson_src_rock "${lonejson_cache_dir}/lonejson-0.49.0-1.src.rock")
set(lonejson_src_rock_url "https://github.com/c89systems/lonejson/releases/download/v0.49.0/lonejson-0.49.0-1.src.rock")
set(lonejson_src_rock_sha256 "e090a4db9d4ae21824c6975eee33e1d7e83573401feda35e1287f0bc7ff540dd")
set(lua_tree_dir "${LOCKDC_BINARY_DIR}/lua-rock-tests/${LOCKDC_TEST_NAME}/tree")
if(DEFINED LOCKDC_LUA_FIXTURE_ROOT AND NOT LOCKDC_LUA_FIXTURE_ROOT STREQUAL "")
    set(lua_tree_dir "${LOCKDC_LUA_FIXTURE_ROOT}/tree")
endif()
set(lua_rock_workdir "${LOCKDC_ROOT}")

file(MAKE_DIRECTORY "${lonejson_cache_dir}")
if(EXISTS "${lonejson_src_rock}")
    file(SHA256 "${lonejson_src_rock}" existing_lonejson_src_rock_sha256)
    if(NOT existing_lonejson_src_rock_sha256 STREQUAL lonejson_src_rock_sha256)
        file(REMOVE "${lonejson_src_rock}")
    endif()
endif()
if(NOT EXISTS "${lonejson_src_rock}")
    file(DOWNLOAD
        "${lonejson_src_rock_url}"
        "${lonejson_src_rock}"
        EXPECTED_HASH "SHA256=${lonejson_src_rock_sha256}"
        STATUS download_status
        SHOW_PROGRESS
    )
    list(GET download_status 0 download_code)
    list(GET download_status 1 download_message)
    if(NOT download_code EQUAL 0)
        file(REMOVE "${lonejson_src_rock}")
        message(FATAL_ERROR
            "failed to download lonejson Lua rock from ${lonejson_src_rock_url}: ${download_message}")
    endif()
endif()

set(lockdc_luarocks_cflags "${CMAKE_C_FLAGS} -fPIC")
if(DEFINED CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE STREQUAL "")
    string(TOUPPER "${CMAKE_BUILD_TYPE}" lockdc_build_type_upper)
    if(lockdc_build_type_upper STREQUAL "DEBUG")
        string(APPEND lockdc_luarocks_cflags " ${CMAKE_C_FLAGS_DEBUG}")
    endif()
endif()

set(lockdc_cpkt_lua_incdir "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/include")
if(NOT EXISTS "${lockdc_cpkt_lua_incdir}/lua.h")
    message(FATAL_ERROR "cpkt Lua headers are required: ${lockdc_cpkt_lua_incdir}/lua.h")
endif()
lockdc_resolve_bootlin_runtime(
    "${CMAKE_SYSROOT}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${LOCKDC_SDK_PREFIX}/lib"
    lockdc_runtime_loader
    lockdc_runtime_dirs)
set(lockdc_lua_external_link_flags "-Wl,--disable-new-dtags")
set(lockdc_lua_pkg_config_path
    "${LOCKDC_SDK_PREFIX}/lib/pkgconfig:${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/pkgconfig")
foreach(lockdc_runtime_dir IN LISTS lockdc_runtime_dirs)
    if(NOT lockdc_runtime_dir STREQUAL "${LOCKDC_SDK_PREFIX}/lib")
        string(APPEND lockdc_lua_external_link_flags " -Wl,-rpath,${lockdc_runtime_dir}")
    endif()
endforeach()
string(FIND "${lockdc_lua_external_link_flags}" "-Wl,-rpath,${LOCKDC_SDK_PREFIX}/lib"
    lockdc_sdk_rpath_injected)
if(NOT lockdc_sdk_rpath_injected EQUAL -1)
    message(FATAL_ERROR
        "Lua rock validation must not inject the selected SDK library directory; "
        "the prefix-selected module must provide that RPATH itself")
endif()

set(lua_build_root "${lua_rock_workdir}/.luarocks-build")

set(test_env
    "LOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}"
    "LOCKDC_LUAROCKS_BIN=${LOCKDC_LUAROCKS_BIN}"
    "LOCKDC_LUA_VERSION=5.5"
    "LOCKDC_LONEJSON_SRC_ROCK=${lonejson_src_rock}"
    "LOCKDC_LUAROCKS_CC=${CMAKE_C_COMPILER}"
    "LOCKDC_LUAROCKS_CFLAGS=${lockdc_luarocks_cflags}"
    "LOCKDC_LUAROCKS_LUA_INCDIR=${lockdc_cpkt_lua_incdir}"
)
list(APPEND test_env "ASAN_OPTIONS=detect_leaks=0")

if(DEFINED LOCKDC_EXTERNAL_ROOT AND NOT LOCKDC_EXTERNAL_ROOT STREQUAL "")
    set(lockdc_lua_external_include_flags "")
    set(lockdc_lua_cpkt_include_flags "")
    if(EXISTS "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/include/lua.h")
        set(lockdc_lua_cpkt_include_flags
            "-I${LOCKDC_EXTERNAL_ROOT}/cpkt/install/include")
    endif()
    foreach(lockdc_lua_external_dep curl openssl nghttp2 pslog lonejson liblql libssh2 zlib)
        set(lockdc_lua_external_prefix "${LOCKDC_EXTERNAL_ROOT}/${lockdc_lua_external_dep}/install")
        if(EXISTS "${lockdc_lua_external_prefix}/include")
            string(APPEND lockdc_lua_external_include_flags " -I${lockdc_lua_external_prefix}/include")
        endif()
        if(EXISTS "${lockdc_lua_external_prefix}/lib")
            string(APPEND lockdc_lua_external_link_flags " -L${lockdc_lua_external_prefix}/lib -Wl,-rpath,${lockdc_lua_external_prefix}/lib")
        endif()
        if(EXISTS "${lockdc_lua_external_prefix}/lib/pkgconfig")
            string(APPEND lockdc_lua_pkg_config_path ":${lockdc_lua_external_prefix}/lib/pkgconfig")
        endif()
    endforeach()

    string(STRIP "${lockdc_lua_external_include_flags}" lockdc_lua_external_include_flags)
    string(STRIP "${lockdc_lua_external_link_flags}" lockdc_lua_external_link_flags)
    if(NOT lockdc_lua_external_include_flags STREQUAL "")
        if(NOT lockdc_lua_cpkt_include_flags STREQUAL "")
            string(APPEND lockdc_lua_external_include_flags " ${lockdc_lua_cpkt_include_flags}")
        endif()
        list(APPEND test_env "LOCKDC_CFLAGS_EXTRA=${lockdc_lua_external_include_flags}")
    elseif(NOT lockdc_lua_cpkt_include_flags STREQUAL "")
        list(APPEND test_env "LOCKDC_CFLAGS_EXTRA=${lockdc_lua_cpkt_include_flags}")
    endif()
    if(NOT lockdc_lua_external_link_flags STREQUAL "")
        list(APPEND test_env "LOCKDC_LIBS_EXTRA=${lockdc_lua_external_link_flags}")
    endif()
    if(EXISTS "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib")
        list(APPEND test_env
            "LONEJSON_LIBDIR=${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib")
    endif()
endif()
list(APPEND test_env "PKG_CONFIG_PATH=${lockdc_lua_pkg_config_path}")

if(DEFINED LOCKDC_LUA_TEST_ENV AND NOT LOCKDC_LUA_TEST_ENV STREQUAL "")
    string(REPLACE "|" ";" LOCKDC_LUA_TEST_ENV_LIST "${LOCKDC_LUA_TEST_ENV}")
    foreach(env_pair IN LISTS LOCKDC_LUA_TEST_ENV_LIST)
        list(APPEND test_env "${env_pair}")
    endforeach()
endif()

if(LOCKDC_RUN_LUA_SMOKE)
    set(lockdc_run_lua_smoke_env "ON")
else()
    set(lockdc_run_lua_smoke_env "OFF")
endif()

list(APPEND test_env "LOCKDC_LUAROCKS_WORKDIR=${lua_rock_workdir}")
list(APPEND test_env "LOCKDC_LUA_SMOKE_ROOT=${lua_tree_dir}/collection-writer.pouch")
if(LOCKDC_LUA_PACKAGE_PATH MATCHES "[.]rockspec$")
    list(APPEND test_env "LOCKDC_LUAROCKS_BUILD_ROOT=${lua_build_root}")
endif()

set(validation_mode)
if(LOCKDC_LUA_PREPARE_FIXTURE)
    set(validation_mode --prepare)
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        ${test_env}
        "LOCKDC_RUN_LUA_SMOKE=${lockdc_run_lua_smoke_env}"
        "${LOCKDC_BASH_BIN}" "${LOCKDC_ROOT}/scripts/validate_luarocks.sh"
        ${validation_mode}
        "${lua_tree_dir}"
        "${LOCKDC_SDK_PREFIX}"
        "${LOCKDC_LUA_PACKAGE_PATH}"
        "${LOCKDC_LUA_TEST_SCRIPT}"
    RESULT_VARIABLE run_result
    OUTPUT_VARIABLE run_stdout
    ERROR_VARIABLE run_stderr
)
if(NOT run_result EQUAL 0 OR
   run_stdout MATCHES "liblockdc allocator:" OR
   run_stderr MATCHES "liblockdc allocator:")
    message(FATAL_ERROR
        "LuaRocks validation failed for ${LOCKDC_TEST_NAME}\n"
        "stdout:\n${run_stdout}\n"
        "stderr:\n${run_stderr}")
endif()
