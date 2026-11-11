if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(lockdc_test_root "${LOCKDC_BINARY_DIR}/lua-release-package-test-artifacts")
if(DEFINED LOCKDC_USE_EXISTING_ARCHIVE AND LOCKDC_USE_EXISTING_ARCHIVE)
    if(NOT DEFINED LOCKDC_DIST_DIR OR LOCKDC_DIST_DIR STREQUAL "")
        message(FATAL_ERROR
            "LOCKDC_DIST_DIR is required when LOCKDC_USE_EXISTING_ARCHIVE is enabled")
    endif()
    set(lockdc_dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(lockdc_dist_dir "${lockdc_test_root}/dist")
endif()

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    message(FATAL_ERROR "missing package metadata: ${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")

function(assert_tar_numeric_owner_group archive_path)
    find_program(LOCKDC_TAR_BIN NAMES tar)
    if(NOT LOCKDC_TAR_BIN)
        message(FATAL_ERROR "tar is required for archive ownership validation")
    endif()

    execute_process(
        COMMAND "${LOCKDC_TAR_BIN}" --numeric-owner -tvzf "${archive_path}"
        RESULT_VARIABLE tar_result
        OUTPUT_VARIABLE archive_metadata
        ERROR_VARIABLE archive_metadata_error
    )
    if(NOT tar_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect archive ownership metadata: ${archive_path}\n${archive_metadata}${archive_metadata_error}")
    endif()

    string(REGEX REPLACE "\n$" "" archive_metadata_trimmed "${archive_metadata}")
    string(REPLACE "\n" ";" archive_metadata_lines "${archive_metadata_trimmed}")
    foreach(archive_metadata_line IN LISTS archive_metadata_lines)
        if(archive_metadata_line STREQUAL "")
            continue()
        endif()
        if(NOT archive_metadata_line MATCHES "^[^ ]+[ ]+0/0([ ]+|$)")
            message(FATAL_ERROR
                "archive entry does not use numeric owner/group 0/0: ${archive_path}\n${archive_metadata_line}")
        endif()
    endforeach()
endfunction()

set(lockdc_lua_rockspec_path "${lockdc_dist_dir}/lockdc-${LOCKDC_VERSION}-1.rockspec")
set(lockdc_lua_src_rock_path "${lockdc_dist_dir}/lockdc-${LOCKDC_VERSION}-1.src.rock")
set(lockdc_lua_source_archive_path "${lockdc_dist_dir}/liblockdc-lua-${LOCKDC_VERSION}.tar.gz")
set(lockdc_release_archive "${lockdc_dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")
set(lockdc_extract_root "${LOCKDC_BINARY_DIR}/lua-release-package-test")
set(lockdc_release_prefix "${lockdc_extract_root}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
set(lockdc_lua_rock_extract_root "${LOCKDC_BINARY_DIR}/lua-release-package-src-rock")
set(lockdc_lua_inner_archive_path "${lockdc_lua_rock_extract_root}/liblockdc-lua-${LOCKDC_VERSION}.tar.gz")
set(lockdc_lua_inner_rockspec_path "${lockdc_lua_rock_extract_root}/lockdc-${LOCKDC_VERSION}-1.rockspec")

file(REMOVE_RECURSE "${lockdc_test_root}")
if(NOT (DEFINED LOCKDC_USE_EXISTING_ARCHIVE AND LOCKDC_USE_EXISTING_ARCHIVE))
    file(MAKE_DIRECTORY "${lockdc_dist_dir}")

    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${lockdc_dist_dir}
            -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
        RESULT_VARIABLE package_archive_result
        OUTPUT_VARIABLE package_archive_stdout
        ERROR_VARIABLE package_archive_stderr
    )
    if(NOT package_archive_result EQUAL 0)
        message(FATAL_ERROR
            "failed to create C release archive for Lua package test\n"
            "stdout:\n${package_archive_stdout}\n"
            "stderr:\n${package_archive_stderr}")
    endif()

    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${lockdc_dist_dir}
            -P "${LOCKDC_ROOT}/cmake/package_lua_rock.cmake"
        RESULT_VARIABLE package_lua_result
        OUTPUT_VARIABLE package_lua_stdout
        ERROR_VARIABLE package_lua_stderr
    )
    if(NOT package_lua_result EQUAL 0)
        message(FATAL_ERROR
            "failed to create standalone Lua release package\n"
            "stdout:\n${package_lua_stdout}\n"
            "stderr:\n${package_lua_stderr}")
    endif()
endif()

foreach(required_path
    "${lockdc_lua_rockspec_path}"
    "${lockdc_lua_src_rock_path}"
    "${lockdc_lua_source_archive_path}"
    "${lockdc_release_archive}"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "missing Lua release artifact: ${required_path}")
    endif()
endforeach()

file(REMOVE_RECURSE "${lockdc_lua_rock_extract_root}")
file(MAKE_DIRECTORY "${lockdc_lua_rock_extract_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_lua_src_rock_path}"
    WORKING_DIRECTORY "${lockdc_lua_rock_extract_root}"
    RESULT_VARIABLE rock_extract_result
    OUTPUT_VARIABLE rock_extract_stdout
    ERROR_VARIABLE rock_extract_stderr
)
if(NOT rock_extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract Lua source rock\n"
        "stdout:\n${rock_extract_stdout}\n"
        "stderr:\n${rock_extract_stderr}")
endif()

if(NOT EXISTS "${lockdc_lua_inner_archive_path}")
    message(FATAL_ERROR "Lua source rock is missing embedded source archive: ${lockdc_lua_inner_archive_path}")
endif()
if(NOT EXISTS "${lockdc_lua_inner_rockspec_path}")
    message(FATAL_ERROR "Lua source rock is missing embedded rockspec: ${lockdc_lua_inner_rockspec_path}")
endif()

assert_tar_numeric_owner_group("${lockdc_lua_inner_archive_path}")
assert_tar_numeric_owner_group("${lockdc_lua_source_archive_path}")
file(SHA256 "${lockdc_lua_inner_archive_path}" lockdc_lua_inner_archive_sha256)
file(SHA256 "${lockdc_lua_source_archive_path}" lockdc_lua_source_archive_sha256)
if(NOT lockdc_lua_inner_archive_sha256 STREQUAL lockdc_lua_source_archive_sha256)
    message(FATAL_ERROR "Lua source rock does not embed the standalone Lua source package verbatim")
endif()

set(lockdc_lua_source_unpack_root "${lockdc_lua_rock_extract_root}/source")
file(MAKE_DIRECTORY "${lockdc_lua_source_unpack_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_lua_inner_archive_path}"
    WORKING_DIRECTORY "${lockdc_lua_source_unpack_root}"
    RESULT_VARIABLE source_extract_result
    OUTPUT_VARIABLE source_extract_stdout
    ERROR_VARIABLE source_extract_stderr
)
if(NOT source_extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract Lua source archive\n"
        "stdout:\n${source_extract_stdout}\n"
        "stderr:\n${source_extract_stderr}")
endif()

set(lockdc_lua_source_extract_root "${lockdc_lua_source_unpack_root}/liblockdc-lua-${LOCKDC_VERSION}")
foreach(required_source
    "LICENSE"
    "README.md"
    "CHANGELOG.md"
    "RELEASE_MANIFEST"
    "VERSION"
    "docs/c-api.md"
    "docs/development.md"
    "docs/releases.md"
    "docs/lua.md"
    "docs/pouch-terminal-retention.md"
    "docs/pouch-dead-letter-retention.md"
    "docs/outbox-dispatch-architecture.md"
    "docs/outbox-vectis-handover.md"
    "docs/inbox-outbox.md"
    "docs/xa.md"
    "docs/pouch-open-settings-api.md"
    "docs/pouch-compression.md"
    "docs/pouch-storage.md"
    "docs/collections.md"
    "docs/collection-writer.md"
    "docs/collection-leases.md"
    "docs/collection-edits.md"
    "docs/collection-read-comparison.md"
    "docs/logging.md"
    "docs/state-retention.md"
    "docs/worst-case-scenario.md"
    "examples/lua/README.md"
    "examples/client_and_lease_methods.c"
    "examples/acquire_for_update.c"
    "examples/state_stream_roundtrip.c"
    "examples/lua/outbox_dispatcher.lua"
    "examples/outbox_producer.c"
    "examples/collection_journal.c"
    "include/lc/lc.h"
    "include/lc/postgres.h"
    "lockdc.rockspec.in"
    "lockdc-postgres.rockspec.in"
    "src/lua/lockdc_lua.c"
    "src/lua/lockdc_collections_lua.c"
    "src/lua/lockdc_xa_lua.c"
    "src/lua/lockdc_xa_resource_lua.c"
    "src/lua/lockdc_xa_provider_lua.c"
    "src/lua/lockdc_lua.h"
    "src/lua/lockdc_lua_internal.h"
    "src/lua/lockdc_postgres_lua.c"
    "src/lua/lockdc_postgres_lua.h"
    "scripts/assert_generated_path.sh"
    "scripts/build_lua_rock.sh"
    "scripts/build_postgres_lua_rock.sh"
)
    if(NOT EXISTS "${lockdc_lua_source_extract_root}/${required_source}")
        message(FATAL_ERROR "Lua source rock is missing required source: ${required_source}")
    endif()
    if(required_source MATCHES "^docs/" OR required_source STREQUAL "README.md"
       OR required_source STREQUAL "CHANGELOG.md")
        file(SHA256 "${LOCKDC_ROOT}/${required_source}" current_doc_digest)
        file(SHA256 "${lockdc_lua_source_extract_root}/${required_source}" packaged_doc_digest)
        if(NOT current_doc_digest STREQUAL packaged_doc_digest)
            message(FATAL_ERROR "Lua source package contains stale documentation: ${required_source}")
        endif()
    endif()
endforeach()

foreach(excluded_guide
    docs/pouch-replay-investigation.md
    docs/lockd-implicit-xa-handover.md
    docs/lockd-memory-queue-watch-race.md
    docs/lockd-durable-crypto-index-freshness.md)
    if(EXISTS "${lockdc_lua_source_extract_root}/${excluded_guide}")
        message(FATAL_ERROR "Lua source package contains an investigation note: ${excluded_guide}")
    endif()
endforeach()

file(READ "${lockdc_lua_inner_rockspec_path}" lockdc_lua_inner_rockspec_text)
foreach(disallowed_path "${LOCKDC_ROOT}" "$ENV{HOME}")
    if(disallowed_path STREQUAL "")
        continue()
    endif()
    string(FIND "${lockdc_lua_inner_rockspec_text}" "${disallowed_path}" disallowed_index)
    if(NOT disallowed_index EQUAL -1)
        message(FATAL_ERROR
            "Lua source rock embedded rockspec contains local path '${disallowed_path}'\n"
            "rockspec:\n${lockdc_lua_inner_rockspec_text}")
    endif()
endforeach()
string(FIND "${lockdc_lua_inner_rockspec_text}" "url = \"liblockdc-lua-${LOCKDC_VERSION}.tar.gz\"" inner_source_index)
if(inner_source_index EQUAL -1)
    message(FATAL_ERROR
        "Lua source rock embedded rockspec should reference the embedded source archive by relative name\n"
        "rockspec:\n${lockdc_lua_inner_rockspec_text}")
endif()
string(FIND "${lockdc_lua_inner_rockspec_text}" "dir = \"liblockdc-lua-${LOCKDC_VERSION}\"" inner_source_dir_index)
if(inner_source_dir_index EQUAL -1)
    message(FATAL_ERROR
        "Lua source rock embedded rockspec should name the staged source root\n"
        "rockspec:\n${lockdc_lua_inner_rockspec_text}")
endif()

set(lockdc_lua_standalone_unpack_root "${lockdc_extract_root}/lua-source")
file(REMOVE_RECURSE "${lockdc_lua_standalone_unpack_root}")
file(MAKE_DIRECTORY "${lockdc_lua_standalone_unpack_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_lua_source_archive_path}"
    WORKING_DIRECTORY "${lockdc_lua_standalone_unpack_root}"
    RESULT_VARIABLE standalone_source_extract_result
    OUTPUT_VARIABLE standalone_source_extract_stdout
    ERROR_VARIABLE standalone_source_extract_stderr
)
if(NOT standalone_source_extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract standalone Lua source archive\n"
        "stdout:\n${standalone_source_extract_stdout}\n"
        "stderr:\n${standalone_source_extract_stderr}")
endif()
if(NOT EXISTS "${lockdc_lua_standalone_unpack_root}/liblockdc-lua-${LOCKDC_VERSION}/RELEASE_MANIFEST")
    message(FATAL_ERROR "standalone Lua source archive is missing its staged release manifest")
endif()
file(STRINGS "${lockdc_lua_source_extract_root}/RELEASE_MANIFEST" lockdc_lua_expected_manifest)
file(GLOB_RECURSE lockdc_lua_actual_manifest
    RELATIVE "${lockdc_lua_source_extract_root}"
    LIST_DIRECTORIES false
    "${lockdc_lua_source_extract_root}/*")
list(SORT lockdc_lua_expected_manifest)
list(SORT lockdc_lua_actual_manifest)
if(NOT lockdc_lua_expected_manifest STREQUAL lockdc_lua_actual_manifest)
    list(JOIN lockdc_lua_expected_manifest "\n  " lockdc_lua_expected_manifest_text)
    list(JOIN lockdc_lua_actual_manifest "\n  " lockdc_lua_actual_manifest_text)
    message(FATAL_ERROR
        "standalone Lua source archive does not match its release manifest\n"
        "expected:\n  ${lockdc_lua_expected_manifest_text}\n"
        "actual:\n  ${lockdc_lua_actual_manifest_text}")
endif()
string(FIND "${lockdc_lua_inner_rockspec_text}" "\"lonejson == 0.49.0-1\"" inner_lonejson_index)
if(inner_lonejson_index EQUAL -1)
    message(FATAL_ERROR
        "Lua source rock embedded rockspec is missing the pinned lonejson dependency\n"
        "rockspec:\n${lockdc_lua_inner_rockspec_text}")
endif()

file(READ "${lockdc_lua_rockspec_path}" lockdc_lua_rockspec_text)
foreach(required_snippet
    "package = \"lockdc\""
    "version = \"${LOCKDC_VERSION}-1\""
    "url = \"git+https://github.com/c89systems/liblockdc.git\""
    "tag = \"v${LOCKDC_VERSION}\""
    "\"lonejson == 0.49.0-1\""
    "scripts/build_lua_rock.sh"
)
    string(FIND "${lockdc_lua_rockspec_text}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "standalone Lua rockspec is missing expected snippet '${required_snippet}'\n"
            "rockspec:\n${lockdc_lua_rockspec_text}")
    endif()
endforeach()

foreach(stale_snippet
    "lonejson == 0.41.0-1"
    "https://github.com/c89systems/lonejson/releases/download/v0.41.0/lonejson-0.41.0-1.src.rock"
)
    string(FIND "${lockdc_lua_rockspec_text}" "${stale_snippet}" standalone_stale_index)
    string(FIND "${lockdc_lua_inner_rockspec_text}" "${stale_snippet}" inner_stale_index)
    if(NOT standalone_stale_index EQUAL -1 OR NOT inner_stale_index EQUAL -1)
        message(FATAL_ERROR
            "Lua release rockspec dependency boundary contains stale snippet '${stale_snippet}'")
    endif()
endforeach()

file(REMOVE_RECURSE "${lockdc_extract_root}")
file(MAKE_DIRECTORY "${lockdc_extract_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_release_archive}"
    WORKING_DIRECTORY "${lockdc_extract_root}"
    RESULT_VARIABLE extract_result
    OUTPUT_VARIABLE extract_stdout
    ERROR_VARIABLE extract_stderr
)
if(NOT extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract C release archive for Lua package test\n"
        "stdout:\n${extract_stdout}\n"
        "stderr:\n${extract_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_TEST_NAME=lua-release-package-smoke
        -DLOCKDC_SDK_PREFIX=${lockdc_release_prefix}
        -DLOCKDC_ROCK_PATH=${lockdc_lua_src_rock_path}
        -DLOCKDC_LUA_TEST_SCRIPT=${LOCKDC_ROOT}/tests/lua/test_lockdc_luarocks_smoke.lua
        -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
        -P "${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake"
    RESULT_VARIABLE lua_result
    OUTPUT_VARIABLE lua_stdout
    ERROR_VARIABLE lua_stderr
)
if(NOT lua_result EQUAL 0)
    message(FATAL_ERROR
        "standalone Lua release package validation failed\n"
        "stdout:\n${lua_stdout}\n"
        "stderr:\n${lua_stderr}")
endif()
