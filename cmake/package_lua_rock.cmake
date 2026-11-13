if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_input_dist_dir "${LOCKDC_DIST_DIR}")
endif()
if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
if(DEFINED lockdc_input_dist_dir)
    set(LOCKDC_DIST_DIR "${lockdc_input_dist_dir}")
elseif(NOT DEFINED LOCKDC_DIST_DIR OR "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(LOCKDC_DIST_DIR "${LOCKDC_ROOT}/dist")
endif()
if(NOT DEFINED LOCKDC_ROOT OR "${LOCKDC_ROOT}" STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR "${LOCKDC_BINARY_DIR}" STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()
include("${CMAKE_CURRENT_LIST_DIR}/LcGeneratedPath.cmake")

find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks)
find_program(LOCKDC_TAR_BIN NAMES tar)
find_program(LOCKDC_GZIP_BIN NAMES gzip)
if(NOT LOCKDC_LUAROCKS_BIN)
    message(FATAL_ERROR "failed to find luarocks for Lua source rock packaging")
endif()
if(NOT LOCKDC_TAR_BIN)
    message(FATAL_ERROR "failed to find tar for Lua source rock packaging")
endif()
if(NOT LOCKDC_GZIP_BIN)
    message(FATAL_ERROR "failed to find gzip for Lua source rock packaging")
endif()

file(GLOB lockdc_generated_rockspec "${LOCKDC_BINARY_DIR}/lockdc-*-1.rockspec")
list(LENGTH lockdc_generated_rockspec lockdc_generated_rockspec_count)
if(NOT lockdc_generated_rockspec_count EQUAL 1)
    message(FATAL_ERROR "expected one generated lockdc rockspec in ${LOCKDC_BINARY_DIR}")
endif()
list(GET lockdc_generated_rockspec 0 lockdc_generated_rockspec_path)
get_filename_component(lockdc_generated_rockspec_name "${lockdc_generated_rockspec_path}" NAME)

set(lockdc_lua_stage_root "${LOCKDC_BINARY_DIR}/lua-package")
set(lockdc_lua_stage_workdir "${lockdc_lua_stage_root}/work")
set(lockdc_lua_source_root_name "liblockdc-lua-${LOCKDC_VERSION}")
set(lockdc_lua_source_root "${lockdc_lua_stage_root}/${lockdc_lua_source_root_name}")
set(lockdc_lua_rockspec_name "lockdc-${LOCKDC_VERSION}-1.rockspec")
set(lockdc_lua_src_rock_name "lockdc-${LOCKDC_VERSION}-1.src.rock")
set(lockdc_lua_source_archive_base "${lockdc_lua_stage_root}/${lockdc_lua_source_root_name}.tar")
set(lockdc_lua_source_archive_path "${lockdc_lua_source_archive_base}.gz")
get_filename_component(lockdc_lua_source_archive_name "${lockdc_lua_source_archive_path}" NAME)

lockdc_assert_generated_path("${LOCKDC_ROOT}" "${LOCKDC_BINARY_DIR}")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${LOCKDC_DIST_DIR}")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${lockdc_lua_stage_root}")
file(REMOVE_RECURSE "${lockdc_lua_stage_root}")
file(MAKE_DIRECTORY "${lockdc_lua_stage_workdir}")
file(MAKE_DIRECTORY "${lockdc_lua_source_root}/lua/lockdc")
file(MAKE_DIRECTORY "${lockdc_lua_source_root}/src/lua")
file(MAKE_DIRECTORY "${lockdc_lua_source_root}/scripts")
file(MAKE_DIRECTORY "${lockdc_lua_source_root}/include/lc")
file(MAKE_DIRECTORY "${LOCKDC_DIST_DIR}")

file(COPY "${lockdc_generated_rockspec_path}" DESTINATION "${lockdc_lua_stage_workdir}")
if(NOT lockdc_generated_rockspec_name STREQUAL lockdc_lua_rockspec_name)
    file(RENAME
        "${lockdc_lua_stage_workdir}/${lockdc_generated_rockspec_name}"
        "${lockdc_lua_stage_workdir}/${lockdc_lua_rockspec_name}"
    )
endif()
file(COPY "${LOCKDC_ROOT}/lua/lockdc/init.lua" DESTINATION "${lockdc_lua_source_root}/lua/lockdc")
file(COPY "${LOCKDC_ROOT}/src/lua/lockdc_lua.c" DESTINATION "${lockdc_lua_source_root}/src/lua")
file(COPY
    "${LOCKDC_ROOT}/src/lua/lockdc_collections_lua.c"
    "${LOCKDC_ROOT}/src/lua/lockdc_xa_lua.c"
    "${LOCKDC_ROOT}/src/lua/lockdc_xa_resource_lua.c"
    "${LOCKDC_ROOT}/src/lua/lockdc_xa_provider_lua.c"
    "${LOCKDC_ROOT}/src/lua/lockdc_lua.h"
    "${LOCKDC_ROOT}/src/lua/lockdc_lua_internal.h"
    "${LOCKDC_ROOT}/src/lua/lockdc_postgres_lua.c"
    "${LOCKDC_ROOT}/src/lua/lockdc_postgres_lua.h"
    DESTINATION "${lockdc_lua_source_root}/src/lua")
file(COPY "${LOCKDC_ROOT}/scripts/build_lua_rock.sh" DESTINATION "${lockdc_lua_source_root}/scripts")
file(COPY "${LOCKDC_ROOT}/scripts/build_postgres_lua_rock.sh" DESTINATION "${lockdc_lua_source_root}/scripts")
file(COPY "${LOCKDC_ROOT}/scripts/assert_generated_path.sh" DESTINATION "${lockdc_lua_source_root}/scripts")
file(COPY "${LOCKDC_ROOT}/include/lc/lc.h" DESTINATION "${lockdc_lua_source_root}/include/lc")
file(COPY "${LOCKDC_ROOT}/include/lc/postgres.h" DESTINATION "${lockdc_lua_source_root}/include/lc")
file(COPY "${LOCKDC_ROOT}/LICENSE" DESTINATION "${lockdc_lua_source_root}")
file(COPY "${LOCKDC_ROOT}/README.md" DESTINATION "${lockdc_lua_source_root}")
file(COPY "${LOCKDC_ROOT}/CHANGELOG.md" DESTINATION "${lockdc_lua_source_root}")
file(COPY "${LOCKDC_ROOT}/lockdc.rockspec.in" DESTINATION "${lockdc_lua_source_root}")
file(COPY "${LOCKDC_ROOT}/lockdc-postgres.rockspec.in" DESTINATION "${lockdc_lua_source_root}")
# Ship the facade's durable API guides and runnable examples, using an explicit
# guide list so investigations and temporary release notes cannot enter a rock.
set(lockdc_lua_documentation_sources
    docs/c-api.md
    docs/development.md
    docs/releases.md
    docs/lua.md
    docs/pouch-terminal-retention.md
    docs/pouch-dead-letter-retention.md
    docs/outbox-dispatch-architecture.md
    docs/outbox-vectis-handover.md
    docs/inbox-outbox.md
    docs/xa.md
    docs/postgres.md
    docs/pouch-open-settings-api.md
    docs/pouch-compression.md
    docs/pouch-storage.md
    docs/pouch-space.md
    docs/collections.md
    docs/collection-writer.md
    docs/collection-leases.md
    docs/collection-edits.md
    docs/collection-read-comparison.md
    docs/logging.md
    docs/state-retention.md
    docs/worst-case-scenario.md
    examples/lua/README.md
    examples/client_and_lease_methods.c
    examples/acquire_for_update.c
    examples/state_stream_roundtrip.c
    examples/outbox_producer.c
    examples/collection_journal.c)
file(GLOB lockdc_lua_example_sources RELATIVE "${LOCKDC_ROOT}"
    "${LOCKDC_ROOT}/examples/lua/*.lua")
list(APPEND lockdc_lua_documentation_sources ${lockdc_lua_example_sources})
foreach(lockdc_lua_doc IN LISTS lockdc_lua_documentation_sources)
    get_filename_component(lockdc_lua_doc_dir "${lockdc_lua_doc}" DIRECTORY)
    file(COPY "${LOCKDC_ROOT}/${lockdc_lua_doc}"
        DESTINATION "${lockdc_lua_source_root}/${lockdc_lua_doc_dir}")
endforeach()
file(WRITE "${lockdc_lua_source_root}/VERSION" "${LOCKDC_VERSION}\n")
file(WRITE "${lockdc_lua_source_root}/RELEASE_MANIFEST"
    "LICENSE\n"
    "README.md\n"
    "CHANGELOG.md\n"
    "RELEASE_MANIFEST\n"
    "VERSION\n"
    "include/lc/lc.h\n"
    "include/lc/postgres.h\n"
    "lockdc.rockspec.in\n"
    "lockdc-postgres.rockspec.in\n"
    "lua/lockdc/init.lua\n"
    "scripts/assert_generated_path.sh\n"
    "scripts/build_lua_rock.sh\n"
    "scripts/build_postgres_lua_rock.sh\n"
    "src/lua/lockdc_lua.c\n"
    "src/lua/lockdc_collections_lua.c\n"
    "src/lua/lockdc_xa_lua.c\n"
    "src/lua/lockdc_xa_resource_lua.c\n"
    "src/lua/lockdc_xa_provider_lua.c\n"
    "src/lua/lockdc_lua.h\n"
    "src/lua/lockdc_lua_internal.h\n"
    "src/lua/lockdc_postgres_lua.c\n"
    "src/lua/lockdc_postgres_lua.h\n")
foreach(lockdc_lua_doc IN LISTS lockdc_lua_documentation_sources)
    file(APPEND "${lockdc_lua_source_root}/RELEASE_MANIFEST" "${lockdc_lua_doc}\n")
endforeach()

file(REMOVE "${lockdc_lua_source_archive_base}" "${lockdc_lua_source_archive_path}")
execute_process(
    COMMAND "${LOCKDC_TAR_BIN}" -cf "${lockdc_lua_source_archive_base}" --format=gnu --owner 0 --group 0 "${lockdc_lua_source_root_name}"
    WORKING_DIRECTORY "${lockdc_lua_stage_root}"
    RESULT_VARIABLE lockdc_lua_tar_result
    OUTPUT_VARIABLE lockdc_lua_tar_stdout
    ERROR_VARIABLE lockdc_lua_tar_stderr
)
if(NOT lockdc_lua_tar_result EQUAL 0)
    message(FATAL_ERROR
        "failed to create Lua source archive\n"
        "stdout:\n${lockdc_lua_tar_stdout}\n"
        "stderr:\n${lockdc_lua_tar_stderr}")
endif()
execute_process(
    COMMAND "${LOCKDC_GZIP_BIN}" -9 -f -n "${lockdc_lua_source_archive_base}"
    RESULT_VARIABLE lockdc_lua_gzip_result
    OUTPUT_VARIABLE lockdc_lua_gzip_stdout
    ERROR_VARIABLE lockdc_lua_gzip_stderr
)
if(NOT lockdc_lua_gzip_result EQUAL 0)
    message(FATAL_ERROR
        "failed to gzip Lua source archive\n"
        "stdout:\n${lockdc_lua_gzip_stdout}\n"
        "stderr:\n${lockdc_lua_gzip_stderr}")
endif()

file(COPY "${lockdc_lua_source_archive_path}" DESTINATION "${lockdc_lua_stage_workdir}")

function(pack_lua_rockspec rockspec_path)
get_filename_component(lockdc_lua_rockspec_name "${rockspec_path}" NAME)
string(REGEX REPLACE "[.]rockspec$" ".src.rock" lockdc_lua_src_rock_name "${lockdc_lua_rockspec_name}")
file(COPY "${rockspec_path}" DESTINATION "${lockdc_lua_stage_workdir}")
file(READ "${lockdc_lua_stage_workdir}/${lockdc_lua_rockspec_name}" lockdc_lua_pack_rockspec_text)
string(REGEX REPLACE "source = \\{[^\\}]*\\}" "source = {\n  url = \"${lockdc_lua_source_archive_path}\",\n  dir = \"${lockdc_lua_source_root_name}\",\n}" lockdc_lua_pack_rockspec_text "${lockdc_lua_pack_rockspec_text}")
file(WRITE "${lockdc_lua_stage_workdir}/${lockdc_lua_rockspec_name}" "${lockdc_lua_pack_rockspec_text}")

execute_process(
    COMMAND "${LOCKDC_LUAROCKS_BIN}" pack "${lockdc_lua_rockspec_name}"
    WORKING_DIRECTORY "${lockdc_lua_stage_workdir}"
    RESULT_VARIABLE lockdc_lua_pack_result
    OUTPUT_VARIABLE lockdc_lua_pack_stdout
    ERROR_VARIABLE lockdc_lua_pack_stderr
)
if(NOT lockdc_lua_pack_result EQUAL 0)
    message(FATAL_ERROR
        "failed to create Lua source rock\n"
        "stdout:\n${lockdc_lua_pack_stdout}\n"
        "stderr:\n${lockdc_lua_pack_stderr}")
endif()

if(NOT EXISTS "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}")
    message(FATAL_ERROR "luarocks pack did not produce ${lockdc_lua_src_rock_name}")
endif()

set(lockdc_lua_src_rock_rewrite_root "${lockdc_lua_stage_root}/src-rock-rewrite")
file(REMOVE_RECURSE "${lockdc_lua_src_rock_rewrite_root}")
file(MAKE_DIRECTORY "${lockdc_lua_src_rock_rewrite_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}"
    WORKING_DIRECTORY "${lockdc_lua_src_rock_rewrite_root}"
    RESULT_VARIABLE lockdc_lua_unpack_result
    OUTPUT_VARIABLE lockdc_lua_unpack_stdout
    ERROR_VARIABLE lockdc_lua_unpack_stderr
)
if(NOT lockdc_lua_unpack_result EQUAL 0)
    message(FATAL_ERROR
        "failed to unpack Lua source rock for local-path rewrite\n"
        "stdout:\n${lockdc_lua_unpack_stdout}\n"
        "stderr:\n${lockdc_lua_unpack_stderr}")
endif()

set(lockdc_lua_embedded_rockspec "${lockdc_lua_src_rock_rewrite_root}/${lockdc_lua_rockspec_name}")
if(NOT EXISTS "${lockdc_lua_embedded_rockspec}")
    message(FATAL_ERROR "Lua source rock is missing embedded rockspec: ${lockdc_lua_embedded_rockspec}")
endif()
file(READ "${lockdc_lua_embedded_rockspec}" lockdc_lua_embedded_rockspec_text)
string(REGEX REPLACE "source = \\{[^\\}]*\\}" "source = {\n  url = \"${lockdc_lua_source_archive_name}\",\n  dir = \"${lockdc_lua_source_root_name}\",\n}" lockdc_lua_embedded_rockspec_text "${lockdc_lua_embedded_rockspec_text}")
file(WRITE "${lockdc_lua_embedded_rockspec}" "${lockdc_lua_embedded_rockspec_text}")

file(REMOVE "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar cf "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}" --format=zip
        "${lockdc_lua_rockspec_name}"
        "${lockdc_lua_source_archive_name}"
    WORKING_DIRECTORY "${lockdc_lua_src_rock_rewrite_root}"
    RESULT_VARIABLE lockdc_lua_repack_result
    OUTPUT_VARIABLE lockdc_lua_repack_stdout
    ERROR_VARIABLE lockdc_lua_repack_stderr
)
if(NOT lockdc_lua_repack_result EQUAL 0)
    message(FATAL_ERROR
        "failed to repack Lua source rock after local-path rewrite\n"
        "stdout:\n${lockdc_lua_repack_stdout}\n"
        "stderr:\n${lockdc_lua_repack_stderr}")
endif()

file(COPY "${rockspec_path}" DESTINATION "${LOCKDC_DIST_DIR}")
file(COPY "${lockdc_lua_source_archive_path}" DESTINATION "${LOCKDC_DIST_DIR}")
file(COPY "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}" DESTINATION "${LOCKDC_DIST_DIR}")
endfunction()
pack_lua_rockspec("${lockdc_generated_rockspec_path}")
if(LOCKDC_BUILD_POSTGRES)
    pack_lua_rockspec("${LOCKDC_BINARY_DIR}/lua-postgres/lockdc-postgres-${LOCKDC_VERSION}-1.rockspec")
endif()
