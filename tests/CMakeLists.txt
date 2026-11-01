if(TARGET lc_test_static)
  set(lc_test_link_target lc_test_static)
elseif(TARGET lc_static)
  set(lc_test_link_target lc_static)
elseif(TARGET lc_shared)
  set(lc_test_link_target lc_shared)
else()
  return()
endif()

# Compile public headers as standalone C89 translation units, outside the SDK
# libraries. Their exact database entries also keep clangd from borrowing an
# unrelated source target's dependency context when opening a header directly.
function(lc_add_public_header_check target header library)
  set(source "${CMAKE_SOURCE_DIR}/${header}")
  set_source_files_properties("${source}" PROPERTIES
    LANGUAGE C HEADER_FILE_ONLY FALSE)
  add_library(${target} OBJECT "${source}")
  lc_configure_c_target(${target})
  target_link_libraries(${target} PRIVATE ${library})
endfunction()

if(TARGET lc_static)
  lc_add_public_header_check(lc_public_header_core include/lc/lc.h lc_static)
elseif(TARGET lc_shared)
  lc_add_public_header_check(lc_public_header_core include/lc/lc.h lc_shared)
endif()
if(TARGET lc_postgres_static)
  lc_add_public_header_check(lc_public_header_postgres include/lc/postgres.h lc_postgres_static)
elseif(TARGET lc_postgres_shared)
  lc_add_public_header_check(lc_public_header_postgres include/lc/postgres.h lc_postgres_shared)
endif()

if(CMAKE_BINARY_DIR STREQUAL "${CMAKE_SOURCE_DIR}/build/debug")
  add_test(NAME clangd_public_api_test COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -DLOCKDC_BUILD_POSTGRES=${LOCKDC_BUILD_POSTGRES}
    -P "${CMAKE_SOURCE_DIR}/tests/clangd_public_api_test.cmake")
  set_tests_properties(clangd_public_api_test PROPERTIES
    LABELS "unit;local;lifecycle-host;editor" RUN_SERIAL TRUE TIMEOUT 90)
endif()

if(LOCKDC_EXTERNAL_ROOT)
  add_test(NAME lonejson_dependency_interface
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
      -DLOCKDC_LONEJSON_VERSION=${LOCKDC_LONEJSON_VERSION}
      -DLOCKDC_LONEJSON_ABI_VERSION=${LOCKDC_LONEJSON_ABI_VERSION}
      -P "${CMAKE_SOURCE_DIR}/tests/lonejson_dependency_interface_test.cmake"
  )
  add_test(NAME liblql_dependency_interface
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
      -DLOCKDC_LIBLQL_VERSION=${LOCKDC_LIBLQL_VERSION}
      -DLOCKDC_LIBLQL_ABI_VERSION=${LOCKDC_LIBLQL_ABI_VERSION}
      -DLOCKDC_TARGET_ARCH=${LOCKDC_TARGET_ARCH}
      -DLOCKDC_TARGET_OS=${LOCKDC_TARGET_OS}
      -DLOCKDC_TARGET_LIBC=${LOCKDC_TARGET_LIBC}
      -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
      -DLOCKDC_CROSSCOMPILING=${CMAKE_CROSSCOMPILING}
      -P "${CMAKE_SOURCE_DIR}/tests/liblql_dependency_interface_test.cmake"
  )
  add_test(NAME zlib_dependency_interface
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
      -P "${CMAKE_SOURCE_DIR}/tests/zlib_dependency_interface_test.cmake"
  )
  add_test(NAME dependency_public_surface_test
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_BUILD_POSTGRES=${LOCKDC_BUILD_POSTGRES}
      -P "${CMAKE_SOURCE_DIR}/tests/dependency_public_surface_test.cmake"
  )
endif()

add_test(NAME public_api_boundary
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/public_api_boundary_test.cmake"
)

if(TARGET lc_shared)
  set(lockdc_lua_module_arg)
  if(TARGET lockdc_lua_core)
    set(lockdc_lua_module_arg
      -DLOCKDC_LUA_MODULE=$<TARGET_FILE:lockdc_lua_core>
    )
  endif()
  add_test(NAME shared_public_symbol_boundary
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_SHARED_LIBRARY=$<TARGET_FILE:lc_shared>
      -DLOCKDC_NM=${CMAKE_NM}
      -DLOCKDC_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}
      ${lockdc_lua_module_arg}
      -P "${CMAKE_SOURCE_DIR}/tests/shared_public_symbol_boundary_test.cmake"
  )
  set_tests_properties(shared_public_symbol_boundary PROPERTIES RUN_SERIAL TRUE)
endif()

add_test(NAME example_receiver_style
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/example_receiver_style_test.cmake"
)

add_test(NAME pouch_liblql_storage_boundary
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/pouch_liblql_storage_boundary_test.cmake"
)

add_test(NAME release_fuzz_targeting_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_fuzz_targeting_test.cmake"
)

add_test(NAME benchmark_go_timeout_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/benchmark_go_timeout_contract_test.cmake"
)
set_tests_properties(benchmark_go_timeout_contract_test PROPERTIES
  LABELS "unit;local;benchmark"
  TIMEOUT 10
)

add_test(NAME run_timed_log_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/run_timed_log_test.cmake"
)
foreach(contract release_test_selection valgrind_targeting)
  add_test(NAME ${contract}_test COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR} -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -DLOCKDC_TARGET_LIBC=${LOCKDC_TARGET_LIBC}
    -DLOCKDC_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -P "${CMAKE_SOURCE_DIR}/tests/${contract}_test.cmake")
  set_tests_properties(${contract}_test PROPERTIES
    LABELS "unit;local;lifecycle-host" RUN_SERIAL TRUE TIMEOUT 20)
endforeach()
set_tests_properties(run_timed_log_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME lua_binding_source_layout_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lua_binding_source_layout_test.cmake"
)

add_test(NAME lua_outbox_numeric_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lua_outbox_numeric_contract_test.cmake"
)

add_test(NAME lua_external_sdk_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lua_external_sdk_contract_test.cmake"
)

add_test(NAME release_package_matrix_targeting_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_package_matrix_targeting_test.cmake"
)

add_test(NAME release_targeting_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_targeting_test.cmake"
)

add_test(NAME release_werror_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_werror_contract_test.cmake"
)
set_tests_properties(release_werror_contract_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME release_compile_werror_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -DLOCKDC_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -P "${CMAKE_SOURCE_DIR}/tests/release_compile_werror_test.cmake"
)
set_tests_properties(release_compile_werror_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME release_privacy_recursive_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_privacy_recursive_test.cmake"
)
set_tests_properties(release_privacy_recursive_test PROPERTIES
  LABELS "unit;local;packaging"
  TIMEOUT 10
)

add_test(NAME darwin_macho_lifecycle_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/darwin_macho_lifecycle_contract_test.cmake"
)
set_tests_properties(darwin_macho_lifecycle_contract_test PROPERTIES
  LABELS "unit;local;packaging"
  TIMEOUT 10
)

add_test(NAME bootlin_toolchain_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/bootlin_toolchain_contract_test.cmake"
)
set_tests_properties(bootlin_toolchain_contract_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_test(NAME bootlin_development_runtime_contract_test
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -P "${CMAKE_SOURCE_DIR}/tests/bootlin_development_runtime_contract_test.cmake"
  )
  set_tests_properties(bootlin_development_runtime_contract_test PROPERTIES
    LABELS "unit;local"
    TIMEOUT 10
  )
endif()

add_test(NAME pouch_storage_allocator_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/pouch_storage_allocator_contract_test.cmake"
)
set_tests_properties(pouch_storage_allocator_contract_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME packaging_portability_source_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/packaging_portability_source_test.cmake"
)

add_test(NAME package_source_dist_dir_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/package_source_dist_dir_test.cmake"
)

add_test(NAME coverage_pkgconfig_metadata_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/coverage_pkgconfig_metadata_test.cmake"
)

add_test(NAME coverage_gcov_toolchain_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/coverage_gcov_toolchain_test.cmake"
)

add_test(NAME lonejson_upload_rewind_transport_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lonejson_upload_rewind_transport_test.cmake"
)

set(LOCKDC_LUA_TEST_ENV_COMMON "ASAN_OPTIONS=detect_leaks=0")
set(LOCKDC_LUA_RUNNABLE OFF)
if(TARGET lockdc_lua_runner
   AND CMAKE_SYSTEM_NAME STREQUAL CMAKE_HOST_SYSTEM_NAME
   AND CMAKE_SYSTEM_PROCESSOR STREQUAL CMAKE_HOST_SYSTEM_PROCESSOR)
  set(LOCKDC_LUA_BIN "$<TARGET_FILE:lockdc_lua_runner>")
  set(LOCKDC_LUA_RUNNABLE ON)
endif()

if(LOCKDC_LUA_RUNNABLE)
  add_test(NAME lua_runner_search_path_test
    COMMAND "${CMAKE_COMMAND}" "-DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}"
      -P "${CMAKE_SOURCE_DIR}/tests/lua_runner_search_path_test.cmake"
  )
  add_test(NAME lua_facade_unit_test
    COMMAND "${LOCKDC_LUA_BIN}" "${CMAKE_SOURCE_DIR}/tests/lua/test_lockdc_facade_unit.lua"
  )
  set_tests_properties(lua_facade_unit_test PROPERTIES
    ENVIRONMENT "LOCKDC_ROOT=${CMAKE_SOURCE_DIR}"
  )
endif()
if(TARGET lockdc_lua_core AND LOCKDC_LUA_RUNNABLE)
  add_test(NAME lua_core_package_test COMMAND ${CMAKE_COMMAND}
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR} -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
    -P ${CMAKE_SOURCE_DIR}/tests/lua_core_package_test.cmake)
  set_tests_properties(lua_core_package_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 180 LABELS "local;lua;packaging")
  if(TARGET lockdc_lua_postgres)
    add_test(NAME lua_postgres_package_test COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR} -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
      -P ${CMAKE_SOURCE_DIR}/tests/lua_postgres_package_test.cmake)
    set_tests_properties(lua_postgres_package_test PROPERTIES
      RUN_SERIAL TRUE TIMEOUT 180 LABELS "local;lua;postgres;packaging")
  endif()
  add_executable(lc_lua_xa_test
    integration/test_lua_xa.c
    ${CMAKE_SOURCE_DIR}/src/lua/lockdc_lua.c
    ${CMAKE_SOURCE_DIR}/src/lua/lockdc_collections_lua.c
    ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_lua.c
    ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_resource_lua.c
    ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_provider_lua.c)
  target_include_directories(lc_lua_xa_test PRIVATE ${CMAKE_SOURCE_DIR}/src/lua)
  target_include_directories(lc_lua_xa_test SYSTEM PRIVATE ${LUA_INCLUDE_DIR})
  target_link_libraries(lc_lua_xa_test PRIVATE lc_shared Lua::Lua
    lc::curl_shared lc::openssl_ssl_shared lc::openssl_crypto_shared
    lc::nghttp2_shared lc::pslog_shared lc::lonejson_shared
    lc::liblql_shared lc::libssh2_shared lc::zlib_shared)
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_options(lc_lua_xa_test PRIVATE "LINKER:--no-as-needed")
  endif()
  lc_configure_c_test_target(lc_lua_xa_test)
  # This additional static runner can drive private storage maintenance without
  # exporting test helpers from the production DSO. Existing Lua suites retain
  # their shared-library runner and installed-module coverage.
  get_target_property(lc_lua_test_sources lc_lua_xa_test SOURCES)
  add_executable(lc_lua_collections_test ${lc_lua_test_sources})
  target_include_directories(lc_lua_collections_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/src/lua)
  target_include_directories(lc_lua_collections_test SYSTEM PRIVATE ${LUA_INCLUDE_DIR})
  target_compile_definitions(lc_lua_collections_test PRIVATE
    LOCKDC_TEST_BUILD=1 LOCKDC_LUA_COLLECTION_MAINTENANCE=1)
  target_link_libraries(lc_lua_collections_test PRIVATE ${lc_test_link_target} Lua::Lua)
  lc_configure_c_test_target(lc_lua_collections_test)
  add_test(NAME lua_collection_edits_shared_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_xa_test>
      -DLOCKDC_LUA_TEST_CASE=collection_edits
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_collection_edits_shared_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;collections")
  add_test(NAME lua_collection_edits_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_collections_test>
      -DLOCKDC_LUA_TEST_CASE=collection_edits
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_collection_edits_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;collections")
  add_test(NAME lua_collection_count_retention_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_collections_test>
      -DLOCKDC_LUA_TEST_CASE=collection_count
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_collection_count_retention_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;collections;retention")
  add_test(NAME lua_collection_writer_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_collections_test>
      -DLOCKDC_LUA_TEST_CASE=collection_writer
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_collection_writer_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;collections;retention")
  add_test(NAME lua_collection_leases_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_xa_test>
      -DLOCKDC_LUA_TEST_CASE=collection_leases
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  add_test(NAME lua_collection_staged_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_collections_test>
      -DLOCKDC_LUA_TEST_CASE=collection_staged
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_collection_staged_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;collections")
  set_tests_properties(lua_collection_leases_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;collections")
  add_test(NAME lua_xa_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_xa_test>
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_xa_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;xa")
  add_test(NAME lua_state_retention_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_xa_test>
      -DLOCKDC_LUA_TEST_CASE=retention
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_state_retention_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;retention")
  add_test(NAME lua_compression_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_xa_test>
      -DLOCKDC_LUA_TEST_CASE=compression
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_compression_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;pouch")
  add_test(NAME lua_collections_test
    COMMAND ${CMAKE_COMMAND}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_LUA_XA_BINARY=$<TARGET_FILE:lc_lua_xa_test>
      -DLOCKDC_LUA_TEST_CASE=collections
      -P ${CMAKE_SOURCE_DIR}/tests/lua_xa_test.cmake)
  set_tests_properties(lua_collections_test PROPERTIES
    RUN_SERIAL TRUE TIMEOUT 30 LABELS "unit;local;lua;collections")
  add_test(NAME lua_binding_core_smoke_test
    COMMAND "${LOCKDC_LUA_BIN}" "${CMAKE_SOURCE_DIR}/tests/lua/test_lockdc_core_smoke.lua"
  )
  set(lockdc_lua_core_smoke_env
    "LOCKDC_LUA_CORE_DIR=${CMAKE_BINARY_DIR}/lua"
    ${LOCKDC_LUA_TEST_ENV_COMMON}
  )
  set_tests_properties(lua_binding_core_smoke_test PROPERTIES
    ENVIRONMENT "${lockdc_lua_core_smoke_env}"
  )
  add_test(NAME lua_binding_core_tracking_smoke_test
    COMMAND "${LOCKDC_LUA_BIN}" "${CMAKE_SOURCE_DIR}/tests/lua/test_lockdc_core_smoke.lua")
  set_tests_properties(lua_binding_core_tracking_smoke_test PROPERTIES
    ENVIRONMENT "${lockdc_lua_core_smoke_env};LOCKDC_TEST_ALLOCATOR_TRACKING=1")
endif()

add_subdirectory(unit)
add_subdirectory(integration)

add_test(NAME collection_test_selection_test COMMAND "${CMAKE_COMMAND}"
  -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
  -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
  -DLOCKDC_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -P "${CMAKE_SOURCE_DIR}/tests/collection_test_selection_test.cmake")
set_tests_properties(collection_test_selection_test PROPERTIES
  LABELS "unit;cross-runtime" TIMEOUT 15)
# Bootlin sets CMAKE_CROSSCOMPILING even for a host-architecture Linux build.
# The Python driver launches the binary directly, without a QEMU wrapper.
if(TARGET lockdc_pouch_replay_probe
   AND CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux"
   AND CMAKE_SYSTEM_PROCESSOR STREQUAL CMAKE_HOST_SYSTEM_PROCESSOR
   AND LOCKDC_TARGET_LIBC STREQUAL "gnu")
  find_program(LOCKDC_REPLAY_PYTHON NAMES python3 REQUIRED)
  foreach(replay_transform IN ITEMS plain encrypted)
    set(replay_args)
    if(replay_transform STREQUAL "encrypted")
      list(APPEND replay_args --encrypted)
    endif()
    add_test(NAME pouch_replay_${replay_transform}
      COMMAND "${LOCKDC_REPLAY_PYTHON}"
        "${CMAKE_SOURCE_DIR}/tests/e2e/pouch_replay.py"
        $<TARGET_FILE:lockdc_pouch_replay_probe>
        --keys 1 --updates 256 --shared --unclean --live-staged
        --segment-bytes 65536 ${replay_args})
    set_tests_properties(pouch_replay_${replay_transform} PROPERTIES
      LABELS "integration;offline;pouch;replay"
      RUN_SERIAL TRUE TIMEOUT 60
      WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
  endforeach()
endif()
set_property(TEST
  public_api_boundary
  example_receiver_style
  pouch_liblql_storage_boundary
  release_fuzz_targeting_test
  benchmark_go_timeout_contract_test
  run_timed_log_test
  lua_outbox_numeric_contract_test
  release_package_matrix_targeting_test
  release_targeting_test
  release_werror_contract_test
  release_privacy_recursive_test
  darwin_macho_lifecycle_contract_test
  bootlin_toolchain_contract_test
  pouch_storage_allocator_contract_test
  packaging_portability_source_test
  package_source_dist_dir_test
  coverage_pkgconfig_metadata_test
  lonejson_upload_rewind_transport_test
  APPEND PROPERTY LABELS lifecycle-host)

if(LOCKDC_BUILD_E2E_TESTS)
  add_subdirectory(e2e)
endif()

# Freeze the runtime build closure only after all test subdirectories have
# registered their executables, including integration tests with modal cases.
get_property(lc_cross_runtime_test_targets GLOBAL PROPERTY
  LOCKDC_CROSS_RUNTIME_TEST_TARGETS)
add_custom_target(lockdc_cross_runtime_tests DEPENDS
  ${lc_cross_runtime_test_targets})
lc_apply_test_defaults()
