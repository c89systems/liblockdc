option(LOCKDC_E2E_LUA_BUILD_TREE "Run the Lua E2E scenarios against native build-tree modules." OFF)

add_test(NAME lc_e2e_scratch_setup
  COMMAND ${CMAKE_COMMAND}
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P ${CMAKE_SOURCE_DIR}/tests/e2e/prepare_scratch.cmake
)
set_tests_properties(lc_e2e_scratch_setup PROPERTIES
  FIXTURES_SETUP lockdc_e2e_scratch
  LABELS "e2e"
  RUN_SERIAL TRUE
)

function(lc_add_e2e_shard target_name group_macro)
  add_executable(${target_name}
    test_lc_e2e.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
  )
  target_link_libraries(${target_name} PRIVATE ${lc_test_link_target} lc::cmocka)
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
  target_compile_definitions(${target_name} PRIVATE ${group_macro}
    LC_E2E_TMP_ROOT="${CMAKE_SOURCE_DIR}/build/devenv/tmp"
  )
  lc_configure_c_test_target(${target_name})
  if(LOCKDC_ENABLE_COVERAGE)
    target_compile_options(${target_name} PRIVATE
      $<$<COMPILE_LANG_AND_ID:C,GNU,Clang>:--coverage -O0 -g>
    )
    target_link_options(${target_name} PRIVATE
      $<$<LINK_LANGUAGE:C>:--coverage>
    )
  endif()
  add_test(NAME ${target_name} COMMAND ${target_name})
  set_tests_properties(${target_name} PROPERTIES
    LABELS "e2e"
    FIXTURES_REQUIRED lockdc_e2e_scratch
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endfunction()

function(lc_add_lua_e2e_test test_name script_path)
  set(options PACKAGE_ONLY)
  set(one_value_args POUCH_ROOT)
  set(multi_value_args ENV_OVERRIDES)
  cmake_parse_arguments(LUA_E2E "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  if(NOT LUA_E2E_PACKAGE_ONLY AND NOT TARGET lockdc_lua_core)
    return()
  endif()
  if(NOT lockdc_lua_e2e)
    return()
  endif()

  set(lua_e2e_command
    "${CMAKE_COMMAND}"
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_TEST_NAME=${test_name}
      -DLOCKDC_LUA_TEST_SCRIPT=${script_path}
      -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
      -DLOCKDC_LUA_FIXTURE_ROOT=${CMAKE_BINARY_DIR}/lua-rock-tests/e2e
  )
  if(LUA_E2E_ENV_OVERRIDES)
    string(REPLACE ";" "|" lua_e2e_env_overrides "${LUA_E2E_ENV_OVERRIDES}")
    list(APPEND lua_e2e_command
      "-DLOCKDC_LUA_TEST_ENV=${lua_e2e_env_overrides}"
    )
  endif()
  if(LUA_E2E_POUCH_ROOT)
    list(APPEND lua_e2e_command
      "-DLOCKDC_POUCH_ROOT=${LUA_E2E_POUCH_ROOT}"
    )
  endif()
  if(LUA_E2E_POUCH_ROOT)
    set(lua_e2e_runner "${CMAKE_SOURCE_DIR}/tests/lua_pouch_rock_install_and_run_test.cmake")
  else()
    set(lua_e2e_runner "${CMAKE_SOURCE_DIR}/tests/lua_rock_install_and_run_test.cmake")
  endif()
  list(APPEND lua_e2e_command
    -P "${lua_e2e_runner}"
  )

  add_test(NAME ${test_name} COMMAND ${lua_e2e_command})
  set_tests_properties(${test_name} PROPERTIES
    LABELS "e2e;lua"
    FIXTURES_REQUIRED lockdc_e2e_lua
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
  if(LUA_E2E_POUCH_ROOT)
    # These scenarios only read the prepared modules and use private roots.
    set_tests_properties(${test_name} PROPERTIES RUN_SERIAL FALSE)
  endif()
endfunction()

lc_add_e2e_shard(lc_e2e_disk_direct LC_E2E_GROUP_DISK_DIRECT)
lc_add_e2e_shard(lc_e2e_pouch_direct LC_E2E_GROUP_POUCH_DIRECT)
lc_add_e2e_shard(lc_e2e_pouch_shared_outbox LC_E2E_GROUP_POUCH_SHARED_OUTBOX)
lc_add_e2e_shard(lc_e2e_s3_direct LC_E2E_GROUP_S3_DIRECT)
lc_add_e2e_shard(lc_e2e_mem_direct LC_E2E_GROUP_MEM_DIRECT)
lc_add_e2e_shard(lc_e2e_disk_consumer LC_E2E_GROUP_DISK_CONSUMER)
lc_add_e2e_shard(lc_e2e_s3_consumer LC_E2E_GROUP_S3_CONSUMER)
lc_add_e2e_shard(lc_e2e_mem_consumer LC_E2E_GROUP_MEM_CONSUMER)
# Each lockd backend has its own fixtures. Serialize scenarios that use the
# same backend and let the local Pouch tests overlap their waits.
foreach(backend disk s3 mem)
  set_tests_properties(lc_e2e_${backend}_direct lc_e2e_${backend}_consumer
    PROPERTIES RUN_SERIAL FALSE RESOURCE_LOCK lockdc_e2e_${backend})
endforeach()
set_tests_properties(lc_e2e_pouch_direct lc_e2e_pouch_shared_outbox
  PROPERTIES RUN_SERIAL FALSE RESOURCE_LOCK lockdc_e2e_pouch_roots)

if(TARGET lc_example_acquire_lease_lifecycle)
  add_test(NAME lc_example_smoke_acquire_lease_lifecycle
    COMMAND $<TARGET_FILE:lc_example_acquire_lease_lifecycle>
  )
  set_tests_properties(lc_example_smoke_acquire_lease_lifecycle PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_acquire_for_update)
  add_test(NAME lc_example_smoke_acquire_for_update
    COMMAND $<TARGET_FILE:lc_example_acquire_for_update>
  )
  set_tests_properties(lc_example_smoke_acquire_for_update PROPERTIES
    LABELS "e2e;examples"
    PASS_REGULAR_EXPRESSION "acquire_for_update updated key"
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_client_and_lease_methods)
  add_test(NAME lc_example_smoke_client_and_lease_methods
    COMMAND $<TARGET_FILE:lc_example_client_and_lease_methods>
  )
  set_tests_properties(lc_example_smoke_client_and_lease_methods PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_management_admin)
  add_test(NAME lc_example_smoke_management_admin
    COMMAND $<TARGET_FILE:lc_example_management_admin>
  )
  set_tests_properties(lc_example_smoke_management_admin PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_curl_fetch_into_lockd)
  add_test(NAME lc_example_smoke_curl_fetch_into_lockd
    COMMAND $<TARGET_FILE:lc_example_curl_fetch_into_lockd>
  )
  set_tests_properties(lc_example_smoke_curl_fetch_into_lockd PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_local_mutate)
  add_test(NAME lc_example_smoke_local_mutate
    COMMAND $<TARGET_FILE:lc_example_local_mutate>
      -k lc-e2e-local-mutate
      -m /kind="example-local-mutate"
      -m /counter=1
      -m time:/ts=NOW
      -m textfile:/note=${CMAKE_SOURCE_DIR}/examples/data/state_stream_input.json
  )
  set_tests_properties(lc_example_smoke_local_mutate PROPERTIES
    LABELS "e2e;examples"
    PASS_REGULAR_EXPRESSION "\"kind\":\"example-local-mutate\""
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_startconsumer)
  add_test(NAME lc_example_smoke_startconsumer
    COMMAND $<TARGET_FILE:lc_example_startconsumer>
  )
  set_tests_properties(lc_example_smoke_startconsumer PROPERTIES
    LABELS "e2e;examples"
    PASS_REGULAR_EXPRESSION "\\{\"counter\":3\\}"
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks)
set(lockdc_lua_e2e OFF)
if(TARGET lockdc_lua_core AND LOCKDC_LUA_RUNNABLE AND
   (LOCKDC_LUAROCKS_BIN OR LOCKDC_E2E_LUA_BUILD_TREE))
  set(lockdc_lua_e2e ON)
endif()
if(LOCKDC_E2E_LUA_BUILD_TREE AND NOT lockdc_lua_e2e)
  message(FATAL_ERROR "Lua E2E build-tree mode requires a native lockdc Lua runner and module")
endif()
if(lockdc_lua_e2e)
  add_test(NAME lc_e2e_lua_setup COMMAND ${CMAKE_COMMAND}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR} -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
    -DLOCKDC_TEST_NAME=e2e-lua-setup
    -DLOCKDC_LUA_TEST_SCRIPT=${CMAKE_SOURCE_DIR}/tests/lua/test_lockdc_luarocks_smoke.lua
    -DLOCKDC_LUA_FIXTURE_ROOT=${CMAKE_BINARY_DIR}/lua-rock-tests/e2e
    -DLOCKDC_LUA_PREPARE_FIXTURE=ON
    -P ${CMAKE_SOURCE_DIR}/tests/lua_rock_install_and_run_test.cmake)
  set_tests_properties(lc_e2e_lua_setup PROPERTIES
    LABELS "e2e;lua" FIXTURES_SETUP lockdc_e2e_lua RUN_SERIAL TRUE TIMEOUT 120)
  add_test(NAME lua_e2e_fixture_test COMMAND ${CMAKE_COMMAND}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR} -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P ${CMAKE_SOURCE_DIR}/tests/lua_e2e_fixture_test.cmake)
  set_tests_properties(lua_e2e_fixture_test PROPERTIES
    LABELS "unit;local;lifecycle-host" RUN_SERIAL TRUE TIMEOUT 15)
  add_test(NAME lc_e2e_lua_allocator_gate COMMAND ${CMAKE_COMMAND}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR} -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
    -DLOCKDC_LUA_FIXTURE_ROOT=${CMAKE_BINARY_DIR}/lua-rock-tests/e2e
    -P ${CMAKE_SOURCE_DIR}/tests/lua_allocator_gate_test.cmake)
  set_tests_properties(lc_e2e_lua_allocator_gate PROPERTIES
    LABELS "e2e;lua;allocator" FIXTURES_REQUIRED lockdc_e2e_lua RUN_SERIAL TRUE TIMEOUT 120)
endif()

lc_add_lua_e2e_test(
  lc_e2e_lua_example_acquire_update_json
  "${CMAKE_SOURCE_DIR}/examples/lua/acquire_update_json.lua"
  ENV_OVERRIDES "LOCKDC_KEY=tests/lua/e2e-acquire-update-json;LOCKDC_OWNER=lua-e2e-acquire-update-json"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_example_acquire_for_update
  "${CMAKE_SOURCE_DIR}/examples/lua/acquire_for_update.lua"
  ENV_OVERRIDES "LOCKDC_KEY=tests/lua/e2e-example-acquire-for-update;LOCKDC_OWNER=lua-e2e-example-acquire-for-update"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_acquire_for_update
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_acquire_for_update.lua"
  ENV_OVERRIDES "LOCKDC_KEY=tests/lua/e2e-acquire-for-update;LOCKDC_OWNER=lua-e2e-acquire-for-update"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_example_queue_roundtrip
  "${CMAKE_SOURCE_DIR}/examples/lua/queue_roundtrip.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-queue-roundtrip;LOCKDC_OWNER=lua-e2e-queue-roundtrip"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_example_namespace_config
  "${CMAKE_SOURCE_DIR}/examples/lua/namespace_config.lua"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_consumer_single_message
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_consumer.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-consumer;LOCKDC_OWNER=lua-e2e-consumer"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_consumer_handler_error
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_consumer_handler_error.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-consumer-handler-error;LOCKDC_OWNER=lua-e2e-consumer-handler-error"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_state_after_ack
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_state_after_ack.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-state-after-ack;LOCKDC_OWNER=lua-e2e-state-after-ack"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_bundle_sources
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_bundle_sources.lua"
  ENV_OVERRIDES "LOCKDC_KEY_PREFIX=tests/lua/e2e-bundle-sources;LOCKDC_OWNER=lua-e2e-bundle-sources"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_pouch_config
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_pouch.lua"
  PACKAGE_ONLY
  POUCH_ROOT "${CMAKE_BINARY_DIR}/lua-pouch-e2e/lc_e2e_lua_pouch_config"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_outbox
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_outbox.lua"
  PACKAGE_ONLY
  POUCH_ROOT "${CMAKE_BINARY_DIR}/lua-pouch-e2e/lc_e2e_lua_outbox"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_terminal_retention
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_terminal_retention.lua"
  PACKAGE_ONLY
  POUCH_ROOT "${CMAKE_BINARY_DIR}/lua-pouch-e2e/lc_e2e_lua_terminal_retention"
)

if(TARGET lc_example_outbox_producer AND lockdc_lua_e2e)
  add_test(NAME lc_e2e_outbox_examples
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
      -DLOCKDC_OUTBOX_PRODUCER=$<TARGET_FILE:lc_example_outbox_producer>
      -DLOCKDC_LUA_FIXTURE_ROOT=${CMAKE_BINARY_DIR}/lua-rock-tests/e2e
      -P "${CMAKE_CURRENT_SOURCE_DIR}/outbox_examples_test.cmake"
  )
  set_tests_properties(lc_e2e_outbox_examples PROPERTIES
    LABELS "e2e;examples;lua"
    FIXTURES_REQUIRED lockdc_e2e_lua
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

# E2E execution is deliberately isolated from the rest of the local test
# fleet.  This closure contains every executable a test labelled `e2e` can
# invoke, so the E2E runner does not build unrelated unit and transport tests.
set(lockdc_e2e_test_targets
  lc_e2e_disk_direct
  lc_e2e_pouch_direct
  lc_e2e_pouch_shared_outbox
  lc_e2e_s3_direct
  lc_e2e_mem_direct
  lc_e2e_disk_consumer
  lc_e2e_s3_consumer
  lc_e2e_mem_consumer
)
set(lockdc_e2e_example_candidates
  lc_example_acquire_lease_lifecycle
  lc_example_acquire_for_update
  lc_example_client_and_lease_methods
  lc_example_management_admin
  lc_example_curl_fetch_into_lockd
  lc_example_local_mutate
  lc_example_startconsumer
  lc_example_outbox_producer
)
set(lockdc_e2e_example_targets)
foreach(lockdc_e2e_example_target IN LISTS lockdc_e2e_example_candidates)
  if(TARGET ${lockdc_e2e_example_target})
    list(APPEND lockdc_e2e_example_targets ${lockdc_e2e_example_target})
  endif()
endforeach()
list(APPEND lockdc_e2e_test_targets ${lockdc_e2e_example_targets})
foreach(tracked_target IN LISTS lockdc_e2e_test_targets)
  target_sources(${tracked_target} PRIVATE
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tracking.c)
endforeach()
# Lifetime probes are service-free E2E sentinels. Build them for standalone E2E
# too, so a dependency/global unload leak cannot hide behind passing behavior.
foreach(lifetime_target lc_library_unload_test lc_static_exit_test lc_tracking_gate_probe
                        lc_openssl_key_failure_test lc_pouch_key_failure_test)
  if(TARGET ${lifetime_target})
    list(APPEND lockdc_e2e_test_targets ${lifetime_target})
    list(APPEND lockdc_e2e_example_targets ${lifetime_target})
  endif()
endforeach()
if(lockdc_lua_e2e)
  list(APPEND lockdc_e2e_test_targets lockdc_lua_runner)
  list(APPEND lockdc_e2e_example_targets lockdc_lua_runner)
  if(TARGET lockdc_lua_core)
    list(APPEND lockdc_e2e_test_targets lockdc_lua_core)
    list(APPEND lockdc_e2e_example_targets lockdc_lua_core)
  endif()
  if(TARGET lc_shared)
    list(APPEND lockdc_e2e_example_targets lc_shared)
  endif()
endif()
add_custom_target(lockdc_e2e_tests DEPENDS ${lockdc_e2e_test_targets})
add_custom_target(lockdc_e2e_example_tests DEPENDS ${lockdc_e2e_example_targets})
get_property(e2e_tests DIRECTORY PROPERTY TESTS)
set_property(TEST ${e2e_tests} APPEND PROPERTY ENVIRONMENT
  "LOCKDC_TEST_ALLOCATOR_TRACKING=1")
if(LOCKDC_E2E_LUA_BUILD_TREE)
  set_property(TEST ${e2e_tests} APPEND PROPERTY ENVIRONMENT
    "LOCKDC_E2E_LUA_BUILD_TREE=1")
endif()
lc_apply_test_defaults()
