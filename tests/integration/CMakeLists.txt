function(lc_add_integration_test target_name)
  add_executable(${target_name} ${ARGN})
  target_link_libraries(${target_name} PRIVATE ${lc_test_link_target} lc::cmocka)
  target_include_directories(${target_name} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${LOCKDC_EXTERNAL_ROOT}/curl/install/include
    ${LOCKDC_EXTERNAL_ROOT}/openssl/install/include
    ${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/include
    ${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include
    ${LOCKDC_EXTERNAL_ROOT}/zlib/install/include
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
    LABELS "integration;offline"
    RUN_SERIAL TRUE
    TIMEOUT 120)
  if(LOCKDC_C_TEST_ENVIRONMENT)
    set_tests_properties(${target_name} PROPERTIES
      ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
  endif()
endfunction()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(lc_openssl_key_failure_test test_openssl_key_failure.c)
  target_link_libraries(lc_openssl_key_failure_test PRIVATE
    lc::openssl_crypto_static Threads::Threads)
  target_link_options(lc_openssl_key_failure_test PRIVATE
    "LINKER:--wrap=pthread_key_create")
  lc_configure_c_test_target(lc_openssl_key_failure_test)
  if(LOCKDC_TARGET_ID STREQUAL "x86_64-linux-gnu" AND NOT LOCKDC_SANITIZER_INSTRUMENTED)
    find_program(LOCKDC_VALGRIND_BIN valgrind)
  endif()
  foreach(cleanup auto cleanup)
    foreach(failure RANGE 0 5)
      add_test(NAME openssl_key_failure_${cleanup}_${failure}_test
        COMMAND lc_openssl_key_failure_test ${failure} ${cleanup})
      set_tests_properties(openssl_key_failure_${cleanup}_${failure}_test PROPERTIES
        LABELS "integration;offline;memory;cross-runtime;e2e;examples" TIMEOUT 10)
      if(LOCKDC_C_TEST_ENVIRONMENT)
        set_tests_properties(openssl_key_failure_${cleanup}_${failure}_test PROPERTIES
          ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
      endif()
      if(LOCKDC_VALGRIND_BIN AND NOT LOCKDC_SANITIZER_INSTRUMENTED
         AND LOCKDC_TARGET_ID STREQUAL "x86_64-linux-gnu")
        add_test(NAME openssl_key_failure_${cleanup}_${failure}_valgrind_test
          COMMAND ${LOCKDC_VALGRIND_BIN} --leak-check=full --show-leak-kinds=all
            --errors-for-leak-kinds=all --error-exitcode=97
            $<TARGET_FILE:lc_openssl_key_failure_test> ${failure} ${cleanup})
        set_tests_properties(openssl_key_failure_${cleanup}_${failure}_valgrind_test PROPERTIES
          LABELS "integration;offline;memory" TIMEOUT 30)
      endif()
    endforeach()
  endforeach()
  set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS
    lc_openssl_key_failure_test)
endif()

if(TARGET lc_static AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(lc_pouch_key_failure_test test_pouch_key_failure.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_link_libraries(lc_pouch_key_failure_test PRIVATE lc_static lc::openssl_crypto_static)
  target_link_options(lc_pouch_key_failure_test PRIVATE
    "LINKER:--wrap=pthread_key_create" "LINKER:--wrap=pthread_key_delete")
  lc_configure_c_test_target(lc_pouch_key_failure_test)
  foreach(mode shared single)
    foreach(failure RANGE 0 5)
      add_test(NAME pouch_key_failure_${mode}_${failure}_test
        COMMAND lc_pouch_key_failure_test ${failure} ${mode})
      set_tests_properties(pouch_key_failure_${mode}_${failure}_test PROPERTIES
        LABELS "integration;offline;memory;cross-runtime;e2e;examples" TIMEOUT 10)
      if(LOCKDC_C_TEST_ENVIRONMENT)
        set_tests_properties(pouch_key_failure_${mode}_${failure}_test PROPERTIES
          ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
      endif()
      if(LOCKDC_VALGRIND_BIN AND NOT LOCKDC_SANITIZER_INSTRUMENTED
         AND LOCKDC_TARGET_ID STREQUAL "x86_64-linux-gnu")
        add_test(NAME pouch_key_failure_${mode}_${failure}_valgrind_test
          COMMAND ${LOCKDC_VALGRIND_BIN} --leak-check=full --show-leak-kinds=all
            --errors-for-leak-kinds=all --error-exitcode=97
            $<TARGET_FILE:lc_pouch_key_failure_test> ${failure} ${mode})
        set_tests_properties(pouch_key_failure_${mode}_${failure}_valgrind_test PROPERTIES
          LABELS "integration;offline;memory" TIMEOUT 30)
      endif()
    endforeach()
  endforeach()
  set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS
    lc_pouch_key_failure_test)

  add_executable(lc_static_exit_test test_client_unload_module.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_compile_definitions(lc_static_exit_test PRIVATE
    LC_TEST_INTERNAL_RUNTIME=1 LC_TEST_STATIC_EXIT=1)
  target_link_libraries(lc_static_exit_test PRIVATE lc_static)
  lc_configure_c_test_target(lc_static_exit_test)
  add_test(NAME static_exit_test COMMAND lc_static_exit_test)
  set_tests_properties(static_exit_test PROPERTIES
    LABELS "integration;offline;memory;cross-runtime;e2e;examples" TIMEOUT 30)
  if(LOCKDC_C_TEST_ENVIRONMENT)
    set_tests_properties(static_exit_test PROPERTIES
      ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
  endif()
  set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS lc_static_exit_test)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND LOCKDC_TARGET_ID STREQUAL "x86_64-linux-gnu")
  add_executable(lc_tracking_gate_probe test_tracking_gate.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tracking.c)
  target_link_libraries(lc_tracking_gate_probe PRIVATE ${lc_test_link_target})
  lc_configure_c_test_target(lc_tracking_gate_probe)
  add_test(NAME e2e_tracking_gate_test COMMAND ${CMAKE_COMMAND}
    -DBINARY_DIR=${CMAKE_BINARY_DIR} -DPROBE=$<TARGET_FILE:lc_tracking_gate_probe>
    -P ${CMAKE_SOURCE_DIR}/tests/tracking_gate_test.cmake)
  set_tests_properties(e2e_tracking_gate_test PROPERTIES
    LABELS "integration;offline;allocator;e2e;examples" TIMEOUT 30)
  add_library(lc_log_unload_module MODULE test_log_unload_module.c
    ${CMAKE_SOURCE_DIR}/src/lc_log.c)
  target_include_directories(lc_log_unload_module PRIVATE
    ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_BINARY_DIR}/generated/include ${LOCKDC_LONEJSON_INCLUDE_DIR})
  target_link_libraries(lc_log_unload_module PRIVATE lc::pslog_shared)
  lc_configure_c_test_target(lc_log_unload_module)
  lc_configure_development_module_runtime(lc_log_unload_module)
  add_executable(lc_library_unload_test test_library_unload.c)
  target_link_libraries(lc_library_unload_test PRIVATE ${CMAKE_DL_LIBS})
  lc_configure_c_test_target(lc_library_unload_test)
  add_dependencies(lc_library_unload_test lc_log_unload_module)
  if(TARGET lc_static)
    add_library(lc_static_unload_module MODULE test_client_unload_module.c
      ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
    target_link_libraries(lc_static_unload_module PRIVATE lc_static)
    target_link_options(lc_static_unload_module PRIVATE
      "LINKER:--exclude-libs,ALL")
    target_compile_definitions(lc_static_unload_module PRIVATE LC_TEST_INTERNAL_RUNTIME=1)
    lc_configure_c_test_target(lc_static_unload_module)
    lc_configure_development_module_runtime(lc_static_unload_module)
    add_dependencies(lc_library_unload_test lc_static_unload_module lc_static_exit_test)
    add_test(NAME runtime_unload_test COMMAND lc_library_unload_test
      $<TARGET_FILE:lc_static_unload_module> runtime-only
      $<TARGET_FILE:lc_static_unload_module>)
    add_test(NAME static_client_unload_test COMMAND lc_library_unload_test
      $<TARGET_FILE:lc_static_unload_module> pouch-encrypted-supplied
      $<TARGET_FILE:lc_static_unload_module>)
    set_tests_properties(runtime_unload_test static_client_unload_test PROPERTIES
      LABELS "integration;offline;memory;e2e;examples" TIMEOUT 30)
    if(LOCKDC_C_TEST_ENVIRONMENT)
      set_tests_properties(runtime_unload_test static_client_unload_test PROPERTIES
        ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
    endif()
  endif()
  if(TARGET lc_shared)
    add_library(lc_client_unload_module MODULE test_client_unload_module.c
      ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
    target_link_libraries(lc_client_unload_module PRIVATE lc_shared
      lc::pslog_shared lc::lonejson_shared lc::liblql_shared lc::curl_shared
      lc::openssl_ssl_shared lc::openssl_crypto_shared lc::nghttp2_shared
      lc::libssh2_shared lc::zlib_shared)
    target_link_options(lc_client_unload_module PRIVATE "LINKER:--no-as-needed")
    lc_configure_c_test_target(lc_client_unload_module)
    lc_configure_development_module_runtime(lc_client_unload_module)
    add_dependencies(lc_library_unload_test lc_client_unload_module)
    add_test(NAME client_unload_test COMMAND lc_library_unload_test
      $<TARGET_FILE:lc_client_unload_module> pouch-encrypted $<TARGET_FILE:lc_shared>)
    set_tests_properties(client_unload_test PROPERTIES
      LABELS "integration;offline;memory;e2e;examples" TIMEOUT 30)
    if(LOCKDC_C_TEST_ENVIRONMENT)
      set_tests_properties(client_unload_test PROPERTIES
        ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
    endif()
  endif()
  add_test(NAME logger_unload_test COMMAND lc_library_unload_test
    $<TARGET_FILE:lc_log_unload_module>)
  set_tests_properties(logger_unload_test PROPERTIES
    LABELS "integration;offline;memory;e2e;examples" TIMEOUT 30)
  if(LOCKDC_C_TEST_ENVIRONMENT)
    set_tests_properties(logger_unload_test PROPERTIES
      ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
  endif()
  find_program(LOCKDC_VALGRIND_BIN valgrind)
  if(LOCKDC_VALGRIND_BIN AND NOT LOCKDC_SANITIZER_INSTRUMENTED)
    if(TARGET lc_static_unload_module)
      add_test(NAME static_unload_test COMMAND ${LOCKDC_VALGRIND_BIN}
        --keep-debuginfo=yes --leak-check=full --show-leak-kinds=all
        --errors-for-leak-kinds=definite,indirect,possible --error-exitcode=97
        $<TARGET_FILE:lc_library_unload_test> $<TARGET_FILE:lc_static_unload_module>
        runtime-only $<TARGET_FILE:lc_static_unload_module>)
      add_test(NAME static_exit_valgrind_test COMMAND ${LOCKDC_VALGRIND_BIN}
        --leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=all
        --error-exitcode=97 $<TARGET_FILE:lc_static_exit_test>)
      add_test(NAME static_client_unload_valgrind_test COMMAND ${LOCKDC_VALGRIND_BIN}
        --keep-debuginfo=yes --leak-check=full --show-leak-kinds=all
        --errors-for-leak-kinds=all --error-exitcode=97
        $<TARGET_FILE:lc_library_unload_test> $<TARGET_FILE:lc_static_unload_module>
        pouch-encrypted-supplied $<TARGET_FILE:lc_static_unload_module>)
      add_test(NAME runtime_key_failure_test COMMAND ${LOCKDC_VALGRIND_BIN}
        --keep-debuginfo=yes --leak-check=full --show-leak-kinds=all
        --errors-for-leak-kinds=all --error-exitcode=97
        $<TARGET_FILE:lc_library_unload_test> $<TARGET_FILE:lc_static_unload_module>
        key-failure $<TARGET_FILE:lc_static_unload_module>)
      set_tests_properties(static_unload_test static_client_unload_valgrind_test
        static_exit_valgrind_test runtime_key_failure_test PROPERTIES
        LABELS "integration;offline;memory" TIMEOUT 30)
    endif()
    add_test(NAME logger_unload_valgrind_test COMMAND ${LOCKDC_VALGRIND_BIN}
      --keep-debuginfo=yes --leak-check=full --show-leak-kinds=all
      --errors-for-leak-kinds=all --error-exitcode=97
      $<TARGET_FILE:lc_library_unload_test> $<TARGET_FILE:lc_log_unload_module>)
    set_tests_properties(logger_unload_valgrind_test PROPERTIES
      LABELS "integration;offline;memory" TIMEOUT 30)
    if(TARGET lc_client_unload_module)
      foreach(mode pouch-default pouch-supplied pouch-supplied-borrowed pouch-failure pouch-failure-supplied
                   pouch-encrypted remote-default remote-supplied remote-supplied-borrowed)
        add_test(NAME client_unload_${mode}_test COMMAND ${LOCKDC_VALGRIND_BIN}
          --keep-debuginfo=yes --leak-check=full --show-leak-kinds=all
          --errors-for-leak-kinds=definite,indirect,possible --error-exitcode=97
          $<TARGET_FILE:lc_library_unload_test> $<TARGET_FILE:lc_client_unload_module> ${mode}
          $<TARGET_FILE:lc_shared>)
        set_tests_properties(client_unload_${mode}_test PROPERTIES
          LABELS "integration;offline;memory" TIMEOUT 30)
      endforeach()
    endif()
  endif()
endif()

if(TARGET lc_static AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(lc_pouch_compaction_stack_test test_pouch_compaction_stack.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_link_libraries(lc_pouch_compaction_stack_test PRIVATE lc_static)
  target_include_directories(lc_pouch_compaction_stack_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  lc_configure_c_test_target(lc_pouch_compaction_stack_test)
  if(LOCKDC_TARGET_LIBC STREQUAL "musl")
    target_link_options(lc_pouch_compaction_stack_test PRIVATE -static)
  endif()
  foreach(mode background small-stack)
    add_test(NAME pouch_compaction_${mode}_test COMMAND lc_pouch_compaction_stack_test ${mode})
    set_tests_properties(pouch_compaction_${mode}_test PROPERTIES
      LABELS "integration;offline;pouch;cross-runtime" RUN_SERIAL TRUE TIMEOUT 60)
    if(LOCKDC_C_TEST_ENVIRONMENT)
      set_tests_properties(pouch_compaction_${mode}_test PROPERTIES
        ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
    endif()
  endforeach()
  set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS
    lc_pouch_compaction_stack_test)
endif()

if(LOCKDC_BUILD_POSTGRES AND LOCKDC_TARGET_ID STREQUAL "x86_64-linux-gnu"
   AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(postgres_xa_executables)
  foreach(variant static shared)
    if(NOT TARGET lc_postgres_${variant})
      continue()
    endif()
    add_executable(lc_integration_postgres_${variant} test_postgres_xa.c)
    target_sources(lc_integration_postgres_${variant} PRIVATE
      ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tracking.c)
    target_link_libraries(lc_integration_postgres_${variant} PRIVATE lc_postgres_${variant})
    lc_configure_c_test_target(lc_integration_postgres_${variant})
    if(variant STREQUAL "static")
      target_compile_definitions(lc_integration_postgres_${variant} PRIVATE LC_POSTGRES_FAULT_TEST=1)
      target_link_options(lc_integration_postgres_${variant} PRIVATE
        "LINKER:--wrap=cpkt_postgres_send_query_params"
        "LINKER:--wrap=cpkt_postgres_get_result_checked"
        "LINKER:--wrap=lc_xa_store_progress_blocking")
    else()
      target_link_libraries(lc_integration_postgres_${variant} PRIVATE
        lc::pslog_shared lc::lonejson_shared lc::liblql_shared)
      target_link_options(lc_integration_postgres_${variant} PRIVATE
        "LINKER:-rpath,${LOCKDC_EXTERNAL_ROOT}/cpktdb/install/lib")
    endif()
    list(APPEND postgres_xa_executables "$<TARGET_FILE:lc_integration_postgres_${variant}>")
  endforeach()
  if(TARGET lc_lua_xa_test)
    foreach(lua_mode embedded module static)
      if(lua_mode STREQUAL "static" AND NOT TARGET lc_postgres_static)
        continue()
      endif()
      set(lua_target lc_integration_lua_postgres)
      if(NOT lua_mode STREQUAL "embedded")
        string(APPEND lua_target "_${lua_mode}")
      endif()
      add_executable(${lua_target}
        test_lua_postgres_xa.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_collections_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_resource_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_provider_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_postgres_lua.c)
      target_include_directories(${lua_target} PRIVATE ${CMAKE_SOURCE_DIR}/src/lua)
      target_include_directories(${lua_target} SYSTEM PRIVATE ${LUA_INCLUDE_DIR})
      target_compile_definitions(${lua_target} PRIVATE
        LOCKDC_SOURCE_ROOT="${CMAKE_SOURCE_DIR}" _POSIX_C_SOURCE=200809L)
      if(lua_mode STREQUAL "static")
        target_link_libraries(${lua_target} PRIVATE lc_postgres_static Lua::Lua)
      else()
        target_link_libraries(${lua_target} PRIVATE lc_postgres_shared Lua::Lua
          lc::curl_shared lc::openssl_ssl_shared lc::openssl_crypto_shared
          lc::nghttp2_shared lc::pslog_shared lc::lonejson_shared
          lc::liblql_shared lc::libssh2_shared lc::zlib_shared)
        target_link_options(${lua_target} PRIVATE "LINKER:--no-as-needed"
          "LINKER:-rpath,${LOCKDC_EXTERNAL_ROOT}/cpktdb/install/lib")
      endif()
      if(lua_mode STREQUAL "module")
        target_compile_definitions(${lua_target} PRIVATE LOCKDC_TEST_LOAD_MODULES=1
          LOCKDC_TEST_LUA_MODULE_PATH="${CMAKE_BINARY_DIR}/lua/?.so")
        target_link_options(${lua_target} PRIVATE "LINKER:--export-dynamic")
        add_dependencies(${lua_target} lockdc_lua_core lockdc_lua_postgres)
      endif()
      lc_configure_c_test_target(${lua_target})
      target_sources(${lua_target} PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tracking.c)
      set_target_properties(${lua_target} PROPERTIES
        C_STANDARD 90 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF)
      list(APPEND postgres_xa_executables "$<TARGET_FILE:${lua_target}>")
    endforeach()
  endif()
  add_test(NAME postgres_xa_native_test
    COMMAND bash "${CMAKE_SOURCE_DIR}/scripts/test-postgres-xa.sh" ${postgres_xa_executables})
  set_tests_properties(postgres_xa_native_test PROPERTIES
    LABELS "integration;postgres;podman" RUN_SERIAL TRUE TIMEOUT 240
    ENVIRONMENT "LOCKDC_TEST_ALLOCATOR_TRACKING=1")
  foreach(server_version 15 16 17 18)
    add_test(NAME postgres_xa_pg${server_version}_test
      COMMAND bash "${CMAKE_SOURCE_DIR}/scripts/test-postgres-xa.sh"
        --server-version ${server_version} ${postgres_xa_executables})
    set_tests_properties(postgres_xa_pg${server_version}_test PROPERTIES
      LABELS "integration;postgres;podman;native-behavior" RUN_SERIAL TRUE TIMEOUT 240
      ENVIRONMENT "LOCKDC_TEST_ALLOCATOR_TRACKING=1")
  endforeach()
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND LOCKDC_TARGET_ARCH STREQUAL "x86_64")
  foreach(linkage runner static shared)
    if(NOT linkage STREQUAL "runner" AND NOT TARGET lc_${linkage})
      continue()
    endif()
    set(target lc_default_stack_${linkage}_test)
    add_executable(${target} test_default_stack.c)
    target_link_libraries(${target} PRIVATE Threads::Threads)
    if(NOT linkage STREQUAL "runner")
      set_property(TARGET ${target} PROPERTY LOCKDC_TEST_STACK_FROM_SDK TRUE)
      target_compile_definitions(${target} PRIVATE LC_TEST_STACK_FROM_SDK=1)
      target_link_libraries(${target} PRIVATE lc_${linkage})
      if(linkage STREQUAL "shared")
        # The shipped DSO uses $ORIGIN. Declare its dependencies directly in
        # this build-tree application so its private toolchain RPATH applies.
        get_target_property(shared_dependencies lc_shared LINK_LIBRARIES)
        target_link_libraries(${target} PRIVATE ${shared_dependencies})
        target_link_options(${target} PRIVATE "LINKER:--no-as-needed")
      endif()
    endif()
    lc_configure_c_test_target(${target})
    add_test(NAME default_worker_stack_${linkage}_test COMMAND ${target})
    set_tests_properties(default_worker_stack_${linkage}_test PROPERTIES
      LABELS "integration;offline;cross-runtime;stack" TIMEOUT 30)
    if(LOCKDC_C_TEST_ENVIRONMENT)
      set_tests_properties(default_worker_stack_${linkage}_test PROPERTIES
        ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
    endif()
    set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS ${target})
  endforeach()
endif()

lc_apply_test_defaults()
