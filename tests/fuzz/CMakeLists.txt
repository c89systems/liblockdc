if(NOT TARGET lc_static)
  message(FATAL_ERROR "Fuzz targets require lc_static.")
endif()

function(lc_add_afl_fuzz_target target_name)
  add_executable(${target_name}
    afl_fuzzer_main.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
    ${ARGN}
  )
  target_link_libraries(${target_name} PRIVATE lc_static)
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
  lc_configure_c_test_target(${target_name})
endfunction()

function(lc_add_pouch_integration_target target_name)
  add_executable(${target_name}
    pouch_integration_driver_main.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
    ${ARGN}
  )
  target_link_libraries(${target_name} PRIVATE lc_static)
  target_compile_definitions(${target_name} PRIVATE
    LC_POUCH_INTEGRATION_MULTIFILE=$<BOOL:${LOCKDC_ENABLE_POUCH_MULTIFILE}>)
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
  lc_configure_c_test_target(${target_name})
endfunction()

function(lc_add_afl_fuzz_smoke target_name corpus_name corpus_seed)
  add_test(NAME ${target_name}_smoke
    COMMAND ${target_name}
      "${LC_AFL_FUZZ_CORPUS_DIR}/${corpus_name}/${corpus_seed}")
  set_tests_properties(${target_name}_smoke PROPERTIES LABELS fuzz)
endfunction()

if(LOCKDC_BUILD_FUZZERS)
  # AFL++ is reserved for deterministic parser, stream, and primitive targets.
  lc_add_afl_fuzz_target(lc_fuzz_streams fuzz_lc_streams.c)
  lc_add_afl_fuzz_target(lc_fuzz_bundle_open fuzz_lc_bundle_open.c)
  lc_add_afl_fuzz_target(lc_fuzz_attachment_decode fuzz_lc_attachment_decode.c)
  lc_add_afl_fuzz_target(lc_fuzz_queue_meta fuzz_lc_queue_meta.c)
  lc_add_afl_fuzz_target(lc_fuzz_query_keys_stream fuzz_lc_query_keys_stream.c)
  lc_add_afl_fuzz_target(lc_fuzz_mutate_parse fuzz_lc_mutate_parse.c)
  lc_add_afl_fuzz_target(lc_fuzz_mutate_apply fuzz_lc_mutate_apply.c)
  lc_add_afl_fuzz_target(lc_fuzz_pouch_record_header fuzz_lc_pouch_record_header.c)
  lc_add_afl_fuzz_target(lc_fuzz_pouch_index_primitives fuzz_lc_pouch_index_primitives.c)
  add_custom_target(lockdc_fuzz_tests DEPENDS
    lc_fuzz_streams lc_fuzz_bundle_open lc_fuzz_attachment_decode
    lc_fuzz_queue_meta lc_fuzz_query_keys_stream lc_fuzz_mutate_parse
    lc_fuzz_mutate_apply lc_fuzz_pouch_record_header lc_fuzz_pouch_index_primitives)

  set(LC_AFL_FUZZ_CORPUS_DIR "${CMAKE_CURRENT_BINARY_DIR}/corpus")
  file(REMOVE_RECURSE "${LC_AFL_FUZZ_CORPUS_DIR}")
  file(MAKE_DIRECTORY "${LC_AFL_FUZZ_CORPUS_DIR}")
  foreach(corpus_name
      streams bundle_open attachment_decode queue_meta query_keys_stream
      mutate_parse mutate_apply pouch_record_header pouch_index_primitives)
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/corpus/${corpus_name}"
      DESTINATION "${LC_AFL_FUZZ_CORPUS_DIR}")
  endforeach()

  lc_add_afl_fuzz_smoke(lc_fuzz_streams streams seed.json)
  lc_add_afl_fuzz_smoke(lc_fuzz_bundle_open bundle_open seed.pem)
  lc_add_afl_fuzz_smoke(lc_fuzz_attachment_decode attachment_decode attach.json)
  lc_add_afl_fuzz_smoke(lc_fuzz_queue_meta queue_meta meta.json)
  lc_add_afl_fuzz_smoke(lc_fuzz_query_keys_stream query_keys_stream two_keys)
  lc_add_afl_fuzz_smoke(lc_fuzz_mutate_parse mutate_parse
    02d6b5dc5e0c4b854ad463736de6ba15229e10fa)
  lc_add_afl_fuzz_smoke(lc_fuzz_mutate_apply mutate_apply
    105064699aa30295ea0a31a6c95d2a7775a230cb)
  lc_add_afl_fuzz_smoke(lc_fuzz_pouch_record_header pouch_record_header
    ascii-invalid.bin)
  lc_add_afl_fuzz_smoke(lc_fuzz_pouch_index_primitives pouch_index_primitives
    term-generation.txt)
endif()

if(LOCKDC_BUILD_POUCH_INTEGRATION_FUZZERS)
  # These execute the unmodified production implementation in a fresh process
  # per mutation. They intentionally do not use AFL++ instrumentation or
  # coverage feedback because Pouch owns concurrent background work.
  lc_add_pouch_integration_target(lc_pouch_integration_lql_plan
    fuzz_lc_pouch_lql_plan.c)
  lc_add_pouch_integration_target(lc_pouch_integration_lifecycle
    fuzz_lc_pouch_lifecycle.c)
  lc_add_pouch_integration_target(lc_pouch_integration_outbox_dispatcher
    fuzz_lc_pouch_outbox_dispatcher.c)

  add_executable(lc_pouch_integration_mutation_runner
    pouch_integration_mutation_runner.c)
  lc_configure_c_test_target(lc_pouch_integration_mutation_runner)

  add_executable(lc_pouch_integration_hang_probe
    pouch_integration_hang_probe.c)
  lc_configure_c_test_target(lc_pouch_integration_hang_probe)

  add_executable(lc_pouch_integration_success_probe
    pouch_integration_success_probe.c)
  lc_configure_c_test_target(lc_pouch_integration_success_probe)
  add_custom_target(lockdc_pouch_integration_tests DEPENDS
    lc_pouch_integration_lql_plan lc_pouch_integration_lifecycle
    lc_pouch_integration_outbox_dispatcher lc_pouch_integration_mutation_runner
    lc_pouch_integration_hang_probe lc_pouch_integration_success_probe)

  add_test(NAME lc_pouch_integration_lql_plan_seed
    COMMAND lc_pouch_integration_lql_plan
      "${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_lql_plan/lql_full_text_any")
  add_test(NAME lc_pouch_integration_lifecycle_seed
    COMMAND lc_pouch_integration_lifecycle
      "${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_lifecycle/maintenance.bin")
  add_test(NAME lc_pouch_integration_outbox_dispatcher_seed
    COMMAND lc_pouch_integration_outbox_dispatcher
      "${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_outbox_dispatcher/seed.bin")
  set_tests_properties(
    lc_pouch_integration_lql_plan_seed
    lc_pouch_integration_lifecycle_seed
    lc_pouch_integration_outbox_dispatcher_seed
    PROPERTIES LABELS "fuzz;integration;offline" TIMEOUT 30
    ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")

  add_test(NAME lc_pouch_integration_lql_plan_mutation_smoke
    COMMAND lc_pouch_integration_mutation_runner
      $<TARGET_FILE:lc_pouch_integration_lql_plan>
      "${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_lql_plan" 8)
  add_test(NAME lc_pouch_integration_lifecycle_mutation_smoke
    COMMAND lc_pouch_integration_mutation_runner
      $<TARGET_FILE:lc_pouch_integration_lifecycle>
      "${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_lifecycle" 8)
  add_test(NAME lc_pouch_integration_outbox_dispatcher_mutation_smoke
    COMMAND lc_pouch_integration_mutation_runner
      $<TARGET_FILE:lc_pouch_integration_outbox_dispatcher>
      "${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_outbox_dispatcher" 8)
  add_test(NAME lc_pouch_integration_mutation_runner_regression
    COMMAND ${CMAKE_COMMAND}
      -DRUNNER=$<TARGET_FILE:lc_pouch_integration_mutation_runner>
      -DHANG_PROBE=$<TARGET_FILE:lc_pouch_integration_hang_probe>
      -DSUCCESS_PROBE=$<TARGET_FILE:lc_pouch_integration_success_probe>
      -DCORPUS=${CMAKE_CURRENT_SOURCE_DIR}/corpus/pouch_lifecycle
      -DTEST_ROOT=${CMAKE_CURRENT_BINARY_DIR}/pouch-integration-runner-regression
      -P ${CMAKE_CURRENT_SOURCE_DIR}/pouch_integration_mutation_runner_test.cmake)
  set_tests_properties(
    lc_pouch_integration_lql_plan_mutation_smoke
    lc_pouch_integration_lifecycle_mutation_smoke
    lc_pouch_integration_outbox_dispatcher_mutation_smoke
    lc_pouch_integration_mutation_runner_regression
    PROPERTIES LABELS "fuzz;integration;offline" TIMEOUT 120 RUN_SERIAL TRUE)
endif()

lc_apply_test_defaults()
