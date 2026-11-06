if(LOCKDC_BUILD_POSTGRES AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(lc_unit_cpkt_postgres_checked test_cpkt_postgres_checked.c)
  target_link_libraries(lc_unit_cpkt_postgres_checked PRIVATE cpkt::postgres)
  target_link_options(lc_unit_cpkt_postgres_checked PRIVATE
    "LINKER:--wrap=malloc" "LINKER:--wrap=PQgetResult" "LINKER:--wrap=PQclear"
    "LINKER:--wrap=PQstatus" "LINKER:--wrap=PQresultErrorField"
    "LINKER:--wrap=PQresultErrorMessage")
  lc_configure_c_test_target(lc_unit_cpkt_postgres_checked)
  set_target_properties(lc_unit_cpkt_postgres_checked PROPERTIES
    C_STANDARD 90 C_EXTENSIONS OFF)
  target_compile_options(lc_unit_cpkt_postgres_checked PRIVATE -pedantic-errors)
  add_test(NAME lc_unit_cpkt_postgres_checked COMMAND lc_unit_cpkt_postgres_checked)
  set_tests_properties(lc_unit_cpkt_postgres_checked PROPERTIES
    LABELS "unit;cross-runtime" TIMEOUT 30)
  set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS
    lc_unit_cpkt_postgres_checked)
endif()

include("${CMAKE_CURRENT_SOURCE_DIR}/portability.cmake")

function(lc_add_unit_test target_name)
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
  set(cases)
  if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(cases ${lc_portability_${target_name}})
  endif()
  if(cases)
    list(GET ARGN 0 source)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/${source}" case_source)
    foreach(case IN LISTS cases)
      if(NOT case_source MATCHES "cmocka_unit_test[a-z_]*\\([ \r\n]*${case}[ ,)\r\n]")
        message(FATAL_ERROR "Unknown portability case for ${target_name}: ${case}")
      endif()
    endforeach()
  endif()
  set(test_names ${target_name})
  if(target_name STREQUAL "lc_unit_collections")
    # Partition the selected table in every configuration. QEMU portability
    # selections need the same bounded fork/fsync work as the full native table.
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
      "${CMAKE_CURRENT_SOURCE_DIR}/test_lc_collections.c")
    set(selected_cases ${cases})
    if(NOT selected_cases)
      file(READ "${CMAKE_CURRENT_SOURCE_DIR}/test_lc_collections.c" case_source)
      string(REGEX MATCHALL "cmocka_unit_test_setup_teardown\\([ \r\n]*[A-Za-z0-9_]+"
        registrations "${case_source}")
      foreach(registration IN LISTS registrations)
        string(REGEX REPLACE ".*\\([ \r\n]*" "" case "${registration}")
        list(APPEND selected_cases ${case})
      endforeach()
    endif()
    list(APPEND test_names ${target_name}_append ${target_name}_recovery)
    set(cases)
    set(append_cases)
    set(recovery_cases)
    foreach(case IN LISTS selected_cases)
      if(case MATCHES "^test_push_")
        list(APPEND append_cases ${case})
      elseif(case MATCHES "migration|compaction|transforms|^test_segment_seals_")
        list(APPEND recovery_cases ${case})
      else()
        list(APPEND cases ${case})
      endif()
    endforeach()
    foreach(group cases append_cases recovery_cases)
      if(NOT ${group})
        message(FATAL_ERROR "Empty collection test partition: ${group}")
      endif()
    endforeach()
    add_test(NAME ${target_name} COMMAND ${target_name} ${cases})
    add_test(NAME ${target_name}_append COMMAND ${target_name} ${append_cases})
    add_test(NAME ${target_name}_recovery COMMAND ${target_name} ${recovery_cases})
  elseif(target_name STREQUAL "lc_unit_collection_staged")
    # Keep all producer cases on every runtime, but separate bounded-memory
    # and conversion work so the cumulative QEMU job stays below 60 seconds.
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
      "${CMAKE_CURRENT_SOURCE_DIR}/test_lc_collection_staged.c")
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/test_lc_collection_staged.c" case_source)
    string(REGEX MATCHALL "cmocka_unit_test_setup_teardown\\([ \r\n]*[A-Za-z0-9_]+"
      registrations "${case_source}")
    set(streaming_cases)
    set(conversion_cases)
    foreach(registration IN LISTS registrations)
      string(REGEX REPLACE ".*\\([ \r\n]*" "" case "${registration}")
      if(case MATCHES "^test_stream_units_survive_compression_conversion$")
        list(APPEND conversion_cases ${case})
      elseif(case MATCHES "^test_push_is_canonical_|^test_stream_units_survive_rolling_")
        list(APPEND streaming_cases ${case})
      else()
        list(APPEND cases ${case})
      endif()
    endforeach()
    foreach(group cases streaming_cases conversion_cases)
      if(NOT ${group})
        message(FATAL_ERROR "Empty staged collection partition: ${group}")
      endif()
    endforeach()
    list(APPEND test_names ${target_name}_streaming ${target_name}_conversion)
    add_test(NAME ${target_name} COMMAND ${target_name} ${cases})
    add_test(NAME ${target_name}_streaming COMMAND ${target_name} ${streaming_cases})
    add_test(NAME ${target_name}_conversion COMMAND ${target_name} ${conversion_cases})
  elseif(target_name STREQUAL "lc_unit_pouch_compression")
    # The native table includes independent codec and journal durability
    # scenarios. Keep each bounded CTest job under the standard limit, with
    # no duplicated cases or extra executable/build configuration.
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
      "${CMAKE_CURRENT_SOURCE_DIR}/test_lc_pouch_compression.c")
    set(selected_cases ${cases})
    if(NOT selected_cases)
      file(READ "${CMAKE_CURRENT_SOURCE_DIR}/test_lc_pouch_compression.c" case_source)
      string(REGEX MATCHALL "cmocka_unit_test_setup_teardown\\([ \r\n]*[A-Za-z0-9_]+"
        registrations "${case_source}")
      foreach(registration IN LISTS registrations)
        string(REGEX REPLACE ".*\\([ \r\n]*" "" case "${registration}")
        list(APPEND selected_cases ${case})
      endforeach()
    endif()
    set(cases)
    set(durability_cases)
    set(recovery_cases)
    foreach(case IN LISTS selected_cases)
      if(case MATCHES "crashes|power_loss|unsynced|recovered_|failed_payload_sync")
        list(APPEND durability_cases ${case})
      elseif(case MATCHES "private_segment|private_journal|prepared_segment|installed_segment|reference_failure|head_publication|repeated_failures|partial_output|opposite_request|inventory_resume|interrupted_index|readonly_sealed")
        list(APPEND recovery_cases ${case})
      else()
        list(APPEND cases ${case})
      endif()
    endforeach()
    if(NOT cases OR NOT durability_cases OR NOT recovery_cases)
      message(FATAL_ERROR "Empty compression test partition")
    endif()
    list(APPEND test_names ${target_name}_durability ${target_name}_recovery)
    add_test(NAME ${target_name} COMMAND ${target_name} ${cases})
    add_test(NAME ${target_name}_durability COMMAND ${target_name} ${durability_cases})
    add_test(NAME ${target_name}_recovery COMMAND ${target_name} ${recovery_cases})
  else()
    add_test(NAME ${target_name} COMMAND ${target_name} ${cases})
  endif()
  # Every portable unit executable runs on each Linux target. The large
  # storage tables use named portability cases in optimized configurations.
  set_tests_properties(${test_names} PROPERTIES
    LABELS "unit;cross-runtime"
    TIMEOUT 60)
  if(cases AND CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(TEST ${test_names} APPEND PROPERTY LABELS portability)
  endif()
  set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS
    ${target_name})
  if(LOCKDC_C_TEST_ENVIRONMENT)
    set_tests_properties(${test_names} PROPERTIES
      ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
  endif()
endfunction()

lc_add_unit_test(lc_unit_streams test_lc_streams.c)
lc_add_unit_test(lc_unit_stream_scratch test_lc_stream_scratch.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
lc_add_unit_test(lc_unit_pouch_query_terms test_lc_pouch_query_terms.c)
lc_add_unit_test(lc_unit_collection_index test_lc_collection_index.c)
lc_add_unit_test(lc_unit_collection_order test_lc_collection_order.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
lc_add_unit_test(lc_unit_collection_json test_lc_collection_json.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
lc_add_unit_test(lc_unit_collection_record test_lc_collection_record.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
lc_add_unit_test(lc_unit_pouch_reader test_lc_pouch_reader.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
lc_add_unit_test(lc_unit_pouch_writer test_lc_pouch_writer.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_pouch_writer PRIVATE LOCKDC_TEST_BUILD=1)
target_compile_definitions(lc_unit_collection_record PRIVATE LOCKDC_TEST_BUILD=1)
lc_add_unit_test(lc_unit_collection_ids test_lc_collection_ids.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
lc_add_unit_test(lc_unit_collection_edits test_lc_collection_edits.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_collection_edits PRIVATE LOCKDC_TEST_BUILD=1
  LC_COLLECTION_TMP_PARENT="${CMAKE_CURRENT_BINARY_DIR}")
lc_add_unit_test(lc_unit_collections test_lc_collections.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_collections PRIVATE LOCKDC_TEST_BUILD=1)
lc_add_unit_test(lc_unit_collection_sync test_lc_collection_sync.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_collection_sync PRIVATE LOCKDC_TEST_BUILD=1)
lc_add_unit_test(lc_unit_collection_staged test_lc_collection_staged.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_collection_staged PRIVATE LOCKDC_TEST_BUILD=1)
lc_add_unit_test(lc_unit_mixed_storage test_lc_mixed_storage.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_mixed_storage PRIVATE LOCKDC_TEST_BUILD=1)
foreach(collection_test lc_unit_collection_order lc_unit_collection_json lc_unit_collection_record lc_unit_pouch_reader lc_unit_pouch_writer lc_unit_collection_ids lc_unit_collections lc_unit_collection_sync lc_unit_collection_staged lc_unit_mixed_storage)
  target_compile_definitions(${collection_test} PRIVATE
    LC_COLLECTION_TMP_PARENT="${CMAKE_CURRENT_BINARY_DIR}")
endforeach()
add_executable(lc_allocator_exit_probe test_lc_allocator_exit.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
if(TARGET lc_static)
  target_link_libraries(lc_allocator_exit_probe PRIVATE lc_static)
else()
  target_link_libraries(lc_allocator_exit_probe PRIVATE ${lc_test_link_target})
endif()
target_include_directories(lc_allocator_exit_probe PRIVATE ${CMAKE_SOURCE_DIR}/src)
lc_configure_c_test_target(lc_allocator_exit_probe)
add_executable(lc_allocator_public_exit_probe test_lc_allocator_public_exit.c)
if(TARGET lc_shared)
  target_link_libraries(lc_allocator_public_exit_probe PRIVATE lc_shared)
else()
  target_link_libraries(lc_allocator_public_exit_probe PRIVATE lc_static)
endif()
lc_configure_c_test_target(lc_allocator_public_exit_probe)
add_dependencies(lc_allocator_exit_probe lc_allocator_public_exit_probe)
set(allocator_probe_command ${CMAKE_CROSSCOMPILING_EMULATOR} $<TARGET_FILE:lc_allocator_exit_probe>)
set(allocator_public_probe_command ${CMAKE_CROSSCOMPILING_EMULATOR})
if(LOCKDC_DEVELOPMENT_RUNTIME_LOADER)
  # The production DSO uses $ORIGIN; development dependencies live in separate
  # prefixes. Supply them to this loader invocation without ambient overrides.
  list(JOIN LOCKDC_DEVELOPMENT_RUNTIME_DIRS ":" allocator_public_library_path)
  list(APPEND allocator_public_probe_command "${LOCKDC_DEVELOPMENT_RUNTIME_LOADER}"
    --library-path "${allocator_public_library_path}")
endif()
list(APPEND allocator_public_probe_command $<TARGET_FILE:lc_allocator_public_exit_probe>)
add_test(NAME lc_allocator_exit_probe COMMAND "${CMAKE_COMMAND}"
  "-DPROBE_COMMAND=${allocator_probe_command}"
  "-DPUBLIC_PROBE_COMMAND=${allocator_public_probe_command}"
  -P "${CMAKE_SOURCE_DIR}/tests/allocator_exit_report_test.cmake")
set_tests_properties(lc_allocator_exit_probe PROPERTIES
  LABELS "unit;allocator;cross-runtime" TIMEOUT 60)
if(LOCKDC_C_TEST_ENVIRONMENT)
  set_tests_properties(lc_allocator_exit_probe PROPERTIES ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
endif()
set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS lc_allocator_exit_probe)
add_executable(lc_unit_tmp_cleanup_probe
  test_lc_tmp_cleanup.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)
lc_configure_c_test_target(lc_unit_tmp_cleanup_probe)
set(lc_unit_tmp_cleanup_probe_command "$<TARGET_FILE:lc_unit_tmp_cleanup_probe>")
if(CMAKE_CROSSCOMPILING AND CMAKE_CROSSCOMPILING_EMULATOR)
  set(lc_unit_tmp_cleanup_probe_command
    ${CMAKE_CROSSCOMPILING_EMULATOR}
    "$<TARGET_FILE:lc_unit_tmp_cleanup_probe>"
  )
endif()
add_test(NAME lc_unit_tmp_cleanup_signal
  COMMAND "${CMAKE_COMMAND}"
    "-DLOCKDC_TMP_CLEANUP_PROBE_COMMAND=${lc_unit_tmp_cleanup_probe_command}"
    -DLOCKDC_TMP_PATH_FILE=${CMAKE_BINARY_DIR}/tmp-cleanup-signal.path
    -P "${CMAKE_SOURCE_DIR}/tests/tmp_cleanup_signal_test.cmake"
)
set_tests_properties(lc_unit_tmp_cleanup_signal PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_abort
  COMMAND "${CMAKE_COMMAND}"
    "-DLOCKDC_TMP_CLEANUP_PROBE_COMMAND=${lc_unit_tmp_cleanup_probe_command}"
    -DLOCKDC_TMP_PATH_FILE=${CMAKE_BINARY_DIR}/tmp-cleanup-abort.path
    -DLOCKDC_TMP_CLEANUP_SIGNAL=ABRT
    -P "${CMAKE_SOURCE_DIR}/tests/tmp_cleanup_signal_test.cmake"
)
set_tests_properties(lc_unit_tmp_cleanup_abort PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_auto_stale
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=auto-stale"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_auto_stale PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_explicit_global_stale
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=explicit-global-stale"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_explicit_global_stale PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_orphan_owner_marker
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=orphan-owner-marker"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_orphan_owner_marker PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_partial_owner_marker
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=partial-owner-marker"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_partial_owner_marker PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_unmarked_global_prefix
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=unmarked-global-prefix"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_unmarked_global_prefix PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
lc_add_unit_test(lc_unit_engine_allocator test_lc_engine_allocator.c)
lc_add_unit_test(lc_unit_contracts test_lc_contracts.c mock_lc_public.c)
lc_add_unit_test(lc_unit_xid_atfork test_lc_xid_atfork.c)
target_link_options(lc_unit_xid_atfork PRIVATE
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_atfork>
)
lc_add_unit_test(lc_unit_public_mock_usage test_lc_public_mock_usage.c mock_lc_public.c)
lc_add_unit_test(lc_unit_consumer_service test_lc_consumer_service.c)
lc_add_unit_test(lc_unit_outbox test_lc_outbox.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_outbox PRIVATE LOCKDC_TEST_BUILD=1)
# The outbox suite intentionally exercises durable expiry/recovery paths.
# Its bounded waits can exceed the generic 60-second unit cap; use the
# project-wide 120-second default rather than a 90-second near-boundary cutoff.
set_tests_properties(lc_unit_outbox PROPERTIES TIMEOUT 120)
target_link_options(lc_unit_consumer_service PRIVATE
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_client_open>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_engine_client_subscribe>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_engine_client_subscribe_with_state>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_message_new>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_create>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_join>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_mutex_init>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_mutex_destroy>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_cond_init>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_cond_destroy>
)
lc_add_unit_test(lc_unit_runtime test_lc_runtime.c)
target_link_options(lc_unit_runtime PRIVATE
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lonejson_new>
)
lc_add_unit_test(lc_unit_mutate_stream test_lc_mutate_stream.c)
target_sources(lc_unit_mutate_stream PRIVATE
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)
lc_add_unit_test(lc_unit_pouch test_lc_pouch.c)
lc_add_unit_test(lc_unit_pouch_cleanup test_lc_pouch_cleanup.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_pouch_cleanup PRIVATE LOCKDC_TEST_BUILD=1)
target_link_options(lc_unit_pouch_cleanup PRIVATE
  "LINKER:--wrap=unlink" "LINKER:--wrap=fsync")
lc_add_unit_test(lc_unit_pouch_compression test_lc_pouch_compression.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_pouch_compression PRIVATE LOCKDC_TEST_BUILD=1
  LC_COLLECTION_TMP_PARENT="${CMAKE_CURRENT_BINARY_DIR}")
target_link_options(lc_unit_pouch_compression PRIVATE "LINKER:--wrap=fsync")
lc_add_unit_test(lc_unit_state_retention test_lc_state_retention.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_state_retention PRIVATE LOCKDC_TEST_BUILD=1)
target_link_options(lc_unit_state_retention PRIVATE "LINKER:--wrap=fsync")
lc_add_unit_test(lc_unit_pouch_xa test_lc_pouch_xa.c)
target_compile_definitions(lc_unit_pouch_xa PRIVATE LOCKDC_TEST_BUILD=1)
lc_add_unit_test(lc_unit_xa_borrow test_lc_xa_borrow.c)
function(lc_add_tracking_case name target filter)
  # Native cases reuse existing binaries. The production allocator probe
  # exercises both settings in every cross-runtime configuration.
  add_test(NAME ${name} COMMAND ${target})
  set_tests_properties(${name} PROPERTIES
    LABELS "unit;allocator" TIMEOUT 30
    ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT};LOCKDC_TEST_ALLOCATOR_TRACKING=1;CMOCKA_TEST_FILTER=${filter}"
    PASS_REGULAR_EXPRESSION "\\[  PASSED  \\] 1 test")
endfunction()
lc_add_tracking_case(lc_allocator_pouch_compaction lc_unit_pouch
  test_compaction_reclaims_expired_obsolete_files)
lc_add_tracking_case(lc_allocator_outbox_lifecycle lc_unit_outbox
  test_pouch_outbox_stateful_delivery_lifecycle)
lc_add_tracking_case(lc_allocator_outbox_renewal lc_unit_outbox
  test_pouch_outbox_stateful_renewal_terminal_versions)
lc_add_tracking_case(lc_allocator_outbox_renewal_failure lc_unit_outbox
  test_pouch_outbox_stateful_renewal_terminal_failure)
lc_add_tracking_case(lc_allocator_pouch_keepalive_versions lc_unit_pouch
  test_pouch_keepalive_preserves_lease_and_body_versions)
add_test(NAME lc_test_case_selection_test COMMAND "${CMAKE_COMMAND}"
  "-DLOCKDC_TEST_RUNNER=${CMAKE_CROSSCOMPILING_EMULATOR}"
  -DLOCKDC_UNIT_DIR=${CMAKE_CURRENT_BINARY_DIR}
  -P "${CMAKE_SOURCE_DIR}/tests/test_case_selection_test.cmake")
set_tests_properties(lc_test_case_selection_test PROPERTIES
  LABELS "unit;lifecycle-host" TIMEOUT 30)
target_link_options(lc_unit_pouch PRIVATE -Wl,--wrap=time)
if(CMAKE_CROSSCOMPILING AND CMAKE_CROSSCOMPILING_EMULATOR)
  set(lc_pouch_cross_emulator ${CMAKE_CROSSCOMPILING_EMULATOR})
  list(GET lc_pouch_cross_emulator 0 lc_pouch_cross_emulator_program)
  if(NOT IS_ABSOLUTE "${lc_pouch_cross_emulator_program}")
    find_program(lc_pouch_cross_emulator_program_path
      NAMES "${lc_pouch_cross_emulator_program}"
      REQUIRED
    )
    list(REMOVE_AT lc_pouch_cross_emulator 0)
    list(INSERT lc_pouch_cross_emulator 0
      "${lc_pouch_cross_emulator_program_path}"
    )
  endif()
  string(REPLACE ";" "|" lc_pouch_cross_emulator_spec
    "${lc_pouch_cross_emulator}")
  target_compile_definitions(lc_unit_pouch PRIVATE
    "LOCKDC_TEST_CROSS_EMULATOR_SPEC=\"${lc_pouch_cross_emulator_spec}\""
  )
endif()
# Pouch combines logstore recovery and durability cases in one debug target;
# its debug execution exceeds the default unit-test window.
set_tests_properties(lc_unit_pouch PROPERTIES TIMEOUT 300)
target_sources(lc_unit_pouch PRIVATE
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)
target_compile_definitions(lc_unit_pouch PRIVATE LOCKDC_TEST_BUILD=1)
target_sources(lc_unit_streams PRIVATE
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)

add_executable(lc_unit_transport_https
  test_lc_transport_https.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_link_libraries(lc_unit_transport_https PRIVATE
  ${lc_test_link_target} lc::cmocka lc::openssl_ssl_shared lc::openssl_crypto_shared Threads::Threads)
target_include_directories(lc_unit_transport_https PRIVATE
  ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
target_link_options(lc_unit_transport_https PRIVATE
  "LINKER:--wrap=lc_strdup_local" "LINKER:--wrap=lc_client_strdup")
lc_configure_c_test_target(lc_unit_transport_https)
if(LOCKDC_ENABLE_COVERAGE)
  target_compile_options(lc_unit_transport_https PRIVATE
    $<$<COMPILE_LANG_AND_ID:C,GNU,Clang>:--coverage -O0 -g>)
  target_link_options(lc_unit_transport_https PRIVATE $<$<LINK_LANGUAGE:C>:--coverage>)
endif()
set(https_cases
    test_public_retention_remote_options_and_observations
    test_retention_remote_rejects_invalid_headers
    test_client_open_accepts_memory_bundle_source
    test_public_client_namespaced_load_rejects_empty_namespace
    test_state_transport_paths_use_mtls
    test_state_transport_parses_buffered_typed_json_response
    test_queue_transport_paths_use_mtls
    test_watch_stream_filters_events_and_finishes_trailing_event
    test_public_lease_attach_retries_node_passive_and_cleans_parser_state
    test_engine_request_deadline_bounds_node_passive_failover
    test_public_attachment_get_preserves_i64_timestamp_headers
    test_enqueue_from_checks_public_metadata_range
    test_public_query_keys_streams_chunks_and_headers
    test_public_query_keys_accepts_empty_results)
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/test_lc_transport_https.c" https_source)
foreach(https_case IN LISTS https_cases)
  if(NOT https_source MATCHES "cmocka_unit_test\\([ \r\n]*${https_case}\\)")
    message(FATAL_ERROR "Unknown cross-runtime HTTPS case: ${https_case}")
  endif()
endforeach()
if(CMAKE_SYSTEM_PROCESSOR STREQUAL CMAKE_HOST_SYSTEM_PROCESSOR)
  # Native GNU and musl run the entire table; QEMU keeps its existing subset.
  set(https_cases)
endif()
add_test(NAME lc_unit_transport_https COMMAND lc_unit_transport_https ${https_cases})
set_tests_properties(lc_unit_transport_https PROPERTIES
  LABELS "unit;transport;cross-runtime" TIMEOUT 180)
if(LOCKDC_C_TEST_ENVIRONMENT)
  set_tests_properties(lc_unit_transport_https PROPERTIES ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
endif()
set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS lc_unit_transport_https)

# Validate unit registration before the parent assembles the runtime build
# closure shared with integration tests. Partitioned suites share one target.
get_property(lc_cross_runtime_test_targets GLOBAL PROPERTY
  LOCKDC_CROSS_RUNTIME_TEST_TARGETS)
get_property(lc_registered_unit_tests DIRECTORY PROPERTY TESTS)
foreach(lc_registered_test IN LISTS lc_registered_unit_tests)
  get_test_property(${lc_registered_test} LABELS lc_registered_test_labels)
  if("cross-runtime" IN_LIST lc_registered_test_labels)
    set(lc_registered_target ${lc_registered_test})
    if(lc_registered_test MATCHES "^lc_unit_collections_(append|recovery)$")
      set(lc_registered_target lc_unit_collections)
    elseif(lc_registered_test MATCHES "^lc_unit_collection_staged_(streaming|conversion)$")
      set(lc_registered_target lc_unit_collection_staged)
    elseif(lc_registered_test MATCHES "^lc_unit_pouch_compression_(durability|recovery)$")
      set(lc_registered_target lc_unit_pouch_compression)
    endif()
    if(NOT TARGET ${lc_registered_target})
      message(FATAL_ERROR
        "cross-runtime test ${lc_registered_test} has no executable target")
    endif()
    if(NOT lc_registered_target IN_LIST lc_cross_runtime_test_targets)
      message(FATAL_ERROR
        "cross-runtime test ${lc_registered_test} is missing from the QEMU build closure")
    endif()
  endif()
endforeach()
lc_apply_test_defaults()
