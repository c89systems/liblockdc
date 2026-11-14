# Native-only experimental target, never installed or linked into the product.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$" OR
   NOT LOCKDC_TARGET_ARCH STREQUAL "x86_64" OR NOT TARGET lc_static)
  return()
endif()

add_library(pouch_space_experiment STATIC space.c directory.c stream.c catalog.c record_io.c control.c root.c namespace.c namespace_index.c namespace_snapshot.c pouch.c maintenance.c)
target_include_directories(pouch_space_experiment PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
  PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(pouch_space_experiment PUBLIC lc_static)
lc_configure_c_target(pouch_space_experiment)

if(LOCKDC_BUILD_BENCHMARKS)
  add_executable(pouch_space_control_compare control_compare.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_control_compare PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_control_compare PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_control_compare PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync")
  lc_configure_c_target(pouch_space_control_compare)
  if(LOCKDC_BUILD_TESTS)
    foreach(mode file space)
      add_test(NAME pouch_space_control_compare_${mode}
        COMMAND pouch_space_control_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 16)
      set_tests_properties(pouch_space_control_compare_${mode} PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endif()
  add_executable(pouch_space_compare compare.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_compare PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_compare PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_compare PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync")
  lc_configure_c_target(pouch_space_compare)
  if(LOCKDC_BUILD_TESTS)
    foreach(mode space-state stream-state runtime-state pouch-state runtime-metadata pouch-metadata runtime-delete pouch-delete runtime-compact pouch-compact runtime-read-many pouch-read-many runtime-read-points pouch-read-points runtime-scan-bodies pouch-scan-bodies runtime-scan-metadata pouch-scan-metadata space-collection stream-collection pouch-collection)
      add_test(NAME pouch_space_compare_${mode}
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 12 4096 0 2 4096)
      set_tests_properties(pouch_space_compare_${mode} PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_compare_${mode}_durable
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 8 65536 1 2 4096)
      set_tests_properties(pouch_space_compare_${mode}_durable PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_compare_${mode}_reopened
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 12 4096 0 2 4096 --reopen)
      set_tests_properties(pouch_space_compare_${mode}_reopened PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_compare_${mode}_reopened_durable
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 8 65536 1 2 4096 --reopen)
      set_tests_properties(pouch_space_compare_${mode}_reopened_durable PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endif()
endif()

if(LOCKDC_BUILD_TESTS)
  add_executable(pouch_space_pouch_test pouch_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_pouch_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_pouch_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_pouch_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_pouch_test)
  foreach(mode lifecycle faults concurrency io-faults validation mutations mutation-faults mutation-io-faults retention-hint mutation-concurrency mutation-capacity key-bounds reads read-faults read-io-faults read-concurrency snapshots projection projection-faults projection-concurrency maintenance maintenance-faults maintenance-io-faults maintenance-concurrency maintenance-checksums)
    add_test(NAME pouch_space_pouch_${mode} COMMAND pouch_space_pouch_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_pouch_${mode} PROPERTIES TIMEOUT 240
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  # Thousands of independent cold/warm allocation failures also run under TSAN.
  set_tests_properties(pouch_space_pouch_read-faults PROPERTIES TIMEOUT 600)
  set_tests_properties(pouch_space_pouch_maintenance-faults pouch_space_pouch_maintenance-io-faults PROPERTIES TIMEOUT 600)
  add_executable(pouch_space_namespace_test namespace_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_namespace_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_namespace_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_namespace_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_namespace_test)
  foreach(mode lifecycle faults concurrency validation reservation publication snapshots snapshot-faults snapshot-concurrency snapshot-churn bodies)
    add_test(NAME pouch_space_namespace_${mode}
      COMMAND pouch_space_namespace_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_namespace_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_state_record_test state_record_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_state_record_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_state_record_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_state_record_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator" "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator" "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_state_record_test)
  foreach(mode lifecycle faults concurrency snapshots snapshot-faults)
    add_test(NAME pouch_space_state_records_${mode}
      COMMAND pouch_space_state_record_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_state_records_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_root_test root_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_root_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_root_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_root_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator"
    "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator"
    "LINKER:--wrap=lc_strdup_with_allocator")
  lc_configure_c_test_target(pouch_space_root_test)
  foreach(mode lifecycle faults concurrency)
    add_test(NAME pouch_space_root_${mode}
      COMMAND pouch_space_root_test ${CMAKE_CURRENT_BINARY_DIR} ${mode})
    set_tests_properties(pouch_space_root_${mode} PROPERTIES TIMEOUT 120
      LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  endforeach()
  add_executable(pouch_space_control_test control_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_control_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_control_test PRIVATE pouch_space_experiment)
  lc_configure_c_test_target(pouch_space_control_test)
  add_test(NAME pouch_space_controls COMMAND pouch_space_control_test ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(pouch_space_controls PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_executable(pouch_space_record_test record_test.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_record_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_record_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_record_test PRIVATE "LINKER:--wrap=writev" "LINKER:--wrap=write")
  if(LOCKDC_TARGET_LIBC STREQUAL "gnu")
    target_compile_definitions(pouch_space_record_test PRIVATE RECORD_PREADV64=1)
    target_link_options(pouch_space_record_test PRIVATE "LINKER:--wrap=preadv64" "LINKER:--wrap=pread64")
  else()
    target_link_options(pouch_space_record_test PRIVATE "LINKER:--wrap=preadv" "LINKER:--wrap=pread")
  endif()
  lc_configure_c_test_target(pouch_space_record_test)
  add_test(NAME pouch_space_records COMMAND pouch_space_record_test ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(pouch_space_records PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_executable(pouch_space_directory_test directory_test.c)
  target_link_libraries(pouch_space_directory_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_directory_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator"
    "LINKER:--wrap=lc_calloc_with_allocator")
  lc_configure_c_test_target(pouch_space_directory_test)
  add_test(NAME pouch_space_directory COMMAND pouch_space_directory_test)
  set_tests_properties(pouch_space_directory PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  add_executable(pouch_space_test space_test.c)
  target_link_libraries(pouch_space_test PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_test PRIVATE
    "LINKER:--wrap=lc_alloc_with_allocator"
    "LINKER:--wrap=lc_calloc_with_allocator"
    "LINKER:--wrap=lc_realloc_with_allocator"
    "LINKER:--wrap=lc_strdup_with_allocator")
  target_include_directories(pouch_space_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  lc_configure_c_test_target(pouch_space_test)
  add_test(NAME pouch_space_foundation COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR})
  add_test(NAME pouch_space_commit COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} commit)
  add_test(NAME pouch_space_reopen COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} reopen)
  add_test(NAME pouch_space_metadata COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} metadata)
  add_test(NAME pouch_space_packing COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} packing)
  add_test(NAME pouch_space_streams COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} streams)
  add_test(NAME pouch_space_lifecycle COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} lifecycle)
  add_test(NAME pouch_space_catalog COMMAND pouch_space_test ${CMAKE_CURRENT_BINARY_DIR} catalog)
  set_tests_properties(pouch_space_catalog PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_lifecycle PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_streams PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_packing PROPERTIES TIMEOUT 60
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_metadata PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_reopen PROPERTIES TIMEOUT 30
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_commit PROPERTIES TIMEOUT 30
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_foundation PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  lc_apply_test_defaults()
endif()
