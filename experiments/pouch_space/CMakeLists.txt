# Native-only experimental target, never installed or linked into the product.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$" OR
   NOT LOCKDC_TARGET_ARCH STREQUAL "x86_64" OR NOT TARGET lc_static)
  return()
endif()

add_library(pouch_space_experiment STATIC space.c)
target_include_directories(pouch_space_experiment PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
  PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(pouch_space_experiment PUBLIC lc_static)
lc_configure_c_target(pouch_space_experiment)

if(LOCKDC_BUILD_BENCHMARKS)
  add_executable(pouch_space_compare compare.c ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(pouch_space_compare PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(pouch_space_compare PRIVATE pouch_space_experiment)
  target_link_options(pouch_space_compare PRIVATE
    "LINKER:--wrap=fdatasync" "LINKER:--wrap=fsync")
  lc_configure_c_target(pouch_space_compare)
  if(LOCKDC_BUILD_TESTS)
    foreach(mode space-state pouch-state space-collection pouch-collection)
      add_test(NAME pouch_space_compare_${mode}
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 12 4096 0 2 4096)
      set_tests_properties(pouch_space_compare_${mode} PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
      add_test(NAME pouch_space_compare_${mode}_durable
        COMMAND pouch_space_compare ${mode} ${CMAKE_CURRENT_BINARY_DIR} 8 65536 1 2 4096)
      set_tests_properties(pouch_space_compare_${mode}_durable PROPERTIES TIMEOUT 60
        LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
    endforeach()
  endif()
endif()

if(LOCKDC_BUILD_TESTS)
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
  set_tests_properties(pouch_space_commit PROPERTIES TIMEOUT 30
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  set_tests_properties(pouch_space_foundation PROPERTIES TIMEOUT 120
    LABELS "unit;local;pouch-space" RUN_SERIAL TRUE)
  lc_apply_test_defaults()
endif()
