if(NOT TARGET lc_static)
    message(FATAL_ERROR "Benchmarks require lc_static.")
endif()

add_executable(lockdc_bench
    bench_main.c
    bench_collections.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)
target_include_directories(lockdc_bench PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)
target_link_libraries(lockdc_bench PRIVATE lc_static)
lc_configure_c_target(lockdc_bench)
lc_enable_coverage_if_requested(lockdc_bench)

# Fresh-process public-API driver for tests/e2e/pouch_replay.py.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(lockdc_collection_edit_probe EXCLUDE_FROM_ALL collection_edit_probe.c)
  target_link_libraries(lockdc_collection_edit_probe PRIVATE lc_static)
  lc_configure_c_target(lockdc_collection_edit_probe)

  # Manual native Linux experiment; excluded from normal builds and gates.
  add_executable(lockdc_collection_layout_probe EXCLUDE_FROM_ALL
      collection_layout_probe.c
      ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_include_directories(lockdc_collection_layout_probe PRIVATE
      ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(lockdc_collection_layout_probe PRIVATE lc_static)
  lc_configure_c_target(lockdc_collection_layout_probe)

  add_executable(lockdc_pouch_replay_probe pouch_replay_probe.c)
  target_link_libraries(lockdc_pouch_replay_probe PRIVATE lc_static)
  lc_configure_c_target(lockdc_pouch_replay_probe)
  lc_enable_coverage_if_requested(lockdc_pouch_replay_probe)
endif()
