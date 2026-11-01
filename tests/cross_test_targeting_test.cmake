if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

# Inspect the generated closure even when its outputs already exist. A script
# mock alone cannot catch executables registered after the aggregate is made.
if(DEFINED LOCKDC_NINJA)
    execute_process(
        COMMAND "${LOCKDC_NINJA}" -C "${LOCKDC_BINARY_DIR}" -t commands lockdc_cross_runtime_tests
        RESULT_VARIABLE closure_result
        OUTPUT_VARIABLE closure_commands
        ERROR_VARIABLE closure_stderr)
    if(NOT closure_result EQUAL 0)
        message(FATAL_ERROR "cannot inspect cross-runtime build closure: ${closure_stderr}")
    endif()
    foreach(required_command "test_pouch_compaction_stack.c" "tests/integration/lc_pouch_compaction_stack_test"
            "test_lc_allocator_exit.c" "tests/unit/lc_allocator_exit_probe"
            "test_lc_allocator_public_exit.c" "tests/unit/lc_allocator_public_exit_probe"
            "test_lc_transport_https.c" "tests/unit/lc_unit_transport_https"
            "test_lc_collections.c" "tests/unit/lc_unit_collections")
        string(FIND "${closure_commands}" "${required_command}" command_index)
        if(command_index EQUAL -1)
            message(FATAL_ERROR "cross-runtime build closure is missing ${required_command}")
        endif()
    endforeach()
    string(REGEX MATCHALL " -c [^\n]*test_lc_transport_https.c" https_compiles "${closure_commands}")
    list(LENGTH https_compiles https_compile_count)
    if(NOT https_compile_count EQUAL 1)
        message(FATAL_ERROR "HTTPS fixture must compile once, got ${https_compile_count}")
    endif()
    string(REGEX MATCHALL " -c [^\n]*test_lc_collections.c" collection_compiles "${closure_commands}")
    list(LENGTH collection_compiles collection_compile_count)
    if(NOT collection_compile_count EQUAL 1)
        message(FATAL_ERROR "Collection partitions must compile once, got ${collection_compile_count}")
    endif()
    execute_process(COMMAND "${LOCKDC_NINJA}" -C "${LOCKDC_BINARY_DIR}" -t commands ${LOCKDC_LIBRARY_TARGETS}
        RESULT_VARIABLE library_result OUTPUT_VARIABLE library_commands ERROR_VARIABLE library_error)
    if(NOT library_result EQUAL 0)
        message(FATAL_ERROR "cannot inspect library build closure: ${library_error}")
    endif()
    string(REGEX MATCHALL " -c [^\n]*/src/lc_api.c" core_compiles "${library_commands}")
    list(LENGTH core_compiles core_compile_count)
    if(NOT core_compile_count EQUAL 1)
        message(FATAL_ERROR "Static/shared core must compile once, got ${core_compile_count}")
    endif()
endif()

set(test_root "${LOCKDC_BINARY_DIR}/cross-test-targeting-test")
set(script_dir "${test_root}/scripts")
set(bin_dir "${test_root}/bin")
set(log_path "${test_root}/invocations.log")
set(fake_build "${script_dir}/build.sh")
set(fake_ctest "${bin_dir}/ctest")
set(fake_qemu "${bin_dir}/qemu")
set(fake_sysroot "${test_root}/sysroot")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${script_dir}")
file(MAKE_DIRECTORY "${bin_dir}")
file(MAKE_DIRECTORY "${fake_sysroot}")

file(COPY "${LOCKDC_ROOT}/scripts/cross_test.sh" DESTINATION "${script_dir}")
file(COPY "${LOCKDC_ROOT}/scripts/run_timed.sh" DESTINATION "${script_dir}")
file(CHMOD "${script_dir}/cross_test.sh"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)
file(CHMOD "${script_dir}/run_timed.sh"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

foreach(preset
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release)
    file(MAKE_DIRECTORY "${test_root}/build/${preset}")
    file(WRITE "${test_root}/build/${preset}/CMakeCache.txt"
        "# fake cache for ${preset}\nCMAKE_CROSSCOMPILING_EMULATOR:STRING=${fake_qemu};-L;${fake_sysroot}\n")
endforeach()

file(WRITE "${fake_build}" [=[
#!/usr/bin/env bash
set -eu
{
  printf 'build|'
  for arg in "$@"; do
    printf '%s|' "$arg"
  done
  printf '\n'
} >> "${LOCKDC_TEST_LOG}"
exit 0
]=])
file(CHMOD "${fake_build}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

file(WRITE "${fake_ctest}" [=[
#!/usr/bin/env bash
set -eu
{
  printf 'ctest|'
  for arg in "$@"; do
    printf '%s|' "$arg"
  done
  printf '\n'
} >> "${LOCKDC_TEST_LOG}"
exit 0
]=])
file(CHMOD "${fake_ctest}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

file(WRITE "${fake_qemu}" "#!/usr/bin/env bash\nexit 0\n")
file(CHMOD "${fake_qemu}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        PATH=${bin_dir}:$ENV{PATH}
        LOCKDC_TEST_LOG=${log_path}
        "${script_dir}/cross_test.sh" release
    WORKING_DIRECTORY "${LOCKDC_BINARY_DIR}"
    RESULT_VARIABLE cross_test_result
    OUTPUT_VARIABLE cross_test_stdout
    ERROR_VARIABLE cross_test_stderr
)
if(NOT cross_test_result EQUAL 0)
    message(FATAL_ERROR
        "expected cross_test.sh release to build its curated runtime closure\n"
        "stdout:\n${cross_test_stdout}\n"
        "stderr:\n${cross_test_stderr}")
endif()

file(READ "${log_path}" log_contents)

function(assert_log_contains pattern description)
    if(NOT log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "missing ${description} in cross_test.sh invocation log:\n${log_contents}")
    endif()
endfunction()

function(assert_log_not_contains pattern description)
    if(log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "unexpected ${description} in cross_test.sh invocation log:\n${log_contents}")
    endif()
endfunction()

assert_log_contains("ctest\\|--preset\\|aarch64-linux-gnu-release\\|--output-on-failure\\|" "aarch64 gnu ctest invocation")
assert_log_contains("ctest\\|--preset\\|aarch64-linux-musl-release\\|--output-on-failure\\|" "aarch64 musl ctest invocation")
assert_log_contains("ctest\\|--preset\\|armhf-linux-gnu-release\\|--output-on-failure\\|" "armhf gnu ctest invocation")
assert_log_contains("ctest\\|--preset\\|armhf-linux-musl-release\\|--output-on-failure\\|" "armhf musl ctest invocation")
assert_log_contains("-L\\|cross-runtime\\|" "curated target-runtime test label")
foreach(preset
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release)
    assert_log_contains("build\\|${preset}\\|lockdc_cross_runtime_tests\\|"
        "${preset} curated runtime build invocation")
endforeach()

file(WRITE "${fake_build}" [=[
#!/usr/bin/env bash
set -eu
{
  printf 'build|'
  for arg in "$@"; do
    printf '%s|' "$arg"
  done
  printf '\n'
} >> "${LOCKDC_TEST_LOG}"
exit 0
]=])
file(WRITE "${log_path}" "")

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        PATH=${bin_dir}:$ENV{PATH}
        LOCKDC_TEST_LOG=${log_path}
        "${script_dir}/cross_test.sh" preset
    WORKING_DIRECTORY "${LOCKDC_BINARY_DIR}"
    RESULT_VARIABLE cross_test_preset_result
    OUTPUT_VARIABLE cross_test_preset_stdout
    ERROR_VARIABLE cross_test_preset_stderr
)
if(NOT cross_test_preset_result EQUAL 0)
    message(FATAL_ERROR
        "expected cross_test.sh preset to succeed\n"
        "stdout:\n${cross_test_preset_stdout}\n"
        "stderr:\n${cross_test_preset_stderr}")
endif()

file(READ "${log_path}" log_contents)

assert_log_contains("build\\|debug\\|" "debug preset build invocation")
assert_log_contains("ctest\\|--preset\\|debug\\|--output-on-failure\\|" "debug preset ctest invocation")
assert_log_not_contains("build\\|asan\\|" "asan compatibility alias build invocation")
assert_log_not_contains("ctest\\|--preset\\|asan\\|" "asan compatibility alias ctest invocation")
assert_log_not_contains("ctest\\|--preset\\|armhf-linux-musl-release\\|" "release ctest invocation during preset-only mode")
