if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(test_root "${CMAKE_BINARY_DIR}/coverage-pkgconfig-metadata-test")
set(test_build_dir "${test_root}/build")

file(REMOVE_RECURSE "${test_root}")

set(configure_args)
if(LOCKDC_EXTERNAL_ROOT)
    list(APPEND configure_args "-DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}")
endif()
if(LOCKDC_C_COMPILER)
    list(APPEND configure_args "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${LOCKDC_ROOT}"
        -B "${test_build_dir}"
        ${configure_args}
        -DCMAKE_BUILD_TYPE=Debug
        -DLOCKDC_ENABLE_COVERAGE=ON
        -DLOCKDC_BUILD_TESTS=OFF
        -DLOCKDC_BUILD_EXAMPLES=OFF
        -DLOCKDC_BUILD_BENCHMARKS=OFF
        -DLOCKDC_BUILD_FUZZERS=OFF
        -DLOCKDC_BUILD_E2E_TESTS=OFF
        -DLOCKDC_INSTALL=OFF
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure coverage metadata test build\n"
        "stdout:\n${configure_stdout}\n"
        "stderr:\n${configure_stderr}")
endif()

file(READ "${test_build_dir}/lockdc.pc" lockdc_pkgconfig_text)
string(FIND "${lockdc_pkgconfig_text}" "-lgcov" lockdc_pkgconfig_gcov_index)
if(lockdc_pkgconfig_gcov_index EQUAL -1)
    message(FATAL_ERROR
        "coverage-enabled pkg-config metadata is missing -lgcov in Libs.private\n"
        "pkg-config:\n${lockdc_pkgconfig_text}")
endif()
