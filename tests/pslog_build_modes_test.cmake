if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT OR LOCKDC_DEPENDENCY_BUILD_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_DEPENDENCY_BUILD_ROOT is required")
endif()

include("${LOCKDC_ROOT}/tests/bootlin_runtime_test_support.cmake")

if(LOCKDC_SYSTEM_NAME STREQUAL "Linux" AND LOCKDC_CMAKE_SYSROOT)
    if(NOT DEFINED LOCKDC_CMAKE_READELF OR LOCKDC_CMAKE_READELF STREQUAL "")
        message(FATAL_ERROR "LOCKDC_CMAKE_READELF is required for Linux development runtime checks")
    endif()
    lockdc_resolve_bootlin_runtime("${LOCKDC_CMAKE_SYSROOT}"
        "${LOCKDC_EXTERNAL_ROOT}" "" lockdc_bootlin_runtime_loader
        lockdc_bootlin_runtime_dirs)
    if(lockdc_bootlin_runtime_loader STREQUAL "")
        message(FATAL_ERROR "Linux development runtime has no Bootlin interpreter")
    endif()
    string(REPLACE ";" "\\;" lockdc_bootlin_runtime_dirs_arg
        "${lockdc_bootlin_runtime_dirs}")
endif()

function(run_checked label)
    execute_process(
        COMMAND ${ARGN}
        RESULT_VARIABLE result
        OUTPUT_VARIABLE stdout
        ERROR_VARIABLE stderr
    )
    if(NOT result EQUAL 0)
        message(FATAL_ERROR
            "${label} failed\n"
            "stdout:\n${stdout}\n"
            "stderr:\n${stderr}")
    endif()
endfunction()

function(append_toolchain_arg out_var)
    set(args ${ARGN})
    if(DEFINED LOCKDC_TOOLCHAIN_FILE AND NOT LOCKDC_TOOLCHAIN_FILE STREQUAL "")
        list(APPEND args -DCMAKE_TOOLCHAIN_FILE=${LOCKDC_TOOLCHAIN_FILE})
    endif()
    if(lockdc_bootlin_runtime_loader)
        list(APPEND args
            -DLOCKDC_BOOTLIN_RUNTIME_LOADER=${lockdc_bootlin_runtime_loader}
            -DLOCKDC_BOOTLIN_RUNTIME_DIRS=${lockdc_bootlin_runtime_dirs_arg})
    endif()
    set(${out_var} "${args}" PARENT_SCOPE)
endfunction()

function(assert_bootlin_development_executable binary_path)
    if(NOT lockdc_bootlin_runtime_loader)
        return()
    endif()
    execute_process(
        COMMAND "${LOCKDC_CMAKE_READELF}" -l "${binary_path}"
        RESULT_VARIABLE interpreter_result
        OUTPUT_VARIABLE interpreter_output
        ERROR_VARIABLE interpreter_error)
    if(NOT interpreter_result EQUAL 0 OR
       NOT interpreter_output MATCHES "Requesting program interpreter: ${lockdc_bootlin_runtime_loader}")
        message(FATAL_ERROR
            "development executable does not use the selected Bootlin interpreter: ${binary_path}\n"
            "${interpreter_error}${interpreter_output}")
    endif()
endfunction()

function(write_shared_consumer_project project_dir)
    file(MAKE_DIRECTORY "${project_dir}")
    set(lockdc_consumer_runtime_config "")
    if(lockdc_bootlin_runtime_loader)
        string(APPEND lockdc_consumer_runtime_config
            "set(CMAKE_SKIP_BUILD_RPATH TRUE)\n"
            "add_link_options(\n"
            "  \"LINKER:--dynamic-linker,${lockdc_bootlin_runtime_loader}\"\n"
            "  \"LINKER:--disable-new-dtags\")\n")
        foreach(lockdc_runtime_dir IN LISTS lockdc_bootlin_runtime_dirs)
            string(APPEND lockdc_consumer_runtime_config
                "add_link_options(\"LINKER:-rpath,${lockdc_runtime_dir}\")\n")
        endforeach()
    endif()
    file(WRITE "${project_dir}/CMakeLists.txt"
        "cmake_minimum_required(VERSION 3.20)\n"
        "project(lockdc_shared_consumer C)\n"
        "${lockdc_consumer_runtime_config}"
        "add_subdirectory(\"${LOCKDC_ROOT}\" lockdc-src)\n"
        "add_executable(shared_consumer main.c)\n"
        "target_link_libraries(shared_consumer PRIVATE lc_shared)\n"
        "lc_add_build_tree_shared_dependency_lookup(shared_consumer)\n"
        "if(CMAKE_SYSTEM_NAME STREQUAL \"Linux\")\n"
        "  target_link_options(shared_consumer PRIVATE \"LINKER:-rpath,$<TARGET_FILE_DIR:lc_shared>\")\n"
        "endif()\n"
        "enable_testing()\n"
        "if(CMAKE_SYSTEM_NAME STREQUAL CMAKE_HOST_SYSTEM_NAME AND CMAKE_SYSTEM_PROCESSOR STREQUAL CMAKE_HOST_SYSTEM_PROCESSOR)\n"
        "  add_test(NAME shared_consumer_runs COMMAND shared_consumer)\n"
        "  add_test(NAME lua_module_loads COMMAND lockdc_lua_runner -e \"package.cpath = [[$<TARGET_FILE_DIR:lockdc_lua_core>/../?.so]]; assert(require('lockdc.core'))\")\n"
        "endif()\n"
    )
    file(WRITE "${project_dir}/main.c" [=[
#include <lc/lc.h>

int main(void) {
  lc_client_config config;
  lc_client_config_init(&config);
  return config.timeout_ms == 30000L ? 0 : 1;
}
]=])
endfunction()

string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef lockdc_test_suffix)
set(test_root "${LOCKDC_BINARY_DIR}/pslog-build-modes-test-${lockdc_test_suffix}")
set(shared_consumer_root "${test_root}/shared-consumer")
set(shared_consumer_build "${shared_consumer_root}/build")
set(shared_root "${test_root}/shared-only")
set(shared_build "${shared_root}/build")
set(shared_prefix "${shared_root}/install")
set(static_root "${test_root}/static-only")
set(static_build "${static_root}/build")
set(static_prefix "${static_root}/install")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_root}")

write_shared_consumer_project("${shared_consumer_root}")
append_toolchain_arg(shared_consumer_configure_args
    "${CMAKE_COMMAND}"
    -S "${shared_consumer_root}"
    -B "${shared_consumer_build}"
    -DLOCKDC_BUILD_STATIC=OFF
    -DLOCKDC_BUILD_SHARED=ON
    -DLOCKDC_BUILD_POSTGRES=OFF
    -DLOCKDC_BUILD_LUA_BINDINGS=ON
    -DLOCKDC_BUILD_TESTS=OFF
    -DLOCKDC_BUILD_E2E_TESTS=OFF
    -DLOCKDC_BUILD_EXAMPLES=OFF
    -DLOCKDC_BUILD_BENCHMARKS=OFF
    -DLOCKDC_BUILD_FUZZERS=OFF
    -DLOCKDC_INSTALL=OFF
    -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
    -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
    -DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}
)
run_checked("shared consumer configure" ${shared_consumer_configure_args})
run_checked("shared consumer build"
    "${CMAKE_COMMAND}" --build "${shared_consumer_build}" --target shared_consumer lockdc_lua_runner lockdc_lua_core
)
assert_bootlin_development_executable("${shared_consumer_build}/shared_consumer")
run_checked("shared consumer and Lua module execution"
    "${CMAKE_COMMAND}" -E env --unset=LD_LIBRARY_PATH --unset=LD_PRELOAD
    "${CMAKE_CTEST_COMMAND}" --test-dir "${shared_consumer_build}" --output-on-failure)

append_toolchain_arg(shared_configure_args
    "${CMAKE_COMMAND}"
    -S "${LOCKDC_ROOT}"
    -B "${shared_build}"
    -DLOCKDC_BUILD_STATIC=OFF
    -DLOCKDC_BUILD_SHARED=ON
    -DLOCKDC_BUILD_POSTGRES=OFF
    -DLOCKDC_BUILD_TESTS=OFF
    -DLOCKDC_BUILD_E2E_TESTS=OFF
    -DLOCKDC_BUILD_EXAMPLES=ON
    -DLOCKDC_BUILD_BENCHMARKS=OFF
    -DLOCKDC_BUILD_FUZZERS=OFF
    -DLOCKDC_INSTALL=ON
    -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
    -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
    -DCMAKE_INSTALL_PREFIX=${shared_prefix}
    -DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}
)
run_checked("shared-only configure" ${shared_configure_args})
run_checked("shared-only example build"
    "${CMAKE_COMMAND}" --build "${shared_build}" --target lc_example_acquire_lease_lifecycle
)
assert_bootlin_development_executable(
    "${shared_build}/examples/lc_example_acquire_lease_lifecycle")
run_checked("shared-only install"
    "${CMAKE_COMMAND}" --install "${shared_build}"
)
if(EXISTS "${shared_prefix}/include/pslog.h")
    message(FATAL_ERROR "shared-only install staged dependency header pslog.h")
endif()

append_toolchain_arg(static_configure_args
    "${CMAKE_COMMAND}"
    -S "${LOCKDC_ROOT}"
    -B "${static_build}"
    -DLOCKDC_BUILD_STATIC=ON
    -DLOCKDC_BUILD_SHARED=OFF
    -DLOCKDC_BUILD_POSTGRES=OFF
    -DLOCKDC_BUILD_TESTS=OFF
    -DLOCKDC_BUILD_E2E_TESTS=OFF
    -DLOCKDC_BUILD_EXAMPLES=ON
    -DLOCKDC_BUILD_BENCHMARKS=OFF
    -DLOCKDC_BUILD_FUZZERS=OFF
    -DLOCKDC_INSTALL=ON
    -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
    -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
    -DCMAKE_INSTALL_PREFIX=${static_prefix}
    -DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}
)
run_checked("static-only configure" ${static_configure_args})
run_checked("static-only example build"
    "${CMAKE_COMMAND}" --build "${static_build}" --target lc_example_acquire_lease_lifecycle
)
assert_bootlin_development_executable(
    "${static_build}/examples/lc_example_acquire_lease_lifecycle")
run_checked("static-only install"
    "${CMAKE_COMMAND}" --install "${static_build}"
)
if(EXISTS "${static_prefix}/include/pslog.h")
    message(FATAL_ERROR "static-only install staged dependency header pslog.h")
endif()
if(EXISTS "${static_prefix}/lib/libpslog.a")
    message(FATAL_ERROR "static-only install staged dependency archive libpslog.a")
endif()
