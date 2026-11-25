cmake_minimum_required(VERSION 3.20)

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/CMakePresets.json" presets_json)
file(READ "${LOCKDC_ROOT}/Makefile" root_makefile)

function(assert_contains haystack needle description)
    string(FIND "${${haystack}}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description}")
    endif()
endfunction()

function(json_length out)
    string(JSON length_value ERROR_VARIABLE json_error LENGTH "${presets_json}" ${ARGN})
    if(json_error)
        message(FATAL_ERROR "unable to read JSON length at ${ARGN}: ${json_error}")
    endif()
    set(${out} "${length_value}" PARENT_SCOPE)
endfunction()

function(json_get out)
    string(JSON value ERROR_VARIABLE json_error GET "${presets_json}" ${ARGN})
    if(json_error)
        message(FATAL_ERROR "unable to read JSON value at ${ARGN}: ${json_error}")
    endif()
    string(REGEX REPLACE "^\"(.*)\"$" "\\1" value "${value}")
    set(${out} "${value}" PARENT_SCOPE)
endfunction()

function(assert_configure_cache_absent name key)
    find_named_object_index(index configurePresets "${name}")
    string(JSON value ERROR_VARIABLE json_error GET "${presets_json}"
        configurePresets ${index} cacheVariables ${key})
    if(NOT json_error)
        message(FATAL_ERROR
            "configure preset ${name} must not set cache variable ${key}, got ${value}")
    endif()
endfunction()

function(find_named_object_index out array_path wanted_name)
    json_length(array_length "${array_path}")
    math(EXPR last_index "${array_length} - 1")
    foreach(index RANGE 0 ${last_index})
        json_get(object_name "${array_path}" ${index} name)
        if(object_name STREQUAL wanted_name)
            set(${out} "${index}" PARENT_SCOPE)
            return()
        endif()
    endforeach()
    message(FATAL_ERROR "missing ${array_path} entry named ${wanted_name}")
endfunction()

function(assert_configure_preset name)
    find_named_object_index(index configurePresets "${name}")
endfunction()

function(assert_build_preset name expected_configure)
    find_named_object_index(index buildPresets "${name}")
    json_get(configure_preset buildPresets ${index} configurePreset)
    if(NOT configure_preset STREQUAL expected_configure)
        message(FATAL_ERROR
            "build preset ${name} should use configure preset ${expected_configure}, got ${configure_preset}")
    endif()
endfunction()

function(assert_test_preset name expected_configure)
    find_named_object_index(index testPresets "${name}")
    json_get(configure_preset testPresets ${index} configurePreset)
    if(NOT configure_preset STREQUAL expected_configure)
        message(FATAL_ERROR
            "test preset ${name} should use configure preset ${expected_configure}, got ${configure_preset}")
    endif()
endfunction()

function(assert_test_filter name key expected)
    find_named_object_index(index testPresets "${name}")
    json_get(actual testPresets ${index} filter include ${key})
    if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "test preset ${name} include.${key} should be ${expected}, got ${actual}")
    endif()
endfunction()

function(assert_configure_cache name key expected)
    find_named_object_index(index configurePresets "${name}")
    json_get(actual configurePresets ${index} cacheVariables ${key})
    if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "configure preset ${name} cache variable ${key} should be ${expected}, got ${actual}")
    endif()
endfunction()

function(assert_configure_toolchain name expected)
    find_named_object_index(index configurePresets "${name}")
    json_get(actual configurePresets ${index} toolchainFile)
    if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "configure preset ${name} toolchainFile should be ${expected}, got ${actual}")
    endif()
endfunction()

function(assert_configure_inherits name expected)
    find_named_object_index(index configurePresets "${name}")
    json_get(actual configurePresets ${index} inherits)
    if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "configure preset ${name} should inherit ${expected}, got ${actual}")
    endif()
endfunction()

function(assert_configure_binary_dir name expected)
    find_named_object_index(index configurePresets "${name}")
    json_get(actual configurePresets ${index} binaryDir)
    if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "configure preset ${name} binaryDir should be ${expected}, got ${actual}")
    endif()
endfunction()

foreach(name
        base
        debug
        debug-lua
        valgrind
        fuzz
        pouch-integration-fuzz
        x86_64-linux-gnu-release
        x86_64-linux-musl-release
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release
        arm64-apple-darwin-release)
    assert_configure_preset("${name}")
endforeach()

foreach(name
        debug
        valgrind
        fuzz
        pouch-integration-fuzz
        x86_64-linux-gnu-release
        x86_64-linux-musl-release
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release
        arm64-apple-darwin-release)
    assert_build_preset("${name}" "${name}")
    assert_test_preset("${name}" "${name}")
endforeach()
assert_build_preset(debug-lua debug-lua)
assert_test_preset(debug-lua debug-lua)
assert_test_filter(debug-lua name "^lua_")

assert_configure_cache(base CMAKE_EXPORT_COMPILE_COMMANDS ON)
assert_configure_cache(base LOCKDC_BUILD_DEPENDENCIES OFF)
assert_configure_cache(base LOCKDC_BUILD_TESTS ON)
assert_configure_cache(base LOCKDC_BUILD_FUZZERS OFF)
assert_configure_cache(base LOCKDC_INSTALL ON)

assert_configure_cache(debug CMAKE_BUILD_TYPE Debug)
assert_configure_cache(debug LOCKDC_BUILD_EXAMPLES ON)
assert_configure_toolchain(debug [=[${sourceDir}/cmake/toolchains/x86_64-linux-gnu.cmake]=])
assert_configure_inherits(debug-lua debug)
assert_configure_binary_dir(debug-lua [=[${sourceDir}/build/debug]=])
foreach(name e2e asan coverage)
    assert_configure_inherits("${name}" debug)
endforeach()
assert_configure_cache(valgrind CMAKE_C_FLAGS_DEBUG "-O1 -g -fno-omit-frame-pointer")
assert_configure_cache(valgrind LOCKDC_BUILD_FUZZERS OFF)
assert_configure_cache(valgrind LOCKDC_TARGET_ARCH x86_64)
assert_configure_cache(valgrind LOCKDC_TARGET_OS linux)
assert_configure_cache(valgrind LOCKDC_TARGET_LIBC gnu)
assert_configure_toolchain(valgrind [=[${sourceDir}/cmake/toolchains/x86_64-linux-gnu.cmake]=])
assert_configure_cache(fuzz LOCKDC_BUILD_FUZZERS ON)
assert_configure_cache(fuzz LOCKDC_TARGET_ARCH x86_64)
assert_configure_cache(fuzz LOCKDC_TARGET_OS linux)
assert_configure_cache(fuzz LOCKDC_TARGET_LIBC gnu)
assert_configure_cache_absent(fuzz CMAKE_C_COMPILER)
assert_configure_cache_absent(fuzz CMAKE_CXX_COMPILER)
assert_configure_toolchain(fuzz [=[${sourceDir}/cmake/toolchains/fuzz-aflpp.cmake]=])
assert_configure_inherits(pouch-integration-fuzz debug)
assert_configure_cache(pouch-integration-fuzz LOCKDC_BUILD_FUZZERS OFF)
assert_configure_cache(pouch-integration-fuzz LOCKDC_BUILD_POUCH_INTEGRATION_FUZZERS ON)
assert_configure_cache(pouch-integration-fuzz LOCKDC_BUILD_TESTS OFF)
assert_configure_cache(pouch-integration-fuzz LOCKDC_BUILD_E2E_TESTS OFF)
assert_configure_cache(pouch-integration-fuzz LOCKDC_BUILD_BENCHMARKS OFF)
assert_build_preset(pouch-integration-fuzz pouch-integration-fuzz)
assert_test_preset(pouch-integration-fuzz pouch-integration-fuzz)
assert_test_filter(pouch-integration-fuzz label fuzz)

assert_configure_toolchain(x86_64-linux-gnu-release [=[${sourceDir}/cmake/toolchains/x86_64-linux-gnu.cmake]=])
assert_configure_toolchain(x86_64-linux-musl-release [=[${sourceDir}/cmake/toolchains/x86_64-linux-musl.cmake]=])
assert_configure_toolchain(aarch64-linux-gnu-release [=[${sourceDir}/cmake/toolchains/aarch64-linux-gnu.cmake]=])
assert_configure_toolchain(aarch64-linux-musl-release [=[${sourceDir}/cmake/toolchains/aarch64-linux-musl.cmake]=])
assert_configure_toolchain(armhf-linux-gnu-release [=[${sourceDir}/cmake/toolchains/armhf-linux-gnu.cmake]=])
assert_configure_toolchain(armhf-linux-musl-release [=[${sourceDir}/cmake/toolchains/armhf-linux-musl.cmake]=])
assert_configure_toolchain(arm64-apple-darwin-release [=[${sourceDir}/cmake/toolchains/arm64-apple-darwin.cmake]=])

foreach(name
        x86_64-linux-gnu-release
        x86_64-linux-musl-release
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release
        arm64-apple-darwin-release)
    assert_configure_cache("${name}" LOCKDC_DIST_DIR [=[${sourceDir}/dist]=])
endforeach()

assert_contains(root_makefile "make valgrind" "make help Valgrind surface")
assert_contains(root_makefile "scripts/valgrind.sh" "Valgrind runner command")
