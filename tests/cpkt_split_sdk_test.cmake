include("${LOCKDC_ROOT}/tests/bootlin_runtime_test_support.cmake")
include("${LOCKDC_ROOT}/cmake/LcGeneratedPath.cmake")
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_SYSROOT)
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_TOOLCHAIN_FILE)
set(test_root "${LOCKDC_BINARY_DIR}/cpkt-split-sdk-test")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${test_root}")
file(REMOVE_RECURSE "${test_root}")
set(relocated "${test_root}/relocated")
set(core "${relocated}/cpkt/install")
set(db "${relocated}/cpktdb/install")
file(COPY "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/" DESTINATION "${core}")
file(COPY "${LOCKDC_EXTERNAL_ROOT}/cpktdb/install/" DESTINATION "${db}")
set(source "${test_root}/consumer")
file(MAKE_DIRECTORY "${source}")
file(WRITE "${source}/main.c" [=[
#include <cpkt/postgres.h>
int main(void) {
  cpkt_postgres_result *result = (cpkt_postgres_result *)1;
  cpkt_postgres_retrieval_error error;
  return cpkt_postgres_get_result_checked(0, &result, &error) !=
             CPKT_POSTGRES_RETRIEVAL_FAILED ||
         result != 0 || error.code != CPKT_POSTGRES_RETRIEVAL_ERROR_ARGUMENT;
}
]=])
file(WRITE "${source}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(split_sdk_consumer C)
list(APPEND CMAKE_FIND_ROOT_PATH ${CMAKE_PREFIX_PATH})
find_package(CpktPostgres CONFIG REQUIRED)
foreach(variant static shared)
  add_executable(consumer_${variant} main.c)
  set_target_properties(consumer_${variant} PROPERTIES C_STANDARD 90
    C_STANDARD_REQUIRED YES C_EXTENSIONS NO SKIP_BUILD_RPATH TRUE)
  target_compile_options(consumer_${variant} PRIVATE -Wall -Wextra -Werror -pedantic-errors)
  if(variant STREQUAL "static")
    target_link_libraries(consumer_${variant} PRIVATE cpkt::postgres)
  else()
    target_link_libraries(consumer_${variant} PRIVATE cpkt::postgres_shared)
  endif()
  target_link_options(consumer_${variant} PRIVATE ${PROBE_RUNTIME_FLAGS})
endforeach()
]=])
lockdc_resolve_bootlin_runtime("${CMAKE_SYSROOT}" "${relocated}" ""
    loader runtime_dirs)
lockdc_bootlin_runtime_link_flags("${loader}" "${runtime_dirs}" runtime_flags)
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${source}" -B "${test_root}/build"
    "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
    "-DCMAKE_PREFIX_PATH=${core};${db}"
    "-DPROBE_RUNTIME_FLAGS=${runtime_flags}"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Relocated split SDK configure failed\n${output}\n${error}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" --build "${test_root}/build" --parallel 1
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Split SDK consumer link failed\n${output}\n${error}")
endif()
foreach(variant static shared)
    execute_process(COMMAND "${test_root}/build/consumer_${variant}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "${variant} SDK consumer failed\n${output}\n${error}")
    endif()
endforeach()

# Test the published core version file through ordinary find_package requests.
# These are real SDK metadata checks; no alternate compatibility implementation.
set(version_source "${test_root}/version-consumer")
file(MAKE_DIRECTORY "${version_source}")
file(WRITE "${version_source}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(core_compatibility_consumer C)
list(APPEND CMAKE_FIND_ROOT_PATH ${CMAKE_PREFIX_PATH})
set(exact)
if(PROBE_EXACT)
  set(exact EXACT)
endif()
find_package(CpktCore ${PROBE_BASELINE} ${exact} CONFIG REQUIRED)
if(NOT CpktCore_COMPATIBLE_SINCE STREQUAL "0.2.0")
  message(FATAL_ERROR "Unexpected core compatibility floor")
endif()
]=])
function(check_core_version label baseline exact accept)
    execute_process(COMMAND "${CMAKE_COMMAND}" -S "${version_source}"
        -B "${test_root}/version-${label}"
        "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
        "-DCMAKE_PREFIX_PATH=${core}" "-DPROBE_BASELINE=${baseline}"
        "-DPROBE_EXACT=${exact}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if((accept AND NOT result EQUAL 0) OR (NOT accept AND result EQUAL 0))
        message(FATAL_ERROR "Core baseline ${baseline}, EXACT=${exact}: unexpected result\n${output}\n${error}")
    endif()
    if(NOT accept AND NOT "${output}${error}" MATCHES "CpktCore")
        message(FATAL_ERROR "Version rejection was unrelated to core compatibility\n${output}\n${error}")
    endif()
endfunction()
check_core_version(floor 0.2.0 OFF TRUE)
check_core_version(current 0.7.0 ON TRUE)
check_core_version(previous-release 0.6.1 OFF TRUE)
check_core_version(previous 0.4.0 OFF TRUE)
check_core_version(previous-patch 0.5.0 OFF TRUE)
check_core_version(provider-baseline 0.7.0 OFF TRUE)
check_core_version(exact-provider-baseline 0.7.0 ON TRUE)
check_core_version(exact-previous-patch 0.5.0 ON FALSE)
check_core_version(range "0.2.0...<0.8.0" OFF TRUE)
check_core_version(below-floor 0.1.0 OFF FALSE)
check_core_version(above-release 0.8.0 OFF FALSE)
check_core_version(exact-old 0.2.0 ON FALSE)
check_core_version(excluded-upper "0.2.0...<0.7.0" OFF FALSE)

# A db-only prefix must fail instead of silently importing an unrelated core.
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${source}" -B "${test_root}/missing-core"
    "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
    "-DCMAKE_PREFIX_PATH=${db}"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT "${output}${error}" MATCHES "CpktCore")
    message(FATAL_ERROR "Missing core prerequisite was not rejected\n${output}\n${error}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${source}" -B "${test_root}/wrong-target"
    "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
    "-DCMAKE_PREFIX_PATH=${core};${db}" "-DCPKT_TARGET_ID=aarch64-linux-gnu"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT "${output}${error}" MATCHES "SDK target mismatch")
    message(FATAL_ERROR "Wrong target prerequisite was not rejected\n${output}\n${error}")
endif()
file(READ "${db}/share/cpkt/packages/db.json" db_manifest)
string(JSON build_pin GET "${db_manifest}" built_with_core package_id)
string(JSON minimum GET "${db_manifest}" requires_core release_version)
string(JSON consumer_pin ERROR_VARIABLE consumer_pin_error TYPE
    "${db_manifest}" requires_core package_id)
if(NOT consumer_pin_error OR NOT minimum STREQUAL "0.7.0")
    message(FATAL_ERROR "Database SDK still treats an exact build identity as a consumer prerequisite")
endif()
file(SHA256 "${db}/lib/libcpkt_postgres.a" original_static)
file(REAL_PATH "${db}/lib/libcpkt_postgres.so" postgres_shared)
file(SHA256 "${postgres_shared}" original_shared)

# Small canonical manifest fixtures alter metadata only. Recompute the fixture's
# own identity so SDK payload verification stays enabled; provider binaries and
# archives remain unchanged throughout the compatibility cases.
function(write_db_fixture fixture)
    string(JSON old_id GET "${fixture}" package_id)
    string(REPLACE "\"package_id\":\"${old_id}\"," "" unsigned "${fixture}")
    string(SHA256 new_id "${unsigned}")
    string(REPLACE "\"package_id\":\"${old_id}\""
        "\"package_id\":\"${new_id}\"" signed "${fixture}")
    file(WRITE "${db}/share/cpkt/packages/db.json" "${signed}")
endfunction()
function(check_db_fixture label accept)
    execute_process(COMMAND "${CMAKE_COMMAND}" -S "${source}"
        -B "${test_root}/${label}"
        "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
        "-DCMAKE_PREFIX_PATH=${core};${db}"
        "-DPROBE_RUNTIME_FLAGS=${runtime_flags}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if((accept AND NOT result EQUAL 0) OR (NOT accept AND result EQUAL 0))
        message(FATAL_ERROR "Provider compatibility ${label}: unexpected result\n${output}\n${error}")
    endif()
    if(NOT accept AND NOT "${output}${error}" MATCHES "CpktCore")
        message(FATAL_ERROR "Provider failure was not a core-version rejection\n${output}\n${error}")
    endif()
    if(accept)
        execute_process(COMMAND "${CMAKE_COMMAND}" --build "${test_root}/${label}" --parallel 1
            RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
        if(NOT result EQUAL 0)
            message(FATAL_ERROR "Provider compatibility link failed\n${output}\n${error}")
        endif()
        foreach(variant static shared)
            execute_process(COMMAND "${test_root}/${label}/consumer_${variant}"
                RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
            if(NOT result EQUAL 0)
                message(FATAL_ERROR "Unchanged ${variant} provider failed\n${output}\n${error}")
            endif()
        endforeach()
    endif()
endfunction()
string(REGEX MATCH "\"built_with_core\":\\{[^}]*\\}" build_member "${db_manifest}")
if(build_member STREQUAL "")
    message(FATAL_ERROR "Missing separated database build provenance")
endif()
string(REPEAT "0" 64 other_build_id)
string(REPLACE "${build_pin}" "${other_build_id}" different_build "${build_member}")
string(REPLACE "${build_member}" "${different_build}" fixture "${db_manifest}")
write_db_fixture("${fixture}")
check_db_fixture(different-build-provenance TRUE)

string(REGEX MATCH "\"requires_core\":\\{[^}]*\\}" requirement "${db_manifest}")
foreach(baseline 0.2.0 0.3.0 0.4.0 0.5.0 0.5.1 0.5.2 0.6.0 0.6.1 0.7.0 0.1.0 0.8.0)
    string(REPLACE "\"release_version\":\"${minimum}\""
        "\"release_version\":\"${baseline}\"" changed_requirement "${requirement}")
    string(REPLACE "${requirement}" "${changed_requirement}" fixture "${db_manifest}")
    write_db_fixture("${fixture}")
    if(NOT baseline VERSION_LESS "0.2.0" AND NOT baseline VERSION_GREATER "0.7.0")
        check_db_fixture(compatible-core-upgrade-${baseline} TRUE)
    else()
        check_db_fixture(rejected-baseline-${baseline} FALSE)
    endif()
endforeach()
file(WRITE "${db}/share/cpkt/packages/db.json" "${db_manifest}")
file(SHA256 "${db}/lib/libcpkt_postgres.a" final_static)
file(SHA256 "${postgres_shared}" final_shared)
if(NOT final_static STREQUAL original_static OR NOT final_shared STREQUAL original_shared)
    message(FATAL_ERROR "Compatibility probes changed the provider's binary payload")
endif()
# Byte integrity remains mandatory, independently of compatible core selection.
file(APPEND "${db}/lib/libcpkt_postgres.a" "corrupted fixture")
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${source}" -B "${test_root}/corrupt-payload"
    "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
    "-DCMAKE_PREFIX_PATH=${core};${db}"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT "${output}${error}" MATCHES "SDK payload differs")
    message(FATAL_ERROR "Corrupt provider payload was not rejected\n${output}\n${error}")
endif()
message(STATUS "Static/shared consumers passed with unchanged providers; compatibility floor, ranges, EXACT, target and payload checks passed")
