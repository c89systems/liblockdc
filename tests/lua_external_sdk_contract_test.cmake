if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()

if(NOT DEFINED LOCKDC_VERSION OR LOCKDC_VERSION STREQUAL "")
    message(FATAL_ERROR "LOCKDC_VERSION is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/lua-external-sdk-contract-test")
set(fake_bin_dir "${test_root}/bin")
set(fake_prefix "${test_root}/fake-prefix")
set(script_path "${LOCKDC_ROOT}/scripts/build_lua_rock.sh")
set(fake_path "${fake_bin_dir}:/usr/bin:/bin")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${fake_bin_dir}" "${fake_prefix}/lib/pkgconfig")
file(WRITE "${fake_bin_dir}/pkg-config" "#!/bin/sh\nexit 1\n")
execute_process(COMMAND chmod +x "${fake_bin_dir}/pkg-config")

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PATH=${fake_path}"
        bash "${script_path}" /bin/true "" -shared o so "${LOCKDC_ROOT}/include" "${LOCKDC_VERSION}"
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE missing_result
    OUTPUT_VARIABLE missing_stdout
    ERROR_VARIABLE missing_stderr
)
if(missing_result EQUAL 0)
    message(FATAL_ERROR "expected missing-SDK Lua rock build to fail")
endif()
foreach(required_snippet
    "liblockdc ${LOCKDC_VERSION} is required by this Lua rock"
    "releases/download/v${LOCKDC_VERSION}/liblockdc-${LOCKDC_VERSION}-"
    "LOCKDC_PREFIX=/path/to/liblockdc-${LOCKDC_VERSION}-<target>"
)
    string(FIND "${missing_stderr}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "missing-SDK failure is missing snippet '${required_snippet}'\n"
            "stderr:\n${missing_stderr}")
    endif()
endforeach()

file(WRITE "${fake_prefix}/lib/pkgconfig/lockdc.pc" "prefix=${fake_prefix}\nVersion: 9.9.9\n")
file(WRITE "${fake_prefix}/lib/liblockdc.so" "")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PATH=${fake_path}"
        "LOCKDC_PREFIX=${fake_prefix}"
        bash "${script_path}" /bin/true "" -shared o so "${LOCKDC_ROOT}/include" "${LOCKDC_VERSION}"
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE mismatch_result
    OUTPUT_VARIABLE mismatch_stdout
    ERROR_VARIABLE mismatch_stderr
)
if(mismatch_result EQUAL 0)
    message(FATAL_ERROR "expected mismatched-SDK Lua rock build to fail")
endif()
foreach(required_snippet
    "found liblockdc 9.9.9, but this Lua rock requires liblockdc ${LOCKDC_VERSION}"
    "releases/download/v${LOCKDC_VERSION}/liblockdc-${LOCKDC_VERSION}-"
)
    string(FIND "${mismatch_stderr}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "mismatched-SDK failure is missing snippet '${required_snippet}'\n"
            "stderr:\n${mismatch_stderr}")
    endif()
endforeach()

file(WRITE "${fake_prefix}/lib/pkgconfig/lockdc.pc" "prefix=${fake_prefix}\nVersion: ${LOCKDC_VERSION}\n")
file(REMOVE "${fake_prefix}/lib/liblockdc.so")
file(WRITE "${fake_prefix}/lib/liblockdc.a" "")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PATH=${fake_path}"
        "LOCKDC_PREFIX=${fake_prefix}"
        bash "${script_path}" /bin/true "" -shared o so "${LOCKDC_ROOT}/include" "${LOCKDC_VERSION}"
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE static_only_result
    OUTPUT_VARIABLE static_only_stdout
    ERROR_VARIABLE static_only_stderr
)
if(static_only_result EQUAL 0)
    message(FATAL_ERROR "expected static-only SDK Lua rock build to fail")
endif()
foreach(required_snippet
    "normal LuaRocks builds require a shared liblockdc SDK"
    "static-only SDKs are for vectis or in-tree embedded Lua builds"
)
    string(FIND "${static_only_stderr}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "static-only SDK failure is missing snippet '${required_snippet}'\n"
            "stderr:\n${static_only_stderr}")
    endif()
endforeach()
