include("${CMAKE_CURRENT_LIST_DIR}/dependency_library_test_support.cmake")

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT OR LOCKDC_DEPENDENCY_BUILD_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_DEPENDENCY_BUILD_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_LIBLQL_VERSION OR LOCKDC_LIBLQL_VERSION STREQUAL "")
    message(FATAL_ERROR "LOCKDC_LIBLQL_VERSION is required")
endif()

if(NOT DEFINED LOCKDC_LIBLQL_ABI_VERSION OR LOCKDC_LIBLQL_ABI_VERSION STREQUAL "")
    message(FATAL_ERROR "LOCKDC_LIBLQL_ABI_VERSION is required")
endif()

set(liblql_root "${LOCKDC_EXTERNAL_ROOT}/liblql/install")
set(liblql_build_root "${LOCKDC_DEPENDENCY_BUILD_ROOT}/liblql/build")
set(liblql_static_archive "${liblql_root}/lib/liblql.a")
lockdc_dependency_versioned_library(liblql_shared_library "${liblql_root}/lib/liblql" "${LOCKDC_LIBLQL_ABI_VERSION}")
set(liblql_header "${liblql_root}/include/lql/lql.h")
set(liblql_version_header "${liblql_root}/include/lql/version.h")

foreach(path IN ITEMS
    "${liblql_static_archive}"
    "${liblql_shared_library}"
    "${liblql_header}"
    "${liblql_version_header}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing liblql dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${liblql_root}/bin")
    if(EXISTS "${path}")
        message(FATAL_ERROR "liblql install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${liblql_root}/lib/pkgconfig/liblql.pc"
    "${liblql_root}/lib/cmake/liblql/liblqlConfig.cmake"
    "${liblql_root}/lib/cmake/liblql/liblqlConfigVersion.cmake")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing liblql consumer metadata: ${path}")
    endif()
endforeach()

file(READ "${liblql_root}/lib/pkgconfig/liblql.pc" liblql_pc_text)
if(NOT liblql_pc_text MATCHES "(^|\n)Version: ${LOCKDC_LIBLQL_VERSION}(\n|$)")
    message(FATAL_ERROR
        "liblql pkg-config metadata does not match configured version "
        "${LOCKDC_LIBLQL_VERSION}")
endif()
file(READ "${liblql_root}/lib/cmake/liblql/liblqlConfigVersion.cmake" liblql_cmake_version_text)
if(NOT liblql_cmake_version_text MATCHES "PACKAGE_VERSION \"${LOCKDC_LIBLQL_VERSION}\"")
    message(FATAL_ERROR
        "liblql CMake package metadata does not match configured version "
        "${LOCKDC_LIBLQL_VERSION}")
endif()

if(EXISTS "${liblql_build_root}")
    message(FATAL_ERROR
        "liblql dependency unexpectedly has a local build tree: ${liblql_build_root}")
endif()

find_program(NM_BIN NAMES nm REQUIRED)

execute_process(
    COMMAND "${NM_BIN}" ${dependency_static_nm_args} "${liblql_static_archive}"
    RESULT_VARIABLE static_nm_result
    OUTPUT_VARIABLE static_symbols
    ERROR_VARIABLE static_nm_stderr
)
if(NOT static_nm_result EQUAL 0)
    message(FATAL_ERROR
        "failed to inspect liblql static archive\n"
        "stderr:\n${static_nm_stderr}")
endif()

execute_process(
    COMMAND "${NM_BIN}" ${dependency_shared_nm_args} "${liblql_shared_library}"
    RESULT_VARIABLE shared_nm_result
    OUTPUT_VARIABLE shared_symbols
    ERROR_VARIABLE shared_nm_stderr
)
if(NOT shared_nm_result EQUAL 0)
    message(FATAL_ERROR
        "failed to inspect liblql shared library\n"
        "stderr:\n${shared_nm_stderr}")
endif()

function(assert_contains text pattern description)
    if(NOT text MATCHES "${pattern}")
        message(FATAL_ERROR "liblql dependency is missing ${description}")
    endif()
endfunction()

foreach(symbol IN ITEMS
    lql_new
    lql_error_init
    lql_status_string
    lql_stream_apply
    lql_stream_apply_spooled
    lql_stream_value_size
    lql_stream_value_write_to
    lql_filter_file_spooled
    lql_rewrite_file_inline_spooled)
    assert_contains("${static_symbols}" "${symbol}" "${symbol} in liblql.a")
    assert_contains("${shared_symbols}" "${symbol}" "${symbol} in ${liblql_shared_library}")
endforeach()

if(DEFINED LOCKDC_CROSSCOMPILING AND LOCKDC_CROSSCOMPILING)
    return()
endif()

if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required for native liblql evaluator probe")
endif()

set(liblql_probe_dir "${CMAKE_CURRENT_BINARY_DIR}/liblql-spooled-evaluator-probe")
set(liblql_probe_source "${liblql_probe_dir}/liblql_spooled_evaluator_probe.c")
set(liblql_probe_binary "${liblql_probe_dir}/liblql_spooled_evaluator_probe")
file(MAKE_DIRECTORY "${liblql_probe_dir}")
file(WRITE "${liblql_probe_source}" [=[
#include <lql/lql.h>

#include <string.h>

typedef struct probe_reader_state {
  const unsigned char *data;
  size_t len;
  size_t pos;
} probe_reader_state;

static lql_status probe_read(void *user, unsigned char *buffer, size_t capacity,
                             size_t *out_len, lql_error *error) {
  probe_reader_state *state;
  size_t remaining;
  size_t take;

  (void)error;
  state = (probe_reader_state *)user;
  if (state == 0 || buffer == 0 || out_len == 0) {
    return LQL_STATUS_INVALID_ARGUMENT;
  }
  remaining = state->len - state->pos;
  take = remaining < capacity ? remaining : capacity;
  if (take != 0U) {
    memcpy(buffer, state->data + state->pos, take);
    state->pos += take;
  }
  *out_len = take;
  return LQL_STATUS_OK;
}

typedef struct probe_decision_state {
  size_t calls;
  size_t matches;
} probe_decision_state;

static lql_stream_callback_result
probe_decision(void *user, const lql_stream_decision *decision,
               lql_error *error) {
  probe_decision_state *state;

  (void)error;
  state = (probe_decision_state *)user;
  if (state == 0 || decision == 0) {
    return LQL_STREAM_CALLBACK_ERROR;
  }
  state->calls += 1U;
  if (decision->matched) {
    state->matches += 1U;
  }
  return LQL_STREAM_CALLBACK_CONTINUE;
}

int main(void) {
  static const char selector_json[] =
      "{\"eq\":{\"field\":\"/value\",\"value\":\"alpha\"}}";
  static const unsigned char ndjson[] =
      "{\"value\":\"alpha\"}\n{\"value\":\"beta\"}\n";
  probe_reader_state reader;
  probe_decision_state decisions;
  lql *runtime;
  lql_selector *selector;
  lql_stream_request request;
  lql_stream_result result;
  lql_error error;
  lql_status status;

  runtime = 0;
  selector = 0;
  lql_error_init(&error);
  status = lql_new(&runtime, &error);
  if (status != LQL_STATUS_OK) {
    return 10;
  }
  status = runtime->selector_parse_json(runtime, selector_json,
                                        strlen(selector_json), &selector,
                                        &error);
  if (status != LQL_STATUS_OK) {
    runtime->destroy(runtime);
    return 11;
  }

  memset(&reader, 0, sizeof(reader));
  reader.data = ndjson;
  reader.len = sizeof(ndjson) - 1U;
  memset(&request, 0, sizeof(request));
  request.reader = probe_read;
  request.reader_user = &reader;
  request.selector = selector;
  request.on_decision = probe_decision;
  request.decision_user = &decisions;
  memset(&decisions, 0, sizeof(decisions));
  memset(&result, 0, sizeof(result));
  lql_error_init(&error);
  status = runtime->stream_apply_spooled(runtime, &request, &result, &error);
  runtime->selector_destroy(runtime, selector);
  runtime->destroy(runtime);

  if (status != LQL_STATUS_OK) {
    return 12;
  }
  if (result.records_seen != 2U || result.records_matched != 1U) {
    return 13;
  }
  if (decisions.calls != 2U || decisions.matches != 1U) {
    return 14;
  }
  return 0;
}
]=])

execute_process(
    COMMAND "${LOCKDC_C_COMPILER}"
        -std=c89
        -Wall
        -Wextra
        -Werror
        "-I${liblql_root}/include"
        "${liblql_probe_source}"
        "-L${liblql_root}/lib"
        "-Wl,-rpath,${liblql_root}/lib"
        -llql
        -o "${liblql_probe_binary}"
    RESULT_VARIABLE liblql_probe_build_result
    OUTPUT_VARIABLE liblql_probe_build_stdout
    ERROR_VARIABLE liblql_probe_build_stderr
)
if(NOT liblql_probe_build_result EQUAL 0)
    message(FATAL_ERROR
        "failed to build native liblql spooled-evaluator probe\n"
        "stdout:\n${liblql_probe_build_stdout}\n"
        "stderr:\n${liblql_probe_build_stderr}")
endif()

execute_process(
    COMMAND "${liblql_probe_binary}"
    RESULT_VARIABLE liblql_probe_result
    OUTPUT_VARIABLE liblql_probe_stdout
    ERROR_VARIABLE liblql_probe_stderr
)
if(NOT liblql_probe_result EQUAL 0)
    message(FATAL_ERROR
        "liblql spooled evaluator did not match full-form JSON Pointer selector expectations\n"
        "exit code: ${liblql_probe_result}\n"
        "stdout:\n${liblql_probe_stdout}\n"
        "stderr:\n${liblql_probe_stderr}")
endif()
