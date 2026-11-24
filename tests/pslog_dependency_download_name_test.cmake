cmake_minimum_required(VERSION 3.20)

if(NOT DEFINED LOCKDC_ROOT)
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/cmake/LcDependencies.cmake" lc_dependencies_text)
file(READ "${LOCKDC_ROOT}/scripts/deps.sh" deps_script_text)

set(expected_asset_name
    "set(asset_name \"libpslog-\${LOCKDC_PSLOG_VERSION}-\${LOCKDC_TARGET_ID}.tar.gz\")")
string(FIND "${lc_dependencies_text}" "${expected_asset_name}" asset_name_pos)
if(asset_name_pos EQUAL -1)
  message(FATAL_ERROR
    "pslog dependency asset naming no longer keys downloads by target.\n"
    "Expected to find: ${expected_asset_name}")
endif()

set(expected_lonejson_asset_name
    "lonejson_asset_name=\"liblonejson-$lonejson_version-$dependency_target_id.tar.gz\"")
string(FIND "${deps_script_text}" "${expected_lonejson_asset_name}" lonejson_asset_name_pos)
if(lonejson_asset_name_pos EQUAL -1)
  message(FATAL_ERROR
    "lonejson dependency asset naming no longer keys downloads by target.\n"
    "Expected to find: ${expected_lonejson_asset_name}")
endif()

set(expected_download_name "DOWNLOAD_NAME \"\${asset_name}\"")
string(FIND "${lc_dependencies_text}" "${expected_download_name}" download_name_pos)
if(download_name_pos EQUAL -1)
  message(FATAL_ERROR
    "pslog dependency download name is no longer derived from the target asset name.\n"
    "Expected to find: ${expected_download_name}")
endif()

set(old_variant_snippet "DOWNLOAD_NAME \"\${variant}-\${asset_name}\"")
string(FIND "${lc_dependencies_text}" "${old_variant_snippet}" old_variant_pos)
if(NOT old_variant_pos EQUAL -1)
  message(FATAL_ERROR
    "pslog dependency download naming still references the removed variant split.\n"
    "Unexpected legacy snippet: ${old_variant_snippet}")
endif()

foreach(legacy_snippet IN ITEMS
    "pslog-\${LOCKDC_PSLOG_VERSION}.h"
    "lonejson-\${LOCKDC_LONEJSON_VERSION}.h"
    "lc_prepare_pslog_single_header_source"
    "lc_prepare_lonejson_source"
    "PSLOG_IMPLEMENTATION"
    "LONEJSON_IMPLEMENTATION")
  string(FIND "${lc_dependencies_text}" "${legacy_snippet}" legacy_pos)
  if(NOT legacy_pos EQUAL -1)
    message(FATAL_ERROR
      "dependency setup still contains local single-header build code.\n"
      "Unexpected legacy snippet: ${legacy_snippet}")
  endif()
endforeach()
