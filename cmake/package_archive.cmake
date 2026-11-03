if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_input_dist_dir "${LOCKDC_DIST_DIR}")
endif()
if(NOT DEFINED LOCKDC_ROOT OR "${LOCKDC_ROOT}" STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR "${LOCKDC_BINARY_DIR}" STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()
include("${CMAKE_CURRENT_LIST_DIR}/LcGeneratedPath.cmake")
if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
if(DEFINED lockdc_input_dist_dir)
    set(LOCKDC_DIST_DIR "${lockdc_input_dist_dir}")
elseif(NOT DEFINED LOCKDC_DIST_DIR OR "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(LOCKDC_DIST_DIR "${LOCKDC_ROOT}/dist")
endif()

set(package_stage_root "${LOCKDC_BINARY_DIR}/package")
set(package_prefix_name "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
set(package_root "${package_stage_root}/${package_prefix_name}")

lockdc_assert_generated_path("${LOCKDC_ROOT}" "${LOCKDC_BINARY_DIR}")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${LOCKDC_DIST_DIR}")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${package_stage_root}")

function(lockdc_import_cache_path var_name)
    if(DEFINED ${var_name} AND NOT "${${var_name}}" STREQUAL "")
        return()
    endif()
    file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" cache_line
         REGEX "^${var_name}(:[^=]+)?=" LIMIT_COUNT 1)
    if(cache_line)
        string(REGEX REPLACE "^[^=]*=" "" cache_value "${cache_line}")
        set(${var_name} "${cache_value}" PARENT_SCOPE)
    endif()
endfunction()

lockdc_import_cache_path(LOCKDC_EXTERNAL_ROOT)
lockdc_import_cache_path(LOCKDC_DEPENDENCY_BUILD_ROOT)
lockdc_import_cache_path(CMAKE_STRIP)
lockdc_import_cache_path(LOCKDC_OTOOL)

file(REMOVE_RECURSE "${package_root}")
file(MAKE_DIRECTORY "${package_root}/include/lc")
file(MAKE_DIRECTORY "${package_root}/include")
file(MAKE_DIRECTORY "${package_root}/lib")
file(MAKE_DIRECTORY "${package_root}/lib/pkgconfig")
file(MAKE_DIRECTORY "${package_root}/lib/cmake/lockdc")
file(MAKE_DIRECTORY "${package_root}/share/lockdc")
file(MAKE_DIRECTORY "${package_root}/share/doc/liblockdc")

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/cmake_install.cmake")
    message(FATAL_ERROR
        "package generation requires a real install-enabled build tree; missing ${LOCKDC_BINARY_DIR}/cmake_install.cmake")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}" --prefix "${package_root}" --component runtime
    RESULT_VARIABLE lockdc_runtime_install_result
)
if(NOT lockdc_runtime_install_result EQUAL 0)
    message(FATAL_ERROR "failed to install runtime package payload")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}" --prefix "${package_root}" --component dev
    RESULT_VARIABLE lockdc_dev_install_result
)
if(NOT lockdc_dev_install_result EQUAL 0)
    message(FATAL_ERROR "failed to install development package payload")
endif()

function(lockdc_read_dependency_manifest_value manifest_path key out_var)
    if(NOT EXISTS "${manifest_path}")
        message(FATAL_ERROR "missing dependency manifest: ${manifest_path}")
    endif()
    file(STRINGS "${manifest_path}" _lockdc_manifest_line
         REGEX "^${key}=" LIMIT_COUNT 1)
    if(NOT _lockdc_manifest_line)
        message(FATAL_ERROR "dependency manifest ${manifest_path} is missing ${key}")
    endif()
    string(REGEX REPLACE "^[^=]*=" "" _lockdc_manifest_value "${_lockdc_manifest_line}")
    set(${out_var} "${_lockdc_manifest_value}" PARENT_SCOPE)
endfunction()

function(lockdc_write_sdk_metadata package_root)
    set(_lockdc_dependency_manifest "${LOCKDC_EXTERNAL_ROOT}/manifest.txt")
    lockdc_read_dependency_manifest_value("${_lockdc_dependency_manifest}" "cpkt_version" _lockdc_cpkt_version)
    lockdc_read_dependency_manifest_value("${_lockdc_dependency_manifest}" "cpkt_asset_name" _lockdc_cpkt_asset_name)
    lockdc_read_dependency_manifest_value("${_lockdc_dependency_manifest}" "cpkt_asset_hash" _lockdc_cpkt_asset_hash)
    lockdc_read_dependency_manifest_value("${_lockdc_dependency_manifest}" "lonejson_asset_name" _lockdc_lonejson_asset_name)
    lockdc_read_dependency_manifest_value("${_lockdc_dependency_manifest}" "lonejson_asset_hash" _lockdc_lonejson_asset_hash)
    lockdc_read_dependency_manifest_value("${_lockdc_dependency_manifest}" "liblql_asset_name" _lockdc_liblql_asset_name)
    lockdc_read_dependency_manifest_value("${_lockdc_dependency_manifest}" "liblql_asset_hash" _lockdc_liblql_asset_hash)

    set(_lockdc_cpkt_source_url
        "https://github.com/c89systems/cpkt/releases/download/v${_lockdc_cpkt_version}/${_lockdc_cpkt_asset_name}")
    set(_lockdc_lonejson_source_url
        "https://github.com/c89systems/lonejson/releases/download/v${LOCKDC_LONEJSON_VERSION}/${_lockdc_lonejson_asset_name}")
    set(_lockdc_liblql_source_url
        "https://github.com/c89systems/liblql/releases/download/v${LOCKDC_LIBLQL_VERSION}/${_lockdc_liblql_asset_name}")
    set(_lockdc_pslog_source_url
        "https://github.com/c89systems/libpslog/releases/download/v${LOCKDC_PSLOG_VERSION}/${LOCKDC_PSLOG_ASSET_NAME}")

    file(WRITE "${package_root}/share/lockdc/package-metadata.cmake"
        "set(LOCKDC_PACKAGE_NAME \"liblockdc\")\n"
        "set(LOCKDC_VERSION \"${LOCKDC_VERSION}\")\n"
        "set(LOCKDC_ABI_VERSION \"${LOCKDC_ABI_VERSION}\")\n"
        "set(LOCKDC_TARGET_ID \"${LOCKDC_TARGET_ID}\")\n"
        "set(LOCKDC_SHARED_LIB_NAME \"${LOCKDC_SHARED_LIB_NAME}\")\n"
        "set(LOCKDC_SHARED_SONAME \"${LOCKDC_SHARED_SONAME}\")\n"
        "set(LOCKDC_SHARED_LINK_NAME \"${LOCKDC_SHARED_LINK_NAME}\")\n"
        "set(LOCKDC_DEPENDENCY_MODE \"external\")\n"
        "set(LOCKDC_METADATA_VERSION \"1\")\n")

    # Core SDK prerequisites exclude optional provider build inputs, even when
    # those providers are built in the same workspace. PostgreSQL owns its own
    # optional artifact/dependency closure; it cannot become a core prerequisite.
    file(APPEND "${package_root}/share/lockdc/package-metadata.cmake"
        "set(LOCKDC_CPKT_VERSION \"${_lockdc_cpkt_version}\")\n")

    file(WRITE "${package_root}/share/lockdc/dependencies.json" "{\n")
    file(APPEND "${package_root}/share/lockdc/dependencies.json"
        "  \"schema\": \"lockdc.dependencies.v1\",\n"
        "  \"package\": \"liblockdc\",\n"
        "  \"version\": \"${LOCKDC_VERSION}\",\n"
        "  \"target_id\": \"${LOCKDC_TARGET_ID}\",\n"
        "  \"dependency_mode\": \"external\",\n"
        "  \"sdk_prerequisites\": [\n"
        "    {\"name\": \"cpkt\", \"version\": \"${_lockdc_cpkt_version}\", \"source_url\": \"${_lockdc_cpkt_source_url}\", \"sha256\": \"${_lockdc_cpkt_asset_hash}\"}\n"
        "  ],\n"
        "  \"dependencies\": [\n")

    set(_lockdc_dependency_rows
        "openssl|${LOCKDC_OPENSSL_VERSION}|cpkt|${_lockdc_cpkt_asset_name}|${_lockdc_cpkt_source_url}|${_lockdc_cpkt_asset_hash}|Apache-2.0|external-static-consumer"
        "zlib|${LOCKDC_ZLIB_VERSION}|cpkt|${_lockdc_cpkt_asset_name}|${_lockdc_cpkt_source_url}|${_lockdc_cpkt_asset_hash}|Zlib|external-static-consumer"
        "curl|${LOCKDC_CURL_VERSION}|cpkt|${_lockdc_cpkt_asset_name}|${_lockdc_cpkt_source_url}|${_lockdc_cpkt_asset_hash}|curl|external-static-consumer"
        "nghttp2|${LOCKDC_NGHTTP2_VERSION}|cpkt|${_lockdc_cpkt_asset_name}|${_lockdc_cpkt_source_url}|${_lockdc_cpkt_asset_hash}|MIT|external-static-consumer"
        "libssh2|${LOCKDC_LIBSSH2_VERSION}|cpkt|${_lockdc_cpkt_asset_name}|${_lockdc_cpkt_source_url}|${_lockdc_cpkt_asset_hash}|BSD-3-Clause|external-static-consumer"
        "libpslog|${LOCKDC_PSLOG_VERSION}|github-release|${LOCKDC_PSLOG_ASSET_NAME}|${_lockdc_pslog_source_url}|${LOCKDC_PSLOG_ASSET_HASH}|MIT|external-static-consumer"
        "lonejson|${LOCKDC_LONEJSON_VERSION}|github-release|${_lockdc_lonejson_asset_name}|${_lockdc_lonejson_source_url}|${_lockdc_lonejson_asset_hash}|MIT|external-static-consumer"
        "liblql|${LOCKDC_LIBLQL_VERSION}|github-release|${_lockdc_liblql_asset_name}|${_lockdc_liblql_source_url}|${_lockdc_liblql_asset_hash}|MIT|external-static-consumer"
    )
    list(LENGTH _lockdc_dependency_rows _lockdc_dependency_row_count)
    math(EXPR _lockdc_dependency_last_index "${_lockdc_dependency_row_count} - 1")
    foreach(_lockdc_dependency_index RANGE 0 ${_lockdc_dependency_last_index})
        list(GET _lockdc_dependency_rows ${_lockdc_dependency_index} _lockdc_dependency_row)
        string(REPLACE "|" ";" _lockdc_dependency_fields "${_lockdc_dependency_row}")
        list(GET _lockdc_dependency_fields 0 _lockdc_dep_name)
        list(GET _lockdc_dependency_fields 1 _lockdc_dep_version)
        list(GET _lockdc_dependency_fields 2 _lockdc_dep_source_system)
        list(GET _lockdc_dependency_fields 3 _lockdc_dep_archive)
        list(GET _lockdc_dependency_fields 4 _lockdc_dep_source_url)
        list(GET _lockdc_dependency_fields 5 _lockdc_dep_sha256)
        list(GET _lockdc_dependency_fields 6 _lockdc_dep_license)
        list(GET _lockdc_dependency_fields 7 _lockdc_dep_role)
        if(_lockdc_dependency_index EQUAL _lockdc_dependency_last_index)
            set(_lockdc_json_comma "")
        else()
            set(_lockdc_json_comma ",")
        endif()
        file(APPEND "${package_root}/share/lockdc/dependencies.json"
            "    {\n"
            "      \"name\": \"${_lockdc_dep_name}\",\n"
            "      \"version\": \"${_lockdc_dep_version}\",\n"
            "      \"target_id\": \"${LOCKDC_TARGET_ID}\",\n"
            "      \"source_system\": \"${_lockdc_dep_source_system}\",\n"
            "      \"source_url\": \"${_lockdc_dep_source_url}\",\n"
            "      \"sha256\": \"${_lockdc_dep_sha256}\",\n"
            "      \"archive_name\": \"${_lockdc_dep_archive}\",\n"
            "      \"license\": \"${_lockdc_dep_license}\",\n"
            "      \"bundled\": false,\n"
            "      \"role\": \"${_lockdc_dep_role}\"\n"
            "    }${_lockdc_json_comma}\n")
    endforeach()
    file(APPEND "${package_root}/share/lockdc/dependencies.json"
        "  ]\n"
        "}\n")
endfunction()

lockdc_write_sdk_metadata("${package_root}")

function(lockdc_find_darwin_otool)
    if(NOT LOCKDC_TARGET_ID MATCHES "apple-darwin$")
        return()
    endif()
    if(NOT LOCKDC_OTOOL OR NOT EXISTS "${LOCKDC_OTOOL}")
        execute_process(
            COMMAND "${LOCKDC_ROOT}/scripts/discover_target_tools.sh"
                --build-dir "${LOCKDC_BINARY_DIR}"
                --target-id "${LOCKDC_TARGET_ID}"
                --tool otool
            RESULT_VARIABLE _lockdc_otool_discovery_result
            OUTPUT_VARIABLE _lockdc_otool_discovery_output
            ERROR_VARIABLE _lockdc_otool_discovery_error
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        if(_lockdc_otool_discovery_result EQUAL 0 AND EXISTS "${_lockdc_otool_discovery_output}")
            set(LOCKDC_OTOOL "${_lockdc_otool_discovery_output}")
        endif()
    endif()
    if(NOT LOCKDC_OTOOL OR NOT EXISTS "${LOCKDC_OTOOL}")
        message(FATAL_ERROR
            "external-tool-unavailable: otool is required for Darwin package verification\n"
            "${_lockdc_otool_discovery_error}")
    endif()
    set(LOCKDC_OTOOL "${LOCKDC_OTOOL}" PARENT_SCOPE)
endfunction()

function(lockdc_assert_darwin_dependency_path dylib dependency_path otool_output)
    if(NOT dependency_path MATCHES "^/")
        return()
    endif()
    if(dependency_path MATCHES "^/usr/lib/" OR dependency_path MATCHES "^/System/Library/")
        return()
    endif()
    message(FATAL_ERROR
        "Darwin package dylib contains non-system absolute dependency path "
        "'${dependency_path}' in ${dylib}\n${otool_output}")
endfunction()

function(lockdc_verify_darwin_macho_metadata package_root)
    if(NOT LOCKDC_TARGET_ID MATCHES "apple-darwin$")
        return()
    endif()

    lockdc_find_darwin_otool()

    file(GLOB _lockdc_dylibs LIST_DIRECTORIES false "${package_root}/lib/*.dylib")
    foreach(_lockdc_dylib IN LISTS _lockdc_dylibs)
        if(IS_SYMLINK "${_lockdc_dylib}")
            continue()
        endif()
        execute_process(
            COMMAND "${LOCKDC_OTOOL}" -D "${_lockdc_dylib}"
            RESULT_VARIABLE _lockdc_id_result
            OUTPUT_VARIABLE _lockdc_id_output
            ERROR_VARIABLE _lockdc_id_error
        )
        if(NOT _lockdc_id_result EQUAL 0)
            message(FATAL_ERROR "failed to inspect Darwin dylib install name ${_lockdc_dylib}\n${_lockdc_id_error}")
        endif()
        if(NOT _lockdc_id_output MATCHES "\n@rpath/")
            message(FATAL_ERROR
                "Darwin package dylib install name is not @rpath-relative: ${_lockdc_dylib}\n"
                "${_lockdc_id_output}")
        endif()

        execute_process(
            COMMAND "${LOCKDC_OTOOL}" -L "${_lockdc_dylib}"
            RESULT_VARIABLE _lockdc_otool_result
            OUTPUT_VARIABLE _lockdc_otool_output
            ERROR_VARIABLE _lockdc_otool_error
        )
        if(NOT _lockdc_otool_result EQUAL 0)
            message(FATAL_ERROR "failed to inspect Darwin dylib ${_lockdc_dylib}\n${_lockdc_otool_error}")
        endif()
        string(REGEX REPLACE "\n$" "" _lockdc_otool_output "${_lockdc_otool_output}")
        string(REPLACE "\n" ";" _lockdc_otool_lines "${_lockdc_otool_output}")
        foreach(_lockdc_otool_line IN LISTS _lockdc_otool_lines)
            string(STRIP "${_lockdc_otool_line}" _lockdc_dependency_line)
            if(_lockdc_dependency_line MATCHES ":$")
                continue()
            endif()
            if(NOT _lockdc_dependency_line MATCHES "^/")
                continue()
            endif()
            string(REGEX MATCH "^[^ \t]+" _lockdc_dependency_path "${_lockdc_dependency_line}")
            if(_lockdc_dependency_path STREQUAL "")
                continue()
            endif()
            lockdc_assert_darwin_dependency_path(
                "${_lockdc_dylib}" "${_lockdc_dependency_path}" "${_lockdc_otool_output}")
        endforeach()

        execute_process(
            COMMAND "${LOCKDC_OTOOL}" -l "${_lockdc_dylib}"
            RESULT_VARIABLE _lockdc_load_result
            OUTPUT_VARIABLE _lockdc_load_output
            ERROR_VARIABLE _lockdc_load_error
        )
        if(NOT _lockdc_load_result EQUAL 0)
            message(FATAL_ERROR "failed to inspect Darwin dylib load commands ${_lockdc_dylib}\n${_lockdc_load_error}")
        endif()
        if(_lockdc_load_output MATCHES "path /")
            message(FATAL_ERROR
                "Darwin package dylib contains absolute rpath: ${_lockdc_dylib}\n${_lockdc_load_output}")
        endif()
    endforeach()
endfunction()

lockdc_verify_darwin_macho_metadata("${package_root}")

function(lockdc_strip_packaged_artifact artifact_path)
    if(NOT CMAKE_STRIP OR NOT EXISTS "${CMAKE_STRIP}")
        return()
    endif()
    if(NOT EXISTS "${artifact_path}" OR IS_SYMLINK "${artifact_path}")
        return()
    endif()
    if(LOCKDC_TARGET_ID MATCHES "apple-darwin$")
        return()
    endif()

    execute_process(
        COMMAND "${CMAKE_STRIP}" -S "${artifact_path}"
        RESULT_VARIABLE _lockdc_strip_result
        ERROR_VARIABLE _lockdc_strip_error
    )
    if(NOT _lockdc_strip_result EQUAL 0)
        message(FATAL_ERROR "failed to strip ${artifact_path}\n${_lockdc_strip_error}")
    endif()
endfunction()

file(GLOB _lockdc_owned_library_artifacts
    LIST_DIRECTORIES false
    "${package_root}/lib/liblockdc.a"
    "${package_root}/lib/liblockdc.so*"
    "${package_root}/lib/liblockdc.*.dylib"
)
foreach(_lockdc_owned_library_artifact IN LISTS _lockdc_owned_library_artifacts)
    lockdc_strip_packaged_artifact("${_lockdc_owned_library_artifact}")
endforeach()

file(MAKE_DIRECTORY "${LOCKDC_DIST_DIR}")
find_program(LOCKDC_TAR_BIN NAMES tar)
find_program(LOCKDC_GZIP_BIN NAMES gzip)
if(NOT LOCKDC_TAR_BIN)
    message(FATAL_ERROR "failed to find tar for archive creation")
endif()
if(NOT LOCKDC_GZIP_BIN)
    message(FATAL_ERROR "failed to find gzip for archive creation")
endif()
function(lockdc_create_archive prefix)
    set(base "${LOCKDC_DIST_DIR}/${prefix}.tar")
    file(REMOVE "${base}" "${base}.gz")
    execute_process(
        COMMAND "${LOCKDC_TAR_BIN}" -cf "${base}" --format=gnu --owner 0 --group 0 "${prefix}"
        WORKING_DIRECTORY "${package_stage_root}"
        RESULT_VARIABLE result)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "failed to create ${prefix} archive")
    endif()
    execute_process(COMMAND "${LOCKDC_GZIP_BIN}" -9 -f "${base}"
        RESULT_VARIABLE result)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "failed to gzip ${prefix} archive")
    endif()
endfunction()
lockdc_create_archive("${package_prefix_name}")

if(LOCKDC_BUILD_POSTGRES)
    set(provider_prefix "liblockdc-postgres-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
    set(provider_root "${package_stage_root}/${provider_prefix}")
    file(REMOVE_RECURSE "${provider_root}")
    foreach(component postgres-runtime postgres-dev)
        execute_process(COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}"
            --prefix "${provider_root}" --component "${component}"
            RESULT_VARIABLE result)
        if(NOT result EQUAL 0)
            message(FATAL_ERROR "failed to install optional provider ${component}")
        endif()
    endforeach()
    file(MAKE_DIRECTORY "${provider_root}/share/lockdc_postgres")
    file(WRITE "${provider_root}/share/lockdc_postgres/package-metadata.cmake"
        "set(LOCKDC_POSTGRES_PACKAGE_NAME \"liblockdc-postgres\")\n"
        "set(LOCKDC_POSTGRES_VERSION \"${LOCKDC_VERSION}\")\n"
        "set(LOCKDC_POSTGRES_TARGET_ID \"${LOCKDC_TARGET_ID}\")\n"
        "set(LOCKDC_POSTGRES_CORE_ABI \"${LOCKDC_ABI_VERSION}\")\n"
        "set(LOCKDC_POSTGRES_CPKT_VERSION \"${LOCKDC_CPKT_VERSION}\")\n"
        "set(LOCKDC_POSTGRES_CPKTDB_VERSION \"${LOCKDC_CPKTDB_VERSION}\")\n")
    lockdc_verify_darwin_macho_metadata("${provider_root}")
    file(GLOB provider_libraries LIST_DIRECTORIES false
        "${provider_root}/lib/liblockdc_postgres.a"
        "${provider_root}/lib/liblockdc_postgres.so*"
        "${provider_root}/lib/liblockdc_postgres*.dylib")
    foreach(library IN LISTS provider_libraries)
        lockdc_strip_packaged_artifact("${library}")
    endforeach()
    lockdc_create_archive("${provider_prefix}")
endif()
