cmake_minimum_required(VERSION 3.21)
if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

include("${LOCKDC_ROOT}/tests/bootlin_runtime_test_support.cmake")

foreach(cache_key CMAKE_C_COMPILER CMAKE_TOOLCHAIN_FILE CMAKE_LINKER
        CMAKE_EXE_LINKER_FLAGS CMAKE_OSX_SYSROOT CMAKE_OSX_DEPLOYMENT_TARGET
        CMAKE_BUILD_TYPE CMAKE_SYSROOT)
    lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" ${cache_key})
endforeach()

if(NOT DEFINED CMAKE_C_COMPILER OR CMAKE_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "CMAKE_C_COMPILER is required")
endif()

if(NOT DEFINED CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE "Release")
endif()

if(NOT DEFINED LOCKDC_RUN_DOWNSTREAM_BINARIES)
    set(LOCKDC_RUN_DOWNSTREAM_BINARIES ON)
endif()

if(NOT DEFINED LOCKDC_USE_EXISTING_ARCHIVE)
    set(LOCKDC_USE_EXISTING_ARCHIVE OFF)
endif()

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    message(FATAL_ERROR "missing package metadata: ${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
if(DEFINED LOCKDC_DIST_DIR AND NOT LOCKDC_DIST_DIR STREQUAL "")
    set(lockdc_requested_dist_dir "${LOCKDC_DIST_DIR}")
endif()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
if(DEFINED lockdc_requested_dist_dir)
    set(LOCKDC_DIST_DIR "${lockdc_requested_dist_dir}")
endif()
include("${LOCKDC_ROOT}/tests/package_config_dependency_provenance.cmake")

set(test_root "${LOCKDC_BINARY_DIR}/release-tarball-sdk-test")
if(LOCKDC_USE_EXISTING_ARCHIVE)
    if(NOT DEFINED LOCKDC_DIST_DIR OR LOCKDC_DIST_DIR STREQUAL "")
        message(FATAL_ERROR
            "LOCKDC_DIST_DIR is required when LOCKDC_USE_EXISTING_ARCHIVE is enabled")
    endif()
    set(dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(dist_dir "${test_root}/dist")
endif()
set(extract_root "${test_root}/release")
set(consumer_src_dir "${test_root}/consumer")
set(consumer_bin_dir "${test_root}/consumer-build")

set(release_archive "${dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")
set(release_prefix "${extract_root}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${extract_root}" "${consumer_src_dir}" "${consumer_bin_dir}")

if(NOT LOCKDC_USE_EXISTING_ARCHIVE)
    file(MAKE_DIRECTORY "${dist_dir}")
    file(REMOVE "${release_archive}")

    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${dist_dir}
            -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
        RESULT_VARIABLE package_result
        OUTPUT_VARIABLE package_stdout
        ERROR_VARIABLE package_stderr
    )
    if(NOT package_result EQUAL 0)
        message(FATAL_ERROR
            "failed to build release archive for SDK test\n"
            "stdout:\n${package_stdout}\n"
            "stderr:\n${package_stderr}")
    endif()
endif()

if(NOT EXISTS "${release_archive}")
    message(FATAL_ERROR "missing release archive for SDK test: ${release_archive}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${release_archive}"
    WORKING_DIRECTORY "${extract_root}"
    RESULT_VARIABLE extract_result
    OUTPUT_VARIABLE extract_stdout
    ERROR_VARIABLE extract_stderr
)
if(NOT extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract release archive for SDK test\n"
        "stdout:\n${extract_stdout}\n"
        "stderr:\n${extract_stderr}")
endif()

if(NOT EXISTS "${release_prefix}")
    message(FATAL_ERROR "release tarball missing expected prefix directory: ${release_prefix}")
endif()

lockdc_resolve_bootlin_runtime("${CMAKE_SYSROOT}" "${LOCKDC_EXTERNAL_ROOT}"
    "${release_prefix}/lib" lockdc_bootlin_runtime_loader
    lockdc_bootlin_runtime_dirs)
string(REPLACE ";" "\\;" lockdc_bootlin_runtime_dirs_arg
    "${lockdc_bootlin_runtime_dirs}")
lockdc_bootlin_runtime_link_flags("${lockdc_bootlin_runtime_loader}"
    "${lockdc_bootlin_runtime_dirs}" lockdc_bootlin_direct_link_flags)

foreach(required_path
    "${release_prefix}/include/lc/lc.h"
    "${release_prefix}/include/lc/version.h"
    "${release_prefix}/lib/liblockdc.a"
    "${release_prefix}/lib/${LOCKDC_SHARED_LINK_NAME}"
    "${release_prefix}/lib/pkgconfig/lockdc.pc"
    "${release_prefix}/lib/cmake/lockdc/lockdcConfig.cmake"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "release tarball is missing required SDK artifact: ${required_path}")
    endif()
endforeach()

foreach(forbidden_path
    "${release_prefix}/include/lonejson.h"
    "${release_prefix}/include/lql"
    "${release_prefix}/include/pslog.h"
    "${release_prefix}/include/curl"
    "${release_prefix}/include/openssl"
    "${release_prefix}/include/nghttp2"
    "${release_prefix}/include/libssh2.h"
    "${release_prefix}/include/zlib.h"
    "${release_prefix}/lib/liblonejson.a"
    "${release_prefix}/lib/liblql.a"
    "${release_prefix}/lib/libpslog.a"
    "${release_prefix}/lib/libcurl.a"
    "${release_prefix}/lib/libssl.a"
    "${release_prefix}/lib/libcrypto.a"
    "${release_prefix}/lib/libnghttp2.a"
    "${release_prefix}/lib/libssh2.a"
    "${release_prefix}/lib/libz.a"
    "${release_prefix}/share/lua/5.5/lockdc/init.lua"
    "${release_prefix}/share/lockdc/luarocks"
    "${release_prefix}/lib/lua/5.5/lockdc"
)
    if(EXISTS "${forbidden_path}")
        message(FATAL_ERROR "release tarball unexpectedly includes Lua artifact: ${forbidden_path}")
    endif()
endforeach()

file(READ "${release_prefix}/lib/cmake/lockdc/lockdcConfig.cmake" lockdc_config_text)
file(READ "${release_prefix}/lib/pkgconfig/lockdc.pc" lockdc_pkgconfig_text)
foreach(forbidden_reference "${LOCKDC_ROOT}" "${LOCKDC_BINARY_DIR}" "${LOCKDC_ROOT}/.cache")
    string(REPLACE "\\" "\\\\" escaped_reference "${forbidden_reference}")
    string(REPLACE "." "\\." escaped_reference "${escaped_reference}")
    if(lockdc_config_text MATCHES "${escaped_reference}")
        message(FATAL_ERROR
            "release tarball CMake package leaks build-tree path '${forbidden_reference}'\n"
            "config:\n${lockdc_config_text}")
    endif()
    if(lockdc_pkgconfig_text MATCHES "${escaped_reference}")
        message(FATAL_ERROR
            "release tarball pkg-config metadata leaks build-tree path '${forbidden_reference}'\n"
            "pkg-config:\n${lockdc_pkgconfig_text}")
    endif()
endforeach()

if(lockdc_config_text MATCHES "atomic")
    string(FIND "${lockdc_pkgconfig_text}" "-latomic" lockdc_pkgconfig_atomic_index)
    if(lockdc_pkgconfig_atomic_index EQUAL -1)
        message(FATAL_ERROR
            "release tarball pkg-config metadata is missing -latomic even though the CMake package exports it\n"
            "pkg-config:\n${lockdc_pkgconfig_text}\n"
            "config:\n${lockdc_config_text}")
    endif()
endif()

string(REGEX MATCH "(^|\n)Requires: pslog lonejson libcurl libssh2 openssl libnghttp2 zlib liblql(\n|$)"
    lockdc_pkgconfig_public_requires_match "${lockdc_pkgconfig_text}")
if(NOT lockdc_pkgconfig_public_requires_match)
    message(FATAL_ERROR
        "release tarball pkg-config metadata must expose shared-link dependency packages through Requires\n"
        "pkg-config:\n${lockdc_pkgconfig_text}")
endif()

string(REGEX MATCH "(^|\n)Libs: ([^\n]*)" lockdc_pkgconfig_public_libs_line
    "${lockdc_pkgconfig_text}")
if(NOT lockdc_pkgconfig_public_libs_line)
    message(FATAL_ERROR
        "release tarball pkg-config metadata is missing Libs\n"
        "pkg-config:\n${lockdc_pkgconfig_text}")
endif()
foreach(forbidden_pkgconfig_public_lib
    -lpslog
    -llonejson
    -llql
    -lcurl
    -lssh2
    -lz
    -lssl
    -lcrypto
    -lnghttp2
    -lpthread
    -ldl
    -latomic)
    string(FIND "${lockdc_pkgconfig_public_libs_line}"
        "${forbidden_pkgconfig_public_lib}" forbidden_pkgconfig_public_lib_index)
    if(NOT forbidden_pkgconfig_public_lib_index EQUAL -1)
        message(FATAL_ERROR
            "release tarball pkg-config Libs overexposes '${forbidden_pkgconfig_public_lib}'\n"
            "Libs line: ${lockdc_pkgconfig_public_libs_line}\n"
            "pkg-config:\n${lockdc_pkgconfig_text}")
    endif()
endforeach()

string(REGEX MATCH "(^|\n)Libs\\.private: ([^\n]*)"
    lockdc_pkgconfig_private_libs_line "${lockdc_pkgconfig_text}")
foreach(forbidden_pkgconfig_private_lib
    -lpslog
    -llonejson
    -llql
    -lcurl
    -lssh2
    -lz
    -lssl
    -lcrypto
    -lnghttp2)
    string(FIND "${lockdc_pkgconfig_private_libs_line}"
        "${forbidden_pkgconfig_private_lib}" forbidden_pkgconfig_private_lib_index)
    if(NOT forbidden_pkgconfig_private_lib_index EQUAL -1)
        message(FATAL_ERROR
            "release tarball pkg-config Libs.private must not duplicate dependency packages represented by Requires: '${forbidden_pkgconfig_private_lib}'\n"
            "Libs.private line: ${lockdc_pkgconfig_private_libs_line}\n"
            "pkg-config:\n${lockdc_pkgconfig_text}")
    endif()
endforeach()

file(SHA256 "${release_prefix}/lib/cmake/lockdc/lockdcConfig.cmake" config_digest)
if(NOT config_digest IN_LIST LOCKDC_VERIFIED_CONFIG_DIGESTS)
    lockdc_assert_config_accepts_non_cpkt_curl(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
    lockdc_assert_config_rejects_missing_curl_target(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
    lockdc_assert_config_accepts_shared_only_dependency_targets(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
    lockdc_assert_config_supports_repeated_required_discovery(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
    lockdc_assert_config_rejects_static_component_without_static_dependency_targets(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
    lockdc_assert_config_accepts_optional_static_component_without_static_dependency_targets(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
    lockdc_assert_config_accepts_optional_shared_component_without_shared_library(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
    lockdc_assert_config_supports_quiet_optional_discovery(
        "${release_prefix}"
        "${LOCKDC_EXTERNAL_ROOT}"
        "${test_root}")
endif()

file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_release_tarball_consumer C)

if(NOT "${LOCKDC_BOOTLIN_RUNTIME_LOADER}" STREQUAL "")
    set(CMAKE_SKIP_BUILD_RPATH TRUE)
    add_link_options(
        "LINKER:--dynamic-linker,${LOCKDC_BOOTLIN_RUNTIME_LOADER}"
        "LINKER:--disable-new-dtags")
    foreach(lockdc_runtime_dir IN LISTS LOCKDC_BOOTLIN_RUNTIME_DIRS)
        add_link_options("LINKER:-rpath,${lockdc_runtime_dir}")
    endforeach()
endif()

find_package(lockdc CONFIG REQUIRED)

function(lockdc_assert_no_raw_dependency_links target_name)
    get_target_property(link_items "${target_name}" INTERFACE_LINK_LIBRARIES)
    foreach(link_item IN LISTS link_items)
        if(link_item MATCHES "^-l(curl|pslog|nghttp2|ssh2|ssl|crypto|z|lonejson|lql)$" OR
           link_item MATCHES "^(curl|pslog|nghttp2|ssh2|ssl|crypto|z|lonejson|lql)$")
            message(FATAL_ERROR
                "${target_name} exposes raw dependency link item '${link_item}' instead of an imported CMake target")
        endif()
    endforeach()
endfunction()

function(lockdc_assert_target_links target_name)
    get_target_property(link_items "${target_name}" INTERFACE_LINK_LIBRARIES)
    foreach(expected_item IN LISTS ARGN)
        list(FIND link_items "${expected_item}" expected_index)
        if(expected_index EQUAL -1)
            message(FATAL_ERROR
                "${target_name} is missing expected dependency target '${expected_item}'\n"
                "INTERFACE_LINK_LIBRARIES=${link_items}")
        endif()
    endforeach()
endfunction()

if(TARGET lockdc::static)
    lockdc_assert_no_raw_dependency_links(lockdc::static)
    lockdc_assert_target_links(lockdc::static
        CURL::libcurl
        OpenSSL::SSL
        OpenSSL::Crypto
        nghttp2::nghttp2
        Libssh2::libssh2
        ZLIB::ZLIB
        pslog::pslog_static
        lonejson::lonejson_static
        liblql::lql_static)
endif()

if(TARGET lockdc::shared)
    lockdc_assert_no_raw_dependency_links(lockdc::shared)
    lockdc_assert_target_links(lockdc::shared
        cpkt::curl_shared
        cpkt::openssl_ssl_shared
        cpkt::openssl_crypto_shared
        cpkt::nghttp2_shared
        cpkt::libssh2_shared
        cpkt::zlib_shared
        pslog::pslog_shared
        lonejson::lonejson
        liblql::lql_shared)
endif()

add_library(api_surface OBJECT api_surface.c)
target_link_libraries(api_surface PRIVATE lockdc::static)

add_executable(example_static example.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(example_static PRIVATE lockdc::static)

add_executable(test_static test.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(test_static PRIVATE lockdc::static)

add_executable(example_shared example.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(example_shared PRIVATE lockdc::shared)
set_target_properties(example_shared PROPERTIES BUILD_RPATH "${LOCKDC_RELEASE_PREFIX}/lib")

add_executable(test_shared test.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(test_shared PRIVATE lockdc::shared)
set_target_properties(test_shared PROPERTIES BUILD_RPATH "${LOCKDC_RELEASE_PREFIX}/lib")

foreach(linkage static shared)
    add_executable(collections_${linkage} collection_journal.c tracking.c)
    target_link_libraries(collections_${linkage} PRIVATE lockdc::${linkage})
    set_target_properties(collections_${linkage} PROPERTIES
        C_STANDARD 90 C_EXTENSIONS OFF
        BUILD_RPATH "${LOCKDC_RELEASE_PREFIX}/lib")
    target_compile_options(collections_${linkage} PRIVATE -Wall -Wextra -Werror -pedantic-errors)
endforeach()
]=])

configure_file("${LOCKDC_ROOT}/examples/collection_journal.c"
    "${consumer_src_dir}/collection_journal.c" COPYONLY)
configure_file("${LOCKDC_ROOT}/tests/support/lc_test_tracking.c"
    "${consumer_src_dir}/tracking.c" COPYONLY)

file(WRITE "${consumer_src_dir}/example.c" [=[
#include <lc/lc.h>

#include <stdio.h>
#include <string.h>

int main(void) {
    const char *version = lc_version_string();

    if (version == NULL) {
        return 10;
    }
    if (strcmp(version, LC_VERSION_STRING) != 0) {
        return 11;
    }

    puts(version);
    return 0;
}
]=])

file(WRITE "${consumer_src_dir}/test.c" [=[
#include <lc/lc.h>

#include <string.h>

int main(void) {
    lc_client_config config;
    lonejson_int64 marker;
    const char *version;

    marker = 42;
    lc_client_config_init(&config);
    version = lc_version_string();
    if (marker != 42) {
        return 20;
    }
    if (version == NULL) {
        return 21;
    }
    if (strcmp(version, LC_VERSION_STRING) != 0) {
        return 22;
    }

    return 0;
}
]=])

file(WRITE "${consumer_src_dir}/pkgconfig_static_main.c" [=[
#include <lc/lc.h>

int main(void) {
    lonejson_int64 value;

    value = 0;
    return lc_version_string() != 0 && value == 0 ? 0 : 1;
}
]=])

file(WRITE "${consumer_src_dir}/pkgconfig_shared_main.c" [=[
#include <lc/lc.h>

int main(void) {
    return lc_version_string() != 0 ? 0 : 1;
}
]=])

file(WRITE "${consumer_src_dir}/api_surface.c" [=[
#include <lc/lc.h>
#include <string.h>

int lockdc_release_tarball_touch_client_api(lc_client *client, lc_lease **lease,
                                            lc_source *src, lc_sink *dst,
                                            const lonejson_map *map) {
    lc_error error;
    lc_acquire_req acquire;
    lc_get_opts get_opts;
    lc_get_res get_res;
    lc_update_req update;
    lc_update_res update_res;
    lc_mutate_op mutate;
    lc_mutate_res mutate_res;
    lc_metadata_op metadata;
    lc_metadata_res metadata_res;
    lc_remove_op remove;
    lc_remove_res remove_res;
    lc_keepalive_op keepalive;
    lc_keepalive_res keepalive_res;
    lc_release_op release;
    lc_release_res release_res;
    lc_query_req query;
    lc_query_res query_res;
    lc_query_key_handler query_key_handler;
    lc_enqueue_req enqueue;
    lc_enqueue_res enqueue_res;
    lc_dequeue_req dequeue;
    lc_queue_stats_req stats;
    lc_queue_stats_res stats_res;
    lc_message *message;
    lc_consumer consumer;
    lc_watch_queue_req watch_req;
    lc_watch_handler watch_handler;
    lc_consumer_service_config service_config;
    lc_consumer_service *service;

    lc_error_init(&error);
    lc_acquire_req_init(&acquire);
    lc_get_opts_init(&get_opts);
    lc_update_req_init(&update);
    lc_mutate_op_init(&mutate);
    lc_metadata_op_init(&metadata);
    lc_remove_op_init(&remove);
    lc_keepalive_op_init(&keepalive);
    lc_release_op_init(&release);
    lc_query_req_init(&query);
    memset(&query_key_handler, 0, sizeof(query_key_handler));
    lc_enqueue_req_init(&enqueue);
    lc_dequeue_req_init(&dequeue);
    lc_queue_stats_req_init(&stats);
    lc_consumer_init(&consumer);
    lc_watch_queue_req_init(&watch_req);
    lc_watch_handler_init(&watch_handler);
    lc_consumer_service_config_init(&service_config);

    message = (lc_message *)0;
    service = (lc_consumer_service *)0;

    return lc_acquire(client, &acquire, lease, &error) +
           lc_get(client, "key", &get_opts, dst, &get_res, &error) +
           lc_load(client, "key", map, (void *)0, &get_opts, &get_res, &error) +
           lc_update(client, &update, src, &update_res, &error) +
           lc_mutate(client, &mutate, &mutate_res, &error) +
           lc_metadata(client, &metadata, &metadata_res, &error) +
           lc_remove(client, &remove, &remove_res, &error) +
           lc_keepalive(client, &keepalive, &keepalive_res, &error) +
           lc_release(client, &release, &release_res, &error) +
           lc_query(client, &query, dst, &query_res, &error) +
           lc_query_keys(client, &query, &query_key_handler, (void *)0,
                         &query_res, &error) +
           lc_enqueue(client, &enqueue, src, &enqueue_res, &error) +
           lc_dequeue(client, &dequeue, &message, &error) +
           lc_queue_stats(client, &stats, &stats_res, &error) +
           lc_subscribe(client, &dequeue, &consumer, &error) +
           lc_subscribe_with_state(client, &dequeue, &consumer, &error) +
           lc_client_new_consumer_service(client, &service_config, &service, &error) +
           lc_watch_queue(client, &watch_req, &watch_handler, &error);
}

int lockdc_release_tarball_touch_lease_api(lc_lease *lease, lc_source *src,
                                           lc_sink *dst,
                                           const lonejson_map *map) {
    lc_error error;
    lc_get_opts get_opts;
    lc_get_res get_res;
    lc_update_opts update_opts;
    lc_mutate_req mutate;
    lc_mutate_local_req mutate_local;
    lc_metadata_req metadata;
    lc_remove_req remove;
    lc_keepalive_req keepalive;
    lc_release_req release;
    lc_attach_req attach;
    lc_attach_res attach_res;
    lc_attachment_list attachments;
    lc_attachment_get_req attachment_get;
    lc_attachment_get_res attachment_get_res;
    lc_attachment_selector selector;
    int deleted;
    int deleted_count;

    lc_error_init(&error);
    lc_get_opts_init(&get_opts);
    lc_update_opts_init(&update_opts);
    lc_mutate_req_init(&mutate);
    lc_mutate_local_req_init(&mutate_local);
    lc_metadata_req_init(&metadata);
    lc_remove_req_init(&remove);
    lc_keepalive_req_init(&keepalive);
    lc_release_req_init(&release);
    lc_attach_req_init(&attach);
    lc_attachment_get_req_init(&attachment_get);
    lc_attachment_selector_init(&selector);
    deleted = 0;
    deleted_count = 0;

    return lc_lease_describe(lease, &error) +
           lc_lease_get(lease, dst, &get_opts, &get_res, &error) +
           lc_lease_load(lease, map, (void *)0, &get_opts, &get_res, &error) +
           lc_lease_save(lease, map, (const void *)0, &update_opts, &error) +
           lc_lease_update(lease, src, &update_opts, &error) +
           lc_lease_mutate(lease, &mutate, &error) +
           lc_lease_mutate_local(lease, &mutate_local, &error) +
           lc_lease_metadata(lease, &metadata, &error) +
           lc_lease_remove(lease, &remove, &error) +
           lc_lease_keepalive(lease, &keepalive, &error) +
           lc_lease_release(lease, &release, &error) +
           lc_lease_attach(lease, &attach, src, &attach_res, &error) +
           lc_lease_list_attachments(lease, &attachments, &error) +
           lc_lease_get_attachment(lease, &attachment_get, dst, &attachment_get_res, &error) +
           lc_lease_delete_attachment(lease, &selector, &deleted, &error) +
           lc_lease_delete_all_attachments(lease, &deleted_count, &error);
}
]=])

set(lockdc_consumer_prefix_path
    "${release_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install"
    "${LOCKDC_EXTERNAL_ROOT}/liblql/install")
string(REPLACE ";" "\\;" lockdc_consumer_prefix_path_arg "${lockdc_consumer_prefix_path}")

set(lockdc_consumer_configure_args
        -S "${consumer_src_dir}"
        -B "${consumer_bin_dir}"
        "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
        "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
        "-DCMAKE_PREFIX_PATH=${lockdc_consumer_prefix_path_arg}"
        "-DCURL_DIR=${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/CURL"
        "-DOpenSSL_DIR=${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/OpenSSL"
        "-DZLIB_DIR=${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/zlib"
        "-Dnghttp2_DIR=${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/nghttp2"
        "-DLibssh2_DIR=${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/libssh2"
        "-Dpslog_DIR=${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib/cmake/pslog"
        "-Dlonejson_DIR=${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib/cmake/lonejson"
        "-Dliblql_DIR=${LOCKDC_EXTERNAL_ROOT}/liblql/install/lib/cmake/liblql"
        "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON"
        "-Dlockdc_DIR=${release_prefix}/lib/cmake/lockdc"
        "-DLOCKDC_RELEASE_PREFIX=${release_prefix}"
        "-DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}"
        "-DLOCKDC_BOOTLIN_RUNTIME_LOADER=${lockdc_bootlin_runtime_loader}"
        "-DLOCKDC_BOOTLIN_RUNTIME_DIRS=${lockdc_bootlin_runtime_dirs_arg}")
if(DEFINED CMAKE_TOOLCHAIN_FILE
        AND NOT "${CMAKE_TOOLCHAIN_FILE}" STREQUAL "")
    list(APPEND lockdc_consumer_configure_args
        "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" ${lockdc_consumer_configure_args}
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure release-tarball consumer\n"
        "stdout:\n${configure_stdout}\n"
        "stderr:\n${configure_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${consumer_bin_dir}"
    RESULT_VARIABLE build_result
    OUTPUT_VARIABLE build_stdout
    ERROR_VARIABLE build_stderr
)
if(NOT build_result EQUAL 0)
    message(FATAL_ERROR
        "failed to build release-tarball consumer\n"
        "stdout:\n${build_stdout}\n"
        "stderr:\n${build_stderr}")
endif()

if(LOCKDC_RUN_DOWNSTREAM_BINARIES)
    foreach(binary_name example_static test_static example_shared test_shared)
        execute_process(
            COMMAND "${consumer_bin_dir}/${binary_name}"
            RESULT_VARIABLE run_result
            OUTPUT_VARIABLE run_stdout
            ERROR_VARIABLE run_stderr
        )
        if(NOT run_result EQUAL 0)
            message(FATAL_ERROR
                "release-tarball consumer binary failed: ${binary_name}\n"
                "stdout:\n${run_stdout}\n"
                "stderr:\n${run_stderr}")
        endif()
    endforeach()
    foreach(linkage static shared)
        set(binary_name collections_${linkage})
        execute_process(
            COMMAND "${CMAKE_COMMAND}" -E env LOCKDC_TEST_ALLOCATOR_TRACKING=1
                "${consumer_bin_dir}/${binary_name}"
                "pouch://${test_root}/${binary_name}.pouch"
            RESULT_VARIABLE run_result
            OUTPUT_VARIABLE run_stdout
            ERROR_VARIABLE run_stderr
            TIMEOUT 30)
        if(NOT run_result EQUAL 0 OR
           run_stderr MATCHES "liblockdc allocator:" OR
           NOT run_stdout STREQUAL "{\"event\":\"created\"}\n{\"event\":\"completed\"}\n")
            message(FATAL_ERROR
                "extracted SDK collection example failed: ${binary_name}\n"
                "stdout:\n${run_stdout}\nstderr:\n${run_stderr}")
        endif()
    endforeach()
endif()

find_program(LOCKDC_PKG_CONFIG_BIN NAMES pkg-config)
find_program(LOCKDC_FILE_BIN NAMES file)
if(NOT LOCKDC_PKG_CONFIG_BIN)
    message(FATAL_ERROR "pkg-config is required for release tarball static SDK validation")
endif()

set(lockdc_pkgconfig_path_entries
    "${release_prefix}/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/liblql/install/lib/pkgconfig")
string(JOIN ":" lockdc_pkgconfig_path ${lockdc_pkgconfig_path_entries})

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --static --cflags lockdc
    RESULT_VARIABLE lockdc_pkgconfig_cflags_result
    OUTPUT_VARIABLE lockdc_pkgconfig_cflags
    ERROR_VARIABLE lockdc_pkgconfig_cflags_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_cflags_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config cflags for release tarball static consumer\n"
        "stdout:\n${lockdc_pkgconfig_cflags}\n"
        "stderr:\n${lockdc_pkgconfig_cflags_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --static --libs lockdc
    RESULT_VARIABLE lockdc_pkgconfig_libs_result
    OUTPUT_VARIABLE lockdc_pkgconfig_libs
    ERROR_VARIABLE lockdc_pkgconfig_libs_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_libs_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config libs for release tarball static consumer\n"
        "stdout:\n${lockdc_pkgconfig_libs}\n"
        "stderr:\n${lockdc_pkgconfig_libs_stderr}")
endif()

separate_arguments(lockdc_pkgconfig_cflags_list UNIX_COMMAND "${lockdc_pkgconfig_cflags}")
separate_arguments(lockdc_pkgconfig_libs_list UNIX_COMMAND "${lockdc_pkgconfig_libs}")
set(lockdc_direct_link_flags)
if(DEFINED CMAKE_EXE_LINKER_FLAGS AND NOT "${CMAKE_EXE_LINKER_FLAGS}" STREQUAL "")
    separate_arguments(lockdc_direct_link_flags UNIX_COMMAND "${CMAKE_EXE_LINKER_FLAGS}")
endif()
if(LOCKDC_TARGET_ID MATCHES "apple-darwin$"
        AND DEFINED CMAKE_LINKER
        AND NOT "${CMAKE_LINKER}" STREQUAL "")
    # Supply one linker selection and the same SDK/minimum as the CMake
    # consumer; a shell-quoted cached selection is not a second input file.
    list(FILTER lockdc_direct_link_flags EXCLUDE REGEX "^--ld-path=")
    list(PREPEND lockdc_direct_link_flags "--ld-path=${CMAKE_LINKER}")
    list(APPEND lockdc_direct_link_flags -isysroot "${CMAKE_OSX_SYSROOT}"
        "-mmacosx-version-min=${CMAKE_OSX_DEPLOYMENT_TARGET}")
endif()

set(lockdc_pkgconfig_static_consumer "${consumer_bin_dir}/release_tarball_pkgconfig_static")
set(lockdc_pkgconfig_static_link_flags)
if(NOT LOCKDC_TARGET_ID MATCHES "apple-darwin$")
    list(APPEND lockdc_pkgconfig_static_link_flags -static)
endif()
execute_process(
    COMMAND "${CMAKE_C_COMPILER}"
        ${lockdc_pkgconfig_cflags_list}
        ${lockdc_pkgconfig_static_link_flags}
        "${consumer_src_dir}/pkgconfig_static_main.c"
        -o "${lockdc_pkgconfig_static_consumer}"
        ${lockdc_direct_link_flags}
        ${lockdc_pkgconfig_libs_list}
    RESULT_VARIABLE lockdc_pkgconfig_build_result
    OUTPUT_VARIABLE lockdc_pkgconfig_build_stdout
    ERROR_VARIABLE lockdc_pkgconfig_build_stderr
)
if(NOT lockdc_pkgconfig_build_result EQUAL 0)
    message(FATAL_ERROR
        "failed to build release tarball pkg-config static consumer\n"
        "cflags: ${lockdc_pkgconfig_cflags}\n"
        "libs: ${lockdc_pkgconfig_libs}\n"
        "stdout:\n${lockdc_pkgconfig_build_stdout}\n"
        "stderr:\n${lockdc_pkgconfig_build_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --cflags lockdc
    RESULT_VARIABLE lockdc_pkgconfig_shared_cflags_result
    OUTPUT_VARIABLE lockdc_pkgconfig_shared_cflags
    ERROR_VARIABLE lockdc_pkgconfig_shared_cflags_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_shared_cflags_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config cflags for release tarball shared consumer\n"
        "stdout:\n${lockdc_pkgconfig_shared_cflags}\n"
        "stderr:\n${lockdc_pkgconfig_shared_cflags_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --libs lockdc
    RESULT_VARIABLE lockdc_pkgconfig_shared_libs_result
    OUTPUT_VARIABLE lockdc_pkgconfig_shared_libs
    ERROR_VARIABLE lockdc_pkgconfig_shared_libs_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_shared_libs_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config libs for release tarball shared consumer\n"
        "stdout:\n${lockdc_pkgconfig_shared_libs}\n"
        "stderr:\n${lockdc_pkgconfig_shared_libs_stderr}")
endif()

foreach(expected_pkgconfig_shared_lib
    -llockdc
    -lpslog
    -llonejson
    -lcurl
    -lssh2
    -lssl
    -lcrypto
    -lnghttp2
    -lz
    -llql)
    string(FIND "${lockdc_pkgconfig_shared_libs}"
        "${expected_pkgconfig_shared_lib}" expected_pkgconfig_shared_lib_index)
    if(expected_pkgconfig_shared_lib_index EQUAL -1)
        message(FATAL_ERROR
            "release pkg-config --libs lockdc is missing shared-link dependency '${expected_pkgconfig_shared_lib}'\n"
            "libs: ${lockdc_pkgconfig_shared_libs}")
    endif()
endforeach()

separate_arguments(lockdc_pkgconfig_shared_cflags_list UNIX_COMMAND "${lockdc_pkgconfig_shared_cflags}")
separate_arguments(lockdc_pkgconfig_shared_libs_list UNIX_COMMAND "${lockdc_pkgconfig_shared_libs}")

set(lockdc_pkgconfig_shared_consumer "${consumer_bin_dir}/release_tarball_pkgconfig_shared")
set(lockdc_pkgconfig_shared_rpath_flag "-Wl,-rpath,${release_prefix}/lib")
execute_process(
    COMMAND "${CMAKE_C_COMPILER}"
        ${lockdc_pkgconfig_shared_cflags_list}
        "${consumer_src_dir}/pkgconfig_shared_main.c"
        ${lockdc_pkgconfig_shared_rpath_flag}
        -o "${lockdc_pkgconfig_shared_consumer}"
        ${lockdc_direct_link_flags}
        ${lockdc_bootlin_direct_link_flags}
        ${lockdc_pkgconfig_shared_libs_list}
    RESULT_VARIABLE lockdc_pkgconfig_shared_build_result
    OUTPUT_VARIABLE lockdc_pkgconfig_shared_build_stdout
    ERROR_VARIABLE lockdc_pkgconfig_shared_build_stderr
)
if(NOT lockdc_pkgconfig_shared_build_result EQUAL 0)
    message(FATAL_ERROR
        "failed to build release tarball pkg-config shared consumer\n"
        "cflags: ${lockdc_pkgconfig_shared_cflags}\n"
        "libs: ${lockdc_pkgconfig_shared_libs}\n"
        "stdout:\n${lockdc_pkgconfig_shared_build_stdout}\n"
        "stderr:\n${lockdc_pkgconfig_shared_build_stderr}")
endif()

if(LOCKDC_TARGET_ID MATCHES "musl" AND LOCKDC_FILE_BIN)
    execute_process(
        COMMAND "${LOCKDC_FILE_BIN}" "${lockdc_pkgconfig_static_consumer}"
        RESULT_VARIABLE lockdc_pkgconfig_file_result
        OUTPUT_VARIABLE lockdc_pkgconfig_file_output
        ERROR_VARIABLE lockdc_pkgconfig_file_stderr
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(NOT lockdc_pkgconfig_file_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect release tarball pkg-config static consumer\n"
            "stdout:\n${lockdc_pkgconfig_file_output}\n"
            "stderr:\n${lockdc_pkgconfig_file_stderr}")
    endif()
    if(NOT lockdc_pkgconfig_file_output MATCHES "statically linked")
        message(FATAL_ERROR
            "musl release tarball pkg-config static consumer is not fully static\n"
            "file:\n${lockdc_pkgconfig_file_output}")
    endif()
endif()
