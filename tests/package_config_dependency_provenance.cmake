function(lockdc_write_fake_curl_config fake_curl_dir define_imported_target)
    file(MAKE_DIRECTORY "${fake_curl_dir}")

    if(define_imported_target)
        set(fake_curl_target_block [=[
if(NOT TARGET CURL::libcurl)
  add_library(CURL::libcurl INTERFACE IMPORTED)
endif()
]=])
    else()
        set(fake_curl_target_block "")
    endif()

    file(WRITE "${fake_curl_dir}/CURLConfig.cmake" "${fake_curl_target_block}
set(CURL_FOUND TRUE)
set(CURL_VERSION \"8.22.0\")
")

    file(WRITE "${fake_curl_dir}/CURLConfigVersion.cmake" [=[
set(PACKAGE_VERSION "8.22.0")
if(PACKAGE_FIND_VERSION VERSION_EQUAL PACKAGE_VERSION)
  set(PACKAGE_VERSION_EXACT TRUE)
endif()
if(PACKAGE_FIND_VERSION VERSION_LESS_EQUAL PACKAGE_VERSION)
  set(PACKAGE_VERSION_COMPATIBLE TRUE)
endif()
]=])
endfunction()

function(lockdc_write_fake_package_version fake_config_dir package_version config_version_name)
    file(WRITE "${fake_config_dir}/${config_version_name}" "
set(PACKAGE_VERSION \"${package_version}\")
if(PACKAGE_FIND_VERSION VERSION_EQUAL PACKAGE_VERSION)
  set(PACKAGE_VERSION_EXACT TRUE)
endif()
if(PACKAGE_FIND_VERSION VERSION_LESS_EQUAL PACKAGE_VERSION)
  set(PACKAGE_VERSION_COMPATIBLE TRUE)
endif()
")
endfunction()

function(lockdc_write_fake_curl_not_found_config fake_curl_dir)
    file(MAKE_DIRECTORY "${fake_curl_dir}")
    file(WRITE "${fake_curl_dir}/CURLConfig.cmake" [=[
set(CURL_FOUND FALSE)
]=])
    lockdc_write_fake_package_version("${fake_curl_dir}" "8.22.0" "CURLConfigVersion.cmake")
endfunction()

function(lockdc_write_fake_shared_only_dependency_configs fake_root out_pslog_dir out_lonejson_dir out_liblql_dir)
    set(fake_pslog_dir "${fake_root}/pslog/lib/cmake/pslog")
    set(fake_lonejson_dir "${fake_root}/lonejson/lib/cmake/lonejson")
    set(fake_liblql_dir "${fake_root}/liblql/lib/cmake/liblql")
    file(MAKE_DIRECTORY "${fake_pslog_dir}" "${fake_lonejson_dir}" "${fake_liblql_dir}")

    file(WRITE "${fake_pslog_dir}/pslogConfig.cmake" [=[
if(NOT TARGET pslog::pslog)
  add_library(pslog::pslog INTERFACE IMPORTED)
endif()
set(pslog_FOUND TRUE)
set(pslog_VERSION "0.13.0")
]=])
    lockdc_write_fake_package_version("${fake_pslog_dir}" "0.13.0" "pslogConfigVersion.cmake")

    file(WRITE "${fake_lonejson_dir}/lonejsonConfig.cmake" [=[
if(NOT TARGET lonejson::lonejson)
  add_library(lonejson::lonejson INTERFACE IMPORTED)
endif()
set(lonejson_FOUND TRUE)
set(lonejson_VERSION "0.49.0")
set(lonejson_curl_FOUND TRUE)
set(lonejson_oidc_FOUND TRUE)
set(lonejson_openssl_FOUND TRUE)
]=])
    lockdc_write_fake_package_version("${fake_lonejson_dir}" "0.49.0" "lonejsonConfigVersion.cmake")

    file(WRITE "${fake_liblql_dir}/liblqlConfig.cmake" [=[
if(NOT TARGET liblql::lql)
  add_library(liblql::lql INTERFACE IMPORTED)
endif()
set(liblql_FOUND TRUE)
set(liblql_VERSION "0.4.1")
]=])
    lockdc_write_fake_package_version("${fake_liblql_dir}" "0.4.1" "liblqlConfigVersion.cmake")

    set("${out_pslog_dir}" "${fake_pslog_dir}" PARENT_SCOPE)
    set("${out_lonejson_dir}" "${fake_lonejson_dir}" PARENT_SCOPE)
    set("${out_liblql_dir}" "${fake_liblql_dir}" PARENT_SCOPE)
endfunction()

function(lockdc_append_package_probe_args out_var package_prefix external_root fake_curl_dir consumer_src_dir consumer_bin_dir)
    set(lockdc_probe_prefix_path
        "${package_prefix}"
        "${external_root}/cpkt/install"
        "${external_root}/pslog/install"
        "${external_root}/lonejson/install"
        "${external_root}/liblql/install")
    string(REPLACE ";" "\\;" lockdc_probe_prefix_path_arg "${lockdc_probe_prefix_path}")

    set(lockdc_probe_configure_args
        -S "${consumer_src_dir}"
        -B "${consumer_bin_dir}"
        "-DCMAKE_PREFIX_PATH=${lockdc_probe_prefix_path_arg}"
        "-Dlockdc_DIR=${package_prefix}/lib/cmake/lockdc"
        "-DCURL_DIR=${fake_curl_dir}"
        "-DOpenSSL_DIR=${external_root}/cpkt/install/lib/cmake/OpenSSL"
        "-DZLIB_DIR=${external_root}/cpkt/install/lib/cmake/zlib"
        "-Dnghttp2_DIR=${external_root}/cpkt/install/lib/cmake/nghttp2"
        "-DLibssh2_DIR=${external_root}/cpkt/install/lib/cmake/libssh2"
        "-Dpslog_DIR=${external_root}/pslog/install/lib/cmake/pslog"
        "-Dlonejson_DIR=${external_root}/lonejson/install/lib/cmake/lonejson"
        "-Dliblql_DIR=${external_root}/liblql/install/lib/cmake/liblql"
        "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON")
    if(DEFINED LOCKDC_C_COMPILER AND NOT LOCKDC_C_COMPILER STREQUAL "")
        list(APPEND lockdc_probe_configure_args "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}")
    elseif(DEFINED CMAKE_C_COMPILER AND NOT CMAKE_C_COMPILER STREQUAL "")
        list(APPEND lockdc_probe_configure_args "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
    endif()
    if(DEFINED CMAKE_TOOLCHAIN_FILE AND NOT CMAKE_TOOLCHAIN_FILE STREQUAL "")
        list(APPEND lockdc_probe_configure_args "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
    endif()

    set("${out_var}" "${lockdc_probe_configure_args}" PARENT_SCOPE)
endfunction()

function(lockdc_append_shared_only_package_probe_args out_var package_prefix external_root fake_curl_dir fake_pslog_dir fake_lonejson_dir fake_liblql_dir consumer_src_dir consumer_bin_dir)
    set(lockdc_probe_prefix_path
        "${package_prefix}"
        "${external_root}/cpkt/install")
    string(REPLACE ";" "\\;" lockdc_probe_prefix_path_arg "${lockdc_probe_prefix_path}")

    set(lockdc_probe_configure_args
        -S "${consumer_src_dir}"
        -B "${consumer_bin_dir}"
        "-DCMAKE_PREFIX_PATH=${lockdc_probe_prefix_path_arg}"
        "-Dlockdc_DIR=${package_prefix}/lib/cmake/lockdc"
        "-DCURL_DIR=${fake_curl_dir}"
        "-DOpenSSL_DIR=${external_root}/cpkt/install/lib/cmake/OpenSSL"
        "-DZLIB_DIR=${external_root}/cpkt/install/lib/cmake/zlib"
        "-Dnghttp2_DIR=${external_root}/cpkt/install/lib/cmake/nghttp2"
        "-DLibssh2_DIR=${external_root}/cpkt/install/lib/cmake/libssh2"
        "-Dpslog_DIR=${fake_pslog_dir}"
        "-Dlonejson_DIR=${fake_lonejson_dir}"
        "-Dliblql_DIR=${fake_liblql_dir}"
        "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON")
    if(DEFINED LOCKDC_C_COMPILER AND NOT LOCKDC_C_COMPILER STREQUAL "")
        list(APPEND lockdc_probe_configure_args "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}")
    elseif(DEFINED CMAKE_C_COMPILER AND NOT CMAKE_C_COMPILER STREQUAL "")
        list(APPEND lockdc_probe_configure_args "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
    endif()
    if(DEFINED CMAKE_TOOLCHAIN_FILE AND NOT CMAKE_TOOLCHAIN_FILE STREQUAL "")
        list(APPEND lockdc_probe_configure_args "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
    endif()

    set("${out_var}" "${lockdc_probe_configure_args}" PARENT_SCOPE)
endfunction()

function(lockdc_assert_config_accepts_non_cpkt_curl package_prefix external_root test_root)
    set(fake_root "${test_root}/non-cpkt-curl-accepted")
    set(fake_curl_dir "${fake_root}/lib/cmake/CURL")
    set(consumer_src_dir "${test_root}/non-cpkt-curl-accepted-consumer")
    set(consumer_bin_dir "${test_root}/non-cpkt-curl-accepted-consumer-build")

    file(REMOVE_RECURSE "${fake_root}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")
    lockdc_write_fake_curl_config("${fake_curl_dir}" TRUE)

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_non_cpkt_dependency_probe C)

find_package(lockdc CONFIG REQUIRED)

function(lockdc_assert_link_uses_provider_neutral_curl target_name)
  if(NOT TARGET "${target_name}")
    return()
  endif()

  get_target_property(link_items "${target_name}" INTERFACE_LINK_LIBRARIES)
  list(FIND link_items "CURL::libcurl" curl_index)
  if(curl_index EQUAL -1)
    message(FATAL_ERROR
      "${target_name} did not preserve provider-neutral CURL::libcurl linkage\n"
      "INTERFACE_LINK_LIBRARIES=${link_items}")
  endif()

  list(FIND link_items "cpkt::curl_shared" cpkt_curl_index)
  if(NOT cpkt_curl_index EQUAL -1)
    message(FATAL_ERROR
      "${target_name} forced cpkt::curl_shared even though the selected CURL config provided CURL::libcurl only\n"
      "INTERFACE_LINK_LIBRARIES=${link_items}")
  endif()
endfunction()

lockdc_assert_link_uses_provider_neutral_curl(lockdc::shared)
lockdc_assert_link_uses_provider_neutral_curl(lockdc::static)
]=])

    lockdc_append_package_probe_args(
        lockdc_probe_configure_args
        "${package_prefix}"
        "${external_root}"
        "${fake_curl_dir}"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(NOT probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package rejected a compatible non-c.pkt CURL config that provides CURL::libcurl\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()

function(lockdc_assert_config_rejects_missing_curl_target package_prefix external_root test_root)
    set(fake_root "${test_root}/missing-curl-target-rejected")
    set(fake_curl_dir "${fake_root}/lib/cmake/CURL")
    set(consumer_src_dir "${test_root}/missing-curl-target-rejected-consumer")
    set(consumer_bin_dir "${test_root}/missing-curl-target-rejected-consumer-build")

    file(REMOVE_RECURSE "${fake_root}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")
    lockdc_write_fake_curl_config("${fake_curl_dir}" FALSE)

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_missing_curl_target_probe C)

find_package(lockdc CONFIG REQUIRED)
]=])

    lockdc_append_package_probe_args(
        lockdc_probe_configure_args
        "${package_prefix}"
        "${external_root}"
        "${fake_curl_dir}"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package accepted a CURL config that does not provide the imported targets used by lockdc\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()

    set(probe_output "${probe_stdout}\n${probe_stderr}")
    if(NOT probe_output MATCHES "CURL::libcurl")
        message(FATAL_ERROR
            "lockdc package rejected the invalid CURL config without the expected imported-target diagnostic\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()

function(lockdc_assert_config_accepts_shared_only_dependency_targets package_prefix external_root test_root)
    set(fake_root "${test_root}/shared-only-dependency-targets")
    set(fake_curl_dir "${fake_root}/curl/lib/cmake/CURL")
    set(consumer_src_dir "${test_root}/shared-only-dependency-targets-consumer")
    set(consumer_bin_dir "${test_root}/shared-only-dependency-targets-consumer-build")

    file(REMOVE_RECURSE "${fake_root}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")
    lockdc_write_fake_curl_config("${fake_curl_dir}" TRUE)
    lockdc_write_fake_shared_only_dependency_configs(
        "${fake_root}"
        fake_pslog_dir
        fake_lonejson_dir
        fake_liblql_dir)

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_shared_only_dependency_probe C)

find_package(lockdc CONFIG REQUIRED)

if(NOT TARGET lockdc::shared)
  message(FATAL_ERROR "lockdc::shared was not provided for shared-compatible dependency configs")
endif()
if(TARGET lockdc::static)
  message(FATAL_ERROR "lockdc::static was provided even though static-only dependency targets were absent")
endif()

get_target_property(shared_links lockdc::shared INTERFACE_LINK_LIBRARIES)
foreach(expected_item CURL::libcurl pslog::pslog lonejson::lonejson liblql::lql)
  list(FIND shared_links "${expected_item}" expected_index)
  if(expected_index EQUAL -1)
    message(FATAL_ERROR
      "lockdc::shared did not link the expected provider-neutral target '${expected_item}'\n"
      "INTERFACE_LINK_LIBRARIES=${shared_links}")
  endif()
endforeach()
foreach(forbidden_item cpkt::curl_shared pslog::pslog_static lonejson::lonejson_static liblql::lql_static)
  list(FIND shared_links "${forbidden_item}" forbidden_index)
  if(NOT forbidden_index EQUAL -1)
    message(FATAL_ERROR
      "lockdc::shared linked static or provider-specific target '${forbidden_item}'\n"
      "INTERFACE_LINK_LIBRARIES=${shared_links}")
  endif()
endforeach()
]=])

    lockdc_append_shared_only_package_probe_args(
        lockdc_probe_configure_args
        "${package_prefix}"
        "${external_root}"
        "${fake_curl_dir}"
        "${fake_pslog_dir}"
        "${fake_lonejson_dir}"
        "${fake_liblql_dir}"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(NOT probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package rejected a shared-only consumer even though shared-compatible dependency targets were available\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()

function(lockdc_assert_config_supports_repeated_required_discovery package_prefix external_root test_root)
    set(fake_root "${test_root}/repeated-required-discovery")
    set(fake_curl_dir "${fake_root}/lib/cmake/CURL")
    set(consumer_src_dir "${test_root}/repeated-required-discovery-consumer")
    set(consumer_bin_dir "${test_root}/repeated-required-discovery-consumer-build")

    file(REMOVE_RECURSE "${fake_root}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")
    lockdc_write_fake_curl_config("${fake_curl_dir}" TRUE)

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_repeated_required_discovery_probe C)

find_package(lockdc CONFIG REQUIRED)
if(NOT TARGET lockdc::shared OR NOT TARGET lockdc::static)
  message(FATAL_ERROR "initial required lockdc discovery did not provide both normal SDK targets")
endif()

find_package(lockdc CONFIG REQUIRED)
if(NOT TARGET lockdc::shared OR NOT TARGET lockdc::static)
  message(FATAL_ERROR "repeated required lockdc discovery lost imported targets")
endif()
]=])

    lockdc_append_package_probe_args(
        lockdc_probe_configure_args
        "${package_prefix}"
        "${external_root}"
        "${fake_curl_dir}"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(NOT probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package failed repeated required discovery in one configure\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()

function(lockdc_assert_config_rejects_static_component_without_static_dependency_targets package_prefix external_root test_root)
    set(fake_root "${test_root}/static-component-missing-dependency-targets")
    set(fake_curl_dir "${fake_root}/curl/lib/cmake/CURL")
    set(consumer_src_dir "${test_root}/static-component-missing-dependency-targets-consumer")
    set(consumer_bin_dir "${test_root}/static-component-missing-dependency-targets-consumer-build")

    file(REMOVE_RECURSE "${fake_root}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")
    lockdc_write_fake_curl_config("${fake_curl_dir}" TRUE)
    lockdc_write_fake_shared_only_dependency_configs(
        "${fake_root}"
        fake_pslog_dir
        fake_lonejson_dir
        fake_liblql_dir)

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_static_component_probe C)

find_package(lockdc CONFIG REQUIRED COMPONENTS static)
]=])

    lockdc_append_shared_only_package_probe_args(
        lockdc_probe_configure_args
        "${package_prefix}"
        "${external_root}"
        "${fake_curl_dir}"
        "${fake_pslog_dir}"
        "${fake_lonejson_dir}"
        "${fake_liblql_dir}"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package accepted a required static component without static dependency targets\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()

    set(probe_output "${probe_stdout}\n${probe_stderr}")
    if(NOT probe_output MATCHES "pslog::pslog_static" OR
       NOT probe_output MATCHES "lonejson::lonejson_static" OR
       NOT probe_output MATCHES "liblql::lql_static")
        message(FATAL_ERROR
            "lockdc package rejected the invalid static component without the expected static-target diagnostics\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()

function(lockdc_assert_config_accepts_optional_static_component_without_static_dependency_targets package_prefix external_root test_root)
    set(fake_root "${test_root}/optional-static-component-missing-dependency-targets")
    set(fake_curl_dir "${fake_root}/curl/lib/cmake/CURL")
    set(consumer_src_dir "${test_root}/optional-static-component-missing-dependency-targets-consumer")
    set(consumer_bin_dir "${test_root}/optional-static-component-missing-dependency-targets-consumer-build")

    file(REMOVE_RECURSE "${fake_root}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")
    lockdc_write_fake_curl_config("${fake_curl_dir}" TRUE)
    lockdc_write_fake_shared_only_dependency_configs(
        "${fake_root}"
        fake_pslog_dir
        fake_lonejson_dir
        fake_liblql_dir)

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_optional_static_component_probe C)

find_package(lockdc CONFIG REQUIRED OPTIONAL_COMPONENTS static)
if(NOT lockdc_FOUND)
  message(FATAL_ERROR "lockdc package was rejected because an optional static component was unavailable")
endif()
if(NOT TARGET lockdc::shared)
  message(FATAL_ERROR "lockdc::shared was not provided when optional static was unavailable")
endif()
if(TARGET lockdc::static)
  message(FATAL_ERROR "lockdc::static was provided even though static dependency targets were absent")
endif()
]=])

    lockdc_append_shared_only_package_probe_args(
        lockdc_probe_configure_args
        "${package_prefix}"
        "${external_root}"
        "${fake_curl_dir}"
        "${fake_pslog_dir}"
        "${fake_lonejson_dir}"
        "${fake_liblql_dir}"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(NOT probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package rejected a required discovery because optional static was unavailable\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()

function(lockdc_assert_config_accepts_optional_shared_component_without_shared_library package_prefix external_root test_root)
    set(test_prefix "${test_root}/optional-shared-component-static-only-prefix")
    set(consumer_src_dir "${test_root}/optional-shared-component-static-only-consumer")
    set(consumer_bin_dir "${test_root}/optional-shared-component-static-only-consumer-build")

    file(REMOVE_RECURSE "${test_prefix}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${test_prefix}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(COPY "${package_prefix}/include" DESTINATION "${test_prefix}")
    file(MAKE_DIRECTORY "${test_prefix}/lib")
    file(COPY "${package_prefix}/lib/liblockdc.a" DESTINATION "${test_prefix}/lib")
    file(COPY "${package_prefix}/lib/cmake" DESTINATION "${test_prefix}/lib")

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_optional_shared_component_probe C)

find_package(lockdc CONFIG REQUIRED OPTIONAL_COMPONENTS shared)
if(NOT lockdc_FOUND)
  message(FATAL_ERROR "lockdc package was rejected because an optional shared component was unavailable")
endif()
if(NOT TARGET lockdc::static)
  message(FATAL_ERROR "lockdc::static was not provided by the static-only package")
endif()
if(TARGET lockdc::shared)
  message(FATAL_ERROR "lockdc::shared was provided even though the shared library was absent")
endif()
]=])

    lockdc_append_package_probe_args(
        lockdc_probe_configure_args
        "${test_prefix}"
        "${external_root}"
        "${external_root}/cpkt/install/lib/cmake/CURL"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(NOT probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package rejected a required discovery because optional shared was unavailable\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()

function(lockdc_assert_config_supports_quiet_optional_discovery package_prefix external_root test_root)
    set(fake_root "${test_root}/quiet-optional-discovery")
    set(fake_curl_dir "${fake_root}/curl/lib/cmake/CURL")
    set(consumer_src_dir "${test_root}/quiet-optional-discovery-consumer")
    set(consumer_bin_dir "${test_root}/quiet-optional-discovery-consumer-build")

    file(REMOVE_RECURSE "${fake_root}" "${consumer_src_dir}" "${consumer_bin_dir}")
    file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")
    lockdc_write_fake_curl_not_found_config("${fake_curl_dir}")

    file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_quiet_optional_probe C)

find_package(lockdc CONFIG QUIET)
if(lockdc_FOUND)
  message(FATAL_ERROR "lockdc_FOUND is true even though CURL was deliberately unavailable")
endif()
if(TARGET lockdc::shared OR TARGET lockdc::static)
  message(FATAL_ERROR "lockdc created imported targets during a failed QUIET optional probe")
endif()
]=])

    lockdc_append_package_probe_args(
        lockdc_probe_configure_args
        "${package_prefix}"
        "${external_root}"
        "${fake_curl_dir}"
        "${consumer_src_dir}"
        "${consumer_bin_dir}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${lockdc_probe_configure_args}
        RESULT_VARIABLE probe_result
        OUTPUT_VARIABLE probe_stdout
        ERROR_VARIABLE probe_stderr
    )
    if(NOT probe_result EQUAL 0)
        message(FATAL_ERROR
            "lockdc package made a QUIET optional discovery probe fatal\n"
            "stdout:\n${probe_stdout}\n"
            "stderr:\n${probe_stderr}")
    endif()
endfunction()
