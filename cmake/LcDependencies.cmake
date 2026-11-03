include(ExternalProject)

function(lc_record_dependency_target target_name)
  set_property(GLOBAL APPEND PROPERTY LOCKDC_DEPENDENCY_TARGETS "${target_name}")
endfunction()

function(lc_require_dependency_file path label)
  if(NOT EXISTS "${path}")
    message(FATAL_ERROR
      "${label} was not found at ${path}\n"
      "Provide the dependency tree for this preset under LOCKDC_EXTERNAL_ROOT.\n"
      "You can provision it explicitly with scripts/deps.sh, but normal build/test/release entry points do not do that for you.")
  endif()
endfunction()

function(lc_normalize_prefix var path)
  file(TO_CMAKE_PATH "${path}" _normalized)
  set(${var} "${_normalized}" PARENT_SCOPE)
endfunction()

function(lc_require_cpkt_config package_name config_path)
  if(NOT EXISTS "${config_path}")
    message(FATAL_ERROR
      "${package_name} CMake package metadata was not found at ${config_path}\n"
      "Provide the published SDK supplying ${package_name}, with its CMake metadata.\n"
      "Run scripts/deps.sh for the target preset or clean stale dependency roots with make clean.")
  endif()
endfunction()

function(lc_add_interface_alias alias_target upstream_target)
  if(TARGET "${alias_target}")
    return()
  endif()
  if(NOT TARGET "${upstream_target}")
    message(FATAL_ERROR "Cannot create ${alias_target}; missing upstream target ${upstream_target}")
  endif()
  add_library(${alias_target} INTERFACE IMPORTED GLOBAL)
  set_target_properties(${alias_target}
    PROPERTIES
      INTERFACE_LINK_LIBRARIES "${upstream_target}"
  )
endfunction()

function(lc_configure_cpkt_package_roots)
  set(cpkt_root "${LOCKDC_EXTERNAL_ROOT}/cpkt/install")
  set(cpktdb_root "${LOCKDC_EXTERNAL_ROOT}/cpktdb/install")
  set(CPKT_TARGET_ID "${LOCKDC_TARGET_ID}")

  # Core discovery must work with no cpktdb files at all. The optional provider
  # alone validates cpktdb and its exact core prerequisite.
  list(PREPEND CMAKE_PREFIX_PATH "${cpkt_root}")
  list(APPEND CMAKE_FIND_ROOT_PATH "${cpkt_root}")
  if(LOCKDC_BUILD_POSTGRES)
    list(APPEND CMAKE_PREFIX_PATH "${cpktdb_root}")
    list(APPEND CMAKE_FIND_ROOT_PATH "${cpktdb_root}")
  endif()
  set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH}" PARENT_SCOPE)
  set(CMAKE_FIND_ROOT_PATH "${CMAKE_FIND_ROOT_PATH}" PARENT_SCOPE)

  # Reproducible liblockdc builds use the selected release. Provider consumers
  # separately use upstream's compatibility floor rather than this build pin.
  lc_require_cpkt_config("CpktCore" "${cpkt_root}/lib/cmake/CpktCore/CpktCoreConfig.cmake")
  set(CpktCore_DIR "${cpkt_root}/lib/cmake/CpktCore" CACHE PATH "Pinned cpkt core SDK package directory." FORCE)
  find_package(CpktCore ${LOCKDC_CPKT_VERSION} EXACT CONFIG REQUIRED)

  lc_require_cpkt_config("OpenSSL" "${cpkt_root}/lib/cmake/OpenSSL/OpenSSLConfig.cmake")
  lc_require_cpkt_config("ZLIB" "${cpkt_root}/lib/cmake/zlib/ZLIBConfig.cmake")
  lc_require_cpkt_config("nghttp2" "${cpkt_root}/lib/cmake/nghttp2/nghttp2Config.cmake")
  lc_require_cpkt_config("libssh2" "${cpkt_root}/lib/cmake/libssh2/libssh2-config.cmake")
  lc_require_cpkt_config("CURL" "${cpkt_root}/lib/cmake/CURL/CURLConfig.cmake")
  lc_require_cpkt_config("Lua" "${cpkt_root}/lib/cmake/Lua/LuaConfig.cmake")
  lc_require_cpkt_config("CpktLuaRuntime" "${cpkt_root}/lib/cmake/CpktLuaRuntime/CpktLuaRuntimeConfig.cmake")

  set(OpenSSL_DIR "${cpkt_root}/lib/cmake/OpenSSL" CACHE PATH "cpkt OpenSSL CMake package directory." FORCE)
  set(ZLIB_DIR "${cpkt_root}/lib/cmake/zlib" CACHE PATH "cpkt zlib CMake package directory." FORCE)
  set(nghttp2_DIR "${cpkt_root}/lib/cmake/nghttp2" CACHE PATH "cpkt nghttp2 CMake package directory." FORCE)
  set(Libssh2_DIR "${cpkt_root}/lib/cmake/libssh2" CACHE PATH "cpkt libssh2 CMake package directory." FORCE)
  set(CURL_DIR "${cpkt_root}/lib/cmake/CURL" CACHE PATH "cpkt CURL CMake package directory." FORCE)
  set(Lua_DIR "${cpkt_root}/lib/cmake/Lua" CACHE PATH "cpkt Lua CMake package directory." FORCE)
  set(CpktLuaRuntime_DIR "${cpkt_root}/lib/cmake/CpktLuaRuntime" CACHE PATH "cpkt Lua runtime CMake package directory." FORCE)
  if(LOCKDC_BUILD_POSTGRES)
    lc_require_cpkt_config("CpktPostgres" "${cpktdb_root}/lib/cmake/CpktPostgres/CpktPostgresConfig.cmake")
    set(CpktPostgres_DIR "${cpktdb_root}/lib/cmake/CpktPostgres" CACHE PATH "cpktdb PostgreSQL C89 facade package directory." FORCE)
    find_package(CpktPostgres CONFIG REQUIRED)
  endif()

  set(OpenSSL_DIR "${OpenSSL_DIR}" PARENT_SCOPE)
  set(ZLIB_DIR "${ZLIB_DIR}" PARENT_SCOPE)
  set(nghttp2_DIR "${nghttp2_DIR}" PARENT_SCOPE)
  set(Libssh2_DIR "${Libssh2_DIR}" PARENT_SCOPE)
  set(CURL_DIR "${CURL_DIR}" PARENT_SCOPE)
  set(Lua_DIR "${Lua_DIR}" PARENT_SCOPE)
  set(CpktLuaRuntime_DIR "${CpktLuaRuntime_DIR}" PARENT_SCOPE)
endfunction()

function(lc_configure_lonejson_package_root)
  set(lonejson_root "${LOCKDC_EXTERNAL_ROOT}/lonejson/install")
  lc_require_cpkt_config("lonejson" "${lonejson_root}/lib/cmake/lonejson/lonejsonConfig.cmake")
  set(lonejson_DIR "${lonejson_root}/lib/cmake/lonejson" CACHE PATH "lonejson CMake package directory." FORCE)
  set(lonejson_DIR "${lonejson_DIR}" PARENT_SCOPE)
endfunction()

function(lc_configure_liblql_package_root)
  set(liblql_root "${LOCKDC_EXTERNAL_ROOT}/liblql/install")
  lc_require_cpkt_config("liblql" "${liblql_root}/lib/cmake/liblql/liblqlConfig.cmake")
  set(liblql_DIR "${liblql_root}/lib/cmake/liblql" CACHE PATH "liblql CMake package directory." FORCE)
  set(liblql_DIR "${liblql_DIR}" PARENT_SCOPE)
endfunction()

function(lc_get_external_c_flags out_var)
  set(_flags "-O2 -DNDEBUG -g0")
  if(CMAKE_C_COMPILER_ID MATCHES "^(AppleClang|Clang|GNU)$")
    string(APPEND _flags
      " -fmacro-prefix-map=${LOCKDC_DEPENDENCY_BUILD_ROOT}=deps-build"
      " -fmacro-prefix-map=${LOCKDC_EXTERNAL_ROOT}=deps"
    )
  endif()
  if(NOT "${CMAKE_C_FLAGS}" STREQUAL "")
    set(_flags "${CMAKE_C_FLAGS} ${_flags}")
  endif()
  string(STRIP "${_flags}" _flags)
  set(${out_var} "${_flags}" PARENT_SCOPE)
endfunction()

function(lc_get_strip_dependency_install_command out_var install_dir)
  if(NOT CMAKE_STRIP)
    message(FATAL_ERROR "CMAKE_STRIP is required when building release dependencies")
  endif()

  set(_strip_static_archives ON)
  set(_darwin_fixup_args "")
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(_strip_static_archives OFF)
    if(NOT CMAKE_INSTALL_NAME_TOOL)
      message(FATAL_ERROR "CMAKE_INSTALL_NAME_TOOL is required when building Darwin dependencies")
    endif()
    if(NOT LOCKDC_OTOOL)
      message(FATAL_ERROR "LOCKDC_OTOOL is required when building Darwin dependencies")
    endif()
    set(_darwin_fixup_args
      -DLOCKDC_DARWIN_DEPENDENCY_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_INSTALL_NAME_TOOL=${CMAKE_INSTALL_NAME_TOOL}
      -DLOCKDC_OTOOL=${LOCKDC_OTOOL}
    )
  endif()

  set(_command
    ${CMAKE_COMMAND}
      -DLOCKDC_STRIP_BIN=${CMAKE_STRIP}
      -DLOCKDC_STRIP_ROOT=${install_dir}
      -DLOCKDC_STRIP_STATIC_ARCHIVES=${_strip_static_archives}
      ${_darwin_fixup_args}
      -P ${CMAKE_SOURCE_DIR}/cmake/strip_dependency_install_tree.cmake
  )
  set(${out_var} "${_command}" PARENT_SCOPE)
endfunction()

function(lc_append_common_external_cmake_args out_var)
  set(_args
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_AR=${CMAKE_AR}
    -DCMAKE_RANLIB=${CMAKE_RANLIB}
    -Wno-dev
  )

  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND _args -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  lc_get_external_c_flags(_lockdc_external_c_flags)
  list(APPEND _args -DCMAKE_C_FLAGS=${_lockdc_external_c_flags})

  set(${out_var} "${_args}" PARENT_SCOPE)
endfunction()

function(lc_add_openssl)
  find_package(OpenSSL ${LOCKDC_OPENSSL_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::openssl_crypto_static OpenSSL::Crypto)
  lc_add_interface_alias(lc::openssl_ssl_static OpenSSL::SSL)
  lc_add_interface_alias(lc::openssl_crypto_shared cpkt::openssl_crypto_shared)
  lc_add_interface_alias(lc::openssl_ssl_shared cpkt::openssl_ssl_shared)
endfunction()

function(lc_add_nghttp2)
  find_package(nghttp2 ${LOCKDC_NGHTTP2_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::nghttp2_static nghttp2::nghttp2)
  lc_add_interface_alias(lc::nghttp2_shared cpkt::nghttp2_shared)
endfunction()

function(lc_add_zlib)
  find_package(ZLIB ${LOCKDC_ZLIB_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::zlib_static ZLIB::ZLIB)
  lc_add_interface_alias(lc::zlib_shared cpkt::zlib_shared)
endfunction()

function(lc_add_libssh2)
  find_package(Libssh2 ${LOCKDC_LIBSSH2_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::libssh2_static Libssh2::libssh2)
  lc_add_interface_alias(lc::libssh2_shared cpkt::libssh2_shared)
endfunction()

function(lc_add_curl)
  find_package(CURL ${LOCKDC_CURL_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::curl_static CURL::libcurl)
  lc_add_interface_alias(lc::curl_shared cpkt::curl_shared)
endfunction()

function(lc_add_lonejson)
  find_package(lonejson ${LOCKDC_LONEJSON_VERSION} CONFIG REQUIRED)
  if(TARGET lonejson::lonejson_static)
    set_target_properties(lonejson::lonejson_static PROPERTIES
      INTERFACE_LINK_LIBRARIES lc::openssl_crypto_static
    )
  endif()
  lc_add_interface_alias(lc::lonejson_static lonejson::lonejson_static)
  lc_add_interface_alias(lc::lonejson_shared lonejson::lonejson)
endfunction()

function(lc_add_liblql)
  find_package(liblql ${LOCKDC_LIBLQL_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::liblql_static liblql::lql_static)
  lc_add_interface_alias(lc::liblql_shared liblql::lql_shared)
endfunction()

function(lc_add_cmocka)
  set(cmocka_DIR "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/cmocka")
  find_package(cmocka ${LOCKDC_CMOCKA_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::cmocka cpkt::cmocka_static)
endfunction()

function(lc_get_pslog_asset_info out_name out_hash)
  set(asset_name "libpslog-${LOCKDC_PSLOG_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")

  if(asset_name STREQUAL "libpslog-0.12.0-x86_64-linux-gnu.tar.gz")
    set(asset_hash "21239262bc06eecb504e45e0902f374ccd86751dcb32087dd84af4d2553ea0e2")
  elseif(asset_name STREQUAL "libpslog-0.12.0-x86_64-linux-musl.tar.gz")
    set(asset_hash "3a43206260bbf1095a363ffc4ddb5514dac2a621aa030db0e7f9b4364a93bb4d")
  elseif(asset_name STREQUAL "libpslog-0.12.0-aarch64-linux-gnu.tar.gz")
    set(asset_hash "304575fa07f14e5ff4d7614f7c450a3e88a586d5f353352bff8b678ee739da76")
  elseif(asset_name STREQUAL "libpslog-0.12.0-aarch64-linux-musl.tar.gz")
    set(asset_hash "72a3b4e1f89c339cadb996137a79db8ee6e67be1f580f482269f6b0088cda115")
  elseif(asset_name STREQUAL "libpslog-0.12.0-armhf-linux-gnu.tar.gz")
    set(asset_hash "993461d481ec6c3912be29f5a0f95bcf5274aae6a1d2d698350e804c688122a9")
  elseif(asset_name STREQUAL "libpslog-0.12.0-armhf-linux-musl.tar.gz")
    set(asset_hash "037fa1d56a01dc8b9fccf54960b8083337cbc5edfaa5f15d715f96413d1f6b77")
  elseif(asset_name STREQUAL "libpslog-0.12.0-arm64-apple-darwin.tar.gz")
    set(asset_hash "a5945f2dc4ad16dd8a71572f40117ddfdfae371d185cb03dab5205cdf780c321")
  else()
    message(FATAL_ERROR "Unsupported libpslog asset: ${asset_name}")
  endif()

  set(${out_name} "${asset_name}" PARENT_SCOPE)
  set(${out_hash} "${asset_hash}" PARENT_SCOPE)
endfunction()

function(lc_add_pslog)
  set(project_name "lc_pslog_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/pslog")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/pslog/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_get_strip_dependency_install_command(strip_install_command "${install_dir}")
  lc_get_pslog_asset_info(asset_name asset_hash)

  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES)
    set(pslog_archive_url "https://github.com/c89systems/libpslog/releases/download/v${LOCKDC_PSLOG_VERSION}/${asset_name}")
    if(NOT LOCKDC_PSLOG_ARCHIVE_PATH STREQUAL "")
      if(NOT EXISTS "${LOCKDC_PSLOG_ARCHIVE_PATH}")
        message(FATAL_ERROR "configured libpslog archive does not exist: ${LOCKDC_PSLOG_ARCHIVE_PATH}")
      endif()
      set(pslog_archive_url "${LOCKDC_PSLOG_ARCHIVE_PATH}")
    endif()
    ExternalProject_Add(${project_name}
      URL "${pslog_archive_url}"
      URL_HASH "SHA256=${asset_hash}"
      DOWNLOAD_NAME "${asset_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND
        ${CMAKE_COMMAND} -E rm -rf "${install_dir}"
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${source_dir}" "${install_dir}"
        COMMAND ${strip_install_command}
      BUILD_IN_SOURCE 1
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::pslog_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::pslog_static
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libpslog${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::pslog_static ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libpslog${CMAKE_STATIC_LIBRARY_SUFFIX}" "libpslog (static)")
    lc_require_dependency_file("${install_dir}/include/pslog.h" "libpslog header")
  endif()

  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(pslog_shared_library "${install_dir}/lib/libpslog.0${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(pslog_shared_library "${install_dir}/lib/libpslog${CMAKE_SHARED_LIBRARY_SUFFIX}.0")
  endif()

  add_library(lc::pslog_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::pslog_shared
    PROPERTIES
      IMPORTED_LOCATION "${pslog_shared_library}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::pslog_shared ${project_name})
  else()
    lc_require_dependency_file("${pslog_shared_library}" "libpslog (shared)")
  endif()
endfunction()

function(lc_configure_dependencies)
  if(LOCKDC_BUILD_STATIC OR LOCKDC_BUILD_SHARED)
    lc_configure_cpkt_package_roots()
    lc_configure_lonejson_package_root()
    lc_configure_liblql_package_root()
    lc_add_openssl()
  endif()

  lc_add_zlib()
  lc_add_libssh2()

  if(LOCKDC_BUILD_STATIC OR LOCKDC_BUILD_SHARED)
    lc_add_pslog()
    lc_add_nghttp2()
    lc_add_curl()
    lc_add_lonejson()
    lc_add_liblql()
  endif()

  if(LOCKDC_BUILD_TESTS)
    lc_add_cmocka()
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    get_property(dep_targets GLOBAL PROPERTY LOCKDC_DEPENDENCY_TARGETS)
    if(dep_targets)
      add_custom_target(lc_deps DEPENDS ${dep_targets})
    endif()
  endif()
endfunction()
