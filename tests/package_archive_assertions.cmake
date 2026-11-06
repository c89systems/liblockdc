include("${CMAKE_CURRENT_LIST_DIR}/release_privacy_scan.cmake")
get_filename_component(LOCKDC_TEST_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)

function(assert_contains archive_listing archive_path pattern description)
    if(NOT archive_listing MATCHES "${pattern}")
        message(FATAL_ERROR "archive missing ${description}: ${archive_path}")
    endif()
endfunction()

function(assert_not_contains archive_listing archive_path pattern description)
    if(archive_listing MATCHES "${pattern}")
        message(FATAL_ERROR "archive unexpectedly contains ${description}: ${archive_path}")
    endif()
endfunction()

function(assert_symlink_target extract_root archive_path symlink_path)
    if(NOT IS_SYMLINK "${extract_root}/${symlink_path}")
        message(FATAL_ERROR "expected symlink entry ${symlink_path} in ${archive_path}")
    endif()

    file(READ_SYMLINK "${extract_root}/${symlink_path}" symlink_target)
    get_filename_component(symlink_dir "${extract_root}/${symlink_path}" DIRECTORY)
    if(NOT EXISTS "${symlink_dir}/${symlink_target}")
        message(FATAL_ERROR
            "archive contains broken symlink ${symlink_path} -> ${symlink_target}: ${archive_path}")
    endif()
endfunction()

function(assert_shared_library_runpath extract_root archive_path shared_lib_name)
    if(shared_lib_name MATCHES "\\.dylib$")
        string(REGEX MATCH "^lib[^.]+\\.[0-9]+\\." _darwin_versioned_name_match "${shared_lib_name}")
        if(NOT _darwin_versioned_name_match)
            message(FATAL_ERROR
                "Darwin archive shared-library validation expects a versioned dylib name: "
                "lib/${shared_lib_name} in ${archive_path}")
        endif()
        execute_process(
            COMMAND "${LOCKDC_TEST_ROOT}/scripts/discover_target_tools.sh"
                --build-dir "${LOCKDC_BINARY_DIR}"
                --target-id "${LOCKDC_TARGET_ID}"
                --tool otool
            RESULT_VARIABLE otool_discovery_result
            OUTPUT_VARIABLE LOCKDC_OTOOL_BIN
            ERROR_VARIABLE otool_discovery_error
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        if(NOT otool_discovery_result EQUAL 0 OR NOT EXISTS "${LOCKDC_OTOOL_BIN}")
            message(FATAL_ERROR
                "external-tool-unavailable: otool is required for Darwin archive shared-library validation\n"
                "${otool_discovery_error}")
        endif()

        execute_process(
            COMMAND "${LOCKDC_OTOOL_BIN}" -D "${extract_root}/lib/${shared_lib_name}"
            RESULT_VARIABLE otool_id_result
            OUTPUT_VARIABLE otool_id_output
            ERROR_VARIABLE otool_id_error
        )
        if(NOT otool_id_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect Darwin shared library install name in ${archive_path}\n"
                "${otool_id_output}${otool_id_error}")
        endif()
        string(REGEX REPLACE "\n$" "" otool_id_output "${otool_id_output}")
        if(NOT otool_id_output MATCHES "\n@rpath/")
            message(FATAL_ERROR
                "archive Darwin shared library install name is not @rpath-relative in "
                "lib/${shared_lib_name}: ${archive_path}\n${otool_id_output}")
        endif()
        if(otool_id_output MATCHES "\n@rpath/liblockdc\\.dylib($|\n)")
            message(FATAL_ERROR
                "archive Darwin shared library has an unversioned install name in "
                "lib/${shared_lib_name}: ${archive_path}\n${otool_id_output}")
        endif()

        execute_process(
            COMMAND "${LOCKDC_OTOOL_BIN}" -L "${extract_root}/lib/${shared_lib_name}"
            RESULT_VARIABLE otool_deps_result
            OUTPUT_VARIABLE otool_deps_output
            ERROR_VARIABLE otool_deps_error
        )
        if(NOT otool_deps_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect Darwin shared library dependencies in ${archive_path}\n"
                "${otool_deps_output}${otool_deps_error}")
        endif()
        string(REGEX REPLACE "\n$" "" otool_deps_output "${otool_deps_output}")
        string(REPLACE "\n" ";" otool_deps_lines "${otool_deps_output}")
        foreach(otool_deps_line IN LISTS otool_deps_lines)
            string(STRIP "${otool_deps_line}" dependency_line)
            if(dependency_line MATCHES ":$")
                continue()
            endif()
            if(NOT dependency_line MATCHES "^/")
                continue()
            endif()
            string(REGEX MATCH "^[^ \t]+" dependency_path "${dependency_line}")
            if(dependency_path MATCHES "^/usr/lib/" OR dependency_path MATCHES "^/System/Library/")
                continue()
            endif()
            message(FATAL_ERROR
                "archive Darwin shared library contains non-system absolute dependency path "
                "'${dependency_path}' in lib/${shared_lib_name}: ${archive_path}\n${otool_deps_output}")
        endforeach()

        execute_process(
            COMMAND "${LOCKDC_OTOOL_BIN}" -l "${extract_root}/lib/${shared_lib_name}"
            RESULT_VARIABLE otool_result
            OUTPUT_VARIABLE otool_output
            ERROR_VARIABLE otool_error
        )
        if(NOT otool_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect Darwin shared library load commands in ${archive_path}\n${otool_output}${otool_error}")
        endif()
        if(otool_output MATCHES "path /")
            message(FATAL_ERROR
                "archive contains absolute Darwin rpath in lib/${shared_lib_name}: ${archive_path}\n${otool_output}")
        endif()
        if(NOT otool_output MATCHES "path @loader_path")
            message(FATAL_ERROR
                "archive Darwin shared library is missing relocatable @loader_path rpath in lib/${shared_lib_name}: ${archive_path}\n${otool_output}")
        endif()
        return()
    endif()

    execute_process(
        COMMAND "${LOCKDC_TEST_ROOT}/scripts/discover_target_tools.sh"
            --build-dir "${LOCKDC_BINARY_DIR}"
            --target-id "${LOCKDC_TARGET_ID}"
            --tool readelf
        RESULT_VARIABLE readelf_discovery_result
        OUTPUT_VARIABLE LOCKDC_READELF_BIN
        ERROR_VARIABLE readelf_discovery_error
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(NOT readelf_discovery_result EQUAL 0 OR NOT EXISTS "${LOCKDC_READELF_BIN}")
        message(FATAL_ERROR
            "external-tool-unavailable: target readelf is required for archive shared-library validation\n"
            "${readelf_discovery_error}")
    endif()

    execute_process(
        COMMAND "${LOCKDC_READELF_BIN}" -d "${extract_root}/lib/${shared_lib_name}"
        RESULT_VARIABLE readelf_result
        OUTPUT_VARIABLE readelf_output
        ERROR_VARIABLE readelf_error
    )
    if(NOT readelf_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect shared library dynamic tags in ${archive_path}\n${readelf_output}${readelf_error}")
    endif()

    if(readelf_output MATCHES "\\(RUNPATH\\).*\\[/")
        message(FATAL_ERROR
            "archive contains absolute RUNPATH in lib/${shared_lib_name}: ${archive_path}\n${readelf_output}")
    endif()

    if(NOT readelf_output MATCHES "\\(RUNPATH\\).*\\[\\$ORIGIN\\]")
        message(FATAL_ERROR
            "archive shared library is missing relocatable $ORIGIN RUNPATH in lib/${shared_lib_name}: ${archive_path}\n${readelf_output}")
    endif()
endfunction()

function(assert_archive_numeric_owner_group archive_path)
    find_program(LOCKDC_TAR_BIN NAMES tar)
    if(NOT LOCKDC_TAR_BIN)
        message(FATAL_ERROR "tar is required for archive ownership validation")
    endif()

    execute_process(
        COMMAND "${LOCKDC_TAR_BIN}" --numeric-owner -tvf "${archive_path}"
        RESULT_VARIABLE tar_result
        OUTPUT_VARIABLE archive_metadata
        ERROR_VARIABLE archive_metadata_error
    )
    if(NOT tar_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect archive ownership metadata: ${archive_path}\n${archive_metadata}${archive_metadata_error}")
    endif()

    string(REGEX REPLACE "\n$" "" archive_metadata_trimmed "${archive_metadata}")
    string(REPLACE "\n" ";" archive_metadata_lines "${archive_metadata_trimmed}")
    foreach(archive_metadata_line IN LISTS archive_metadata_lines)
        if(archive_metadata_line STREQUAL "")
            continue()
        endif()
        if(NOT archive_metadata_line MATCHES "^[^ ]+[ ]+0/0([ ]+|$)")
            message(FATAL_ERROR
                "archive entry does not use numeric owner/group 0/0: ${archive_path}\n${archive_metadata_line}")
        endif()
    endforeach()
endfunction()

function(assert_lockdc_owned_binaries_have_no_local_paths extract_root archive_path)
    find_program(LOCKDC_STRINGS_BIN NAMES strings)
    if(NOT LOCKDC_STRINGS_BIN)
        message(FATAL_ERROR "strings is required for archive local-path validation")
    endif()

    set(disallowed_paths "${LOCKDC_ROOT}" "$ENV{HOME}")
    file(GLOB lockdc_owned_binaries
        LIST_DIRECTORIES false
        "${extract_root}/lib/liblockdc.a"
        "${extract_root}/lib/liblockdc.so*"
        "${extract_root}/lib/liblockdc.*.dylib"
    )
    foreach(lockdc_owned_binary IN LISTS lockdc_owned_binaries)
        if(IS_SYMLINK "${lockdc_owned_binary}")
            continue()
        endif()
        execute_process(
            COMMAND "${LOCKDC_STRINGS_BIN}" "${lockdc_owned_binary}"
            RESULT_VARIABLE strings_result
            OUTPUT_VARIABLE strings_output
            ERROR_VARIABLE strings_error
        )
        if(NOT strings_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect local paths in ${lockdc_owned_binary}: ${archive_path}\n${strings_error}")
        endif()
        foreach(disallowed_path IN LISTS disallowed_paths)
            if(disallowed_path STREQUAL "")
                continue()
            endif()
            string(FIND "${strings_output}" "${disallowed_path}" disallowed_index)
            if(NOT disallowed_index EQUAL -1)
                message(FATAL_ERROR
                    "archive liblockdc-owned binary contains local path '${disallowed_path}': "
                    "${lockdc_owned_binary} in ${archive_path}")
            endif()
        endforeach()
    endforeach()
endfunction()

function(assert_packaged_binaries_have_no_sanitizer_runtime extract_root archive_path)
    find_program(LOCKDC_STRINGS_BIN NAMES strings)
    if(NOT LOCKDC_STRINGS_BIN)
        message(FATAL_ERROR "strings is required for archive sanitizer validation")
    endif()

    file(GLOB packaged_binaries
        LIST_DIRECTORIES false
        "${extract_root}/lib/*.a"
        "${extract_root}/lib/*.so"
        "${extract_root}/lib/*.so.*"
        "${extract_root}/lib/*.dylib"
        "${extract_root}/lib/*.dll"
    )
    foreach(packaged_binary IN LISTS packaged_binaries)
        if(IS_SYMLINK "${packaged_binary}")
            continue()
        endif()
        execute_process(
            COMMAND "${LOCKDC_STRINGS_BIN}" "${packaged_binary}"
            RESULT_VARIABLE strings_result
            OUTPUT_VARIABLE strings_output
            ERROR_VARIABLE strings_error
        )
        if(NOT strings_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect sanitizer references in ${packaged_binary}: ${archive_path}\n${strings_error}")
        endif()
        foreach(disallowed_sanitizer_reference
            "libasan"
            "libubsan"
            "__asan_"
            "__ubsan_"
            "AddressSanitizer"
            "UndefinedBehaviorSanitizer"
            "ASAN_OPTIONS"
            "UBSAN_OPTIONS"
            "-fsanitize=address"
            "-fsanitize=undefined"
        )
            string(FIND "${strings_output}" "${disallowed_sanitizer_reference}" sanitizer_reference_index)
            if(NOT sanitizer_reference_index EQUAL -1)
                message(FATAL_ERROR
                    "release archive binary contains sanitizer reference '${disallowed_sanitizer_reference}': "
                    "${packaged_binary} in ${archive_path}")
            endif()
        endforeach()
    endforeach()
endfunction()

function(assert_sdk_metadata extract_root archive_path version target_id shared_lib_name shared_soname shared_link_name)
    set(metadata_path "${extract_root}/share/lockdc/package-metadata.cmake")
    set(dependencies_path "${extract_root}/share/lockdc/dependencies.json")
    if(NOT EXISTS "${metadata_path}")
        message(FATAL_ERROR "archive missing SDK package metadata: ${archive_path}")
    endif()
    if(NOT EXISTS "${dependencies_path}")
        message(FATAL_ERROR "archive missing SDK dependency provenance: ${archive_path}")
    endif()

    file(READ "${metadata_path}" metadata_text)
    foreach(expected_metadata
            "set(LOCKDC_PACKAGE_NAME \"liblockdc\")"
            "set(LOCKDC_VERSION \"${version}\")"
            "set(LOCKDC_TARGET_ID \"${target_id}\")"
            "set(LOCKDC_SHARED_LIB_NAME \"${shared_lib_name}\")"
            "set(LOCKDC_SHARED_SONAME \"${shared_soname}\")"
            "set(LOCKDC_SHARED_LINK_NAME \"${shared_link_name}\")"
            "set(LOCKDC_DEPENDENCY_MODE \"external\")")
        string(FIND "${metadata_text}" "${expected_metadata}" metadata_index)
        if(metadata_index EQUAL -1)
            message(FATAL_ERROR
                "archive SDK metadata missing '${expected_metadata}': ${archive_path}\n${metadata_text}")
        endif()
    endforeach()

    file(READ "${dependencies_path}" dependencies_text)
    foreach(expected_dependency
            "\"schema\": \"lockdc.dependencies.v1\""
            "\"package\": \"liblockdc\""
            "\"version\": \"${version}\""
            "\"target_id\": \"${target_id}\""
            "\"dependency_mode\": \"external\""
            "\"name\": \"openssl\""
            "\"name\": \"zlib\""
            "\"name\": \"curl\""
            "\"name\": \"nghttp2\""
            "\"name\": \"libssh2\""
            "\"name\": \"libpslog\""
            "\"name\": \"lonejson\""
            "\"name\": \"liblql\""
            "\"bundled\": false"
            "\"role\": \"external-static-consumer\"")
        string(FIND "${dependencies_text}" "${expected_dependency}" dependency_index)
        if(dependency_index EQUAL -1)
            message(FATAL_ERROR
                "archive dependency provenance missing '${expected_dependency}': ${archive_path}\n${dependencies_text}")
        endif()
    endforeach()
    string(REGEX MATCHALL "\"sha256\": \"[0-9a-f]+\"" dependency_sha_entries "${dependencies_text}")
    list(LENGTH dependency_sha_entries dependency_sha_count)
    if(NOT dependency_sha_count EQUAL 9)
        message(FATAL_ERROR "archive dependency provenance is missing SHA-256 values: ${archive_path}\n${dependencies_text}")
    endif()
    string(JSON sdk_count LENGTH "${dependencies_text}" sdk_prerequisites)
    if(NOT sdk_count EQUAL 1)
        message(FATAL_ERROR "Core SDK must require only cpkt, without optional cpktdb")
    endif()
    set(providers cpkt)
    foreach(index RANGE 0 0)
        list(GET providers ${index} expected_name)
        string(JSON name GET "${dependencies_text}" sdk_prerequisites ${index} name)
        string(JSON release GET "${dependencies_text}" sdk_prerequisites ${index} version)
        string(JSON url GET "${dependencies_text}" sdk_prerequisites ${index} source_url)
        string(JSON sha GET "${dependencies_text}" sdk_prerequisites ${index} sha256)
        string(LENGTH "${sha}" sha_length)
        if(NOT name STREQUAL expected_name OR NOT release STREQUAL "0.6.1"
           OR NOT sha_length EQUAL 64 OR NOT sha MATCHES "^[a-f0-9]+$"
           OR NOT url STREQUAL "https://github.com/c89systems/${name}/releases/download/v${release}/${name}-${release}-${target_id}.tar.gz")
            message(FATAL_ERROR "Invalid ${expected_name} SDK provenance in ${archive_path}")
        endif()
    endforeach()
    if(dependencies_text MATCHES "\"sha256\": \"\"")
        message(FATAL_ERROR "archive dependency provenance has an empty SHA-256 value: ${archive_path}\n${dependencies_text}")
    endif()
endfunction()

function(assert_archive_layout archive_path version target_id shared_lib_name shared_soname shared_link_name)
    if(NOT EXISTS "${archive_path}")
        message(FATAL_ERROR "missing archive: ${archive_path}")
    endif()

    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar tf "${archive_path}"
        RESULT_VARIABLE tar_result
        OUTPUT_VARIABLE archive_listing
    )
    if(NOT tar_result EQUAL 0)
        message(FATAL_ERROR "failed to list archive contents: ${archive_path}")
    endif()

    assert_archive_numeric_owner_group("${archive_path}")

    set(archive_prefix "liblockdc-${version}-${target_id}")
    string(REPLACE "." "\\." archive_prefix_regex "${archive_prefix}")
    string(REGEX REPLACE "\n$" "" archive_listing_trimmed "${archive_listing}")
    string(REPLACE "\n" ";" archive_entries "${archive_listing_trimmed}")
    foreach(archive_entry IN LISTS archive_entries)
        if(archive_entry STREQUAL "")
            continue()
        endif()
        if(archive_entry MATCHES "^\\./")
            message(FATAL_ERROR "archive contains invalid ./-prefixed entry '${archive_entry}': ${archive_path}")
        endif()
        if(NOT archive_entry MATCHES "^${archive_prefix_regex}(/|$)")
            message(FATAL_ERROR "archive contains entry outside release prefix '${archive_prefix}/': ${archive_entry} in ${archive_path}")
        endif()
    endforeach()

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/(\n|$)" "archive root directory")

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/LICENSE(\n|$)" "share/doc/liblockdc/LICENSE")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/README.md(\n|$)" "share/doc/liblockdc/README.md")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/lockdc/package-metadata\\.cmake(\n|$)" "share/lockdc/package-metadata.cmake")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/lockdc/dependencies\\.json(\n|$)" "share/lockdc/dependencies.json")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/lc/lc\\.h(\n|$)" "include/lc/lc.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/lc/version\\.h(\n|$)" "include/lc/version.h")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/pslog(_version)?\\.h(\n|$)" "libpslog headers")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/curl(/|\n|$)" "curl headers")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/openssl(/|\n|$)" "OpenSSL headers")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/nghttp2(/|\n|$)" "nghttp2 headers")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/libssh2(_publickey|_sftp)?\\.h(\n|$)" "libssh2 headers")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/z(conf|lib)\\.h(\n|$)" "zlib headers")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/lonejson\\.h(\n|$)" "lonejson header")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/lql(/|\n|$)" "liblql headers")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/liblockdc(/|\n|$)" "engine share/liblockdc path")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/lockdc/luarocks(/|\n|$)" "embedded LuaRocks payload")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/lua/5\\.5/lockdc(/|\n|$)" "embedded Lua runtime wrapper")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/lua/5\\.5/lockdc(/|\n|$)" "embedded Lua native module")
    string(REPLACE "." "\\." shared_name_regex "${shared_lib_name}")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${shared_name_regex}(\n|$)" "versioned shared library")
    if(DEFINED shared_soname
       AND NOT "${shared_soname}" STREQUAL ""
       AND NOT "${shared_soname}" STREQUAL "${shared_lib_name}")
        string(REPLACE "." "\\." shared_soname_regex "${shared_soname}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${shared_soname_regex}(\n|$)" "shared-library SONAME symlink")
    endif()
    if(DEFINED shared_link_name AND NOT "${shared_link_name}" STREQUAL "")
        string(REPLACE "." "\\." shared_link_regex "${shared_link_name}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${shared_link_regex}(\n|$)" "shared-library linker symlink")
    endif()

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/liblockdc\\.a(\n|$)" "static library")
    foreach(forbidden_dependency_archive
        libpslog
        libcurl
        libssl
        libcrypto
        libnghttp2
        libssh2
        libz
        liblonejson
        liblql)
        assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${forbidden_dependency_archive}\\.a(\n|$)" "${forbidden_dependency_archive} static archive")
    endforeach()
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/pkgconfig/lockdc\\.pc(\n|$)" "pkg-config metadata")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/cmake/lockdc/lockdcConfig\\.cmake(\n|$)" "CMake package config")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/cmake/lockdc/lockdcConfigVersion\\.cmake(\n|$)" "CMake package version file")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party(/|\n|$)" "third-party license directory")
    if(target_id MATCHES "apple-darwin$")
        foreach(forbidden_dependency_library
            libcurl
            libpslog
            libssl
            libcrypto
            libnghttp2
            libssh2
            libz
            liblonejson
            liblql)
            assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${forbidden_dependency_library}[^/\n]*\\.dylib" "${forbidden_dependency_library} dylib")
        endforeach()
    else()
        foreach(forbidden_dependency_library
            libcurl
            libpslog
            libssl
            libcrypto
            libnghttp2
            libssh2
            libz
            liblonejson
            liblql)
            assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${forbidden_dependency_library}\\.so" "${forbidden_dependency_library} shared library")
        endforeach()
    endif()

    string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef extract_suffix)
    get_filename_component(archive_name "${archive_path}" NAME_WE)
    set(extract_root "${CMAKE_CURRENT_BINARY_DIR}/archive-assert-${archive_name}-${extract_suffix}")
    file(REMOVE_RECURSE "${extract_root}")
    file(MAKE_DIRECTORY "${extract_root}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar xf "${archive_path}"
        WORKING_DIRECTORY "${extract_root}"
        RESULT_VARIABLE extract_result
    )
    if(NOT extract_result EQUAL 0)
        message(FATAL_ERROR "failed to extract archive for validation: ${archive_path}")
    endif()
    foreach(public_doc README.md CHANGELOG.md docs/c-api.md docs/development.md
        docs/releases.md docs/xa.md docs/postgres.md
        docs/lua.md docs/pouch-terminal-retention.md)
        set(packaged_doc "${extract_root}/${archive_prefix}/share/doc/liblockdc/${public_doc}")
        if(NOT EXISTS "${packaged_doc}")
            message(FATAL_ERROR "SDK archive is missing documentation ${public_doc}: ${archive_path}")
        endif()
        file(SHA256 "${LOCKDC_TEST_ROOT}/${public_doc}" expected_doc_digest)
        file(SHA256 "${packaged_doc}" packaged_doc_digest)
        if(NOT expected_doc_digest STREQUAL packaged_doc_digest)
            message(FATAL_ERROR "SDK archive has stale documentation ${public_doc}: ${archive_path}")
        endif()
    endforeach()
    foreach(investigation pouch-replay-investigation.md lockd-implicit-xa-handover.md
        lockd-memory-queue-watch-race.md lockd-durable-crypto-index-freshness.md)
        if(EXISTS "${extract_root}/${archive_prefix}/share/doc/liblockdc/docs/${investigation}")
            message(FATAL_ERROR "SDK archive contains an investigation report: ${investigation}")
        endif()
    endforeach()
    if(DEFINED shared_soname
       AND NOT "${shared_soname}" STREQUAL ""
       AND NOT "${shared_soname}" STREQUAL "${shared_lib_name}")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/${shared_soname}")
    endif()
    if(DEFINED shared_link_name AND NOT "${shared_link_name}" STREQUAL "")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/${shared_link_name}")
    endif()
    assert_shared_library_runpath("${extract_root}/${archive_prefix}" "${archive_path}" "${shared_lib_name}")
    assert_sdk_metadata("${extract_root}/${archive_prefix}" "${archive_path}" "${version}" "${target_id}" "${shared_lib_name}" "${shared_soname}" "${shared_link_name}")
    if(NOT DEFINED LOCKDC_SANITIZER_INSTRUMENTED OR LOCKDC_SANITIZER_INSTRUMENTED STREQUAL "" OR
       LOCKDC_SANITIZER_INSTRUMENTED STREQUAL "0")
        lockdc_assert_tree_has_no_private_traces("${extract_root}/${archive_prefix}" "${archive_path}")
        assert_packaged_binaries_have_no_sanitizer_runtime("${extract_root}/${archive_prefix}" "${archive_path}")
    endif()
    file(REMOVE_RECURSE "${extract_root}")

    file(READ "${archive_path}" archive_xfl HEX OFFSET 8 LIMIT 1)
    string(TOLOWER "${archive_xfl}" archive_xfl)
    if(NOT archive_xfl STREQUAL "02")
        message(FATAL_ERROR "archive is not using gzip maximum compression header: ${archive_path}")
    endif()
endfunction()
if(NOT DEFINED LOCKDC_ZLIB_VERSION OR LOCKDC_ZLIB_VERSION STREQUAL "")
  set(LOCKDC_ZLIB_VERSION "1.3.2")
endif()
