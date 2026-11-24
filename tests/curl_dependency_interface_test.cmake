include("${CMAKE_CURRENT_LIST_DIR}/dependency_library_test_support.cmake")

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

set(curl_root "${LOCKDC_EXTERNAL_ROOT}/curl/install")
set(curl_header "${curl_root}/include/curl/curl.h")
set(curl_static_archive "${curl_root}/lib/libcurl.a")
set(curl_shared_library "${curl_root}/lib/libcurl.${dependency_shared_suffix}")
set(cpkt_manifest "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/share/cpkt/packages/core.json")

foreach(path IN ITEMS "${curl_header}" "${curl_static_archive}" "${curl_shared_library}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing curl public dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${curl_root}/bin"
    "${curl_root}/lib/pkgconfig"
    "${curl_root}/lib/cmake")
    if(EXISTS "${path}")
        message(FATAL_ERROR "curl install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

if(NOT EXISTS "${cpkt_manifest}")
    message(FATAL_ERROR "missing cpkt manifest: ${cpkt_manifest}")
endif()

foreach(path IN ITEMS
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/CURL/CURLConfig.cmake"
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/pkgconfig/libcurl.pc")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing curl dependency consumer metadata: ${path}")
    endif()
endforeach()

include("${CMAKE_CURRENT_LIST_DIR}/cpkt_manifest_test_support.cmake")
lockdc_assert_cpkt_component("${cpkt_manifest}" curl "8.22.0")
lockdc_assert_cpkt_component("${cpkt_manifest}" libssh2 "1.11.1")
lockdc_assert_cpkt_component("${cpkt_manifest}" nghttp2 "1.70.0")
