include("${CMAKE_CURRENT_LIST_DIR}/dependency_library_test_support.cmake")

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

set(libssh2_root "${LOCKDC_EXTERNAL_ROOT}/libssh2/install")
set(cpkt_manifest "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/share/cpkt/packages/core.json")

foreach(path IN ITEMS
    "${libssh2_root}/lib/libssh2.a"
    "${libssh2_root}/lib/libssh2.${dependency_shared_suffix}"
    "${libssh2_root}/include/libssh2.h")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing libssh2 dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${libssh2_root}/lib/pkgconfig"
    "${libssh2_root}/lib/cmake"
    "${libssh2_root}/bin")
    if(EXISTS "${path}")
        message(FATAL_ERROR "libssh2 install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

if(NOT EXISTS "${cpkt_manifest}")
    message(FATAL_ERROR "missing cpkt manifest: ${cpkt_manifest}")
endif()

foreach(path IN ITEMS
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/libssh2/libssh2-config.cmake"
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/pkgconfig/libssh2.pc")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing libssh2 dependency consumer metadata: ${path}")
    endif()
endforeach()

include("${CMAKE_CURRENT_LIST_DIR}/cpkt_manifest_test_support.cmake")
lockdc_assert_cpkt_component("${cpkt_manifest}" libssh2 "1.11.1")
lockdc_assert_cpkt_component("${cpkt_manifest}" zlib "1.3.2")
