include("${CMAKE_CURRENT_LIST_DIR}/dependency_library_test_support.cmake")

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

set(zlib_root "${LOCKDC_EXTERNAL_ROOT}/zlib/install")
set(zlib_static_archive "${zlib_root}/lib/libz.a")
set(zlib_shared_library "${zlib_root}/lib/libz.${dependency_shared_suffix}")
set(zlib_header "${zlib_root}/include/zlib.h")
set(cpkt_manifest "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/share/cpkt/packages/core.json")

foreach(path IN ITEMS
    "${zlib_static_archive}"
    "${zlib_shared_library}"
    "${zlib_header}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing zlib dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${zlib_root}/lib/pkgconfig"
    "${zlib_root}/lib/cmake"
    "${zlib_root}/share/man")
    if(EXISTS "${path}")
        message(FATAL_ERROR "zlib install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

if(NOT EXISTS "${cpkt_manifest}")
    message(FATAL_ERROR "missing cpkt manifest: ${cpkt_manifest}")
endif()

foreach(path IN ITEMS
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/cmake/zlib/ZLIBConfig.cmake"
    "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/lib/pkgconfig/zlib.pc")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing zlib dependency consumer metadata: ${path}")
    endif()
endforeach()

include("${CMAKE_CURRENT_LIST_DIR}/cpkt_manifest_test_support.cmake")
lockdc_assert_cpkt_component("${cpkt_manifest}" zlib "1.3.2")
