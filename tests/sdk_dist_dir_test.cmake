cmake_minimum_required(VERSION 3.21)
set(test_root "${LOCKDC_BINARY_DIR}/sdk-dist-dir-test")
set(fixture "${test_root}/fixture")
set(requested "${test_root}/requested")
file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${fixture}" "${requested}")
# No compiler runs: archive selection must fail first, at the caller's path.
file(WRITE "${fixture}/CMakeCache.txt"
    "CMAKE_C_COMPILER:FILEPATH=${CMAKE_COMMAND}\n")
file(WRITE "${fixture}/package-metadata.cmake"
    "set(LOCKDC_VERSION \"0.0.0\")\n"
    "set(LOCKDC_TARGET_ID \"x86_64-linux-gnu\")\n"
    "set(LOCKDC_DIST_DIR \"${test_root}/metadata\")\n")
execute_process(COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${LOCKDC_ROOT}
    -DLOCKDC_BINARY_DIR=${fixture}
    -DLOCKDC_DIST_DIR=${requested}
    -DLOCKDC_USE_EXISTING_ARCHIVE=ON
    -P "${LOCKDC_ROOT}/tests/release_tarball_sdk_test.cmake"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
set(expected "${requested}/liblockdc-0.0.0-x86_64-linux-gnu.tar.gz")
string(FIND "${error}" "${expected}" found)
if(result EQUAL 0 OR found EQUAL -1)
    message(FATAL_ERROR "SDK verifier ignored its explicit archive directory:\n${output}\n${error}")
endif()
file(REMOVE_RECURSE "${test_root}")
