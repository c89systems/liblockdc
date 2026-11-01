if(NOT LOCKDC_BINARY_DIR OR NOT LOCKDC_LUA_XA_BINARY OR NOT LOCKDC_ROOT)
    message(FATAL_ERROR "Lua XA test requires binary and source roots")
endif()
if(NOT DEFINED LOCKDC_LUA_TEST_CASE)
    set(LOCKDC_LUA_TEST_CASE xa)
endif()
if(NOT LOCKDC_LUA_TEST_CASE MATCHES "^(xa|retention|collections|collection_count|collection_writer|collection_staged|collection_leases|collection_edits|compression)$")
    message(FATAL_ERROR "Unknown Lua test case")
endif()
set(root "${LOCKDC_BINARY_DIR}/lua-${LOCKDC_LUA_TEST_CASE}-test")
file(REMOVE_RECURSE "${root}")
file(MAKE_DIRECTORY "${root}")
set(runner)
if("$ENV{LOCKDC_LUA_XA_VALGRIND}" STREQUAL "1")
    set(runner valgrind --error-exitcode=99 --leak-check=full
        --errors-for-leak-kinds=definite,indirect
        "--log-file=${LOCKDC_BINARY_DIR}/lua-xa-valgrind.log")
endif()
execute_process(
    COMMAND ${runner} "${LOCKDC_LUA_XA_BINARY}"
        "${LOCKDC_ROOT}/tests/lua/test_lockdc_${LOCKDC_LUA_TEST_CASE}.lua" "${root}/pouch"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
file(REMOVE_RECURSE "${root}")
if(NOT result EQUAL 0 OR output MATCHES "liblockdc allocator:" OR
   error MATCHES "liblockdc allocator:")
    message(FATAL_ERROR "Lua XA failed (${result})\n${output}\n${error}")
endif()
message(STATUS "${output}")
