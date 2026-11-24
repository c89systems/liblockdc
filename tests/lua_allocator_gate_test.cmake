foreach(stream stdout stderr)
  execute_process(COMMAND ${CMAKE_COMMAND} -E env
      LOCKDC_TEST_ALLOCATOR_TRACKING=1 LOCKDC_TEST_REPORT_STREAM=${stream}
      ${CMAKE_COMMAND}
      -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
      -DLOCKDC_ROOT=${LOCKDC_ROOT}
      -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}
      -DLOCKDC_LUA_FIXTURE_ROOT=${LOCKDC_LUA_FIXTURE_ROOT}
      -DLOCKDC_TEST_NAME=lua_allocator_gate_${stream}
      -DLOCKDC_LUA_TEST_SCRIPT=${LOCKDC_ROOT}/tests/lua/test_allocator_report_fixture.lua
      -P ${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
  if(result EQUAL 0 OR NOT error MATCHES "(LuaRocks validation|Lua build-tree run) failed" OR
     NOT error MATCHES "liblockdc allocator: captured-report-regression")
    message(FATAL_ERROR "captured Lua ${stream} leak report did not fail: ${output}\n${error}")
  endif()
endforeach()
