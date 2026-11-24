execute_process(COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${LOCKDC_BINARY_DIR}"
    --show-only=json-v1 -L lua
    RESULT_VARIABLE result OUTPUT_VARIABLE inventory ERROR_VARIABLE error)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Cannot inspect Lua fixtures: ${error}")
endif()
string(JSON count LENGTH "${inventory}" tests)
math(EXPR last "${count} - 1")
set(consumers 0)
set(setups 0)
foreach(index RANGE 0 ${last})
    string(JSON name GET "${inventory}" tests ${index} name)
    if(NOT name MATCHES "^lc_e2e_")
        continue()
    endif()
    string(JSON properties GET "${inventory}" tests ${index} properties)
    if(name STREQUAL "lc_e2e_lua_setup")
        set(property FIXTURES_SETUP)
        math(EXPR setups "${setups} + 1")
    else()
        set(property FIXTURES_REQUIRED)
        math(EXPR consumers "${consumers} + 1")
    endif()
    if(NOT properties MATCHES "\"${property}\"[^}]*\"lockdc_e2e_lua\"")
        message(FATAL_ERROR "${name}: missing shared Lua fixture dependency")
    endif()
endforeach()
if(NOT setups EQUAL 1 OR consumers LESS 1)
    message(FATAL_ERROR "Expected one Lua setup and nonempty scenarios, got ${setups}/${consumers}")
endif()

# Run-only must never silently recreate a missing installation, even when an
# old directory happens to exist. These failures occur before Lua is invoked.
set(root "${LOCKDC_BINARY_DIR}/lua-fixture-regression")
file(REMOVE_RECURSE "${root}")
file(MAKE_DIRECTORY "${root}/tree" "${root}/prefix")
execute_process(COMMAND "${CMAKE_COMMAND}" -E env LOCKDC_LUA_BIN=true LOCKDC_LUAROCKS_BIN=true
    "LOCKDC_LUAROCKS_WORKDIR=${LOCKDC_ROOT}"
    bash "${LOCKDC_ROOT}/scripts/validate_luarocks.sh" --run
    "${root}/tree" "${root}/prefix" unused
    "${LOCKDC_ROOT}/tests/lua/test_lockdc_luarocks_smoke.lua"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result EQUAL 0 OR NOT error MATCHES "missing required path:.*lockdc/core[.]so")
    message(FATAL_ERROR "Missing installed module did not fail: ${output}\n${error}")
endif()
