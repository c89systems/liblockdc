if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

find_program(LOCKDC_CTEST_BIN NAMES ctest REQUIRED)
execute_process(
    COMMAND "${LOCKDC_CTEST_BIN}" --test-dir "${LOCKDC_BINARY_DIR}" --show-only=json-v1
    RESULT_VARIABLE ctest_result
    OUTPUT_VARIABLE ctest_json
    ERROR_VARIABLE ctest_error)
if(NOT ctest_result EQUAL 0)
    message(FATAL_ERROR "could not inspect CTest timeout properties:\n${ctest_error}")
endif()

string(JSON test_count LENGTH "${ctest_json}" tests)
if(test_count EQUAL 0)
    message(FATAL_ERROR "no CTest registrations found")
endif()

# CTest emits one TIMEOUT property per test. Count the property names in
# one pass, then parse individual registrations only for failure diagnostics.
string(REGEX MATCHALL "\"name\"[ \t\r\n]*:[ \t\r\n]*\"TIMEOUT\"" timeout_properties "${ctest_json}")
list(LENGTH timeout_properties timeout_count)
if(timeout_count EQUAL test_count)
    return()
endif()

math(EXPR last_test_index "${test_count} - 1")
foreach(test_index RANGE 0 ${last_test_index})
    string(JSON test_json GET "${ctest_json}" tests ${test_index})
    string(JSON test_name GET "${test_json}" name)
    string(JSON property_count LENGTH "${test_json}" properties)
    set(has_timeout FALSE)
    if(property_count GREATER 0)
        math(EXPR last_property_index "${property_count} - 1")
        foreach(property_index RANGE 0 ${last_property_index})
            string(JSON property_name GET "${test_json}"
                properties ${property_index} name)
            if(property_name STREQUAL "TIMEOUT")
                set(has_timeout TRUE)
                break()
            endif()
        endforeach()
    endif()
    if(NOT has_timeout)
        message(FATAL_ERROR "CTest registration has no explicit TIMEOUT: ${test_name}")
    endif()
endforeach()
