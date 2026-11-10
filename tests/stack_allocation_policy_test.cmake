cmake_minimum_required(VERSION 3.24)

file(MAKE_DIRECTORY "${LOCKDC_WORK}")
file(WRITE "${LOCKDC_WORK}/fixed.c"
  "int fixed(int n) { char data[32]; data[0]=(char)n; return data[0]; }\n")
file(WRITE "${LOCKDC_WORK}/vla.c"
  "int variable(int n) { char data[n]; data[0]=1; return data[n-1]; }\n")
file(WRITE "${LOCKDC_WORK}/alloca.c"
  "int dynamic(int n) { char *data=__builtin_alloca(n); data[0]=1; return data[n-1]; }\n")
foreach(scope CORE TEST)
  set(options ${LOCKDC_${scope}_OPTIONS})
  foreach(required -Wvla -Walloca -Werror)
    if(NOT required IN_LIST options)
      message(FATAL_ERROR "${scope} target lacks ${required}")
    endif()
  endforeach()
  foreach(kind fixed vla alloca)
    execute_process(COMMAND "${LOCKDC_COMPILER}" "--sysroot=${LOCKDC_SYSROOT}"
      ${options} -c "${LOCKDC_WORK}/${kind}.c" -o "${LOCKDC_WORK}/${kind}.o"
      RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE diagnostic)
    if(kind STREQUAL "fixed")
      if(NOT rc EQUAL 0)
        message(FATAL_ERROR "${scope} rejected fixed scratch: ${diagnostic}")
      endif()
    elseif(rc EQUAL 0 OR NOT diagnostic MATCHES "${kind}|variable length")
      message(FATAL_ERROR "${scope} did not reject ${kind}: ${diagnostic}")
    endif()
  endforeach()
endforeach()
