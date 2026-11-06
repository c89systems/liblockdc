if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()

file(GLOB generated_rockspec "${LOCKDC_BINARY_DIR}/lockdc-*-1.rockspec")
list(LENGTH generated_rockspec generated_rockspec_count)
if(NOT generated_rockspec_count EQUAL 1)
    message(FATAL_ERROR "expected one generated lockdc rockspec in ${LOCKDC_BINARY_DIR}")
endif()
list(GET generated_rockspec 0 generated_rockspec_path)

foreach(required_path
    "${LOCKDC_ROOT}/lua/lockdc/init.lua"
    "${LOCKDC_ROOT}/src/lua/lockdc_lua.c"
    "${LOCKDC_ROOT}/src/lua/lockdc_collections_lua.c"
    "${LOCKDC_ROOT}/src/lc_api_internal.h"
    "${LOCKDC_ROOT}/src/lc_engine_api.h"
    "${LOCKDC_ROOT}/src/lc_pouch.h"
    "${LOCKDC_ROOT}/scripts/build_lua_rock.sh"
    "${LOCKDC_ROOT}/lockdc.rockspec.in"
    "${generated_rockspec_path}"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "missing Lua binding artifact: ${required_path}")
    endif()
endforeach()

file(READ "${generated_rockspec_path}" rockspec_text)
file(READ "${LOCKDC_ROOT}/scripts/validate_luarocks.sh" validate_luarocks_script)
file(READ "${LOCKDC_ROOT}/scripts/build_lua_rock.sh" build_luarock_script)
foreach(required_snippet
    "package = \"lockdc\""
    "\"lonejson == 0.46.0-1\""
    "url = \"git+https://github.com/c89systems/liblockdc.git\""
    "tag = \"v"
    "scripts/build_lua_rock.sh"
    [==[\"$(LUA_INCDIR)\"]==]
    "\\\"${LOCKDC_VERSION}\\\""
    "[\"lockdc.init\"] = \"lua/lockdc/init.lua\""
    "[\"lockdc.core\"] = \".luarocks-build/lockdc/core.so\""
)
    string(FIND "${rockspec_text}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "generated rockspec is missing expected snippet '${required_snippet}'\n"
            "rockspec:\n${rockspec_text}")
    endif()
endforeach()

foreach(required_snippet
    "set -euo pipefail"
    "pkg-config --libs lockdc"
    "pkg-config --cflags lockdc"
    "LOCKDC_CFLAGS_EXTRA"
    "LOCKDC_LIBS_EXTRA")
    string(FIND "${build_luarock_script}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua rock builder is missing expected public-SDK linkage snippet '${required_snippet}'")
    endif()
endforeach()

foreach(forbidden_snippet
    "PSLOG_IMPLEMENTATION"
    "pslog-${LOCKDC_PSLOG_VERSION}.h"
    "pslog.c"
    "pslog.o")
    string(FIND "${build_luarock_script}" "${forbidden_snippet}" snippet_index)
    if(NOT snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua rock builder embeds pslog implementation instead of using the public SDK ABI.\n"
            "Unexpected snippet: ${forbidden_snippet}")
    endif()
endforeach()

foreach(required_snippet
    "set -euo pipefail"
    "https://github.com/c89systems/lonejson/releases/download/v0.46.0/lonejson-0.46.0-1.src.rock"
    "export LONEJSON_LIBDIR"
    "unset LD_LIBRARY_PATH"
    "unset LD_PRELOAD"
)
    string(FIND "${validate_luarocks_script}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "LuaRocks validation script is missing expected snippet '${required_snippet}'")
    endif()
endforeach()

foreach(stale_snippet
    "lonejson == 0.41.0-1"
    "https://github.com/c89systems/lonejson/releases/download/v0.41.0/lonejson-0.41.0-1.src.rock"
    "https://github.com/c89systems/lonejson/releases/download/v0.16.0/lonejson-0.16.0-1.src.rock"
)
    string(FIND "${rockspec_text}" "${stale_snippet}" rockspec_stale_index)
    string(FIND "${validate_luarocks_script}" "${stale_snippet}" script_stale_index)
    if(NOT rockspec_stale_index EQUAL -1 OR NOT script_stale_index EQUAL -1)
        message(FATAL_ERROR
            "Lua rock dependency boundary contains stale snippet '${stale_snippet}'")
    endif()
endforeach()
