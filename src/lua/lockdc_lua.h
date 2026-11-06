#ifndef LOCKDC_LUA_H
#define LOCKDC_LUA_H

#include <lc/lc.h>
#ifdef __cplusplus
extern "C" {
#endif
#include <lua.h>
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(default)
#endif

/** Public facade interop, compiled/linked with the Lua binding. It introduces
 * no Lua or native-client dependency into the core C library. */
int luaopen_lockdc_core(lua_State *L);
/** Reads a live client userdata without transferring ownership. Its Lua owner
 * must stay reachable until the consuming public C operation has retained it.
 */
int lc_lua_client_view(lua_State *L, int index, lc_client **out,
                       lc_error *error);

/** Build copied provider configuration from a Lua value. Return project status
 * for validation failures. Set *out before any later Lua call that can throw;
 * cleanup runs on both returned errors and protected Lua exceptions. Native
 * open copies configuration before this temporary is released. */
typedef int (*lc_lua_xa_configure_fn)(lua_State *L, int index, void **out,
                                      lc_error *error);
/** Releases temporary provider configuration after native open or a protected
 * configuration failure. It does not close the copied native resource. */
typedef void (*lc_lua_xa_config_cleanup_fn)(void *configuration);
/** Explicitly registers linked callbacks for coordinator:register_start(name,
 * configuration) and journal:resource(name, configuration). Copies the table
 * into this Lua state's registry; callbacks remain linked until every Lua
 * state and retained C handle using them has closed. No loader/discovery runs.
 * Replace a name to affect future registrations only. */
int lc_lua_xa_register_driver(lua_State *L, const char *name,
                              const lc_xa_driver *driver,
                              lc_lua_xa_configure_fn configure,
                              lc_lua_xa_config_cleanup_fn cleanup,
                              lc_error *error);
/** Retains a checked generic connection token for a query adapter. Release it
 * with lc_xa_borrow_release. Before each native access call lc_xa_borrow_view;
 * neither this conversion nor Lua close transfers native connection ownership.
 * Accepts either core borrow userdata or its public Lua facade wrapper. */
int lc_lua_xa_borrow_retain(lua_State *L, int index, lc_xa_borrow **out,
                            lc_error *error);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif
#ifdef __cplusplus
}
#endif
#endif
