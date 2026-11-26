local lockdc = require("lockdc")

local root = os.getenv("LOCKDC_POUCH_ROOT") or "/var/lib/lockdc-lua"
local namespace = os.getenv("LOCKDC_NAMESPACE") or "default"
local key = os.getenv("LOCKDC_KEY") or "examples/lua/pouch-local-storage"
local owner = os.getenv("LOCKDC_OWNER") or "lua-pouch-example"
local key_file = os.getenv("LOCKDC_POUCH_KEY_FILE") or (root .. ".key")
local compression = os.getenv("LOCKDC_POUCH_COMPRESSION") or "zlib"

if root:sub(1, 1) ~= "/" then
  error("LOCKDC_POUCH_ROOT must be an absolute filesystem path")
end

local client, err = lockdc.open({
  endpoints = { "pouch://" .. root },
  default_namespace = namespace,
  pouch = {
    crypto_key_file = key_file,
    crypto_generate_key_file = true,
    compression = compression,
  },
})

if client == nil then
  error(("lockdc.open Pouch failed: %s"):format(err.message))
end

local lease, acquire_err = client:acquire({
  key = key,
  owner = owner,
  ttl_seconds = 60,
})
if lease == nil then
  client:close()
  error(("client:acquire failed: %s"):format(acquire_err.message))
end

local updated, update_err = lease:update_json({
  storage = "pouch",
  example = "pouch_local_storage.lua",
})
if updated == nil then
  lease:close()
  client:close()
  error(("lease:update_json failed: %s"):format(update_err.message))
end
assert(lease:release())

local state, state_meta_or_err = client:read_json({ key = key })
if state == nil then
  client:close()
  error(("client:read_json failed: %s"):format(state_meta_or_err.message))
end
print(lockdc.encode_json(state))
client:close()
