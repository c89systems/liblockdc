local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local endpoint = "pouch://" .. root .. "/store"
local key_file = root .. "/pouch.key"
local explicit_false_key_file = root .. "/explicit-false/pouch.key"
local namespace = "lua-pouch"

local typed_invalid_client, typed_invalid_err = lockdc.open({
  endpoints = { endpoint .. "-typed-invalid" },
  pouch = { compression = "invalid" },
})
if typed_invalid_client ~= nil then
  typed_invalid_client:close()
  error("invalid typed Pouch compression unexpectedly opened")
end
if type(typed_invalid_err) ~= "table" or not (typed_invalid_err.message or ""):match("compression") then
  error("invalid typed Pouch compression did not return a structured error")
end

local remote_settings_client, remote_settings_err = lockdc.open({
  endpoints = { "https://lockd.invalid" },
  pouch = { durable_sync = true },
})
if remote_settings_client ~= nil then
  remote_settings_client:close()
  error("remote client unexpectedly accepted typed Pouch settings")
end
if type(remote_settings_err) ~= "table" or
    not (remote_settings_err.message or ""):match("require exactly one pouch endpoint") then
  error("remote typed Pouch settings did not return a structured local-only error")
end

local unknown_pouch_client, unknown_pouch_err = lockdc.open({
  endpoints = { endpoint .. "-typed-unknown" },
  pouch = { unexpected = true },
})
if unknown_pouch_client ~= nil then
  unknown_pouch_client:close()
  error("unknown typed Pouch setting unexpectedly opened")
end
if type(unknown_pouch_err) ~= "table" or
    not (unknown_pouch_err.message or ""):match("unknown pouch setting") then
  error("unknown typed Pouch setting did not return a structured error")
end

local wrong_type_client, wrong_type_err = lockdc.open({
  endpoints = { endpoint .. "-typed-wrong-type" },
  pouch = { query_indexing = "false" },
})
if wrong_type_client ~= nil then
  wrong_type_client:close()
  error("wrong typed Pouch value unexpectedly opened")
end
if type(wrong_type_err) ~= "table" or
    not (wrong_type_err.message or ""):match("must be a boolean") then
  error("wrong typed Pouch value did not return a structured error")
end

local typed_root = root .. "/typed"
local typed_client, typed_err = lockdc.open({
  endpoints = { "pouch://" .. typed_root },
  pouch = {
    single_writer = false,
    durable_sync = false,
    fsync_batch_max_ops = 0,
    segment_target_bytes = 4096,
    indexer_flush_docs = 64,
    indexer_flush_interval_seconds = 1,
    background_compaction = false,
    disable_compaction_throttling = true,
    terminal_reclaim_min_bytes = 8192,
    queue_watch = false,
    query_engine = "scan",
    query_fallback_engine = "index",
    query_indexing = false,
    compression = "none",
    disable_migration = true,
  },
})
if typed_client == nil then
  error(("typed Pouch open failed: %s"):format(typed_err and typed_err.message or tostring(typed_err)))
end
typed_client:close()

local invalid_client, invalid_err = lockdc.open({
  endpoints = { endpoint },
  pouch = { compression = "invalid" },
})
if invalid_client ~= nil then
  invalid_client:close()
  error("invalid Pouch compression unexpectedly opened")
end
if type(invalid_err) ~= "table" or not (invalid_err.message or ""):match("compression") then
  error("invalid Pouch compression did not return a structured error")
end

local disabled_generation_client, disabled_generation_err = lockdc.open({
  endpoints = { endpoint },
  pouch = {
    crypto_key_file = explicit_false_key_file,
    crypto_generate_key_file = false,
  },
})
if disabled_generation_client ~= nil or disabled_generation_err == nil then
  error("explicit Lua false did not prevent Pouch key-file generation")
end
local explicit_false_key = io.open(explicit_false_key_file, "rb")
if explicit_false_key ~= nil then
  explicit_false_key:close()
  error("Pouch key-file generation ignored explicit Lua false")
end

local legacy_open_ok, legacy_open_err = pcall(function()
  return lockdc.open({
    endpoints = { endpoint },
    pouch_compression = "zlib",
  })
end)
if legacy_open_ok or not tostring(legacy_open_err):find("pouch.compression", 1, true) then
  error("Lua lockdc.open accepted the removed Pouch compatibility field")
end

local legacy_endpoint, legacy_endpoint_err = lockdc.open({
  endpoints = { endpoint .. "?compression=zlib" },
})
if legacy_endpoint ~= nil or
    not tostring(legacy_endpoint_err and legacy_endpoint_err.message):find("Pouch endpoint options", 1, true) then
  error("Lua lockdc.open accepted Pouch endpoint options")
end

local client, err = lockdc.open({
  endpoints = { endpoint },
  default_namespace = namespace,
  pouch = {
    crypto_key_file = key_file,
    crypto_generate_key_file = true,
    compression = "zlib",
  },
})
if client == nil then
  error(("encrypted Pouch open failed: %s"):format(err and err.message or tostring(err)))
end

local second_client, second_err = lockdc.open({
  endpoints = { endpoint },
  pouch = { crypto_key_file = key_file, compression = "zlib" },
})
if second_client ~= nil then
  second_client:close()
  client:close()
  error("second exclusive Pouch writer unexpectedly opened")
end
if type(second_err) ~= "table" or
    (second_err.server_code ~= "pouch_root_busy" and not (second_err.message or ""):match("writer")) then
  client:close()
  error("second Pouch writer did not return a structured writer error")
end

local lease, acquire_err = client:acquire({
  key = "state",
  owner = "lua-pouch-e2e",
  ttl_seconds = 30,
})
if lease == nil then
  client:close()
  error(("Pouch acquire failed: %s"):format(acquire_err and acquire_err.message or tostring(acquire_err)))
end

local updated, update_err = lease:update_json({ source = "lua-pouch", value = 1 })
if updated == nil then
  lease:close()
  client:close()
  error(("Pouch state update failed: %s"):format(update_err and update_err.message or tostring(update_err)))
end
assert(lease:release())

local invalid_sink_ok = pcall(function()
  client:get({ key = "state" }, { write = false })
end)
if invalid_sink_ok then
  client:close()
  error("Lua client:get accepted an invalid streaming sink")
end
if type(client:info()) ~= "table" then
  client:close()
  error("Lua client remained marked streaming after invalid sink validation")
end

local state, state_meta_or_err = client:read_json({ key = "state" })
if state == nil or state.source ~= "lua-pouch" or state.value ~= 1 then
  client:close()
  error(("Pouch state read failed: %s"):format(state_meta_or_err and state_meta_or_err.message or tostring(state_meta_or_err)))
end

local component_lease, component_acquire_err = client:acquire({
  namespace = "lua-component",
  key = "state",
  owner = "lua-pouch-e2e",
  ttl_seconds = 30,
})
if component_lease == nil then
  client:close()
  error(("Pouch component acquire failed: %s"):format(component_acquire_err and component_acquire_err.message or tostring(component_acquire_err)))
end
local component_updated, component_update_err =
    component_lease:update_json({ source = "lua-component", value = 7 })
if component_updated == nil then
  component_lease:close()
  client:close()
  error(("Pouch component update failed: %s"):format(component_update_err and component_update_err.message or tostring(component_update_err)))
end
assert(component_lease:release())

local component_state, component_state_err = client:read_json({
  namespace = "lua-component",
  key = "state",
})
if component_state == nil or component_state.source ~= "lua-component" or
    component_state.value ~= 7 then
  client:close()
  error(("Pouch explicit namespace read failed: %s"):format(component_state_err and component_state_err.message or tostring(component_state_err)))
end
local default_component_state, default_component_meta = client:read({ key = "state" })
if default_component_state == nil or default_component_meta == nil then
  client:close()
  error("Pouch default namespace read unexpectedly failed")
end
if default_component_state:find("lua-component", 1, true) then
  client:close()
  error("Pouch explicit namespace read leaked into the default namespace")
end

local state_streamed_bytes = 0
local state_streamed_chunks = 0
local state_sink_closed = 0
local streamed_state, state_meta = client:get({ key = "state" }, {
  write = function(chunk)
    state_streamed_chunks = state_streamed_chunks + 1
    state_streamed_bytes = state_streamed_bytes + #chunk
    local closed, close_err = pcall(function() client:close() end)
    if closed or not tostring(close_err):find("not allowed while output is streaming", 1, true) then
      error("Lua client callback closed its active streaming receiver")
    end
  end,
  close = function()
    state_sink_closed = state_sink_closed + 1
  end,
})
if streamed_state ~= nil or type(state_meta) ~= "table" or
    state_streamed_chunks == 0 or state_streamed_bytes == 0 or
    state_sink_closed ~= 1 then
  client:close()
  error("Pouch client:get did not stream to the Lua callback sink")
end

local enqueued, enqueue_err = client:enqueue({
  queue = "work",
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, lockdc.encode_json({ source = "lua-pouch" }))
if enqueued == nil then
  client:close()
  error(("Pouch enqueue failed: %s"):format(enqueue_err and enqueue_err.message or tostring(enqueue_err)))
end

-- Numeric-looking Lua strings are payload bytes, never file descriptors.
local numeric_enqueued, numeric_enqueue_err = client:enqueue({
  queue = "numeric-payload",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, "123")
if numeric_enqueued == nil then
  client:close()
  error(("Pouch numeric payload enqueue failed: %s"):format(
    numeric_enqueue_err and numeric_enqueue_err.message or tostring(numeric_enqueue_err)))
end
local numeric_message, numeric_dequeue_err = client:dequeue({
  queue = "numeric-payload",
  owner = "lua-pouch-numeric-payload",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
})
if numeric_message == nil then
  client:close()
  error(("Pouch numeric payload dequeue failed: %s"):format(
    numeric_dequeue_err and numeric_dequeue_err.message or tostring(numeric_dequeue_err)))
end
local numeric_sink_path = "9037712049"
os.remove(numeric_sink_path)
local numeric_sink_result, numeric_written_or_err = numeric_message:write_payload(numeric_sink_path)
if numeric_sink_result ~= nil or type(numeric_written_or_err) ~= "number" then
  numeric_message:close()
  client:close()
  error("Pouch numeric Lua string sink did not write its payload")
end
local numeric_sink_file = assert(io.open(numeric_sink_path, "rb"))
local numeric_payload = assert(numeric_sink_file:read("*a"))
numeric_sink_file:close()
os.remove(numeric_sink_path)
if numeric_payload ~= "123" then
  numeric_message:close()
  client:close()
  error("Pouch numeric Lua string payload did not round-trip as bytes")
end
assert(numeric_message:ack())

local message, dequeue_err = client:dequeue({
  queue = "work",
  owner = "lua-pouch-e2e",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
})
if message == nil then
  client:close()
  error(("Pouch dequeue failed: %s"):format(dequeue_err and dequeue_err.message or tostring(dequeue_err)))
end
local payload, written_or_err = message:read_payload_json()
if payload == nil or payload.source ~= "lua-pouch" or type(written_or_err) ~= "number" then
  message:close()
  client:close()
  error("Pouch queue payload did not round-trip through the Lua facade")
end
assert(message:ack())

local subscribed, subscribe_err = client:enqueue({
  queue = "direct-subscribe",
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, lockdc.encode_json({ source = "lua-direct-subscribe" }))
if subscribed == nil then
  client:close()
  error(("direct subscription enqueue failed: %s"):format(
    subscribe_err and subscribe_err.message or tostring(subscribe_err)))
end
local subscription_calls = 0
local retained_delivery
local subscription_ok, subscription_err = client:subscribe({
  queue = "direct-subscribe",
  owner = "lua-direct-subscribe",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
}, function(delivery)
  subscription_calls = subscription_calls + 1
  retained_delivery = delivery
  assert(type(client:info()) == "table")
  local payload = assert(delivery:read_payload_json())
  assert(payload.source == "lua-direct-subscribe")
  assert(delivery:is_open())
  local closed, close_err = pcall(function()
    client:close()
  end)
  assert(not closed and tostring(close_err):find(
    "cannot close during a native callback", 1, true))
  assert(delivery:ack())
  return false, "stop direct subscription after its asserted delivery"
end)
if subscription_ok ~= nil or subscription_calls ~= 1 or
    type(subscription_err) ~= "table" or
    not tostring(subscription_err.message):find("stop direct subscription", 1, true) then
  client:close()
  error(("Lua direct subscription failed (ok=%s calls=%d error=%s)"):format(
    tostring(subscription_ok), subscription_calls,
    tostring(subscription_err and subscription_err.message)))
end
local retained_ok = pcall(function()
  retained_delivery:ack()
end)
if retained_ok or retained_delivery:is_open() then
  client:close()
  error("Lua retained a direct-subscription delivery beyond its native callback")
end

-- A subscribe request is only borrowed by native code.  It must nevertheless
-- remain valid when its own handler drops the request and collects between
-- deliveries from the same page.
do
  local rooted_subscription_request = {
    queue = "lua-rooted-subscribe-" .. assert(lockdc.xid_new()),
    owner = "lua-rooted-owner-" .. assert(lockdc.xid_new()),
    visibility_timeout_seconds = 30,
    wait_seconds = 0,
    page_size = 2,
  }
  local rooted_queue = rooted_subscription_request.queue
  for index = 1, 2 do
    local rooted_enqueued, rooted_enqueue_err = client:enqueue({
      queue = rooted_queue,
      content_type = "application/json",
      visibility_timeout_seconds = 30,
      ttl_seconds = 30,
    }, lockdc.encode_json({ source = "lua-rooted-subscribe", index = index }))
    if rooted_enqueued == nil then
      client:close()
      error(("Lua rooted subscription enqueue failed: %s"):format(
        rooted_enqueue_err and rooted_enqueue_err.message or
        tostring(rooted_enqueue_err)))
    end
  end
  rooted_queue = nil
  local rooted_subscription_calls = 0
  local rooted_subscription_ok, rooted_subscription_err = client:subscribe(
    rooted_subscription_request, function(delivery)
      rooted_subscription_calls = rooted_subscription_calls + 1
      if rooted_subscription_calls == 1 then
        rooted_subscription_request.queue = nil
        rooted_subscription_request.owner = nil
        collectgarbage("collect")
        collectgarbage("collect")
      end
      assert(delivery:ack())
      return true
    end)
  if rooted_subscription_ok ~= true or rooted_subscription_calls ~= 2 or
      rooted_subscription_err ~= nil then
    client:close()
    error(("Lua rooted subscription did not process its page safely " ..
      "(ok=%s calls=%d error=%s)"):format(tostring(rooted_subscription_ok),
      rooted_subscription_calls, tostring(rooted_subscription_err)))
  end
end

local subscribed_state, subscribe_state_err = client:enqueue({
  queue = "direct-subscribe-state",
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, lockdc.encode_json({ source = "lua-direct-subscribe-state" }))
if subscribed_state == nil then
  client:close()
  error(("direct state subscription enqueue failed: %s"):format(
    subscribe_state_err and subscribe_state_err.message or tostring(subscribe_state_err)))
end
local state_subscription_calls = 0
local retained_state
local state_subscription_ok, state_subscription_err = client:subscribe_with_state({
  queue = "direct-subscribe-state",
  owner = "lua-direct-subscribe-state",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
}, function(delivery, state)
  state_subscription_calls = state_subscription_calls + 1
  retained_state = state
  assert(type(state:info()) == "table")
  local state_release, state_release_err = state:release()
  if state_release ~= nil or type(state_release_err) ~= "table" or
      not tostring(state_release_err.message):find("owned by its delivery", 1, true) then
    error("callback state lease was allowed to release its delivery-owned lease")
  end
  assert(delivery:ack())
  local state_after_ack_ok = pcall(function()
    state:info()
  end)
  if state_after_ack_ok then
    error("callback state lease remained usable after its delivery was acknowledged")
  end
  return false, "stop direct state subscription after its asserted delivery"
end)
if state_subscription_ok ~= nil or state_subscription_calls ~= 1 or
    type(state_subscription_err) ~= "table" or
    not tostring(state_subscription_err.message):find("stop direct state subscription", 1, true) then
  client:close()
  error(("Lua direct state subscription failed (ok=%s calls=%d error=%s)"):format(
    tostring(state_subscription_ok), state_subscription_calls,
    tostring(state_subscription_err and state_subscription_err.message)))
end
local retained_state_ok = pcall(function()
  retained_state:info()
end)
if retained_state_ok then
  client:close()
  error("Lua retained a direct-subscription state lease beyond its native callback")
end

local subscribed_nack_state, subscribe_nack_state_err = client:enqueue({
  queue = "direct-subscribe-state-nack",
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, lockdc.encode_json({ source = "lua-direct-subscribe-state-nack" }))
if subscribed_nack_state == nil then
  client:close()
  error(("direct state subscription nack enqueue failed: %s"):format(
    subscribe_nack_state_err and subscribe_nack_state_err.message or tostring(subscribe_nack_state_err)))
end
local nack_state_ok, nack_state_err = client:subscribe_with_state({
  queue = "direct-subscribe-state-nack",
  owner = "lua-direct-subscribe-state-nack",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
}, function(delivery, state)
  assert(type(state:info()) == "table")
  assert(delivery:nack({ intent = "defer", delay_seconds = 1 }))
  local state_after_nack_ok = pcall(function()
    state:info()
  end)
  if state_after_nack_ok then
    error("callback state lease remained usable after its delivery was negatively acknowledged")
  end
  return false, "stop direct state subscription after its asserted nack"
end)
if nack_state_ok ~= nil or type(nack_state_err) ~= "table" or
    not tostring(nack_state_err.message):find("stop direct state subscription", 1, true) then
  client:close()
  error(("Lua direct state nack subscription failed (ok=%s error=%s)"):format(
    tostring(nack_state_ok), tostring(nack_state_err and nack_state_err.message)))
end

local nested_subscribe_message, nested_subscribe_enqueue_err = client:enqueue({
  queue = "direct-subscribe-nested-guard",
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, lockdc.encode_json({ source = "lua-direct-subscribe-nested-guard" }))
if nested_subscribe_message == nil then
  client:close()
  error(("nested subscribe enqueue failed: %s"):format(
    nested_subscribe_enqueue_err and nested_subscribe_enqueue_err.message or tostring(nested_subscribe_enqueue_err)))
end
local nested_guard_ok, nested_guard_err = client:subscribe({
  queue = "direct-subscribe-nested-guard",
  owner = "lua-direct-subscribe-nested-guard",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
}, function(delivery)
  local nested_ok, nested_err = client:subscribe({
    queue = "",
    owner = "lua-direct-subscribe-nested-invalid",
    wait_seconds = 0,
  }, function() end)
  if nested_ok ~= nil or type(nested_err) ~= "table" then
    error("nested invalid subscription did not report its validation failure")
  end
  local closed, close_err = pcall(function()
    client:close()
  end)
  if closed or not tostring(close_err):find("cannot close during a native callback", 1, true) then
    error("nested subscription cleared the outer callback close guard")
  end
  assert(delivery:ack())
  return false, "stop direct nested guard subscription after its asserted delivery"
end)
if nested_guard_ok ~= nil or type(nested_guard_err) ~= "table" or
    not tostring(nested_guard_err.message):find("stop direct nested guard subscription", 1, true) then
  client:close()
  error(("Lua nested guard subscription failed (ok=%s error=%s)"):format(
    tostring(nested_guard_ok), tostring(nested_guard_err and nested_guard_err.message)))
end

local watch_stop_ok, watch_stop_err = client:watch_queue({
  queue = "direct-watch-clean-stop",
}, function()
  return false
end)
if watch_stop_ok ~= true or watch_stop_err ~= nil then
  client:close()
  error(("Lua queue watch did not treat an intentional stop as success (ok=%s error=%s)"):format(
    tostring(watch_stop_ok), tostring(watch_stop_err and watch_stop_err.message)))
end

-- A watch callback may discard its own dynamically allocated request fields.
-- The binding retains private values until the native watch returns.
do
  local rooted_watch_request = {
    namespace = "lua-rooted-watch-" .. assert(lockdc.xid_new()),
    queue = "queue-" .. assert(lockdc.xid_new()),
  }
  local rooted_watch_calls = 0
  local rooted_watch_ok, rooted_watch_err = client:watch_queue(
    rooted_watch_request, function()
      rooted_watch_calls = rooted_watch_calls + 1
      rooted_watch_request.namespace = nil
      rooted_watch_request.queue = nil
      collectgarbage("collect")
      collectgarbage("collect")
      return false
    end)
  if rooted_watch_ok ~= true or rooted_watch_err ~= nil or
      rooted_watch_calls ~= 1 then
    client:close()
    error(("Lua rooted queue watch did not stop safely (ok=%s calls=%d " ..
      "error=%s)"):format(tostring(rooted_watch_ok), rooted_watch_calls,
      tostring(rooted_watch_err)))
  end
end

local watch_failure_ok, watch_failure_err = client:watch_queue({
  queue = "direct-watch-handler-failure",
}, function()
  return false, "intentional Lua queue watch failure"
end)
if watch_failure_ok ~= nil or type(watch_failure_err) ~= "table" or
    not tostring(watch_failure_err.message):find("intentional Lua queue watch failure", 1, true) then
  client:close()
  error(("Lua queue watch discarded handler failure (ok=%s error=%s)"):format(
    tostring(watch_failure_ok), tostring(watch_failure_err and watch_failure_err.message)))
end

-- Callback error records are application-owned values. Their field lookup
-- must not be able to bypass native cleanup through an __index exception.
local callback_error_record = setmetatable({}, {
  __index = function()
    error("callback error record must be read without invoking __index")
  end,
})
local callback_error_message, callback_error_enqueue_err = client:enqueue({
  queue = "direct-subscribe-error-record",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, "callback-error-record")
if callback_error_message == nil then
  client:close()
  error(("Lua callback error-record enqueue failed: %s"):format(
    callback_error_enqueue_err and callback_error_enqueue_err.message or tostring(callback_error_enqueue_err)))
end
local callback_error_subscribe_ok, callback_error_subscribe_err = client:subscribe({
  queue = "direct-subscribe-error-record",
  owner = "lua-direct-subscribe-error-record",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
}, function()
  return nil, callback_error_record
end)
if callback_error_subscribe_ok ~= nil or type(callback_error_subscribe_err) ~= "table" or
    not tostring(callback_error_subscribe_err.message):find("Lua subscribe handler failed", 1, true) then
  client:close()
  error("Lua subscription did not safely decode a callback error record")
end
local callback_error_watch_ok, callback_error_watch_err = client:watch_queue({
  queue = "direct-watch-error-record",
}, function()
  return nil, callback_error_record
end)
if callback_error_watch_ok ~= nil or type(callback_error_watch_err) ~= "table" or
    not tostring(callback_error_watch_err.message):find("Lua queue watch failed", 1, true) then
  client:close()
  error("Lua queue watch did not safely decode a callback error record")
end

local function assert_ok(operation, value, value_err)
  if value == nil then
    error(("%s failed: %s"):format(operation,
      value_err and value_err.message or tostring(value_err)))
  end
  return value
end

-- Transaction participants are parsed before any C-owned participant array is
-- allocated.  This exercises Lua's longjmp argument-error path in the real
-- binding rather than merely checking a returned lockdc error.
local malformed_txn_ok, malformed_txn_err = pcall(function()
  client:txn_commit({
    txn_id = assert(lockdc.xid_new()),
    participants = {
      "not a participant table",
    },
  })
end)
if malformed_txn_ok or not tostring(malformed_txn_err):match("table") then
  client:close()
  error("Lua XA participant validation did not reject a non-table participant")
end
local malformed_term_ok, malformed_term_err = pcall(function()
  client:txn_commit({
    txn_id = assert(lockdc.xid_new()),
    participants = {
      { namespace = namespace, key = "invalid-term" },
    },
    tc_term = -1,
  })
end)
if malformed_term_ok or not tostring(malformed_term_err):match("tc_term") then
  client:close()
  error("Lua XA validation did not reject a negative coordinator term")
end

-- Metamethod-backed XA request strings must stay rooted while later fields are
-- resolved. The transaction is intentionally unknown; this asserts a normal
-- structured decision error instead of a stale string dereference.
do
  local rooted_request_result, rooted_request_err = client:txn_commit(
    setmetatable({}, {
      __index = function(_, field)
        if field == "txn_id" then
          return assert(lockdc.xid_new())
        end
        if field == "tc_term" then
          collectgarbage("collect")
          collectgarbage("collect")
          return 1
        end
        if field == "participants" then
          return {}
        end
        return nil
      end,
    }))
  if rooted_request_result == nil and type(rooted_request_err) ~= "table" then
    client:close()
    error("Lua XA request parsing did not retain metamethod-backed strings")
  end
end

local raw_txn_id = assert(lockdc.xid_new())
local raw_txn_participant = {
  namespace = namespace,
  key = "xa-state",
}
local raw_txn_lease = assert_ok("Lua raw XA acquire", client:acquire({
  key = raw_txn_participant.key,
  owner = "lua-pouch-xa",
  ttl_seconds = 30,
  txn_id = raw_txn_id,
}))
assert_ok("Lua raw XA stage",
          raw_txn_lease:update_json({ source = "lua-pouch-xa", committed = true }))
local prepared = assert_ok("Lua raw XA prepare", client:txn_prepare({
  txn_id = raw_txn_id,
  participants = { raw_txn_participant },
  expires_at_unix = 2147483648,
  tc_term = 1,
}))
if prepared.state ~= "prepare" then
  raw_txn_lease:close()
  client:close()
  error("Lua raw XA prepare did not return prepare state")
end
local committed = assert_ok("Lua raw XA commit", client:txn_commit({
  txn_id = raw_txn_id,
  participants = { raw_txn_participant },
  tc_term = 1,
}))
raw_txn_lease:close()
if committed.state ~= "commit" then
  client:close()
  error("Lua raw XA commit did not return commit state")
end
local replayed = assert_ok("Lua raw XA replay",
                           client:txn_replay({ txn_id = raw_txn_id }))
if replayed.state ~= "commit" then
  client:close()
  error("Lua raw XA replay did not preserve commit state")
end
local xa_state, xa_state_err = client:read_json({ key = raw_txn_participant.key })
if xa_state == nil or not xa_state.committed then
  client:close()
  error(("Lua raw XA state was not committed: %s"):format(
    xa_state_err and xa_state_err.message or tostring(xa_state_err)))
end
local query_keys = {}
local current_key
local query_result = assert_ok("Lua query_keys", client:query_keys({
  namespace = namespace,
  selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
  engine = "scan",
}, {
  begin = function()
    current_key = ""
  end,
  chunk = function(bytes)
    current_key = current_key .. bytes
  end,
  finish = function()
    table.insert(query_keys, current_key)
  end,
}))
if #query_keys ~= 1 or query_keys[1] ~= raw_txn_participant.key or
    query_result.return_mode ~= "keys" then
  client:close()
  error("Lua query_keys did not stream the matching Pouch key")
end

-- Parsing a later metatable field may collect a dynamically generated query
-- selector. The native query keeps its private normalized request until done.
do
  local rooted_query_result, rooted_query_err = client:query_keys(
    setmetatable({ namespace = namespace, engine = "scan" }, {
      __index = function(_, key)
        if key == "selector_json" then
          return '{"eq":{"field":"/source","value":"' ..
              "not-present-" .. assert(lockdc.xid_new()) .. '"}}'
        end
        if key == "limit" then
          collectgarbage("collect")
          collectgarbage("collect")
          return 1
        end
        return nil
      end,
    }), function() end)
  rooted_query_result = assert_ok("Lua rooted query_keys",
                                  rooted_query_result, rooted_query_err)
  if rooted_query_result.return_mode ~= "keys" then
    client:close()
    error("Lua rooted query_keys did not return its key-stream metadata")
  end
end

do
  local rooted_query_body, rooted_query_meta = client:read_query(
    setmetatable({ namespace = namespace, engine = "scan" }, {
      __index = function(_, key)
        if key == "selector_json" then
          return '{"eq":{"field":"/source","value":"' ..
              "not-present-" .. assert(lockdc.xid_new()) .. '"}}'
        end
        if key == "limit" then
          collectgarbage("collect")
          collectgarbage("collect")
          return 1
        end
        return nil
      end,
    }))
  if type(rooted_query_body) ~= "string" or type(rooted_query_meta) ~= "table" then
    client:close()
    error(("Lua rooted query did not safely materialize an empty result " ..
      "(body=%s metadata=%s)"):format(tostring(rooted_query_body),
      tostring(rooted_query_meta and rooted_query_meta.message or
      rooted_query_meta)))
  end
end

local selector_free_keys = {}
local selector_free_cursor
repeat
  local page_keys = {}
  local current_page_key
  local selector_free_result = assert_ok("Lua selector-free query_keys",
                                         client:query_keys({
    namespace = namespace,
    engine = "scan",
    limit = 1,
    cursor = selector_free_cursor,
  }, {
    begin = function()
      current_page_key = ""
    end,
    chunk = function(bytes)
      current_page_key = current_page_key .. bytes
    end,
    finish = function()
      table.insert(page_keys, current_page_key)
    end,
  }))
  if #page_keys ~= 1 then
    client:close()
    error("Lua selector-free query_keys did not stream one paginated key")
  end
  table.insert(selector_free_keys, page_keys[1])
  selector_free_cursor = selector_free_result.cursor
until selector_free_cursor == nil
local found_raw_txn_key = false
for _, key in ipairs(selector_free_keys) do
  if key == raw_txn_participant.key then
    found_raw_txn_key = true
    break
  end
end
if #selector_free_keys < 2 or not found_raw_txn_key then
  client:close()
  error("Lua selector-free query_keys did not enumerate all Pouch keys")
end

local conflicting_history, conflicting_history_err = client:new_history_consumer({
  namespace = namespace,
  consumer_id = "lua-history-invalid",
  initial_acknowledged_index_seq = 0,
  start_at_current = true,
})
if conflicting_history ~= nil or type(conflicting_history_err) ~= "table" or
    not tostring(conflicting_history_err.message):match("cannot both be supplied") then
  client:close()
  error("Lua history consumer accepted conflicting initial positions")
end

-- The constructor parses numerical settings after its strings.  A metatable
-- may collect a dynamically generated namespace during the next field lookup.
do
  local rooted_history, rooted_history_err = client:new_history_consumer(
    setmetatable({}, {
      __index = function(_, key)
        if key == "namespace" then
          return "lua-rooted-history-" .. assert(lockdc.xid_new())
        end
        if key == "consumer_id" then
          collectgarbage("collect")
          collectgarbage("collect")
          return "consumer-" .. assert(lockdc.xid_new())
        end
        return nil
      end,
    }))
  rooted_history = assert_ok("Lua rooted history consumer create",
                             rooted_history, rooted_history_err)
  assert_ok("Lua rooted history consumer unregister",
            rooted_history:unregister())
  rooted_history:close()
end

local history_consumer_id = assert(lockdc.xid_new())
local history, history_err = client:new_history_consumer({
  namespace = namespace,
  consumer_id = history_consumer_id,
  initial_acknowledged_index_seq = 0,
})
history = assert_ok("Lua history consumer create", history, history_err)
local history_position = assert_ok("Lua history consumer position",
                                   history:position())
if history_position.acknowledged_index_seq ~= 0 or
    type(history_position.current_index_seq) ~= "number" or
    history_position.current_index_seq < 1 then
  history:close()
  client:close()
  error("Lua history consumer did not report its durable initial position")
end
local advanced_history_position = assert_ok("Lua history consumer advance",
                                            history:advance(
  history_position.current_index_seq))
if advanced_history_position.acknowledged_index_seq ~=
    advanced_history_position.current_index_seq then
  history:close()
  client:close()
  error("Lua history consumer did not persist its acknowledgement")
end
local backward_history, backward_history_err = history:advance(
  advanced_history_position.current_index_seq - 1)
if backward_history ~= nil or type(backward_history_err) ~= "table" then
  history:close()
  client:close()
  error("Lua history consumer accepted a backward acknowledgement")
end
history:close()

local reopened_history = assert_ok("Lua history consumer reopen",
                                   client:new_history_consumer({
  namespace = namespace,
  consumer_id = history_consumer_id,
  initial_acknowledged_index_seq = 0,
}))
local reopened_history_position = assert_ok("Lua history consumer persisted position",
                                            reopened_history:position())
if reopened_history_position.acknowledged_index_seq ~=
    advanced_history_position.acknowledged_index_seq then
  reopened_history:close()
  client:close()
  error("Lua history consumer did not retain its durable acknowledgement")
end
assert_ok("Lua history consumer unregister", reopened_history:unregister())
local unregistered_history, unregistered_history_err = reopened_history:position()
if unregistered_history ~= nil or type(unregistered_history_err) ~= "table" then
  reopened_history:close()
  client:close()
  error("Lua history consumer remained usable after unregister")
end
reopened_history:close()

local current_history = assert_ok("Lua history consumer start current",
                                  client:new_history_consumer({
  namespace = namespace,
  consumer_id = assert(lockdc.xid_new()),
  start_at_current = true,
}))
local current_history_position = assert_ok("Lua history consumer current position",
                                           current_history:position())
if current_history_position.acknowledged_index_seq ~=
    current_history_position.current_index_seq then
  current_history:close()
  client:close()
  error("Lua history consumer start_at_current did not select current sequence")
end
assert_ok("Lua history consumer current unregister", current_history:unregister())
current_history:close()
local callback_result, callback_err = client:query_keys({
  namespace = namespace,
  selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
  engine = "scan",
}, function()
  error("query key handler failure")
end)
if callback_result ~= nil or type(callback_err) ~= "table" or
    not (callback_err.message or ""):match("query key handler failure", 1, true) then
  client:close()
  error("Lua query_keys did not propagate handler failure")
end
local retained_callbacks = setmetatable({}, { __mode = "v" })
do
  local callback_payload = {}
  local function begin()
    return callback_payload
  end
  retained_callbacks[1] = callback_payload
  local invalid_callback_ok, invalid_callback_err = pcall(function()
    client:query_keys({
      namespace = namespace,
      selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
      engine = "scan",
    }, {
      begin = begin,
      chunk = false,
    })
  end)
  if invalid_callback_ok or not tostring(invalid_callback_err):match("function") then
    client:close()
    error("Lua query_keys did not reject an invalid callback")
  end
end
do
  local callback_payload = {}
  local function begin()
    return callback_payload
  end
  local function chunk()
    return callback_payload
  end
  retained_callbacks[2] = callback_payload
  local invalid_callback_ok, invalid_callback_err = pcall(function()
    client:query_keys({
      namespace = namespace,
      selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
      engine = "scan",
    }, {
      begin = begin,
      chunk = chunk,
      finish = false,
    })
  end)
  if invalid_callback_ok or not tostring(invalid_callback_err):match("function") then
    client:close()
    error("Lua query_keys did not reject an invalid finish callback")
  end
end
collectgarbage("collect")
collectgarbage("collect")
if retained_callbacks[1] ~= nil or retained_callbacks[2] ~= nil then
  client:close()
  error("Lua query_keys retained a callback after argument validation failed")
end

local rollback_txn_id = assert(lockdc.xid_new())
local rollback_participant = {
  namespace = namespace,
  key = "xa-rollback-state",
}
local rollback_lease = assert_ok("Lua raw XA rollback acquire", client:acquire({
  key = rollback_participant.key,
  owner = "lua-pouch-xa-rollback",
  ttl_seconds = 30,
  txn_id = rollback_txn_id,
}))
assert_ok("Lua raw XA rollback stage", rollback_lease:update_json({ rolled_back = true }))
local rolled_back = assert_ok("Lua raw XA rollback", client:txn_rollback({
  txn_id = rollback_txn_id,
  participants = { rollback_participant },
  tc_term = 1,
}))
rollback_lease:close()
if rolled_back.state ~= "rollback" then
  client:close()
  error("Lua raw XA rollback did not return rollback state")
end

local tc_lease = assert_ok("Lua TC lease acquire", client:tc_lease_acquire(
  setmetatable({ term = 1, ttl_ms = 60000 }, {
    __index = function(_, key)
      if key == "candidate_id" then
        return assert(lockdc.xid_new())
      end
      if key == "candidate_endpoint" then
        collectgarbage("collect")
        collectgarbage("collect")
        return "pouch://lua-pouch-node-" .. assert(lockdc.xid_new())
      end
      return nil
    end,
  })))
if not tc_lease.granted then
  client:close()
  error("Lua TC lease was not granted")
end
local tc_renewed = assert_ok("Lua TC lease renew", client:tc_lease_renew({
  leader_id = tc_lease.leader_id,
  term = tc_lease.term,
  ttl_ms = 60000,
}))
if not tc_renewed.renewed then
  client:close()
  error("Lua TC lease was not renewed")
end
local tc_leader = assert_ok("Lua TC leader", client:tc_leader())
if tc_leader.leader_id ~= tc_lease.leader_id then
  client:close()
  error("Lua TC leader did not return the current leader")
end
assert_ok("Lua TC lease release", client:tc_lease_release({
  leader_id = tc_lease.leader_id,
  term = tc_renewed.term,
}))

local announced = assert_ok("Lua TC cluster announce", client:tc_cluster_announce({
  self_endpoint = "pouch://lua-pouch-node",
}))
if #announced.endpoints ~= 1 or announced.endpoints[1] ~= "pouch://lua-pouch-node" then
  client:close()
  error("Lua TC cluster announce did not return its endpoint")
end
local listed_cluster = assert_ok("Lua TC cluster list", client:tc_cluster_list())
if #listed_cluster.endpoints ~= 1 then
  client:close()
  error("Lua TC cluster list did not preserve membership")
end
local left_cluster = assert_ok("Lua TC cluster leave", client:tc_cluster_leave())
if #left_cluster.endpoints ~= 0 then
  client:close()
  error("Lua TC cluster leave did not remove membership")
end

local registered = assert_ok("Lua TC RM register", client:tc_rm_register({
  backend_hash = "lua-pouch-backend",
  endpoint = "pouch://lua-pouch-rm",
}))
if #registered.endpoints ~= 1 then
  client:close()
  error("Lua TC RM register did not return its endpoint")
end
local registered_rms = assert_ok("Lua TC RM list", client:tc_rm_list())
if #registered_rms.backends ~= 1 or
    registered_rms.backends[1].backend_hash ~= "lua-pouch-backend" then
  client:close()
  error("Lua TC RM list did not return the registered backend")
end
local unregistered = assert_ok("Lua TC RM unregister", client:tc_rm_unregister({
  backend_hash = "lua-pouch-backend",
  endpoint = "pouch://lua-pouch-rm",
}))
if #unregistered.endpoints ~= 0 then
  client:close()
  error("Lua TC RM unregister did not remove its endpoint")
end

-- The installed facade links against the public SDK for ordered collections,
-- including raw payload delivery and both retention metadata targets.
do
  local journal = assert(client:create_collection({ namespace = namespace,
    name = "installed-journal", retention_seconds = 3600 }))
  local identity = journal:describe().collection_id
  local first = assert(journal:append({ append_id = "one", retention_seconds = 60 },
    '{ "collection_event" : 1 }'))
  assert(first.position == "1" and first.retention_support == "supported")
  assert(journal:entry_metadata("1", { retention_seconds = 0 }).etag == first.etag)
  assert(journal:metadata({ retention_seconds = 0 }).retention_expires_at_unix == 0)
  assert(journal:append({ append_id = "two" }, '{"collection_event":2}').position == "2")
  local bytes, positions = {}, {}
  local streamed, result, code = journal:read({ on_metadata = function(meta)
    if meta.kind == "entry" then
      positions[#positions + 1] = meta.position
      assert(table.concat(bytes):sub(-1) == "\n")
      assert(meta.retention_expires_at_unix == 0)
    end
  end }, { write = function(chunk) bytes[#bytes + 1] = chunk end })
  assert(streamed == nil and code == nil and result.complete)
  assert(result.completed_records == "2" and result.upper_position == "2")
  assert(table.concat(positions, ",") == "1,2")
  assert(table.concat(bytes) == '{"collection_event":1}\n{"collection_event":2}\n')
  local checkpoint = result.checkpoint_cursor
  assert(journal:append({ append_id = "three" }, '{"collection_event":3}').position == "3")
  bytes = {}
  streamed, result, code = journal:read({ cursor = checkpoint },
    { write = function(chunk) bytes[#bytes + 1] = chunk end })
  assert(streamed == nil and code == nil and result.completed_records == "1")
  assert(table.concat(bytes) == '{"collection_event":3}\n')
  bytes = {}
  streamed, result, code = journal:read({ selector_lql = "eq{field=/collection_event,value=2}" },
    { write = function(chunk) bytes[#bytes + 1] = chunk end })
  assert(streamed == nil and code == nil and result.completed_records == "1")
  assert(table.concat(bytes) == '{"collection_event":2}\n')
  assert(journal:delete_entry("1").deleted)
  local retry = assert(journal:append({ append_id = "one", retention_seconds = 600 },
    '{"collection_event":1}'))
  assert(retry.duplicate and retry.deleted and retry.position == "1")
  assert(journal:append_status("one").state == "deleted")
  local peer = assert(client:open_collection({ namespace = namespace, name = "installed-journal" }))
  assert(peer:describe().collection_id == identity)
  assert(journal:drop())
  local stale, stale_err, stale_code = peer:describe()
  assert(stale == nil and stale_err.server_code == "collection_stale" and stale_code ~= nil)
  peer:close()
  journal:close()
  journal = assert(client:create_collection({ namespace = namespace, name = "installed-journal" }))
  assert(journal:describe().collection_id ~= identity)
  assert(journal:describe().live_count == "0")
  assert(journal:drop())
  journal:close()
end

local flush, flush_err = client:flush_index({ namespace = namespace, mode = "wait" })
if flush == nil then
  client:close()
  error(("Pouch index flush failed: %s"):format(flush_err and flush_err.message or tostring(flush_err)))
end

-- Request tables may have metamethods.  Closing the client from one must
-- yield a normal API error, rather than dereferencing the released receiver.
local close_during_request_xid = assert(lockdc.xid_new())
local close_during_request = setmetatable({}, {
  __index = function(_, key)
    if key == "txn_id" then
      client:close()
      return close_during_request_xid
    end
    return nil
  end,
})
local close_during_request_result, close_during_request_err =
  client:txn_replay(close_during_request)
if close_during_request_result ~= nil or type(close_during_request_err) ~= "table" or
    not tostring(close_during_request_err.message):find("closed while preparing request", 1, true) then
  error("Lua transaction replay did not reject a client closed by request parsing")
end
client:close()

local watch_close_client, watch_close_open_err = lockdc.open({
  endpoints = { endpoint .. "-watch-close" },
})
if watch_close_client == nil then
  error(("Lua watch close regression client failed to open: %s"):format(
    watch_close_open_err and watch_close_open_err.message or tostring(watch_close_open_err)))
end
local close_during_watch_request = setmetatable({ queue = "watch-close" }, {
  __index = function(_, key)
    if key == "namespace" then
      watch_close_client:close()
      return namespace
    end
    return nil
  end,
})
local watch_close_result, watch_close_err = watch_close_client:watch_queue(
  close_during_watch_request, function() return false end)
if watch_close_result ~= nil or type(watch_close_err) ~= "table" or
    not tostring(watch_close_err.message):find("closed while preparing request", 1, true) then
  error("Lua queue watch did not reject a client closed by request parsing")
end
watch_close_client:close()

local missing_key_client, missing_key_err = lockdc.open({
  endpoints = { endpoint },
  pouch = { crypto_key_file = root .. "/missing.key", compression = "zlib" },
})
if missing_key_client ~= nil then
  missing_key_client:close()
  error("Pouch open with a missing configured key file unexpectedly succeeded")
end
if type(missing_key_err) ~= "table" or not (missing_key_err.message or ""):match("key file") then
  error("missing Pouch key file did not return a structured error")
end

local reopened, reopen_err = lockdc.open({
  endpoints = { endpoint },
  default_namespace = namespace,
  pouch = { crypto_key_file = key_file, compression = "zlib" },
})
if reopened == nil then
  error(("encrypted Pouch reopen failed: %s"):format(reopen_err and reopen_err.message or tostring(reopen_err)))
end
local reopened_state, reopened_meta_or_err = reopened:read_json({ key = "state" })
if reopened_state == nil or reopened_state.value ~= 1 then
  reopened:close()
  error(("encrypted Pouch persistence read failed: %s"):format(reopened_meta_or_err and reopened_meta_or_err.message or tostring(reopened_meta_or_err)))
end
reopened:close()
