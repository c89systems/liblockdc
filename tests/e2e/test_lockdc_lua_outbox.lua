local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
-- Keep auxiliary stores below the scratch root cleared before every test run.
local client, open_err = lockdc.open({
  endpoints = { "pouch://" .. root .. "/store" },
  default_namespace = "lua-outbox-domain",
  pouch = {
    single_writer = false,
    segment_target_bytes = 4096,
  },
})

if client == nil then
  error(("Pouch outbox client open failed: %s"):format(
    open_err and open_err.message or tostring(open_err)))
end

-- A settings table may synthesize strings through __index.  The native parser
-- must retain the selected key while a later lookup runs collection.
do
  local settings_root = root .. "/settings-gc"
  local generated_key = lockdc.pouch_crypto_generate_key()
  local settings = setmetatable({}, {
    __index = function(_, name)
      if name == "crypto_key" then
        return generated_key
      end
      if name == "crypto_key_file" then
        collectgarbage("collect")
      end
      return nil
    end,
  })
  local settings_client, settings_err = lockdc.open({
    endpoints = { "pouch://" .. settings_root },
    pouch = settings,
  })
  if settings_client == nil then
    client:close()
    error(("metatable-backed Pouch settings failed: %s"):format(
      settings_err and settings_err.message or tostring(settings_err)))
  end
  settings_client:close()
end

local oversized_attempts_ok = pcall(function()
  return client:new_outbox({
    namespace = "lua-outbox-invalid-max-attempts",
    max_attempts = 4294967296,
  })
end)
if oversized_attempts_ok then
  client:close()
  error("Lua outbox accepted max_attempts outside the C int range")
end

local negative_notification_capacity_ok = pcall(function()
  return client:new_outbox({
    namespace = "lua-outbox-invalid-notification-capacity",
    notification_capacity = -1,
  })
end)
if negative_notification_capacity_ok then
  client:close()
  error("Lua outbox accepted a negative notification_capacity")
end

local invalid_dead_letter_capacity_ok = pcall(function()
  return client:new_outbox({
    namespace = "lua-outbox-invalid-dead-letter-capacity",
    dead_letter_max_count = true,
  })
end)
if invalid_dead_letter_capacity_ok then
  client:close()
  error("Lua outbox accepted true as a dead-letter capacity bound")
end

local oversized_queue_attempts_ok = pcall(function()
  return client:enqueue({
    namespace = "lua-outbox-invalid-max-attempts",
    queue = "invalid-attempts",
    max_attempts = 4294967296,
  }, "ignored")
end)
if oversized_queue_attempts_ok then
  client:close()
  error("Lua enqueue accepted max_attempts outside the C int range")
end

local legacy_namespace_ok = pcall(function()
  return client:new_outbox({ namespace_name = "lua-outbox-legacy-namespace" })
end)
if legacy_namespace_ok then
  client:close()
  error("Lua outbox accepted the retired namespace_name field")
end

local outbox, outbox_err = client:new_outbox({
  namespace = "lua-outbox-records",
  owner = "lua-outbox-worker",
  transaction_ttl_seconds = 30,
  claim_ttl_seconds = 30,
  recovery_interval_seconds = 0,
  dead_letter_retention_seconds = -1,
  dead_letter_max_count = false,
  dead_letter_max_bytes = false,
})
if outbox == nil then
  client:close()
  error(("Lua outbox creation failed: %s"):format(
    outbox_err and outbox_err.message or tostring(outbox_err)))
end

-- Producer construction is threadless.  Consumption is an explicit lifecycle
-- decision and remains separate from the request-domain outbox handle.
local dispatcher, dispatcher_err = outbox:dispatcher()
if dispatcher == nil then
  outbox:close()
  client:close()
  error(("Lua outbox dispatcher creation failed: %s"):format(
    dispatcher_err and dispatcher_err.message or tostring(dispatcher_err)))
end
local invalid_handlers, invalid_handlers_err = dispatcher:pump({
  handlers = { [""] = function() end },
})
if invalid_handlers ~= nil or invalid_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted an invalid handler map")
end
local empty_handlers, empty_handlers_err = dispatcher:pump({ handlers = {} })
if empty_handlers ~= nil or empty_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted an empty handler map")
end
local numeric_kind_handlers, numeric_kind_handlers_err = dispatcher:pump({
  handlers = { [1] = function() end },
})
if numeric_kind_handlers ~= nil or numeric_kind_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted a non-string handler kind")
end
local nul_kind_handlers, nul_kind_handlers_err = dispatcher:pump({
  handlers = { ["http\0invalid"] = function() end },
})
if nul_kind_handlers ~= nil or nul_kind_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted an embedded-NUL handler kind")
end

local function assert_ok(value, err, operation)
  if value == nil then
    error(("%s failed: %s"):format(operation, err and err.message or tostring(err)))
  end
  return value
end

-- A renewed paired claim must retain the parent CAS snapshot through deferred
-- terminalization. Keep this independent of Vectis and real-time expiry waits.
do
  local renewal_client = assert(lockdc.open({
    endpoints = { "pouch://" .. root .. "/renewal" },
    pouch = { single_writer = false,
              crypto_key = assert(lockdc.pouch_crypto_generate_key()) },
  }))
  for index, sequence in ipairs({ "", "u", "r", "ru", "ur", "ruru" }) do
    for _, terminal in ipairs({ "completed", "retry_wait", "dead_letter" }) do
      local namespace = "renewal-" .. index .. "-" .. terminal
      local producer = assert(renewal_client:new_outbox({
        namespace = namespace, claim_ttl_seconds = 30,
      }))
      local committed = assert(producer:transaction(function(tx)
        return tx:append({
          operation_id = "renewal", effect_id = "renewal", effect_key = "renewal",
          payload_digest = "sha256:renewal", kind = "renewal", destination = "local",
        }, "hello")
      end))
      local key = committed.outbox_receipts[1].outbox_key
      local receiver = assert(producer:dispatcher())
      assert(receiver:notify_outbox_key(key))
      local checkpoint_key, updates = nil, 0
      local count, pump_err = receiver:pump({
        with_state = true, max_jobs = 1, timeout_ms = 0,
        handlers = { renewal = function(renewed_job)
          local checkpoint = assert(renewed_job:state())
          checkpoint_key = checkpoint:info().key
          for action in sequence:gmatch(".") do
            if action == "r" then
              assert(renewed_job:renew(30))
            else
              updates = updates + 1
              assert(checkpoint:update_json({ seen = updates }))
            end
          end
          if updates > 0 then
            assert(checkpoint:read_json().seen == updates)
          end
          if terminal == "completed" then return renewed_job:complete() end
          if terminal == "retry_wait" then
            return renewed_job:retry({ delay_seconds = 30 })
          end
          return renewed_job:dead_letter("renewed")
        end },
      })
      assert(count == 1, pump_err and pump_err.message)
      assert(receiver:stop(1000))
      local envelope = assert(renewal_client:acquire({
        namespace = namespace, key = key, owner = "inspect", ttl_seconds = 30,
      }))
      assert(envelope:read_json().dispatch_state == terminal)
      assert(envelope:release())
      if terminal ~= "completed" and updates > 0 then
        local checkpoint = assert(renewal_client:acquire({
          namespace = namespace, key = checkpoint_key,
          owner = "inspect", ttl_seconds = 30,
        }))
        assert(checkpoint:read_json().seen == updates)
        assert(checkpoint:release())
      end
      receiver:close()
      producer:close()
    end
  end
  renewal_client:close()
end

-- The Lua acquire_for_update facade must preserve Pouch's staged lease rather
-- than manufacturing a generic clone.  Retaining the callback lease proves it
-- is invalid once the staging scope has ended.
do
  local update_key = "lua-outbox-acquire-for-update"
  local seed = assert_ok(client:acquire({
    key = update_key,
    owner = "lua-outbox-acquire-for-update-seed",
    ttl_seconds = 30,
  }), nil, "Lua Pouch acquire_for_update seed acquire")
  assert_ok(seed:update_json({ generation = 1 }), nil,
            "Lua Pouch acquire_for_update seed update")
  assert_ok(seed:release(), nil, "Lua Pouch acquire_for_update seed release")

  local escaped_lease = nil
  local callback_close_rejected = false
  assert_ok(client:acquire_for_update({
    key = update_key,
    owner = "lua-outbox-acquire-for-update",
    ttl_seconds = 30,
  }, function(af)
    callback_close_rejected = not pcall(function()
      client:close()
    end)
    local state, metadata = af:load_json()
    if state == nil or metadata == nil or metadata.no_content or
        state.generation ~= 1 then
      return "Lua Pouch acquire_for_update did not receive the staged snapshot"
    end
    escaped_lease = af.lease
    return af:update_json({ generation = 2 })
  end), nil, "Lua Pouch acquire_for_update")

  local updated = assert_ok(client:read_json({
    key = update_key,
    public_read = true,
  }), nil, "Lua Pouch acquire_for_update read")
  if updated.generation ~= 2 then
    error("Lua Pouch acquire_for_update did not commit the staged update")
  end
  if pcall(function() return escaped_lease:info() end) then
    error("Lua acquire_for_update callback lease escaped its staging scope")
  end
  if not callback_close_rejected then
    error("Lua acquire_for_update callback closed its active client")
  end
end

-- Input-source cleanup is still part of the caller's native operation. Client
-- update, attachment, and enqueue sources must reject a reentrant close from
-- their close callback just as they do from read callbacks.
do
  local source_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "/source-close-client" },
    default_namespace = "lua-source-close-client",
  }), nil, "Lua source-close client creation")
  local source_lease = assert_ok(source_client:acquire({
    key = "source-close-state",
    owner = "lua-source-close-client",
    ttl_seconds = 30,
  }), nil, "Lua source-close client lease")
  local function source_with_close(bytes, mark)
    local sent = false
    return {
      read = function()
        if sent then
          return nil
        end
        sent = true
        return bytes
      end,
      close = function()
        mark.rejected = not pcall(function()
          source_client:close()
        end)
      end,
    }
  end
  local update_close = {}
  assert_ok(source_client:update(source_lease,
                                 source_with_close("source-close-state", update_close)),
            nil, "Lua source-close client update")
  local attach_close = {}
  assert_ok(source_client:attach({
    lease = source_lease,
    name = "source-close-attachment",
    content_type = "text/plain",
  }, source_with_close("source-close-attachment", attach_close)), nil,
            "Lua source-close client attachment")
  local enqueue_close = {}
  assert_ok(source_client:enqueue({
    queue = "source-close-queue",
  }, source_with_close("source-close-message", enqueue_close)), nil,
            "Lua source-close client enqueue")
  if not update_close.rejected or not attach_close.rejected or
      not enqueue_close.rejected then
    error("Lua client input source cleanup allowed a reentrant close")
  end
  assert_ok(source_lease:release(), nil, "Lua source-close client lease release")
  source_client:close()
end

-- Source-table field lookup is also Lua code. Closing an owner from __index
-- must make the operation fail before it enters native I/O, for every input
-- receiver shape.
do
  local function closing_source(close_owner)
    return setmetatable({}, {
      __index = function()
        close_owner()
        return function()
          return nil
        end
      end,
    })
  end
  local preparation_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "/source-prepare-client" },
    default_namespace = "lua-source-prepare-client",
  }), nil, "Lua source-prepare client creation")
  local preparation_lease = assert_ok(preparation_client:acquire({
    key = "source-prepare-state",
    owner = "lua-source-prepare-client",
    ttl_seconds = 30,
  }), nil, "Lua source-prepare client lease")
  local update_result, update_err = preparation_client:update(preparation_lease,
      closing_source(function()
        preparation_client:close()
      end))
  if update_result ~= nil or update_err == nil then
    error("Lua client source setup continued after closing its client")
  end
  preparation_lease:close()

  local message_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "/message-client-close" },
    default_namespace = "lua-message-client-close",
  }), nil, "Lua message client-close creation")
  assert_ok(message_client:enqueue({
    queue = "message-client-close",
  }, "message-client-close"), nil, "Lua message client-close enqueue")
  local live_message = assert_ok(message_client:dequeue({
    queue = "message-client-close",
    owner = "lua-message-client-close",
  }), nil, "Lua message client-close dequeue")
  message_client:close()
  live_message:close()

  local preparation_outbox = assert_ok(client:new_outbox({
    namespace = "lua-source-prepare-outbox",
  }), nil, "Lua source-prepare outbox creation")
  local outbox_result, outbox_err = preparation_outbox:append({
    operation_id = "lua-source-prepare-outbox",
    effect_id = "lua-source-prepare-outbox",
    effect_key = "lua-source-prepare-outbox",
    payload_digest = "sha256:lua-source-prepare-outbox",
    kind = "http",
    destination = "https://example.test/lua-source-prepare-outbox",
  }, closing_source(function()
    preparation_outbox:close()
  end))
  if outbox_result ~= nil or outbox_err == nil then
    error("Lua outbox source setup continued after closing its outbox")
  end

  local preparation_txn_outbox = assert_ok(client:new_outbox({
    namespace = "lua-source-prepare-transaction",
  }), nil, "Lua source-prepare transaction outbox creation")
  local preparation_txn = assert_ok(preparation_txn_outbox:begin(), nil,
                                    "Lua source-prepare transaction begin")
  local txn_result, txn_err = preparation_txn:append({
    operation_id = "lua-source-prepare-transaction",
    effect_id = "lua-source-prepare-transaction",
    effect_key = "lua-source-prepare-transaction",
    payload_digest = "sha256:lua-source-prepare-transaction",
    kind = "http",
    destination = "https://example.test/lua-source-prepare-transaction",
  }, closing_source(function()
    preparation_txn:close()
  end))
  if txn_result ~= nil or txn_err == nil then
    error("Lua transaction source setup continued after closing its transaction")
  end
  preparation_txn_outbox:close()

  local preparation_participant_outbox = assert_ok(client:new_outbox({
    namespace = "lua-source-prepare-participant",
  }), nil, "Lua source-prepare participant outbox creation")
  local preparation_participant_txn = assert_ok(preparation_participant_outbox:begin(),
                                                nil,
                                                "Lua source-prepare participant begin")
  local preparation_participant = assert_ok(preparation_participant_txn:acquire({
    key = "source-prepare-participant",
    owner = "lua-source-prepare-participant",
  }), nil, "Lua source-prepare participant acquire")
  local participant_result, participant_err = preparation_participant:update(
      closing_source(function()
        preparation_participant:close()
      end))
  if participant_result ~= nil or participant_err == nil then
    error("Lua participant source setup continued after closing its participant")
  end
  preparation_participant_txn:rollback()
  preparation_participant_txn:close()
  preparation_participant_outbox:close()
end

-- Request-table decoding is also a Lua callback boundary. Closing any outbox
-- receiver from __index must fail as a normal status result before the native
-- operation starts; it must not reach a stale receiver. Cover one operation
-- from each concrete receiver family.
do
  local function assert_closed_while_preparing(result, err, name)
    if result ~= nil or err == nil or
        not tostring(err.message):find("closed while preparing input", 1, true) then
      error(name .. " continued after request-table lookup closed its receiver")
    end
  end

  local function close_core_once(wrapper)
    local closed = false

    return function()
      if not closed then
        wrapper._core:close()
        closed = true
      end
    end
  end

  local closing_constructor_client, closing_constructor_client_err = lockdc.open({
    endpoints = { "pouch://" .. root .. "/request-close-client" },
  })
  closing_constructor_client = assert_ok(closing_constructor_client,
                                         closing_constructor_client_err,
                                         "Lua request-close client creation")
  local close_constructor_client_core = close_core_once(closing_constructor_client)
  local closing_outbox_config = setmetatable({}, {
    __index = function(_, field)
      close_constructor_client_core()
      if field == "namespace" then return "lua-request-close-client" end
      return nil
    end,
  })
  local closed_constructor_result, closed_constructor_err =
      closing_constructor_client:new_outbox(closing_outbox_config)
  if closed_constructor_result ~= nil or closed_constructor_err == nil or
      not tostring(closed_constructor_err.message):find(
          "closed while preparing request", 1, true) then
    error("Lua client new_outbox continued after config lookup closed its client")
  end

  local rooted_config_client, rooted_config_client_err = lockdc.open({
    endpoints = { "pouch://" .. root .. "/request-rooted-config" },
  })
  rooted_config_client = assert_ok(rooted_config_client, rooted_config_client_err,
                                   "Lua rooted-config client creation")
  local rooted_config = setmetatable({}, {
    __index = function(_, field)
      if field == "namespace" then
        return string.rep("lua-rooted-config-namespace-", 16)
      end
      if field == "owner" then
        collectgarbage("collect")
        return string.rep("lua-rooted-config-owner-", 16)
      end
      return nil
    end,
  })
  local rooted_config_outbox, rooted_config_outbox_err =
      rooted_config_client:new_outbox(rooted_config)
  rooted_config_outbox = assert_ok(rooted_config_outbox,
                                   rooted_config_outbox_err,
                                   "Lua rooted outbox config")
  rooted_config_outbox:close()
  rooted_config_client:close()

  local closing_history_client, closing_history_client_err = lockdc.open({
    endpoints = { "pouch://" .. root .. "/request-close-history" },
  })
  closing_history_client = assert_ok(closing_history_client,
                                    closing_history_client_err,
                                    "Lua request-close history client creation")
  local close_history_client_core = close_core_once(closing_history_client)
  local closing_history_config = setmetatable({}, {
    __index = function(_, field)
      close_history_client_core()
      if field == "consumer_id" then return "lua-request-close-history" end
      return nil
    end,
  })
  local closed_history_result, closed_history_err =
      closing_history_client._core:new_history_consumer(closing_history_config)
  if closed_history_result ~= nil or closed_history_err == nil or
      not tostring(closed_history_err.message):find("closed while preparing request",
                                                     1, true) then
    error("Lua history consumer creation continued after config lookup closed its client")
  end

  local closing_outbox = assert_ok(client:new_outbox({
    namespace = "lua-outbox-request-close-outbox",
  }), nil, "Lua request-close outbox creation")
  local close_outbox_core = close_core_once(closing_outbox)
  local closing_inbox = setmetatable({}, {
    __index = function(_, field)
      close_outbox_core()
      if field == "consumer_id" then return "lua-request-close-consumer" end
      if field == "source_kind" then return "http" end
      if field == "source_id" then return "lua-request-close-source" end
      if field == "message_id" then return "lua-request-close-message" end
      return nil
    end,
  })
  local closed_outbox_result, closed_outbox_err =
      closing_outbox._core:accept_inbox(closing_inbox)
  assert_closed_while_preparing(closed_outbox_result, closed_outbox_err,
                                "Lua outbox accept_inbox")

  local closing_txn_outbox = assert_ok(client:new_outbox({
    namespace = "lua-outbox-request-close-transaction",
  }), nil, "Lua request-close transaction outbox creation")
  local closing_txn = assert_ok(closing_txn_outbox:begin(), nil,
                                "Lua request-close transaction begin")
  local close_txn_core = close_core_once(closing_txn)
  local closing_participant_request = setmetatable({}, {
    __index = function(_, field)
      close_txn_core()
      if field == "key" then return "lua-request-close-participant" end
      if field == "owner" then return "lua-request-close-owner" end
      return nil
    end,
  })
  local closed_txn_result, closed_txn_err =
      closing_txn._core:acquire(closing_participant_request)
  assert_closed_while_preparing(closed_txn_result, closed_txn_err,
                                "Lua outbox transaction acquire")
  closing_txn_outbox:close()

  local closing_participant_outbox = assert_ok(client:new_outbox({
    namespace = "lua-outbox-request-close-participant",
  }), nil, "Lua request-close participant outbox creation")
  local closing_participant_txn = assert_ok(closing_participant_outbox:begin(),
                                            nil,
                                            "Lua request-close participant begin")
  local closing_participant = assert_ok(closing_participant_txn:acquire({
    key = "lua-request-close-participant",
    owner = "lua-request-close-owner",
  }), nil, "Lua request-close participant acquire")
  local close_participant_core = close_core_once(closing_participant)
  local closing_keepalive = setmetatable({}, {
    __index = function(_, field)
      close_participant_core()
      if field == "ttl_seconds" then return 30 end
      return nil
    end,
  })
  local closed_participant_result, closed_participant_err =
      closing_participant._core:keepalive(closing_keepalive)
  assert_closed_while_preparing(closed_participant_result,
                                closed_participant_err,
                                "Lua outbox participant keepalive")
  closing_participant_txn:close()
  closing_participant_outbox:close()

  local closing_dispatcher_outbox = assert_ok(client:new_outbox({
    namespace = "lua-outbox-request-close-dispatcher",
  }), nil, "Lua request-close dispatcher outbox creation")
  local closing_dispatcher = assert_ok(closing_dispatcher_outbox:dispatcher(),
                                       nil,
                                       "Lua request-close dispatcher creation")
  local close_dispatcher_core = close_core_once(closing_dispatcher)
  local closing_export_options = setmetatable({}, {
    __index = function(_, field)
      close_dispatcher_core()
      if field == "format" then return "json" end
      return nil
    end,
  })
  local closed_dispatcher_result, closed_dispatcher_err =
      closing_dispatcher._core:export_dead_letters(closing_export_options)
  assert_closed_while_preparing(closed_dispatcher_result, closed_dispatcher_err,
                                "Lua outbox dispatcher export_dead_letters")
  closing_dispatcher_outbox:close()

  local closing_job_outbox = assert_ok(client:new_outbox({
    namespace = "lua-outbox-request-close-job",
  }), nil, "Lua request-close job outbox creation")
  local closing_job_dispatcher = assert_ok(closing_job_outbox:dispatcher(), nil,
                                           "Lua request-close job dispatcher creation")
  local closing_job_txn, closing_job_append_err = closing_job_outbox:append({
    operation_id = "lua-request-close-job",
    effect_id = "lua-request-close-job",
    effect_key = "lua-request-close-job",
    payload_digest = "sha256:lua-request-close-job",
    kind = "http",
    destination = "https://example.test/lua-request-close-job",
  }, "request-close-job")
  closing_job_txn = assert_ok(closing_job_txn, closing_job_append_err,
                              "Lua request-close job append")
  assert_ok(closing_job_txn:commit(), nil, "Lua request-close job commit")
  closing_job_txn:close()
  local closing_job = assert_ok(closing_job_dispatcher:next(3000), nil,
                                "Lua request-close job next")
  local close_job_core = close_core_once(closing_job)
  local closing_completion = setmetatable({}, {
    __index = function(_, field)
      close_job_core()
      if field == "delivery_reference" then return "ignored" end
      return nil
    end,
  })
  local closed_job_result, closed_job_err =
      closing_job._core:complete(closing_completion)
  if closed_job_result ~= nil or closed_job_err == nil or
      not tostring(closed_job_err.message):find("closed while preparing input",
                                                 1, true) then
    error("Lua outbox job complete continued after result lookup closed its job")
  end
  assert_ok(closing_job_dispatcher:stop(-1), nil,
            "Lua request-close job dispatcher stop")
  closing_job_dispatcher:close()
  closing_job_outbox:close()

  local closing_payload_outbox = assert_ok(client:new_outbox({
    namespace = "lua-outbox-request-close-payload",
  }), nil, "Lua request-close payload outbox creation")
  local closing_payload_dispatcher = assert_ok(
      closing_payload_outbox:dispatcher(), nil,
      "Lua request-close payload dispatcher creation")
  local closing_payload_txn, closing_payload_append_err =
      closing_payload_outbox:append({
        operation_id = "lua-request-close-payload",
        effect_id = "lua-request-close-payload",
        effect_key = "lua-request-close-payload",
        payload_digest = "sha256:lua-request-close-payload",
        kind = "http",
        destination = "https://example.test/lua-request-close-payload",
      }, "request-close-payload")
  closing_payload_txn = assert_ok(closing_payload_txn,
                                  closing_payload_append_err,
                                  "Lua request-close payload append")
  assert_ok(closing_payload_txn:commit(), nil,
            "Lua request-close payload commit")
  closing_payload_txn:close()
  local closing_payload_job = assert_ok(closing_payload_dispatcher:next(3000),
                                        nil, "Lua request-close payload next")
  local close_payload_job_core = close_core_once(closing_payload_job)
  local closing_payload_sink = setmetatable({}, {
    __index = function(_, field)
      close_payload_job_core()
      if field == "write" then
        return function() end
      end
      return nil
    end,
  })
  local closed_payload_result, closed_payload_err =
      closing_payload_job._core:write_payload(closing_payload_sink)
  if closed_payload_result ~= nil or closed_payload_err == nil or
      not tostring(closed_payload_err.message):find("closed while preparing input",
                                                     1, true) then
    error("Lua outbox job payload continued after sink lookup closed its job")
  end
  assert_ok(closing_payload_dispatcher:stop(-1), nil,
            "Lua request-close payload dispatcher stop")
  closing_payload_dispatcher:close()
  closing_payload_outbox:close()

  local closing_lease = assert_ok(client:acquire({
    namespace = "lua-request-close-lease",
    key = "lease",
    owner = "lua-request-close-lease",
    ttl_seconds = 30,
  }), nil, "Lua request-close lease acquire")
  local close_lease_core = close_core_once(closing_lease)
  local closing_lease_options = setmetatable({}, {
    __index = function(_, field)
      close_lease_core()
      if field == "ttl_seconds" then return 30 end
      return nil
    end,
  })
  local closed_lease_result, closed_lease_err =
      closing_lease._core:keepalive(closing_lease_options)
  if closed_lease_result ~= nil or closed_lease_err == nil or
      not tostring(closed_lease_err.message):find("closed while preparing request",
                                                   1, true) then
    error("Lua lease keepalive continued after request lookup closed its lease")
  end

  assert_ok(client:enqueue({
    namespace = "lua-request-close-message",
    queue = "message",
  }, "request-close-message"), nil, "Lua request-close message enqueue")
  local closing_message = assert_ok(client:dequeue({
    namespace = "lua-request-close-message",
    queue = "message",
    owner = "lua-request-close-message",
  }), nil, "Lua request-close message dequeue")
  local close_message_core = close_core_once(closing_message)
  local closing_nack_options = setmetatable({}, {
    __index = function(_, field)
      close_message_core()
      if field == "delay_seconds" then return 0 end
      return nil
    end,
  })
  local closed_message_result, closed_message_err =
      closing_message._core:nack(closing_nack_options)
  if closed_message_result ~= nil or closed_message_err == nil or
      not tostring(closed_message_err.message):find("closed while preparing request",
                                                     1, true) then
    error("Lua message nack continued after request lookup closed its message")
  end

  local closing_batch_client, closing_batch_client_err = lockdc.open({
    endpoints = { "pouch://" .. root .. "/request-close-batch" },
  })
  closing_batch_client = assert_ok(closing_batch_client, closing_batch_client_err,
                                   "Lua request-close batch client creation")
  local close_batch_client_core = close_core_once(closing_batch_client)
  local closing_batch_request = setmetatable({}, {
    __index = function(_, field)
      close_batch_client_core()
      if field == "queue" then return "request-close-batch" end
      if field == "owner" then return "lua-request-close-batch" end
      return nil
    end,
  })
  local closed_batch_result, closed_batch_err =
      closing_batch_client._core:dequeue_batch(closing_batch_request)
  if closed_batch_result ~= nil or closed_batch_err == nil or
      not tostring(closed_batch_err.message):find("closed while preparing request",
                                                   1, true) then
    error("Lua dequeue_batch continued after request lookup closed its client")
  end

  local closing_acquire_client, closing_acquire_client_err = lockdc.open({
    endpoints = { "pouch://" .. root .. "/request-close-acquire" },
  })
  closing_acquire_client = assert_ok(closing_acquire_client,
                                     closing_acquire_client_err,
                                     "Lua request-close acquire client creation")
  local close_acquire_client_core = close_core_once(closing_acquire_client)
  local closing_acquire_request = setmetatable({}, {
    __index = function(_, field)
      close_acquire_client_core()
      if field == "namespace" then return "lua-request-close-acquire" end
      if field == "key" then return "lease" end
      if field == "owner" then return "lua-request-close-acquire" end
      if field == "ttl_seconds" then return 30 end
      return nil
    end,
  })
  local closed_acquire_result, closed_acquire_err =
      closing_acquire_client._core:acquire(closing_acquire_request)
  if closed_acquire_result ~= nil or closed_acquire_err == nil or
      not tostring(closed_acquire_err.message):find("closed while preparing request",
                                                     1, true) then
    error("Lua acquire continued after request lookup closed its client")
  end
end

-- Participant source/sink callbacks can re-enter the explicit transaction.
-- They must not be allowed to release the lease that the suspended native I/O
-- operation still owns.
do
local reentrant_entry = {
  operation_id = "lua-streaming-duplicate",
  effect_id = "lua-streaming-duplicate",
  effect_key = "lua-streaming-duplicate",
  payload_digest = "sha256:lua-streaming-duplicate",
  kind = "http",
  destination = "https://example.test/lua-streaming-duplicate",
  content_type = "text/plain",
}
local streaming_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-streaming-records",
  owner = "lua-outbox-streaming-worker",
  transaction_ttl_seconds = 30,
}), nil, "Lua streaming outbox creation")
local outbox_append_close_rejected = false
local outbox_append_sent = false
local reentrant_seed, reentrant_seed_receipt = streaming_outbox:append(
    reentrant_entry, {
      read = function()
        if outbox_append_sent then
          return nil
        end
        outbox_append_sent = true
        return "seed"
      end,
      close = function()
        outbox_append_close_rejected = not pcall(function()
          streaming_outbox:close()
        end)
      end,
    })
reentrant_seed = assert_ok(reentrant_seed, reentrant_seed_receipt,
                           "Lua streaming duplicate seed")
if not outbox_append_close_rejected then
  error("Lua outbox append source cleanup allowed a reentrant close")
end
assert_ok(reentrant_seed:commit(), nil,
          "Lua streaming duplicate seed commit")
reentrant_seed:close()
local streaming_txn = assert_ok(streaming_outbox:begin(), nil,
                                "Lua streaming transaction begin")
local streaming_participant = assert_ok(streaming_txn:acquire({
  namespace = "lua-outbox-streaming",
  key = "state",
  owner = "lua-outbox-streaming",
}), nil, "Lua streaming transaction acquire")
local function assert_streaming_terminal_guard()
  for _, terminal in ipairs({ "close", "commit", "rollback" }) do
    local terminal_ok, terminal_err = pcall(function()
      return streaming_txn[terminal](streaming_txn)
    end)
    if terminal_ok or not tostring(terminal_err):find("participant I/O is streaming", 1, true) then
      error("Lua transaction terminal operation escaped a participant streaming callback")
    end
  end
  local staged_ok, staged_err = pcall(function()
    return streaming_txn:append(reentrant_entry, "duplicate")
  end)
  if staged_ok or not tostring(staged_err):find("participant I/O is streaming", 1, true) then
    error("Lua transaction staging escaped a participant streaming callback")
  end
end
local txn_append_close_rejected = false
local txn_append_sent = false
assert_ok(streaming_txn:append({
  operation_id = "lua-streaming-source-close",
  effect_id = "lua-streaming-source-close",
  effect_key = "lua-streaming-source-close",
  payload_digest = "sha256:lua-streaming-source-close",
  kind = "http",
  destination = "https://example.test/lua-streaming-source-close",
}, {
  read = function()
    if txn_append_sent then
      return nil
    end
    txn_append_sent = true
    return "streamed transaction payload"
  end,
  close = function()
    txn_append_close_rejected = not pcall(function()
      streaming_txn:close()
    end)
  end,
}), nil, "Lua streaming transaction append")
if not txn_append_close_rejected then
  error("Lua transaction append source cleanup allowed a reentrant close")
end
local source_sent = false
local participant_update_close_rejected = false
assert_ok(streaming_participant:update({
  read = function()
    if source_sent then
      return nil
    end
    source_sent = true
    assert_streaming_terminal_guard()
    return "streamed participant state"
  end,
  close = function()
    participant_update_close_rejected = not pcall(function()
      streaming_participant:close()
    end)
  end,
}), nil, "Lua streaming transaction state update")
if not participant_update_close_rejected then
  error("Lua participant update source cleanup allowed a reentrant close")
end
local streaming_get_output, streaming_get_result = streaming_participant:get({}, {
  write = function()
    assert_streaming_terminal_guard()
    return true
  end,
})
if streaming_get_output ~= nil or type(streaming_get_result) ~= "table" then
  error("Lua streaming transaction state read did not preserve sink semantics")
end
assert_ok(streaming_txn:rollback(), nil,
          "Lua streaming transaction rollback after read")
streaming_participant:close()
streaming_txn:close()
streaming_outbox:close()
end

local command_txn, command_receipt = outbox:accept_command({
  scope = "lua-command-status",
  command_type = "orders.create.v1",
  generate_idempotency_key = true,
  request_digest = "sha256:lua-command-status",
})
if command_txn == nil or command_receipt == nil or
    type(command_receipt.idempotency_key) ~= "string" or
    #command_receipt.idempotency_key ~= 20 then
  error("Lua generated command idempotency key was not returned")
end
assert_ok(command_txn:commit(), nil, "Lua generated command commit")
command_txn:close()
local pending_receipt, pending_err =
    outbox:wait_command(command_receipt.command_id, 0)
if pending_receipt == nil or pending_receipt.state ~= lockdc.COMMAND_PENDING or
    pending_err == nil or pending_err.code ~= lockdc.ERR_TIMEOUT then
  error("Lua command wait did not expose a pending timeout receipt")
end
local resumed_txn, resumed_receipt =
    outbox:resume_command_by_id(command_receipt.command_id)
if resumed_txn == nil or resumed_receipt == nil then
  error("Lua command resume by id did not return a pending transaction")
end
local command_result_close_rejected = false
local command_result_sent = false
assert_ok(resumed_txn:complete_command({
  result_code = "created",
  content_type = "text/plain",
  body = {
    read = function()
      if command_result_sent then
        return nil
      end
      command_result_sent = true
      return "lua command result"
    end,
    close = function()
      command_result_close_rejected = not pcall(function()
        resumed_txn:close()
      end)
    end,
  },
}), nil,
          "Lua command terminal result")
if not command_result_close_rejected then
  error("Lua command result source cleanup allowed a reentrant transaction close")
end
assert_ok(resumed_txn:commit(), nil, "Lua command terminal commit")
resumed_txn:close()
local completed_receipt, completed_err =
    outbox:wait_command(command_receipt.command_id, 0)
if completed_receipt == nil or
    completed_receipt.state ~= lockdc.COMMAND_COMPLETED or completed_err ~= nil then
  error("Lua command wait did not expose the durable terminal receipt")
end
local completed_body, completed_written = outbox:read_command_result({
  scope = "lua-command-status",
  command_type = "orders.create.v1",
  idempotency_key = command_receipt.idempotency_key,
})
if completed_body ~= "lua command result" or
    completed_written ~= #"lua command result" then
  error("Lua command result read did not materialize the durable result")
end

do
local generated_with_key_txn, generated_with_key_err = outbox:accept_command({
  scope = "lua-command-status",
  command_type = "orders.create.v1",
  idempotency_key = "must-not-be-supplied",
  generate_idempotency_key = true,
  request_digest = "sha256:lua-command-status-invalid",
})
if generated_with_key_txn ~= nil or generated_with_key_err == nil or
    generated_with_key_err.code ~= lockdc.ERR_INVALID then
  error("Lua generated command key validation accepted a supplied key")
end
local invalid_wait_receipt, invalid_wait_err = outbox:wait_command("cmd_invalid", 0)
if invalid_wait_receipt ~= nil or invalid_wait_err == nil or
    invalid_wait_err.code ~= lockdc.ERR_INVALID then
  error("Lua command wait accepted an invalid command id")
end

local invalid_resume_txn, invalid_resume_err = outbox:resume_command_by_id("cmd_invalid")
if invalid_resume_txn ~= nil or invalid_resume_err == nil or
    invalid_resume_err.code ~= lockdc.ERR_INVALID then
  error("Lua command resume accepted an invalid command id")
end
local unknown_command_id = "cmd_" .. string.rep("A", 43)
local unknown_receipt, unknown_err = outbox:get_command_receipt_by_id(unknown_command_id)
if unknown_receipt ~= nil or unknown_err == nil or
    unknown_err.code ~= lockdc.ERR_INVALID then
  error("Lua command status accepted an unknown command id")
end
local unknown_wait_receipt, unknown_wait_err = outbox:wait_command(unknown_command_id, 0)
if unknown_wait_receipt ~= nil or unknown_wait_err == nil or
    unknown_wait_err.code ~= lockdc.ERR_INVALID then
  error("Lua command wait accepted an unknown command id")
end
local unknown_resume_txn, unknown_resume_err =
    outbox:resume_command_by_id(unknown_command_id)
if unknown_resume_txn ~= nil or unknown_resume_err == nil or
    unknown_resume_err.code ~= lockdc.ERR_INVALID then
  error("Lua command resume accepted an unknown command id")
end

-- Two independently opened Lua clients deliberately share the configured
-- Pouch root.  The second supervisor owns terminalization while the route
-- client observes the durable failure receipt.
local supervisor_client = assert_ok(lockdc.open({
  endpoints = { "pouch://" .. root .. "/store" },
  pouch = {
    single_writer = false,
    segment_target_bytes = 4096,
  },
}), nil, "Lua shared command supervisor client creation")
local supervisor_outbox = assert_ok(supervisor_client:new_outbox({
  namespace = "lua-outbox-records",
  owner = "lua-command-supervisor",
  claim_ttl_seconds = 30,
  recovery_interval_seconds = 0,
}), nil, "Lua shared command supervisor outbox creation")
local failed_command_txn, failed_command_receipt = outbox:accept_command({
  scope = "lua-command-status",
  command_type = "orders.create.v1",
  idempotency_key = "lua-command-failure",
  request_digest = "sha256:lua-command-failure",
})
if failed_command_txn == nil or failed_command_receipt == nil then
  error("Lua failed command acceptance did not return a transaction")
end
assert_ok(failed_command_txn:commit(), nil, "Lua failed command commit")
failed_command_txn:close()
local observed_pending = assert_ok(
    supervisor_outbox:get_command_receipt_by_id(failed_command_receipt.command_id),
    nil, "Lua shared command pending receipt")
if observed_pending.state ~= lockdc.COMMAND_PENDING then
  error("Lua shared command observer did not see the pending receipt")
end
local failed_resume_txn, failed_resume_receipt =
    supervisor_outbox:resume_command_by_id(failed_command_receipt.command_id)
if failed_resume_txn == nil or failed_resume_receipt == nil then
  error("Lua shared command supervisor did not resume the pending receipt")
end
assert_ok(failed_resume_txn:fail_command({
  failure_code = "declined",
  failure_message = "route request was declined",
}), nil, "Lua shared command terminal failure")
assert_ok(failed_resume_txn:commit(), nil, "Lua shared command failure commit")
failed_resume_txn:close()
local failed_receipt, failed_err =
    outbox:wait_command(failed_command_receipt.command_id, 0)
if failed_receipt == nil or failed_receipt.state ~= lockdc.COMMAND_FAILED or
    failed_receipt.failure_code ~= "declined" or failed_err ~= nil then
  error("Lua command wait did not expose the durable terminal failure receipt")
end
supervisor_outbox:close()
supervisor_client:close()
end

local function append_effect(effect_id, payload)
  local txn, receipt_or_err = outbox:append({
    operation_id = "lua-order-1",
    effect_id = effect_id,
    effect_key = "lua-effect:" .. effect_id,
    payload_digest = "sha256:lua-effect:" .. effect_id,
    kind = "http",
    destination = "https://example.test/effects/" .. effect_id,
    content_type = "application/json",
    headers_json = lockdc.encode_json({ ["x-outbox"] = "lua" }),
  }, lockdc.encode_json(payload))
  if txn == nil then
    outbox:close()
    client:close()
    error(("Lua outbox append failed: %s"):format(
      receipt_or_err and receipt_or_err.message or tostring(receipt_or_err)))
  end
  if receipt_or_err.duplicate then
    outbox:close()
    client:close()
    error("fresh Lua outbox append unexpectedly reported duplicate")
  end
  return txn, receipt_or_err
end

local legacy_headers_ok, legacy_headers_err = pcall(function()
  return outbox:append({
    operation_id = "lua-legacy-headers",
    effect_id = "legacy-headers",
    effect_key = "lua-effect:legacy-headers",
    payload_digest = "sha256:lua-legacy-headers",
    kind = "http",
    destination = "https://example.test/effects/legacy-headers",
    headers = { ["x-outbox"] = "legacy" },
  }, "legacy")
end)
if legacy_headers_ok or
    not tostring(legacy_headers_err):find("headers_json", 1, true) then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua outbox retained the headers compatibility field")
end

local txn = append_effect("first", { sequence = 1 })
local participant, participant_err = txn:acquire({
  namespace = "lua-outbox-domain",
  key = "order-1",
  owner = "lua-outbox-worker",
  ttl_seconds = 30,
})
participant = assert_ok(participant, participant_err, "Lua outbox participant acquire")
assert_ok(participant:update_json({ status = "paid", revision = 1 }), nil,
          "Lua outbox participant update")
assert_ok(participant:mutate({ mutations = { "/revision++" } }), nil,
          "Lua outbox participant mutation")
local participant_attach_close_rejected = false
local participant_attach_sent = false
local attachment = assert_ok(participant:attach({
  name = "outbox-proof",
  content_type = "text/plain",
}, {
  read = function()
    if participant_attach_sent then
      return nil
    end
    participant_attach_sent = true
    return "proof"
  end,
  close = function()
    participant_attach_close_rejected = not pcall(function()
      participant:close()
    end)
  end,
}), nil, "Lua outbox participant attachment")
if not participant_attach_close_rejected then
  error("Lua participant attachment source cleanup allowed a reentrant close")
end
if attachment.attachment.name ~= "outbox-proof" then
  participant:close()
  txn:close()
  outbox:close()
  client:close()
  error("Lua outbox participant attachment did not return its name")
end
local attachments = assert_ok(participant:list_attachments(), nil,
                              "Lua outbox participant attachment list")
if #attachments.items ~= 1 or attachments.items[1].name ~= "outbox-proof" then
  participant:close()
  txn:close()
  outbox:close()
  client:close()
  error("Lua outbox participant attachment list lost staged attachment")
end
assert_ok(participant:delete_attachment({ name = "outbox-proof" }), nil,
          "Lua outbox participant attachment delete")
local described, describe_err = participant:describe()
assert_ok(described, describe_err, "Lua outbox participant describe")
if described.version < 2 then
  participant:close()
  txn:close()
  outbox:close()
  client:close()
  error("Lua outbox participant describe lost staged version")
end
participant:close()
assert_ok(txn:commit(), nil, "Lua outbox transaction commit")
txn:close()

local state, state_err = client:read_json({
  namespace = "lua-outbox-domain",
  key = "order-1",
})
if state == nil or state.status ~= "paid" or state.revision ~= 2 then
  outbox:close()
  client:close()
  error(("Lua outbox committed state was not visible: %s"):format(
    state_err and state_err.message or tostring(state_err)))
end

local job, next_err = dispatcher:next(3000)
job = assert_ok(job, next_err, "Lua outbox first job")
if job:info().effect_key ~= "lua-effect:first" then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox returned the wrong first outbox job")
end
if job:state() ~= nil then
  job:close()
  outbox:close()
  client:close()
  error("Lua stateless outbox pull unexpectedly exposed a checkpoint lease")
end
local mixed_mode, mixed_mode_err = dispatcher:pump({
  handlers = { http = function() end },
})
if mixed_mode ~= nil or mixed_mode_err == nil then
  job:close()
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher allowed handler mode after raw pull activation")
end
local second_dispatcher = assert_ok(outbox:dispatcher(), nil,
                                    "Lua outbox second dispatcher wrapper")
mixed_mode, mixed_mode_err = second_dispatcher:pump({
  handlers = { http = function() end },
})
if mixed_mode ~= nil or mixed_mode_err == nil then
  job:close()
  second_dispatcher:close()
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher wrapper bypassed shared raw pull ownership")
end
second_dispatcher:close()
if job.payload ~= nil or job.payload_json ~= nil then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox retained payload materializer aliases")
end
local streamed_bytes = 0
local streamed_chunks = 0
local sink_closed = 0
local streamed, stream_written = job:write_payload({
  write = function(chunk)
    streamed_chunks = streamed_chunks + 1
    streamed_bytes = streamed_bytes + #chunk
    local closed, close_err = pcall(function() job:close() end)
    if closed or not tostring(close_err):find("not allowed while payload is streaming", 1, true) then
      error("Lua payload sink re-entered and closed its active job")
    end
    return true
  end,
  close = function()
    sink_closed = sink_closed + 1
  end,
})
if streamed ~= nil or type(stream_written) ~= "number" or
    streamed_bytes ~= stream_written or streamed_chunks == 0 or sink_closed ~= 1 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox callback sink did not stream and close exactly once")
end
local failed_sink_closed = 0
local failed_stream, failed_stream_err, failed_stream_code = job:write_payload({
  write = function()
    return nil, "injected Lua sink failure"
  end,
  close = function()
    failed_sink_closed = failed_sink_closed + 1
  end,
})
if failed_stream ~= nil or failed_stream_err == nil or failed_stream_code == nil or
    not tostring(failed_stream_err.message):find("injected Lua sink failure", 1, true) or
    failed_sink_closed ~= 1 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox callback sink failure was not a structured stream error")
end
local payload, written_or_err = job:read_payload_json()
if payload == nil or payload.sequence ~= 1 or type(written_or_err) ~= "number" then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox payload did not stream through the façade")
end
assert_ok(job:complete(), nil, "Lua outbox first job completion")

do
-- Stateful pulling is an explicit Pouch capability. The checkpoint is an
-- ordinary Lua lease surface, remains fenced by the claimed job, survives a
-- expired-claim and dead-letter/replay recovery, and is not exposed through
-- the normal query path.
local stateful_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-stateful",
  owner = "lua-outbox-stateful-worker",
  -- Whole-second expiry must leave at least a full second for checkpoint I/O.
  claim_ttl_seconds = 2,
  recovery_interval_seconds = 0,
}), nil, "Lua stateful outbox creation")
local stateful_txn = assert_ok(stateful_outbox:append({
  operation_id = "lua-stateful-operation",
  effect_id = "lua-stateful-effect",
  effect_key = "lua-stateful-idempotency",
  payload_digest = "sha256:lua-stateful",
  kind = "http",
  destination = "https://example.test/lua-stateful",
}, "stateful-payload"), nil, "Lua stateful outbox append")
assert_ok(stateful_txn:commit(), nil, "Lua stateful outbox commit")
stateful_txn:close()
local stateful_dispatcher = assert_ok(stateful_outbox:dispatcher(), nil,
                                      "Lua stateful dispatcher creation")
local stateful_job, stateful_job_err = stateful_dispatcher:next_with_state(3000)
stateful_job = assert_ok(stateful_job, stateful_job_err,
                         "Lua stateful dispatcher next")
local stateful_key = stateful_job:info().outbox_key
local stateful_lease = stateful_job:state()
if stateful_lease == nil then
  error("Lua stateful dispatcher returned a job without a checkpoint lease")
end
assert_ok(stateful_lease:update_json({ checkpoint = 1, source = "lua" }), nil,
          "Lua stateful checkpoint update")
local stateful_attachment = assert_ok(stateful_lease:attach({
  name = "checkpoint.txt",
  content_type = "text/plain",
}, "checkpoint attachment"), nil, "Lua stateful checkpoint attachment")
if stateful_attachment.attachment.name ~= "checkpoint.txt" then
  error("Lua stateful checkpoint attachment did not retain its name")
end
local stateful_attachments = assert_ok(stateful_lease:list_attachments(), nil,
                                      "Lua stateful checkpoint attachment list")
if #stateful_attachments.items ~= 1 or
    stateful_attachments.items[1].name ~= "checkpoint.txt" then
  error("Lua stateful checkpoint attachment was not listed")
end
assert_ok(stateful_job:renew(2), nil, "Lua stateful checkpoint renewal")
-- Closing a claimed job models a handler process stopping before it publishes
-- a terminal outcome. The next pull waits for the known two-second claim
-- expiry, then must receive the committed checkpoint rather than repeat the
-- foreign action blindly.
stateful_job:close()
if pcall(function() return stateful_lease:info() end) then
  error("Lua stateful checkpoint view survived its abandoned job")
end
stateful_job = assert_ok(stateful_dispatcher:next_with_state(3000), nil,
                         "Lua stateful expired-claim recovery delivery")
stateful_lease = stateful_job:state()
local recovered_checkpoint = assert_ok(stateful_lease:read_json(), nil,
                                      "Lua stateful expired-claim checkpoint read")
if recovered_checkpoint.checkpoint ~= 1 or recovered_checkpoint.source ~= "lua" then
  error("Lua stateful expired-claim recovery lost its durable checkpoint")
end
local recovered_attachments = assert_ok(stateful_lease:list_attachments(), nil,
                                       "Lua stateful expired-claim attachment list")
if #recovered_attachments.items ~= 1 or
    recovered_attachments.items[1].name ~= "checkpoint.txt" then
  error("Lua stateful expired-claim recovery lost its checkpoint attachment")
end
-- Retry is a terminal transition for this delivery attempt, but not for the
-- command. It must preserve the same durable checkpoint for the next claimant.
assert_ok(stateful_job:retry({ delay_seconds = 1, diagnostic = "retry state" }), nil,
          "Lua stateful retry")
if pcall(function() return stateful_lease:info() end) then
  error("Lua stateful checkpoint view survived its retried job")
end
stateful_job = assert_ok(stateful_dispatcher:next_with_state(3000), nil,
                         "Lua stateful retry delivery")
stateful_lease = stateful_job:state()
local retried_checkpoint = assert_ok(stateful_lease:read_json(), nil,
                                    "Lua stateful retry checkpoint read")
if retried_checkpoint.checkpoint ~= 1 or retried_checkpoint.source ~= "lua" then
  error("Lua stateful retry lost its durable checkpoint")
end
local retried_attachments = assert_ok(stateful_lease:list_attachments(), nil,
                                     "Lua stateful retry attachment list")
if #retried_attachments.items ~= 1 or
    retried_attachments.items[1].name ~= "checkpoint.txt" then
  error("Lua stateful retry lost its checkpoint attachment")
end
assert_ok(stateful_job:dead_letter("exercise state replay"), nil,
          "Lua stateful dead letter")
if pcall(function() return stateful_lease:info() end) then
  error("Lua stateful checkpoint view survived its terminal job")
end
assert_ok(stateful_dispatcher:replay_dead_letter(stateful_key), nil,
          "Lua stateful replay")
stateful_job = assert_ok(stateful_dispatcher:next_with_state(3000), nil,
                         "Lua stateful replay delivery")
local replay_state = stateful_job:state()
local replay_checkpoint = assert_ok(replay_state:read_json(), nil,
                                   "Lua stateful replay checkpoint read")
if replay_checkpoint.checkpoint ~= 1 or replay_checkpoint.source ~= "lua" then
  error("Lua stateful replay did not retain its durable checkpoint")
end
local replay_attachments = assert_ok(replay_state:list_attachments(), nil,
                                    "Lua stateful replay attachment list")
if #replay_attachments.items ~= 1 or
    replay_attachments.items[1].name ~= "checkpoint.txt" then
  error("Lua stateful replay did not retain its checkpoint attachment")
end
-- Checkpoint update source callbacks must not be able to close, terminally
-- decide, or renew their owning job. The source close callback is still inside
-- the native update operation and therefore receives the same protection.
local checkpoint_update_reads = 0
local checkpoint_update_closes = 0
local function assert_checkpoint_job_is_busy(operation, message)
  local ok, err = pcall(operation)
  if ok or not tostring(err):find("not allowed while checkpoint is streaming", 1, true) then
    error(message)
  end
end
assert_ok(replay_state:update({
  read = function()
    checkpoint_update_reads = checkpoint_update_reads + 1
    assert_checkpoint_job_is_busy(function() stateful_job:close() end,
                                  "Lua checkpoint update source closed its job")
    assert_checkpoint_job_is_busy(function() stateful_job:complete() end,
                                  "Lua checkpoint update source completed its job")
    assert_checkpoint_job_is_busy(function() stateful_job:renew(1) end,
                                  "Lua checkpoint update source renewed its job")
    if checkpoint_update_reads == 1 then
      return '{"checkpoint":"callback-guard"}'
    end
    return nil
  end,
  close = function()
    checkpoint_update_closes = checkpoint_update_closes + 1
    assert_checkpoint_job_is_busy(function() stateful_job:close() end,
                                  "Lua checkpoint update close callback closed its job")
  end,
}), nil, "Lua stateful checkpoint callback update")
if checkpoint_update_reads < 2 or checkpoint_update_closes ~= 1 then
  error("Lua checkpoint update did not consume and close its callback source")
end
-- A checkpoint stream borrows the job's native state lease.  Re-entrant
-- closure must be rejected until the stream unwinds, or the active Pouch read
-- would dereference a released lease.
local checkpoint_streamed = 0
local checkpoint_output, checkpoint_meta = replay_state:get(nil, {
  write = function(chunk)
    checkpoint_streamed = checkpoint_streamed + #chunk
    local closed, close_err = pcall(function() stateful_job:close() end)
    if closed or not tostring(close_err):find(
        "not allowed while checkpoint is streaming", 1, true) then
      error("Lua checkpoint sink closed its active outbox job")
    end
    return true
  end,
})
if checkpoint_output ~= nil or type(checkpoint_meta) ~= "table" or
    checkpoint_streamed == 0 then
  error("Lua stateful checkpoint stream did not preserve sink semantics")
end
-- A checkpoint keeps its job alive while a source callback runs, but does not
-- form a permanent registry cycle. Dropping the last Lua job reference inside
-- the attachment source and forcing collection must leave the active native
-- checkpoint valid until attach has completely returned.
local checkpoint_attach_reads = 0
stateful_job = nil
assert_ok(replay_state:attach({
  name = "checkpoint-callback.txt",
  content_type = "text/plain",
}, {
  read = function()
    checkpoint_attach_reads = checkpoint_attach_reads + 1
    collectgarbage("collect")
    if checkpoint_attach_reads == 1 then
      return "callback-pinned-checkpoint"
    end
    return nil
  end,
}), nil, "Lua stateful checkpoint callback attachment")
if checkpoint_attach_reads < 2 then
  error("Lua checkpoint attachment did not consume its callback source")
end
replay_state:close()
if pcall(function() return replay_state:info() end) then
  error("Lua closed checkpoint view remained usable")
end
stateful_dispatcher:close()
stateful_outbox:close()

-- Handler failure retries the claimed job and consumes the native checkpoint
-- lease. A Lua closure may retain the borrowed checkpoint userdata, but it
-- must become a safe closed handle rather than retaining a freed C lease.
local escaped_state_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-escaped-state",
  owner = "lua-outbox-escaped-state-worker",
  recovery_interval_seconds = 0,
  dead_letter_retention_seconds = -1,
}), nil, "Lua escaped-state outbox creation")
local escaped_state_txn = assert_ok(escaped_state_outbox:append({
  operation_id = "lua-escaped-state-operation",
  effect_id = "lua-escaped-state-effect",
  effect_key = "lua-escaped-state-idempotency",
  payload_digest = "sha256:lua-escaped-state",
  kind = "escaped-state",
  destination = "https://example.test/lua-escaped-state",
}, "escaped-state-payload"), nil, "Lua escaped-state append")
assert_ok(escaped_state_txn:commit(), nil, "Lua escaped-state commit")
escaped_state_txn:close()
local escaped_state_dispatcher = assert_ok(escaped_state_outbox:dispatcher(), nil,
                                           "Lua escaped-state dispatcher")
local escaped_state = nil
local escaped_state_calls = 0
local escaped_state_handlers = {
  ["escaped-state"] = function(escaped_job)
    escaped_state_calls = escaped_state_calls + 1
    if escaped_state_calls == 1 then
      escaped_state = escaped_job:state()
      error("intentional escaped checkpoint failure")
    end
    return escaped_job:complete()
  end,
}
local escaped_state_pumped = assert_ok(escaped_state_dispatcher:pump({
  with_state = true,
  max_jobs = 1,
  timeout_ms = 3000,
  handlers = escaped_state_handlers,
}), nil, "Lua escaped-state failure pump")
if escaped_state_pumped ~= 1 or escaped_state == nil or
    pcall(function() return escaped_state:info() end) then
  error("Lua failed stateful handler retained a live checkpoint lease")
end
escaped_state_pumped = assert_ok(escaped_state_dispatcher:pump({
  with_state = true,
  max_jobs = 1,
  timeout_ms = 3000,
  handlers = escaped_state_handlers,
}), nil, "Lua escaped-state recovery pump")
if escaped_state_pumped ~= 1 or escaped_state_calls ~= 2 then
  error("Lua escaped-state handler did not recover its retry")
end
escaped_state_dispatcher:close()
escaped_state_outbox:close()

-- Managed pumping takes the same explicit opt-in. This exercises the Lua VM
-- callback path without allowing the C dispatcher's private thread to invoke
-- a Lua closure.
local managed_stateful_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-managed-stateful",
  owner = "lua-outbox-managed-stateful-worker",
  recovery_interval_seconds = 0,
}), nil, "Lua managed stateful outbox creation")
local managed_stateful_txn = assert_ok(managed_stateful_outbox:append({
  operation_id = "lua-managed-stateful-operation",
  effect_id = "lua-managed-stateful-effect",
  effect_key = "lua-managed-stateful-idempotency",
  payload_digest = "sha256:lua-managed-stateful",
  kind = "stateful-http",
  destination = "https://example.test/lua-managed-stateful",
}, "managed-stateful-payload"), nil, "Lua managed stateful append")
assert_ok(managed_stateful_txn:commit(), nil, "Lua managed stateful commit")
managed_stateful_txn:close()
local managed_stateful_dispatcher = assert_ok(managed_stateful_outbox:dispatcher(), nil,
                                              "Lua managed stateful dispatcher")
local managed_stateful_calls = 0
local managed_stateful_pumped = assert_ok(managed_stateful_dispatcher:pump({
  with_state = true,
  max_jobs = 1,
  timeout_ms = 3000,
  handlers = {
    ["stateful-http"] = function(managed_job)
      local managed_state = managed_job:state()
      if managed_state == nil then
        error("Lua managed stateful handler did not receive a checkpoint lease")
      end
      managed_state:update_json({ handled = true })
      managed_stateful_calls = managed_stateful_calls + 1
      return managed_job:complete()
    end,
  },
}), nil, "Lua managed stateful pump")
if managed_stateful_pumped ~= 1 or managed_stateful_calls ~= 1 then
  error("Lua managed stateful pump did not complete exactly one checkpointed job")
end
assert_ok(managed_stateful_dispatcher:stop(-1), nil,
          "Lua managed stateful dispatcher stop")
managed_stateful_dispatcher:close()
managed_stateful_outbox:close()
end

local inbound_txn, accepted_or_err = outbox:accept_inbox({
  consumer_id = "lua-outbox-consumer",
  source_kind = "http",
  source_id = "orders",
  message_id = "message-1",
  payload_digest = "digest-1",
})
if inbound_txn == nil or not accepted_or_err.accepted then
  outbox:close()
  client:close()
  error(("Lua inbox first acceptance failed: %s"):format(
    accepted_or_err and accepted_or_err.message or tostring(accepted_or_err)))
end
assert_ok(inbound_txn:append({
  operation_id = "lua-order-1",
  effect_id = "inbound-effect",
  effect_key = "lua-effect:inbound",
  payload_digest = "sha256:lua-inbound-payload",
  kind = "http",
  destination = "https://example.test/effects/inbound",
}, "inbound-payload"), nil, "Lua inbox transaction outbox append")
assert_ok(inbound_txn:commit(), nil, "Lua inbox transaction commit")
inbound_txn:close()

local duplicate_txn, duplicate_or_err = outbox:accept_inbox({
  consumer_id = "lua-outbox-consumer",
  source_kind = "http",
  source_id = "orders",
  message_id = "message-1",
  payload_digest = "digest-1",
})
if duplicate_txn ~= nil or duplicate_or_err == nil or not duplicate_or_err.duplicate then
  outbox:close()
  client:close()
  error("Lua inbox redelivery was not reported as a duplicate")
end

job = assert_ok(dispatcher:next(3000), nil, "Lua outbox inbound job")
if job:info().effect_key ~= "lua-effect:inbound" then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox returned the wrong inbox-triggered job")
end
assert_ok(job:complete(), nil, "Lua outbox inbound job completion")

txn = append_effect("retry", { sequence = 2 })
assert_ok(txn:commit(), nil, "Lua outbox retry transaction commit")
txn:close()
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox retry first job")
assert_ok(job:retry({ delay_seconds = 1, diagnostic = "temporary" }), nil,
          "Lua outbox retry scheduling")
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox retry redelivery")
if job:info().attempt ~= 2 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox retry did not increment the attempt")
end
local dead_letter_key = job:info().outbox_key
assert_ok(job:dead_letter("permanent"), nil, "Lua outbox dead letter")

local stats = assert_ok(dispatcher:stats(), nil, "Lua outbox dispatcher stats")
if not stats.running then
  outbox:close()
  client:close()
  error("Lua outbox stats did not report a running dispatcher")
end
if type(stats.dead_letter_reclaims) ~= "number" or
    type(stats.dead_letter_reclaim_failures) ~= "number" then
  outbox:close()
  client:close()
  error("Lua outbox stats omitted dead-letter retention counters")
end
local exported, export_result = dispatcher:read_dead_letters({ format = "jsonl" })
if exported == nil or export_result == nil or export_result.exported ~= 1 or
    not exported:find("dead_letter", 1, true) or
    exported:find("retry-payload", 1, true) then
  outbox:close()
  client:close()
  error("Lua outbox dead-letter export did not return a metadata-only envelope")
end
assert_ok(dispatcher:replay_dead_letter(dead_letter_key), nil,
          "Lua outbox dead-letter replay")
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox replayed job")
if job:info().effect_key ~= "lua-effect:retry" or job:info().attempt ~= 1 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox dead-letter replay did not reset the delivery attempt")
end
assert_ok(job:complete(), nil, "Lua outbox replayed completion")
assert_ok(dispatcher:reconcile(), nil, "Lua outbox reconciliation signal")

-- A delayed retry must leave one delayed wake-up, not repeatedly trigger
-- durable recovery while this single next() call waits for its deadline.
txn = append_effect("retry-spin", { sequence = 3 })
assert_ok(txn:commit(), nil, "Lua outbox retry-spin transaction commit")
txn:close()
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox retry-spin first job")
assert_ok(job:retry({ delay_seconds = 30, diagnostic = "delayed" }), nil,
          "Lua outbox retry-spin scheduling")
local retry_spin_before = assert_ok(dispatcher:stats(), nil,
                                   "Lua outbox retry-spin stats before")
local retry_spin_job, retry_spin_err = dispatcher:next(250)
if retry_spin_job ~= nil or retry_spin_err ~= nil then
  error("Lua outbox retry-spin returned work before its retry deadline")
end
local retry_spin_after = assert_ok(dispatcher:stats(), nil,
                                  "Lua outbox retry-spin stats after")
if retry_spin_after.recovery_queries - retry_spin_before.recovery_queries > 4 then
  error(("Lua outbox delayed retry repeatedly scanned durable outbox state (%d queries; %s)"):
      format(retry_spin_after.recovery_queries - retry_spin_before.recovery_queries,
             tostring(retry_spin_after.last_error)))
end

assert_ok(dispatcher:stop(-1), nil, "Lua outbox dispatcher stop")
dispatcher:close()
outbox:close()

-- Handler mode has its own dispatcher.  It deliberately shares neither a
-- wrapper nor consumption mode with the raw-pull dispatcher above.
local handler_outbox, handler_outbox_err = client:new_outbox({
  namespace = "lua-outbox-handler-records",
  owner = "lua-outbox-handler-worker",
  transaction_ttl_seconds = 30,
  claim_ttl_seconds = 30,
  recovery_interval_seconds = 0,
})
handler_outbox = assert_ok(handler_outbox, handler_outbox_err,
                             "Lua handler outbox creation")
local handler_dispatcher, handler_dispatcher_err = handler_outbox:dispatcher()
handler_dispatcher = assert_ok(handler_dispatcher, handler_dispatcher_err,
                               "Lua handler dispatcher creation")

local function append_handler_effect(effect_id, kind, payload)
  local handler_txn, handler_receipt_or_err = handler_outbox:append({
    operation_id = "lua-handler-order",
    effect_id = effect_id,
    effect_key = "lua-handler-effect:" .. effect_id,
    payload_digest = "sha256:lua-handler:" .. effect_id,
    kind = kind,
    destination = "https://example.test/handler/" .. effect_id,
    content_type = "application/json",
  }, lockdc.encode_json(payload))
  handler_txn = assert_ok(handler_txn, handler_receipt_or_err,
                          "Lua handler outbox append")
  if handler_receipt_or_err.duplicate then
    handler_txn:close()
    error("fresh Lua handler outbox append unexpectedly reported duplicate")
  end
  assert_ok(handler_txn:commit(), nil, "Lua handler transaction commit")
  handler_txn:close()
end

-- A valid Lua handler map must not be remembered until native option
-- validation has accepted the pump.  Otherwise a rejected call would make the
-- following valid call invoke a stale snapshot rather than the corrected
-- application handler.
local handler_calls = {}
local rejected_handler_map = {
  http = function()
    handler_calls.stale_after_rejection = true
  end,
}
local rejected_pump_ok, rejected_pump_err = pcall(function()
  return handler_dispatcher:pump({
    handlers = rejected_handler_map,
    max_jobs = 0,
  })
end)
if rejected_pump_ok or not tostring(rejected_pump_err):find("pump limits are invalid", 1, true) then
  handler_dispatcher:close()
  handler_outbox:close()
  client:close()
  error("Lua dispatcher accepted an invalid native pump option")
end
local infinite_pump_ok, infinite_pump_err = pcall(function()
  return handler_dispatcher:pump({
    handlers = rejected_handler_map,
    max_jobs = 1,
    timeout_ms = -1,
  })
end)
if infinite_pump_ok or
    not tostring(infinite_pump_err):find("pump limits are invalid", 1, true) then
  handler_dispatcher:close()
  handler_outbox:close()
  client:close()
  error("Lua dispatcher accepted an unbounded pump timeout")
end

-- Lua pump follows the public dispatcher next() timeout contract. Its pull
-- deadline is independent from the remote-request shutdown timeout retained
-- in the outbox configuration, and must not require a private C accessor.
do
  local independent_timeout_outbox = assert_ok(client:new_outbox({
    namespace = "lua-outbox-pump-timeout-contract",
    shutdown_timeout_ms = 1,
  }), nil, "Lua independent-pump-timeout outbox creation")
  local independent_timeout_dispatcher = assert_ok(
    independent_timeout_outbox:dispatcher(), nil,
    "Lua independent-pump-timeout dispatcher creation")
  local independent_pump, independent_pump_err =
    independent_timeout_dispatcher:pump({
      handlers = { noop = function() error("unexpected independent-timeout job") end },
      max_jobs = 1,
      timeout_ms = 2,
    })
  if independent_pump ~= 0 or independent_pump_err ~= nil then
    independent_timeout_dispatcher:close()
    independent_timeout_outbox:close()
    handler_dispatcher:close()
    handler_outbox:close()
    client:close()
    error("Lua dispatcher coupled pump timeout to shutdown_timeout_ms")
  end
  independent_timeout_dispatcher:close()
  independent_timeout_outbox:close()
end

local handlers = rejected_handler_map
handlers.http = function(handler_job)
    local handler_info = handler_job:info()

    if handler_job:state() ~= nil then
      error("Lua stateless dispatcher handler unexpectedly exposed job state")
    end
    handler_calls.started = (handler_calls.started or 0) + 1
    local payload, payload_err = handler_job:read_payload_json()
    payload = assert_ok(payload, payload_err, "Lua handler payload decode")
    if payload.effect ~= handler_info.effect_key then
      error("Lua handler payload did not match its claimed job")
    end
    handler_calls[handler_info.effect_key] =
      (handler_calls[handler_info.effect_key] or 0) + 1
    assert_ok(handler_job:renew(30), nil, "Lua handler explicit claim renewal")
    if payload.mode == "no-outcome" and handler_info.attempt == 1 then
      return
    end
    if payload.mode == "exception" and handler_info.attempt == 1 then
      error("intentional Lua handler failure")
    end
    if payload.mode == "long-exception" and handler_info.attempt == 1 then
      error(string.rep("x", 5000))
    end
    if payload.mode == "stop" and handler_info.attempt == 1 then
      assert_ok(handler_job:complete(), nil,
                "Lua handler stop-rejection deferred completion")
      local stopped, stop_err, stop_code = handler_dispatcher:stop(0)
      if stopped ~= nil or stop_err == nil or stop_code == nil or
          not tostring(stop_err.message):find("not allowed inside its handler", 1, true) then
        error("Lua outbox handler was allowed to block its own dispatcher stop")
      end
      return
    end
    if payload.mode == "close" and handler_info.attempt == 1 then
      local closed, close_err = pcall(function()
        handler_job:close()
      end)
      if closed or not tostring(close_err):find("not allowed inside its dispatcher handler", 1, true) then
        error("Lua handler was allowed to close its dispatcher-owned job")
      end
      return
    end
    if payload.mode == "dispatcher-close" and handler_info.attempt == 1 then
      local closed, close_err = pcall(function()
        handler_dispatcher:close()
      end)
      if closed or not tostring(close_err):find("not allowed inside its handler", 1, true) then
        error("Lua handler was allowed to close its consuming dispatcher")
      end
      assert_ok(handler_job:complete(), nil,
                "Lua handler dispatcher-close completion")
      return
    end
    if payload.mode == "reentrant" and handler_info.attempt == 1 then
      for nested_attempt = 1, 2 do
        local nested, nested_err = handler_dispatcher:pump({
          handlers = handlers,
          max_jobs = 1,
          timeout_ms = 0,
        })
        if nested ~= nil or nested_err == nil or
            not tostring(nested_err.message):find("cannot consume recursively", 1, true) then
          error("Lua dispatcher allowed recursive handler consumption")
        end
      end
    end
    if payload.mode == "invalid-outcome" and handler_info.attempt == 1 then
      return handler_job:retry({ delay_seconds = "invalid" })
    end
    if payload.mode == "dead-letter" then
      assert_ok(handler_job:dead_letter("handler permanent failure"), nil,
                "Lua handler dead letter")
      return
    end
    assert_ok(handler_job:complete({ delivery_reference = "lua-handler" }), nil,
              "Lua handler completion")
end

append_handler_effect("complete", "http", {
  effect = "lua-handler-effect:complete",
  mode = "complete",
})
local pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler completion pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:complete"] ~= 1 or
    handler_calls.stale_after_rejection then
  error("Lua handler completion did not execute exactly once")
end

append_handler_effect("reentrant", "http", {
  effect = "lua-handler-effect:reentrant",
  mode = "reentrant",
})
append_handler_effect("reentrant-followup", "http", {
  effect = "lua-handler-effect:reentrant-followup",
  mode = "complete",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler reentrant pump rejection")
if pumped ~= 1 or handler_calls["lua-handler-effect:reentrant"] ~= 1 then
  error("Lua handler recursive dispatcher call was not rejected safely")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler reentrant followup pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:reentrant-followup"] ~= 1 then
  error("Lua handler recursive dispatcher call consumed a second job")
end

append_handler_effect("dispatcher-close", "http", {
  effect = "lua-handler-effect:dispatcher-close",
  mode = "dispatcher-close",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler dispatcher-close rejection")
if pumped ~= 1 or handler_calls["lua-handler-effect:dispatcher-close"] ~= 1 then
  error("Lua handler dispatcher-close rejection did not preserve its job")
end

local raw_after_handler_ok = pcall(function()
  return handler_dispatcher:next(0)
end)
if raw_after_handler_ok then
  error("Lua dispatcher allowed raw pull after handler activation")
end

append_handler_effect("no-outcome", "http", {
  effect = "lua-handler-effect:no-outcome",
  mode = "no-outcome",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler no-outcome retry pump")
if pumped ~= 1 then
  error("Lua handler no-outcome did not schedule a retry")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler no-outcome redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:no-outcome"] ~= 2 then
  error("Lua handler no-outcome was not retried exactly once")
end

append_handler_effect("exception", "http", {
  effect = "lua-handler-effect:exception",
  mode = "exception",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler exception retry pump")
if pumped ~= 1 then
  error("Lua handler exception did not schedule a retry")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler exception redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:exception"] ~= 2 then
  error("Lua handler exception was not retried exactly once")
end

append_handler_effect("long-exception", "http", {
  effect = "lua-handler-effect:long-exception",
  mode = "long-exception",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler long exception retry pump")
if pumped ~= 1 then
  error("Lua handler long exception did not schedule a bounded retry")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler long exception redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:long-exception"] ~= 2 then
  error("Lua handler long exception stranded its claimed job")
end

append_handler_effect("stop", "http", {
  effect = "lua-handler-effect:stop",
  mode = "stop",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler stop rejection pump")
if pumped ~= 1 then
  error("Lua handler stop rejection did not preserve its deferred outcome")
end

append_handler_effect("invalid-outcome", "http", {
  effect = "lua-handler-effect:invalid-outcome",
  mode = "invalid-outcome",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler invalid-outcome retry pump")
if pumped ~= 1 then
  error("Lua handler invalid outcome did not follow the retry path")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler invalid-outcome redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:invalid-outcome"] ~= 2 then
  error("Lua handler invalid outcome stranded its claimed job")
end

append_handler_effect("close", "http", {
  effect = "lua-handler-effect:close",
  mode = "close",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler close rejection pump")
if pumped ~= 1 then
  error("Lua handler close rejection did not retry safely")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler close redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:close"] ~= 2 then
  error("Lua handler close rejection was not safely redelivered")
end

append_handler_effect("dead-letter", "http", {
  effect = "lua-handler-effect:dead-letter",
  mode = "dead-letter",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler dead-letter pump")
if pumped ~= 1 then
  error("Lua handler dead-letter outcome was not applied")
end
local handler_export, handler_export_res = handler_dispatcher:read_dead_letters({
  format = "jsonl",
})
if handler_export == nil or handler_export_res == nil or
    handler_export_res.exported ~= 1 or
    not handler_export:find("handler permanent failure", 1, true) then
  error("Lua handler dead-letter export lost its terminal diagnostic")
end

assert_ok(handler_dispatcher:stop(-1), nil, "Lua handler dispatcher stop")
handler_dispatcher:close()
handler_outbox:close()

-- A failed C terminal operation must release the handler-owned claim before
-- pump returns. Dispatcher shutdown must not depend on a later Lua GC pass.
local terminal_failure_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-terminal-failure",
  recovery_interval_seconds = 0,
}), nil, "Lua terminal-failure outbox creation")
local terminal_failure_dispatcher = assert_ok(terminal_failure_outbox:dispatcher(), nil,
                                              "Lua terminal-failure dispatcher creation")
local terminal_failure_txn, terminal_failure_receipt_or_err =
  terminal_failure_outbox:append({
    operation_id = "lua-terminal-failure-order",
    effect_id = "terminal-failure",
    effect_key = "lua-terminal-failure-effect",
    payload_digest = "sha256:lua-terminal-failure",
    kind = "http",
    destination = "https://example.test/terminal-failure",
  }, "terminal-failure-payload")
terminal_failure_txn = assert_ok(terminal_failure_txn, terminal_failure_receipt_or_err,
                                 "Lua terminal-failure append")
assert_ok(terminal_failure_txn:commit(), nil, "Lua terminal-failure commit")
terminal_failure_txn:close()
local terminal_failure_pumped, terminal_failure_err = terminal_failure_dispatcher:pump({
  handlers = {
    http = function(job)
      return job:retry({ delay_seconds = -1 })
    end,
  },
  max_jobs = 1,
  timeout_ms = 3000,
})
if terminal_failure_pumped ~= nil or terminal_failure_err == nil then
  error("Lua terminal-failure handler did not surface its invalid retry")
end
assert_ok(terminal_failure_dispatcher:stop(100), nil,
          "Lua terminal-failure dispatcher immediate stop")
terminal_failure_dispatcher:close()
terminal_failure_outbox:close()

-- A handler exception after selecting an outcome must release the registry
-- reference, including a value that closes over the handler-owned job.
local outcome_failure_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-outcome-failure",
  recovery_interval_seconds = 0,
}), nil, "Lua outcome-failure outbox creation")
local outcome_failure_dispatcher = assert_ok(outcome_failure_outbox:dispatcher(), nil,
                                             "Lua outcome-failure dispatcher creation")
local outcome_failure_txn, outcome_failure_receipt_or_err =
  outcome_failure_outbox:append({
    operation_id = "lua-outcome-failure-order",
    effect_id = "outcome-failure",
    effect_key = "lua-outcome-failure-effect",
    payload_digest = "sha256:lua-outcome-failure",
    kind = "http",
    destination = "https://example.test/outcome-failure",
  }, "outcome-failure-payload")
outcome_failure_txn = assert_ok(outcome_failure_txn, outcome_failure_receipt_or_err,
                                "Lua outcome-failure append")
assert_ok(outcome_failure_txn:commit(), nil, "Lua outcome-failure commit")
outcome_failure_txn:close()
local weak_outcomes = setmetatable({}, { __mode = "v" })
assert_ok(outcome_failure_dispatcher:pump({
  handlers = {
    http = function(job)
      local outcome = { context = job }
      weak_outcomes[1] = outcome
      assert_ok(job:complete(outcome), nil, "Lua deferred outcome selection")
      error("intentional Lua deferred outcome failure")
    end,
  },
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua deferred outcome fallback retry")
collectgarbage("collect")
collectgarbage("collect")
if weak_outcomes[1] ~= nil then
  error("Lua deferred outcome failure retained its outcome graph")
end
assert_ok(outcome_failure_dispatcher:stop(100), nil,
          "Lua outcome-failure dispatcher stop")
outcome_failure_dispatcher:close()
outcome_failure_outbox:close()

local missing_outbox, missing_outbox_err = client:new_outbox({
  namespace = "lua-outbox-missing-handler-records",
  owner = "lua-outbox-missing-handler-worker",
  transaction_ttl_seconds = 30,
  claim_ttl_seconds = 1,
  recovery_interval_seconds = 0,
})
missing_outbox = assert_ok(missing_outbox, missing_outbox_err,
                             "Lua missing-handler outbox creation")
local missing_dispatcher, missing_dispatcher_err = missing_outbox:dispatcher()
missing_dispatcher = assert_ok(missing_dispatcher, missing_dispatcher_err,
                               "Lua missing-handler dispatcher creation")
local missing_txn, missing_receipt_or_err = missing_outbox:append({
  operation_id = "lua-missing-handler-order",
  effect_id = "missing-handler",
  effect_key = "lua-missing-handler-effect",
  payload_digest = "sha256:lua-missing-handler",
  kind = "unhandled-kind",
  destination = "https://example.test/handler/missing",
}, "missing-handler-payload")
missing_txn = assert_ok(missing_txn, missing_receipt_or_err,
                        "Lua missing-handler outbox append")
assert_ok(missing_txn:commit(), nil, "Lua missing-handler transaction commit")
missing_txn:close()
local missing_handlers = setmetatable({
  http = function()
    error("Lua missing-handler test invoked an unrelated handler")
  end,
}, {
  __index = function()
    error("Lua dispatcher invoked a handler-map metamethod")
  end,
})
local missing_pumped, missing_pump_err = missing_dispatcher:pump({
  handlers = missing_handlers,
  max_jobs = 1,
  timeout_ms = 3000,
})
if missing_pumped ~= nil or missing_pump_err == nil or
    not tostring(missing_pump_err.message):find("no handler", 1, true) then
  error("Lua handler dispatcher did not return a structured missing-handler error")
end
assert_ok(missing_dispatcher:stop(-1), nil, "Lua missing-handler dispatcher stop")
missing_dispatcher:close()
missing_outbox:close()

-- Separate façade wrappers for one native dispatcher must be able to reuse the
-- same application handler table. The binding owns a stable wrapped map.
do
local alias_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-alias",
  owner = "lua-outbox-dispatcher-alias-worker",
  recovery_interval_seconds = 0,
}), nil, "Lua dispatcher alias outbox creation")
local alias_dispatcher = assert_ok(alias_outbox:dispatcher(), nil,
                                   "Lua first dispatcher alias")
local alias_dispatcher_again = assert_ok(alias_outbox:dispatcher(), nil,
                                         "Lua second dispatcher alias")
local alias_calls = 0
local alias_capture = {}
local alias_capture_weak = setmetatable({}, { __mode = "v" })
alias_capture_weak[1] = alias_capture
local alias_handlers = {
  http = function(alias_job)
    alias_calls = alias_calls + 1
    alias_capture.calls = alias_calls
    assert_ok(alias_job:complete(), nil, "Lua alias handler completion")
  end,
}
local alias_txn, alias_receipt_or_err = alias_outbox:append({
  operation_id = "lua-alias-order",
  effect_id = "alias",
  effect_key = "lua-alias-effect",
  payload_digest = "sha256:lua-alias",
  kind = "http",
  destination = "https://example.test/alias",
}, "alias-payload")
alias_txn = assert_ok(alias_txn, alias_receipt_or_err,
                      "Lua dispatcher alias outbox append")
assert_ok(alias_txn:commit(), nil, "Lua dispatcher alias transaction commit")
alias_txn:close()
local alias_pumped = assert_ok(alias_dispatcher:pump({
  handlers = alias_handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua first dispatcher alias pump")
if alias_pumped ~= 1 or alias_calls ~= 1 then
  error("Lua first dispatcher alias did not run its shared handler")
end
alias_dispatcher:close()
alias_dispatcher = nil
collectgarbage("collect")
collectgarbage("collect")
alias_pumped = assert_ok(alias_dispatcher_again:pump({
  handlers = alias_handlers,
  max_jobs = 1,
  timeout_ms = 0,
}), nil, "Lua second dispatcher alias pump")
if alias_pumped ~= 0 then
  error("Lua second dispatcher alias unexpectedly consumed another job")
end
alias_dispatcher_again:close()
alias_handlers.http = function(alias_job)
  alias_calls = alias_calls + 1
  alias_capture.calls = alias_calls
  assert_ok(alias_job:complete(), nil, "Lua replacement alias handler completion")
end
alias_txn, alias_receipt_or_err = alias_outbox:append({
  operation_id = "lua-alias-rebound-order",
  effect_id = "alias-rebound",
  effect_key = "lua-alias-rebound-effect",
  payload_digest = "sha256:lua-alias-rebound",
  kind = "http",
  destination = "https://example.test/alias-rebound",
}, "alias-rebound-payload")
alias_txn = assert_ok(alias_txn, alias_receipt_or_err,
                      "Lua replacement dispatcher alias append")
assert_ok(alias_txn:commit(), nil,
          "Lua replacement dispatcher alias transaction commit")
alias_txn:close()
local alias_reopened = assert_ok(alias_outbox:dispatcher(), nil,
                                 "Lua reopened dispatcher alias")
local alias_rebound, alias_rebound_err = alias_reopened:pump({
  handlers = alias_handlers,
  max_jobs = 1,
  timeout_ms = 3000,
})
if alias_rebound ~= 1 or alias_rebound_err ~= nil or alias_calls ~= 2 then
  error("Lua dispatcher retained a stale handler after every wrapper closed")
end
assert_ok(alias_reopened:stop(-1), nil, "Lua dispatcher alias stop")
alias_reopened:close()
alias_outbox:close()
alias_handlers = nil
alias_capture = nil
collectgarbage("collect")
collectgarbage("collect")
if alias_capture_weak[1] ~= nil then
  error("Lua dispatcher close retained handler captures")
end
end

-- Closing the wrapper that entered pump() from its own handler must not leave
-- the shared binding in recursive-consumption state. A surviving alias owns
-- the same native dispatcher and must continue with the same handler map.
local closing_alias_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-close-alias",
  owner = "lua-outbox-dispatcher-close-alias-worker",
  recovery_interval_seconds = 0,
}), nil, "Lua closing dispatcher alias outbox creation")
local closing_alias_first = assert_ok(closing_alias_outbox:dispatcher(), nil,
                                     "Lua closing dispatcher first alias")
local closing_alias_second = assert_ok(closing_alias_outbox:dispatcher(), nil,
                                      "Lua closing dispatcher second alias")
local closing_alias_calls = 0
local closing_alias_handlers = {
  http = function(closing_alias_job)
    closing_alias_calls = closing_alias_calls + 1
    if closing_alias_calls == 1 then
      closing_alias_first:close()
    end
    assert_ok(closing_alias_job:complete(), nil,
              "Lua closing dispatcher alias handler completion")
  end,
}
for closing_alias_index = 1, 2 do
  local closing_alias_txn, closing_alias_receipt_or_err =
      closing_alias_outbox:append({
        operation_id = "lua-closing-alias-order-" .. closing_alias_index,
        effect_id = "closing-alias-" .. closing_alias_index,
        effect_key = "lua-closing-alias-effect-" .. closing_alias_index,
        payload_digest = "sha256:lua-closing-alias-" .. closing_alias_index,
        kind = "http",
        destination = "https://example.test/closing-alias",
      }, "closing-alias-payload")
  closing_alias_txn = assert_ok(closing_alias_txn, closing_alias_receipt_or_err,
                                "Lua closing dispatcher alias append")
  assert_ok(closing_alias_txn:commit(), nil,
            "Lua closing dispatcher alias transaction commit")
  closing_alias_txn:close()
  local closing_alias_pumped = assert_ok(
      (closing_alias_index == 1 and closing_alias_first or closing_alias_second):pump({
        handlers = closing_alias_handlers,
        max_jobs = 1,
        timeout_ms = 3000,
      }), nil, "Lua closing dispatcher alias pump")
  if closing_alias_pumped ~= 1 then
    error("Lua closing dispatcher alias did not consume its effect")
  end
end
if closing_alias_calls ~= 2 then
  error("Lua closing dispatcher alias retained recursive consumption state")
end
assert_ok(closing_alias_second:stop(-1), nil,
          "Lua closing dispatcher alias stop")
closing_alias_second:close()
closing_alias_outbox:close()

-- Options-table metamethod failures happen before consumption begins. After a
-- caller catches the Lua error, a valid handler mode must still be usable.
local options_failure_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-options-failure",
  recovery_interval_seconds = 0,
}), nil, "Lua dispatcher options-failure outbox creation")
local options_failure_dispatcher = assert_ok(options_failure_outbox:dispatcher(), nil,
                                             "Lua dispatcher options-failure creation")
local bad_pump_options = setmetatable({ max_jobs = 1, timeout_ms = 0 }, {
  __index = function(_, key)
    if key == "handlers" then
      error("intentional Lua outbox handlers lookup failure")
    end
  end,
})
local bad_pump_ok = pcall(function()
  options_failure_dispatcher:pump(bad_pump_options)
end)
if bad_pump_ok then
  error("Lua dispatcher options metamethod failure did not escape pump")
end
local options_failure_pumped = assert_ok(options_failure_dispatcher:pump({
  handlers = { http = function() end },
  max_jobs = 1,
  timeout_ms = 0,
}), nil, "Lua dispatcher remained usable after options failure")
if options_failure_pumped ~= 0 then
  error("Lua dispatcher options-failure probe unexpectedly consumed work")
end
assert_ok(options_failure_dispatcher:stop(-1), nil,
          "Lua dispatcher options-failure stop")
options_failure_dispatcher:close()
options_failure_outbox:close()

-- Binding acknowledges the facade only on its private field. That assignment
-- must be raw: a caller-controlled __newindex hook cannot longjmp after the
-- immutable handler map has been installed and strand the dispatcher halfway
-- through activation.
local newindex_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-newindex",
  recovery_interval_seconds = 0,
}), nil, "Lua dispatcher newindex outbox creation")
local newindex_dispatcher = assert_ok(newindex_outbox:dispatcher(), nil,
                                      "Lua dispatcher newindex creation")
local hostile_newindex_options = setmetatable({
  handlers = { http = function() end },
  max_jobs = 1,
  timeout_ms = 0,
}, {
  __newindex = function()
    error("dispatcher binding must not invoke options __newindex")
  end,
})
local hostile_newindex_pumped, hostile_newindex_err =
    newindex_dispatcher._core:pump(hostile_newindex_options)
if hostile_newindex_pumped ~= 0 or hostile_newindex_err ~= nil then
  error("Lua dispatcher binding invoked the options __newindex hook")
end
assert_ok(newindex_dispatcher:stop(-1), nil,
          "Lua dispatcher newindex stop")
newindex_dispatcher:close()
newindex_outbox:close()

-- The scoped callback owns transaction completion. Explicit close is rejected
-- so the binding cannot commit or roll back a freed native transaction.
local callback_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-callback-close",
}), nil, "Lua callback outbox creation")
local callback_close_ok, callback_close_err = pcall(function()
  callback_outbox:transaction(function(callback_txn)
    callback_txn:close()
  end)
end)
if callback_close_ok or
    not tostring(callback_close_err):find("not allowed inside its callback", 1, true) then
  error("Lua scoped transaction allowed callback-driven close")
end
local callback_terminal_result, callback_terminal_err = callback_outbox:transaction(function(callback_txn)
  local committed, commit_err = pcall(function() callback_txn:commit() end)
  if committed or not tostring(commit_err):find("not allowed inside its callback", 1, true) then
    error("Lua scoped transaction allowed callback-driven commit")
  end
  local rolled_back, rollback_err = pcall(function() callback_txn:rollback() end)
  if rolled_back or not tostring(rollback_err):find("not allowed inside its callback", 1, true) then
    error("Lua scoped transaction allowed callback-driven rollback")
  end
  assert_ok(callback_txn:append({
    operation_id = "lua-callback-terminal-order",
    effect_id = "callback-terminal",
    effect_key = "lua-callback-terminal-effect",
    payload_digest = "sha256:lua-callback-terminal",
    kind = "http",
    destination = "https://example.test/callback-terminal",
  }, "callback-terminal-payload"), nil, "Lua callback terminal outbox append")
  -- Callback values are not transaction results. In particular, an explicit
  -- nil must not precede the commit result and make normal result/error callers
  -- interpret this durable success as failure.
  return nil
end)
if callback_terminal_result == nil or callback_terminal_err ~= nil or
    #callback_terminal_result.outbox_receipts ~= 1 then
  error("Lua callback façade did not retain exclusive transaction completion")
end

-- A callback participant mutation returns one Lua result on success. Its
-- return count is not an lc_status and must never make the callback rollback.
local callback_participant_result, callback_participant_err =
    callback_outbox:transaction(function(callback_txn)
  local participant = assert_ok(callback_txn:acquire({
    key = "callback-successful-participant-domain",
    owner = "lua-callback-successful-participant",
    ttl_seconds = 30,
  }), nil, "Lua callback successful participant acquire")
  assert_ok(participant:update_json({ committed = true }), nil,
            "Lua callback successful participant update")
  participant:close()
end)
if callback_participant_result == nil or callback_participant_err ~= nil then
  error("Lua callback rolled back a successful participant update")
end
-- Client:read_json addresses the client's default namespace. Read the distinct
-- outbox namespace through a fresh lease, which also proves the state was
-- committed rather than merely remaining visible to the callback participant.
local callback_participant_reader = assert_ok(client:acquire({
  namespace = "lua-outbox-callback-close",
  key = "callback-successful-participant-domain",
  owner = "lua-callback-successful-participant-reader",
  ttl_seconds = 30,
}), nil, "Lua callback successful participant durable read acquire")
local callback_participant_state, callback_participant_meta =
    callback_participant_reader:read_json()
assert_ok(callback_participant_reader:release({ rollback = true }), nil,
          "Lua callback successful participant durable read release")
if callback_participant_state == nil or callback_participant_meta == nil or
    callback_participant_state.committed ~= true then
  error("Lua callback did not persist a successful participant update")
end

local callback_seed_txn = assert_ok(callback_outbox:append({
  operation_id = "lua-callback-duplicate-order",
  effect_id = "callback-duplicate",
  effect_key = "lua-callback-duplicate-effect",
  payload_digest = "sha256:lua-callback-duplicate",
  kind = "http",
  destination = "https://example.test/callback-duplicate",
}, "callback-duplicate-payload"), nil, "Lua callback duplicate seed append")
assert_ok(callback_seed_txn:commit(), nil, "Lua callback duplicate seed commit")
callback_seed_txn:close()
local callback_duplicate, callback_duplicate_err = callback_outbox:transaction(function(callback_txn)
  -- Deliberately do not return this receipt. The callback wrapper must retain
  -- the duplicate result because it is the only durable outcome of this
  -- no-participant transaction.
  assert_ok(callback_txn:append({
    operation_id = "lua-callback-duplicate-order",
    effect_id = "callback-duplicate",
    effect_key = "lua-callback-duplicate-effect",
    payload_digest = "sha256:lua-callback-duplicate",
    kind = "http",
    destination = "https://example.test/callback-duplicate",
  }, "callback-duplicate-payload"), nil, "Lua callback duplicate append")
end)
if callback_duplicate == nil or callback_duplicate_err ~= nil or
    not callback_duplicate.duplicate then
  error("Lua callback façade did not return its duplicate-only durable result")
end

local callback_rollback, callback_rollback_err = callback_outbox:transaction(function(callback_txn)
  local participant = assert_ok(callback_txn:acquire({
    key = "callback-conflicting-outbox-domain",
    owner = "lua-callback-conflicting-outbox",
    ttl_seconds = 30,
  }), nil, "Lua callback conflicting outbox acquire")
  assert_ok(participant:update_json({ committed = false }), nil,
            "Lua callback conflicting outbox update")
  participant:close()
  local append_result, append_err = callback_txn:append({
    operation_id = "lua-callback-duplicate-order",
    effect_id = "callback-duplicate",
    effect_key = "lua-callback-duplicate-effect",
    payload_digest = "sha256:lua-callback-duplicate-conflict",
    kind = "http",
    destination = "https://example.test/callback-duplicate",
  }, "callback-duplicate-conflict-payload")
  if append_result ~= nil or append_err == nil then
    error("Lua callback conflicting outbox append unexpectedly succeeded")
  end
  -- Returning normally must still roll back the earlier domain participant.
end)
if callback_rollback ~= nil or callback_rollback_err == nil then
  error("Lua callback committed after an ignored staging failure")
end
local callback_rollback_state, callback_rollback_meta =
    client:read_json({
      namespace = "lua-outbox-callback-close",
      key = "callback-conflicting-outbox-domain",
      public_read = true,
    })
if callback_rollback_state ~= nil or callback_rollback_meta == nil or
    not callback_rollback_meta.no_content then
  error("Lua callback staging failure persisted prior domain state")
end

do
  -- A validation exception can be caught by user code. It is nevertheless a
  -- failed staging operation, so the callback remains rollback-only and cannot
  -- commit the earlier domain participant without its required outbox effect.
  local callback_validation_rollback, callback_validation_rollback_err =
      callback_outbox:transaction(function(callback_txn)
  local participant = assert_ok(callback_txn:acquire({
    key = "callback-caught-validation-domain",
    owner = "lua-callback-caught-validation",
    ttl_seconds = 30,
  }), nil, "Lua callback caught-validation acquire")
  assert_ok(participant:update_json({ committed = false }), nil,
            "Lua callback caught-validation update")
  participant:close()
  local caught = pcall(function()
    callback_txn:append({}, "ignored")
  end)
  if caught then
    error("Lua callback malformed append unexpectedly succeeded")
  end
  end)
  if callback_validation_rollback ~= nil or
      callback_validation_rollback_err == nil then
    error("Lua callback committed after a caught validation error")
  end
  local callback_validation_state, callback_validation_meta = client:read_json({
    namespace = "lua-outbox-callback-close",
    key = "callback-caught-validation-domain",
    public_read = true,
  })
  if callback_validation_state ~= nil or callback_validation_meta == nil or
      not callback_validation_meta.no_content then
    error("Lua callback validation error persisted prior domain state")
  end

  -- Metatable-backed inbox fields may allocate and collect while subsequent
  -- fields are resolved. The binding roots every normalized value until native
  -- acceptance returns.
  local rooted_inbox_txn = assert_ok(callback_outbox:begin(), nil,
                                     "Lua rooted inbox transaction begin")
  local rooted_inbox_request = setmetatable({}, {
  __index = function(_, field)
    collectgarbage("collect")
    if field == "consumer_id" then return string.rep("consumer-", 32) end
    if field == "source_kind" then return string.rep("kind-", 32) end
    if field == "source_id" then return string.rep("source-", 32) end
    if field == "message_id" then return string.rep("message-", 32) end
    return nil
  end,
  })
  assert_ok(rooted_inbox_txn:accept_inbox(rooted_inbox_request), nil,
            "Lua rooted inbox acceptance")
  assert_ok(rooted_inbox_txn:commit(), nil, "Lua rooted inbox commit")
  rooted_inbox_txn:close()
end

local callback_command_rollback, callback_command_rollback_err =
    callback_outbox:transaction(function(callback_txn)
  assert_ok(callback_txn:accept_command({
    scope = "lua-callback-command-source-failure",
    command_type = "complete",
    idempotency_key = "one",
    request_digest = "sha256:lua-callback-command-source-failure",
  }), nil, "Lua callback command acceptance")
  local participant = assert_ok(callback_txn:acquire({
    key = "callback-command-source-failure-domain",
    owner = "lua-callback-command-source-failure",
    ttl_seconds = 30,
  }), nil, "Lua callback command source failure acquire")
  assert_ok(participant:update_json({ committed = false }), nil,
            "Lua callback command source failure update")
  participant:close()
  local completed, complete_err = callback_txn:complete_command({
    result_code = "ok",
    content_type = "text/plain",
    body = { path = root .. "/missing-command-result" },
  })
  if completed ~= nil or complete_err == nil then
    error("Lua callback command source failure unexpectedly completed")
  end
  -- Returning normally after a source-construction failure is rollback-only.
end)
if callback_command_rollback ~= nil or callback_command_rollback_err == nil then
  error("Lua callback committed after a command result source failure")
end
local callback_command_state, callback_command_meta = client:read_json({
  namespace = "lua-outbox-callback-close",
  key = "callback-command-source-failure-domain",
  public_read = true,
})
if callback_command_state ~= nil or callback_command_meta == nil or
    not callback_command_meta.no_content then
  error("Lua callback command source failure persisted prior domain state")
end
callback_outbox:close()

-- Lua table lookups may execute user code. Closing a wrapper from an options
-- or handler metatable must be a normal structured failure, never a native
-- use-after-free.
local close_dispatcher_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-reentrant-dispatcher-close",
}), nil, "Lua reentrant dispatcher-close outbox creation")
local close_dispatcher = assert_ok(close_dispatcher_outbox:dispatcher(), nil,
                                   "Lua reentrant dispatcher creation")
local close_dispatcher_core = close_dispatcher._core
local close_dispatcher_options = setmetatable({}, {
  __index = function(_, field)
    if field == "handlers" then
      close_dispatcher_core:close()
      return { http = function() end }
    end
  end,
})
local close_dispatcher_result, close_dispatcher_err =
    close_dispatcher_core:pump(close_dispatcher_options)
if close_dispatcher_result ~= nil or close_dispatcher_err == nil then
  error("Lua dispatcher continued after options lookup closed its wrapper")
end
close_dispatcher_outbox:close()

local close_query_client, close_query_open_err = lockdc.open({
  endpoints = { "pouch://" .. root .. "/reentrant-query-client" },
  default_namespace = "lua-outbox-reentrant-query-close",
})
close_query_client = assert_ok(close_query_client, close_query_open_err,
                               "Lua reentrant query-close client open")
local close_query_client_core = close_query_client._core
local close_query_handler = setmetatable({
  chunk = function() end,
}, {
  __index = function(_, field)
    if field == "begin" then
      close_query_client_core:close()
    end
  end,
})
local close_query_result, close_query_err = close_query_client_core:query_keys({
  engine = "scan",
}, close_query_handler)
if close_query_result ~= nil or close_query_err == nil then
  error("Lua query_keys continued after handler setup closed its client")
end

-- Field decoding for a deferred handler outcome is still part of the handler
-- ownership boundary.  A hostile completion-table metatable must not close
-- the native claim below its terminal call; it follows the normal retry path.
local outcome_close_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-reentrant-outcome-close",
}), nil, "Lua reentrant outcome-close outbox creation")
local outcome_close_dispatcher = assert_ok(outcome_close_outbox:dispatcher(), nil,
                                           "Lua reentrant outcome-close dispatcher")
local outcome_close_txn, outcome_close_append_err = outcome_close_outbox:append({
  operation_id = "lua-reentrant-outcome-close-order",
  effect_id = "reentrant-outcome-close",
  effect_key = "lua-reentrant-outcome-close-effect",
  payload_digest = "sha256:lua-reentrant-outcome-close",
  kind = "http",
  destination = "https://example.test/reentrant-outcome-close",
}, "reentrant-outcome-close-payload")
outcome_close_txn = assert_ok(outcome_close_txn, outcome_close_append_err,
                              "Lua reentrant outcome-close outbox append")
assert_ok(outcome_close_txn:commit(), nil,
          "Lua reentrant outcome-close transaction commit")
outcome_close_txn:close()
local outcome_close_pumped, outcome_close_pump_err = outcome_close_dispatcher:pump({
  handlers = {
    http = function(job)
      return job:complete(setmetatable({}, {
        __index = function(_, field)
          if field == "delivery_reference" then
            job:close()
          end
          return nil
        end,
      }))
    end,
  },
  max_jobs = 1,
  timeout_ms = 0,
})
outcome_close_pumped = assert_ok(outcome_close_pumped, outcome_close_pump_err,
                                 "Lua reentrant outcome-close pump")
if outcome_close_pumped ~= 1 then
  error("Lua reentrant outcome-close did not retry its claimed job")
end
assert_ok(outcome_close_dispatcher:stop(-1), nil,
          "Lua reentrant outcome-close dispatcher stop")
outcome_close_dispatcher:close()
outcome_close_outbox:close()

-- Query callback setup must not leave registry references behind when a later
-- metamethod lookup fails.  This keeps a failed request from retaining user
-- callback graphs until Lua state teardown.
local query_leak_payload = { marker = "query-handler-registry-leak" }
local query_leak_weak = setmetatable({ query_leak_payload }, { __mode = "v" })
local query_leak_begin = function()
  return query_leak_payload
end
local query_leak_handler = setmetatable({}, {
  __index = function(_, field)
    if field == "begin" then
      return query_leak_begin
    end
    if field == "chunk" then
      error("intentional query handler setup failure")
    end
  end,
})
local query_leak_ok = pcall(function()
  client._core:query_keys({ engine = "scan" }, query_leak_handler)
end)
if query_leak_ok then
  error("Lua query handler setup failure unexpectedly succeeded")
end
query_leak_handler = nil
query_leak_begin = nil
query_leak_payload = nil
collectgarbage("collect")
collectgarbage("collect")
if query_leak_weak[1] ~= nil then
  error("Lua failed query handler setup retained callback state")
end

-- A reconciliation scan can rediscover a key already handed to the local
-- notification queue. That is a no-op, not an overflow or an eager retry.
local duplicate_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-duplicate-notification",
  recovery_interval_seconds = 0,
}), nil, "Lua duplicate notification outbox creation")
local duplicate_dispatcher = assert_ok(duplicate_outbox:dispatcher(), nil,
                                       "Lua duplicate notification dispatcher")
local duplicate_txn, duplicate_receipt_or_err = duplicate_outbox:append({
  operation_id = "lua-duplicate-notification-order",
  effect_id = "duplicate-notification",
  effect_key = "lua-duplicate-notification-effect",
  payload_digest = "sha256:lua-duplicate-notification",
  kind = "http",
  destination = "https://example.test/duplicate-notification",
}, "duplicate-notification-payload")
duplicate_txn = assert_ok(duplicate_txn, duplicate_receipt_or_err,
                          "Lua duplicate notification outbox append")
assert_ok(duplicate_txn:commit(), nil,
          "Lua duplicate notification transaction commit")
duplicate_txn:close()
assert_ok(duplicate_dispatcher:reconcile(), nil,
          "Lua duplicate notification reconciliation")
local duplicate_stats = assert_ok(duplicate_dispatcher:stats(), nil,
                                  "Lua duplicate notification stats")
if duplicate_stats.notification_overflows ~= 0 then
  error("Lua duplicate notification scheduled a recovery overflow")
end
local duplicate_job = assert_ok(duplicate_dispatcher:next(3000), nil,
                                "Lua duplicate notification job")
assert_ok(duplicate_job:complete(), nil,
          "Lua duplicate notification completion")
assert_ok(duplicate_dispatcher:stop(-1), nil,
          "Lua duplicate notification dispatcher stop")
duplicate_dispatcher:close()
duplicate_outbox:close()

-- Replacing a stopped attachment on one producer must release the old
-- attachment before starting the replacement dispatcher.
local restart_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-restart",
  recovery_interval_seconds = 0,
}), nil, "Lua restart outbox creation")
for restart_index = 1, 3 do
  local restart_dispatcher = assert_ok(restart_outbox:dispatcher(), nil,
                                       "Lua restart dispatcher creation")
  assert_ok(restart_dispatcher:stop(-1), nil, "Lua restart dispatcher stop")
  restart_dispatcher:close()
end
restart_outbox:close()

-- Closing a client is a native dispatcher stop too. Once every Lua wrapper is
-- closed, that shutdown must release the binding's registry-held handler map.
local binding_client = assert_ok(lockdc.open({
  endpoints = { "pouch://" .. root .. "/binding-close" },
}), nil, "Lua binding-close client creation")
local binding_outbox = assert_ok(binding_client:new_outbox({
  namespace = "lua-outbox-binding-close",
  recovery_interval_seconds = 0,
}), nil, "Lua binding-close outbox creation")
local binding_dispatcher = assert_ok(binding_outbox:dispatcher(), nil,
                                     "Lua binding-close dispatcher creation")
local weak_handlers = setmetatable({}, { __mode = "v" })
do
  local binding_handlers = { http = function() end }
  weak_handlers[1] = binding_handlers
  local pumped = assert_ok(binding_dispatcher:pump({
    handlers = binding_handlers,
    max_jobs = 1,
    timeout_ms = 0,
  }), nil, "Lua binding-close handler binding")
  if pumped ~= 0 then
    error("Lua binding-close dispatcher unexpectedly consumed a job")
  end
  binding_handlers = nil
end
binding_dispatcher:close()
binding_outbox:close()
binding_client:close()
collectgarbage("collect")
collectgarbage("collect")
if weak_handlers[1] ~= nil then
  error("Lua client shutdown retained a closed dispatcher handler map")
end

-- Dispatcher bindings must not registry-root their owner client. When an
-- embedding runtime drops a complete outbox graph, client finalization is
-- responsible for stopping its private worker and releasing Pouch resources.
local weak_clients = setmetatable({}, { __mode = "v" })
do
  local collected_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "/binding-gc" },
  }), nil, "Lua binding-gc client creation")
  weak_clients[1] = collected_client
  local collected_outbox = assert_ok(collected_client:new_outbox({
    namespace = "lua-outbox-binding-gc",
    recovery_interval_seconds = 0,
  }), nil, "Lua binding-gc outbox creation")
  local collected_dispatcher = assert_ok(collected_outbox:dispatcher(), nil,
                                         "Lua binding-gc dispatcher creation")
  collected_dispatcher:close()
  collected_outbox:close()
end
collectgarbage("collect")
collectgarbage("collect")
if weak_clients[1] ~= nil then
  error("Lua dispatcher binding retained an unreachable client")
end

-- A handler may naturally close over its client. The C binding must not turn
-- that otherwise collectible Lua graph into a registry root after its final
-- dispatcher wrapper is gone.
local captured_clients = setmetatable({}, { __mode = "v" })
do
  local captured_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "/binding-captured-client" },
  }), nil, "Lua captured-client binding creation")
  captured_clients[1] = captured_client
  local captured_outbox = assert_ok(captured_client:new_outbox({
    namespace = "lua-outbox-binding-captured-client",
    recovery_interval_seconds = 0,
  }), nil, "Lua captured-client outbox creation")
  local captured_dispatcher = assert_ok(captured_outbox:dispatcher(), nil,
                                        "Lua captured-client dispatcher creation")
  assert_ok(captured_dispatcher:pump({
    handlers = { http = function() return captured_client:info() end },
    max_jobs = 1,
    timeout_ms = 0,
  }), nil, "Lua captured-client handler binding")
  captured_dispatcher:close()
  captured_outbox:close()
end
collectgarbage("collect")
collectgarbage("collect")
if captured_clients[1] ~= nil then
  error("Lua dispatcher handler closure retained an unreachable client")
end

-- A handler may also close over its dispatcher. The handler table and its
-- dispatcher userdata form a Lua cycle, which must stay collectible rather
-- than being held alive by a binding registry reference.
local captured_dispatchers = setmetatable({}, { __mode = "v" })
do
  local captured_dispatcher_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "/binding-captured-dispatcher" },
  }), nil, "Lua captured-dispatcher client creation")
  local captured_dispatcher_outbox = assert_ok(captured_dispatcher_client:new_outbox({
    namespace = "lua-outbox-binding-captured-dispatcher",
    recovery_interval_seconds = 0,
  }), nil, "Lua captured-dispatcher outbox creation")
  local captured_dispatcher = assert_ok(captured_dispatcher_outbox:dispatcher(), nil,
                                        "Lua captured-dispatcher creation")
  local captured_dispatcher_ref = captured_dispatcher
  captured_dispatchers[1] = captured_dispatcher
  assert_ok(captured_dispatcher:pump({
    handlers = { http = function() return captured_dispatcher_ref:stats() end },
    max_jobs = 1,
    timeout_ms = 0,
  }), nil, "Lua captured-dispatcher handler binding")
end
collectgarbage("collect")
collectgarbage("collect")
if captured_dispatchers[1] ~= nil then
  error("Lua dispatcher handler closure retained an unreachable dispatcher")
end

-- The coroutine that creates a binding and the main thread that closes the
-- client share one Lua VM. Main-thread close must therefore retire that
-- coroutine-owned binding and its handler map.
local coroutine_handlers = setmetatable({}, { __mode = "v" })
local coroutine_client = assert_ok(lockdc.open({
  endpoints = { "pouch://" .. root .. "/binding-coroutine" },
}), nil, "Lua coroutine binding client creation")
local coroutine_ok, coroutine_dispatcher = coroutine.resume(coroutine.create(function()
  local coroutine_outbox = assert_ok(coroutine_client:new_outbox({
    namespace = "lua-outbox-binding-coroutine",
    recovery_interval_seconds = 0,
  }), nil, "Lua coroutine outbox creation")
  local dispatcher = assert_ok(coroutine_outbox:dispatcher(), nil,
                               "Lua coroutine dispatcher creation")
  local handlers = { http = function() end }
  coroutine_handlers[1] = handlers
  assert_ok(dispatcher:pump({ handlers = handlers, max_jobs = 1, timeout_ms = 0 }),
            nil, "Lua coroutine handler binding")
  coroutine_outbox:close()
  return dispatcher
end))
if not coroutine_ok then
  error("Lua coroutine dispatcher creation failed: " .. tostring(coroutine_dispatcher))
end
coroutine_dispatcher:close()
coroutine_client:close()
coroutine_client = nil
collectgarbage("collect")
collectgarbage("collect")
if coroutine_handlers[1] ~= nil then
  error("Lua main-thread client close retained a coroutine handler map")
end

-- A streaming key callback may close its client. The active native call keeps
-- its own reference until the Pouch scan and Lua callback unwind, so closure
-- cannot invalidate the scan's state mid-visit.
local callback_close_client = assert_ok(lockdc.open({
  endpoints = { "pouch://" .. root .. "/query-callback-close" },
}), nil, "Lua query callback-close client creation")
local callback_close_lease = assert_ok(callback_close_client:acquire({
  namespace = "lua-query-callback-close",
  key = "callback-close-key",
  owner = "lua-query-callback-close-owner",
  ttl_seconds = 30,
}), nil, "Lua query callback-close lease acquisition")
assert_ok(callback_close_lease:update_json({ visible = true }), nil,
          "Lua query callback-close state update")
assert_ok(callback_close_lease:release(), nil,
          "Lua query callback-close lease release")
local callback_close_seen = false
local callback_close_result, callback_close_err = callback_close_client:query_keys({
  namespace = "lua-query-callback-close",
  engine = "scan",
}, function(key)
  if key ~= "callback-close-key" then
    error("Lua query callback-close received an unexpected key")
  end
  callback_close_seen = true
  callback_close_client:close()
end)
if callback_close_result == nil or callback_close_err ~= nil or not callback_close_seen then
  error("Lua query callback-close did not complete after client closure")
end

-- Client close must stop its private worker, but not wait on the durable job
-- handed to the caller. The retained job remains safely closable afterwards.
local shutdown_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-client-close-active-job",
  owner = "lua-outbox-client-close-worker",
  recovery_interval_seconds = 0,
}), nil, "Lua client-close outbox creation")
local shutdown_dispatcher = assert_ok(shutdown_outbox:dispatcher(), nil,
                                      "Lua client-close dispatcher creation")
local shutdown_txn, shutdown_receipt_or_err = shutdown_outbox:append({
  operation_id = "lua-client-close-order",
  effect_id = "client-close",
  effect_key = "lua-client-close-effect",
  payload_digest = "sha256:lua-client-close",
  kind = "http",
  destination = "https://example.test/client-close",
}, "client-close-payload")
shutdown_txn = assert_ok(shutdown_txn, shutdown_receipt_or_err,
                         "Lua client-close outbox append")
assert_ok(shutdown_txn:commit(), nil, "Lua client-close transaction commit")
shutdown_txn:close()
local shutdown_job = assert_ok(shutdown_dispatcher:next(3000), nil,
                               "Lua client-close active job")
shutdown_outbox:close()
client:close()
shutdown_job:close()
shutdown_dispatcher:close()
