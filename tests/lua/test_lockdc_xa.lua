local core = require("lockdc.core")
local integers = lockdc_test_integer_fields()
assert(integers.zero == "0")
assert(integers.above_exact == "9007199254740993")
assert(integers.maximum == "18446744073709551615")
local client = assert(core.open({
  endpoints = { "pouch://" .. lockdc_test_root },
  pouch = { single_writer = true },
  disable_mtls = true,
}))
local function await(op)
  for _ = 1, 200 do
    local done, err, code = op:advance()
    if done == nil then return nil, err, code end
    if done then return op:result() end
    lockdc_test_wait(assert(op:wait()))
  end
  error("XA operation did not finish")
end
local function completed(op)
  local result, err = await(assert(op))
  assert(result, err and err.message)
  assert(math.type(result.retired_count) == "integer" and result.retired_count >= 0)
  return result
end
local function begin(coordinator)
  local op = assert(coordinator:begin_start())
  assert(#op:result().txn_id == 20) -- Available before durable work starts.
  completed(op)
  local tx = assert(op:take_transaction())
  assert(op:take_transaction() == nil) -- Ownership transfers once.
  assert(op:close_blocking())
  return tx
end

local bad, err, code = client:xa_coordinator({ completed_retention_seconds = -2 })
assert(bad == nil and code == core.ERR_INVALID and err.message)
bad, err, code = client:xa_coordinator({ owner_ttl_seconds = 0.5 })
assert(bad == nil and code == core.ERR_INVALID)
local coordinator = assert(client:xa_coordinator())
for _, method in ipairs({ "recover_start", "adopt_start", "resource" }) do
  bad, err, code = coordinator[method](coordinator, assert(core.xid_new()) .. "\0suffix")
  assert(bad == nil and code == core.ERR_INVALID)
end
assert(lockdc_test_invalid_borrow({ _core = coordinator }))
bad, err, code = coordinator:register_start("postgres", {})
assert(bad == nil and code == core.ERR_INVALID)
bad, err, code = coordinator:register_start("test-rejected", { probe = true })
assert(bad == nil and code == core.ERR_INVALID)
local allocated, freed = lockdc_test_counters()
assert(allocated == 1 and freed == 1)
bad, err, code = coordinator:register_start("test-rejected", setmetatable({}, {
  __index = function() error("protected configuration exception") end,
}))
assert(bad == nil and code == core.ERR_INVALID)
allocated, freed = lockdc_test_counters()
assert(allocated == 2 and freed == 2)

-- Public journal parity, exact native identifiers and protected conversion.
local store = assert(client:xa_store())
bad, err, code = store:resource("test-rejected", { probe = true })
assert(bad == nil and code == core.ERR_INVALID)
bad, err, code = store:resource("test-rejected", setmetatable({}, {
  __index = function() error("protected journal configuration exception") end,
}))
assert(bad == nil and code == core.ERR_INVALID)
allocated, freed = lockdc_test_counters()
assert(allocated == 4 and freed == 4)
local record = {
  txn_id = assert(core.xid_new()), resource_id = assert(core.xid_new()),
  branch_id = assert(core.xid_new()), gid = "lua-public-journal",
  full_xid = "9007199254740993", system_id = "9007199254740995",
  database_oid = "1", role_oid = "1", phase = core.XA_ENROLLED,
}
assert(store:register_blocking(record))
assert(store:enroll_blocking(record))
local snapshot, prepared = store:read_blocking(record.txn_id, record.branch_id)
assert(snapshot.full_xid == record.full_xid and snapshot.system_id == record.system_id)
assert(snapshot.phase == core.XA_ENROLLED and not prepared)
record.phase = core.XA_PREPARE_SENT
assert(store:progress_blocking(record))
record.phase, record.prepared_observed = core.XA_PREPARED, true
assert(store:progress_blocking(record))
snapshot, prepared = store:read_blocking(record.txn_id, record.branch_id)
assert(prepared and snapshot.prepared_observed and snapshot.phase == core.XA_PREPARED)
local copy = assert(store:retain())
assert(store:close())
assert(store:close())
assert(store:read_blocking(record.txn_id, record.branch_id) == nil)
snapshot = assert(copy:read_blocking(record.txn_id, record.branch_id))
assert(snapshot.full_xid == record.full_xid)
for _, malformed in ipairs({ { gid = string.rep("x", 200) }, { full_xid = 42 },
    { phase = -1 }, { evidence = 256 }, { prepared_observed = 1 },
    { completed_at_unix = 0.5 }, { txn_id = "embedded\0NUL" } }) do
  bad, err, code = copy:enroll_blocking(malformed)
  assert(bad == nil and code == core.ERR_INVALID)
end
assert(copy:close())
local reentrant_store = assert(client:xa_store())
bad, err, code = reentrant_store:progress_blocking(setmetatable({}, {
  __index = function(_, name)
    assert(reentrant_store:close())
    return record[name]
  end,
}))
assert(bad == nil and code == core.ERR_INVALID and err.message)

local input_tx = begin(coordinator)
for _, field in ipairs({ "namespace", "key", "owner" }) do
  local request = { namespace = "orders", key = "identity", owner = "lua", ttl_seconds = 30 }
  request[field] = request[field] .. "\0alias"
  bad, err, code = input_tx:acquire_start(request)
  assert(bad == nil and code == core.ERR_INVALID and err.message,
    "managed acquisition accepted embedded NUL in " .. field)
end
for _, method in ipairs({ "enroll_start", "borrow" }) do
  bad, err, code = input_tx[method](input_tx, assert(core.xid_new()) .. "\0suffix")
  assert(bad == nil and code == core.ERR_INVALID)
end
local input_rollback = assert(input_tx:rollback_start())
assert(completed(input_rollback).decision == core.XA_DECISION_ABORT)
assert(input_rollback:close_blocking())
assert(input_tx:close_blocking())
local tx = begin(coordinator)
local id = tx:id()
local op = assert(tx:acquire_start({ namespace = "orders", key = "one", owner = "lua", ttl_seconds = 30 }))
completed(op)
local lease = assert(op:take_lease())
assert(op:take_lease() == nil)
assert(op:close_blocking())
assert(lease:update('{"value":42}'))
assert(coordinator:close_blocking()) -- Child handles keep the native owner alive.
op = assert(tx:commit_start())
local result = completed(op)
assert(result.applied and result.decision == 1 and result.txn_id == id)
assert(op:close_blocking())
lease:close()
assert(tx:close_blocking())
assert(tx:close_blocking())
assert(tx:id() == nil)

coordinator = assert(client:xa_coordinator())
op = assert(coordinator:recover_start(id))
result = completed(op)
assert(result.applied and result.decision == 1)
assert(op:close_blocking())
-- Abandoned userdata hands undecided work to recovery; it never makes COMMIT.
tx = begin(coordinator)
local abandoned = tx:id()
tx = nil
collectgarbage("collect")
op = assert(coordinator:recover_start(abandoned))
result = completed(op)
assert(result.applied and result.decision == 2)
assert(op:close_blocking())

-- A caught staging error still prevents implicit commit.
tx = begin(coordinator)
op = assert(tx:acquire_start({ key = "invalid", owner = "lua", ttl_seconds = -1 }))
bad, err, code = await(op)
assert(bad == nil and code == core.ERR_INVALID)
assert(op:close_blocking())
bad, err, code = tx:commit_start()
assert(bad == nil and code == core.ERR_INVALID and err.message:match("rollback"))
op = assert(tx:rollback_start())
assert(completed(op).decision == 2)
assert(op:close_blocking())
assert(tx:close_blocking())
assert(coordinator:close_blocking())

-- The facade stages under the same global ID and collects receipts only after
-- the coordinator has applied every participant.
coordinator = assert(client:xa_coordinator())
local outbox = assert(client:new_outbox({ namespace = "effects" }))
tx = begin(coordinator)
local box = assert(outbox:begin())
assert(#box:id() == 20)
assert(box:bind_xa(tx))
assert(box:id() == tx:id())
local participant = assert(box:acquire({ namespace = "orders", key = "mixed" }))
assert(participant:update('{"value":42}'))
assert(box:append({ operation_id = tx:id(), effect_id = "effect", effect_key = tx:id(),
  payload_digest = "sha256:test", destination = "test://sink", kind = "test" }, "payload"))
bad, err, code = box:commit()
assert(bad == nil and code == core.ERR_PENDING)
op = assert(box:commit_start())
assert(completed(op).applied)
assert(op:close_blocking())
local receipt = assert(box:commit())
assert(#receipt.outbox_receipts == 1)
participant:close()
box:close()
assert(tx:close_blocking())

tx = begin(coordinator)
box = assert(outbox:begin())
assert(box:bind_xa(tx))
local protected = pcall(function() box:acquire({ key = {} }) end)
assert(not protected)
bad, err, code = tx:commit_start()
assert(bad == nil and code == core.ERR_INVALID)
op = assert(box:rollback_start())
assert(completed(op).decision == 2)
assert(op:close_blocking())
box:close()
assert(tx:close_blocking())
outbox:close()
assert(coordinator:close_blocking())
-- A failure on the transaction, or on another bound box, gates every box.
coordinator = assert(client:xa_coordinator())
outbox = assert(client:new_outbox({ namespace = "rollback-only-effects" }))
for scenario = 1, 2 do
  tx = begin(coordinator)
  box = assert(outbox:begin())
  assert(box:bind_xa(tx))
  participant = assert(box:acquire({ key = "global-failure-" .. scenario }))
  assert(participant:update('{"value":42}'))
  local other
  if scenario == 1 then
    op = assert(tx:acquire_start({ key = "invalid-global", owner = "lua", ttl_seconds = -1 }))
    assert(await(op) == nil)
    assert(op:close_blocking())
  else
    other = assert(outbox:begin())
    assert(other:bind_xa(tx))
    assert(not pcall(function() other:acquire({ key = {} }) end))
  end
  bad, err, code = box:commit_start()
  assert(bad == nil and code == core.ERR_INVALID and err.message:match("rollback"))
  op = assert(box:rollback_start())
  assert(completed(op).decision == 2)
  assert(op:close_blocking())
  participant:close()
  box:close()
  if other then other:close() end
  assert(tx:close_blocking())
end
outbox:close()
assert(coordinator:close_blocking())
-- Successful outer staging must not erase a caught nested failure. Exercise
-- shared and same-box guards, transaction acquisition and successful nesting.
coordinator = assert(client:xa_coordinator())
outbox = assert(client:new_outbox({ namespace = "nested-staging" }))
for scenario = 1, 6 do
  tx = begin(coordinator)
  local a, b = assert(outbox:begin()), assert(outbox:begin())
  assert(a:bind_xa(tx))
  assert(b:bind_xa(tx))
  local key, invoked, nested = "nested-" .. scenario, false, nil
  local request = setmetatable({ namespace = "nested-domain", key = key, owner = "lua", ttl_seconds = 30 }, {
    __index = function()
      if not invoked then
        invoked = true
        if scenario == 1 or scenario == 4 then
          assert(not pcall(function() b:acquire({ key = {} }) end))
        elseif scenario == 2 then
          assert(not pcall(function() a:acquire({ key = {} }) end))
        elseif scenario == 3 then
          local value, e, c = tx:acquire_start({ key = {}, owner = "lua" })
          assert(value == nil and c == core.ERR_INVALID and e.message)
        elseif scenario == 5 then
          nested = assert(b:acquire({ key = key .. "-child" }))
        else
          local child = assert(tx:acquire_start({ key = key .. "-child", owner = "lua", ttl_seconds = 30 }))
          completed(child)
          nested = assert(child:take_lease())
          assert(child:close_blocking())
        end
      end
      return nil
    end,
  })
  local staged
  if scenario == 4 or scenario == 6 then
    op = assert(tx:acquire_start(request))
    completed(op)
    staged = assert(op:take_lease())
    assert(op:close_blocking())
  else staged = assert(a:acquire(request)) end
  assert(invoked and staged:update('{"published":true}'))
  if scenario <= 4 then
    for _, receiver in ipairs({ a, b, tx }) do
      bad, err, code = receiver:commit_start()
      assert(bad == nil and code == core.ERR_INVALID and err.message:match("rollback"),
        "nested failure allowed commit in scenario " .. scenario)
    end
    op = assert(tx:rollback_start())
  else op = assert(tx:commit_start()) end
  result = completed(op)
  assert(result.applied and result.decision == (scenario <= 4 and 2 or 1))
  assert(op:close_blocking())
  staged:close()
  if nested then nested:close() end
  a:close(); b:close()
  assert(tx:close_blocking())
  local verify = assert(client:acquire({ namespace = "nested-domain", key = key, owner = "verify", ttl_seconds = 30 }))
  local payload = ""
  local _, metadata = verify:get(nil, { write = function(chunk)
    payload = payload .. chunk
    return true
  end })
  if scenario <= 4 then assert(metadata.no_content)
  else assert(payload == '{"published":true}', "nested readback " .. scenario .. ": " .. payload) end
  verify:close()
end
outbox:close()
assert(coordinator:close_blocking())
-- Raw decision/replay must reject a different Lua identity whose C prefix
-- happens to be an existing committed transaction.
for _, method in ipairs({ "txn_prepare", "txn_commit", "txn_rollback", "txn_replay" }) do
  for _, malformed in ipairs({ id .. "\0alias", false, 123, {} }) do
    bad, err, code = client[method](client, { txn_id = malformed })
    assert(bad == nil and code == core.ERR_INVALID and err.message,
      method .. " accepted an invalid transaction identity")
  end
end
assert(client:txn_replay({ txn_id = id }).state == "commit")
-- The same invariant applies to ordinary callback-owned outbox transactions.
outbox = assert(client:new_outbox({ namespace = "nested-callback" }))
for _, fail_nested in ipairs({ false, true }) do
  local invoked, key = false, "standalone-nested-" .. tostring(fail_nested)
  local value, e, c = outbox:transaction(function(box)
    local request = setmetatable({ namespace = "nested-domain", key = key }, {
      __index = function()
        if not invoked then
          invoked = true
          if fail_nested then
            assert(not pcall(function() box:acquire({ key = {} }) end))
          else assert(box:acquire({ namespace = "nested-domain", key = key .. "-child" })) end
        end
        return nil
      end,
    })
    assert(box:acquire(request):update('{"published":true}'))
  end)
  assert(invoked)
  if fail_nested then assert(value == nil and c == core.ERR_INVALID and e.message)
  else assert(value) end
  local verify = assert(client:acquire({ namespace = "nested-domain", key = key, owner = "verify", ttl_seconds = 30 }))
  local payload = ""
  local _, metadata = verify:get(nil, { write = function(chunk)
    payload = payload .. chunk
    return true
  end })
  if fail_nested then assert(metadata.no_content)
  else assert(payload == '{"published":true}') end
  verify:close()
end
outbox:close()
client:close()
collectgarbage("collect")
-- Exercise the actual high-level wrappers against the same native binding.
-- JSON encoding is outside this test; every payload below is already bytes.
package.preload.lonejson = function()
  return { json_null = {}, encode_json = function() error("unexpected JSON encoding") end }
end
local source = debug.getinfo(1, "S").source:sub(2)
local repo = assert(source:match("^(.*)/tests/lua/[^/]+$"))
package.path = repo .. "/lua/?.lua;" .. repo .. "/lua/?/init.lua;" .. package.path
local lockdc = require("lockdc")
local facade_client = assert(lockdc.open({ endpoints = { "pouch://" .. lockdc_test_root .. "-facade" },
  pouch = { single_writer = true } }))
local facade_coordinator = assert(facade_client:xa_coordinator())
op = assert(facade_coordinator:begin_start())
completed(op)
local facade_tx = assert(op:take_transaction())
assert(op:close_blocking())
assert(#facade_tx:id() == 20)
op = assert(facade_tx:acquire_start({ key = "facade", owner = "lua", ttl_seconds = 30 }))
completed(op)
local facade_lease = assert(op:take_lease())
assert(op:close_blocking())
assert(facade_lease:update('{"value":42}'))
op = assert(facade_tx:commit_start())
assert(completed(op).applied)
assert(op:close_blocking())
facade_lease:close()
assert(facade_tx:close_blocking())
local receipt = assert(facade_coordinator:transaction(function(callback_tx)
  local state_lease = assert(callback_tx:acquire({ key = "callback", owner = "lua", ttl_seconds = 30 }))
  assert(state_lease:update('{"value":42}'))
  return "returned", nil
end, completed))
assert(receipt.applied and receipt.decision == 1)
assert(receipt.values.n == 2 and receipt.values[1] == "returned")
for scenario = 1, 2 do
  local key = "async-lease-controls-" .. scenario
  bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
    local acquire = assert(callback_tx:acquire_start({ key = key, owner = "lua", ttl_seconds = 30 }))
    completed(acquire)
    local state_lease = assert(acquire:take_lease())
    assert(acquire:close_blocking())
    assert(state_lease:update('{"published":true}'))
    if scenario == 1 then
      local released, release_error = pcall(state_lease.release, state_lease)
      assert(not released and tostring(release_error):match("callback owns"))
      error("callback failure after blocked async lease release")
    else
      assert(not pcall(function() state_lease:update("payload", { if_version = "bad" }) end))
    end
  end, completed)
  assert(bad == nil and result.applied and result.decision == 2)
  local verify = assert(facade_client:acquire({ key = key, owner = "verify", ttl_seconds = 30 }))
  local _, metadata = verify:read()
  assert(metadata.no_content)
  verify:close()
end

-- Managed acquisition retains the ordinary create-only precondition.
local create_key = "managed-create-only"
local create_lease = assert(facade_client:acquire({ key = create_key, owner = "seed", ttl_seconds = 30 }))
assert(create_lease:update('{"original":true}'))
assert(create_lease:release())
create_lease:close()
for _, convenience in ipairs({ false, true }) do
  bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
    local request = { key = create_key, owner = "lua", ttl_seconds = 30, if_not_exists = true }
    if convenience then
      local acquired = callback_tx:acquire(request)
      assert(acquired == nil)
    else
      local acquire = assert(callback_tx:acquire_start(request))
      assert(await(acquire) == nil)
      assert(acquire:close_blocking())
    end
  end, completed)
  assert(bad == nil and result.applied and result.decision == 2)
end
local verify_create = assert(facade_client:acquire({ key = create_key, owner = "verify", ttl_seconds = 30 }))
assert(verify_create:read() == '{"original":true}')
verify_create:close()
for _, rollback in ipairs({ false, true }) do
  local key = "lease-release-guard-" .. tostring(rollback)
  bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
    local callback_lease = assert(callback_tx:acquire({ key = key, owner = "lua", ttl_seconds = 30 }))
    assert(callback_lease:update('{"published":true}'))
    local released, release_error = pcall(callback_lease.release, callback_lease, { rollback = rollback })
    assert(not released and tostring(release_error):match("callback owns"))
    error("callback must abort after rejected lease release")
  end, completed)
  assert(bad == nil and result.applied and result.decision == 2)
  local verify = assert(facade_client:acquire({ key = key, owner = "verify", ttl_seconds = 30 }))
  local _, metadata = verify:read()
  assert(metadata.no_content)
  verify:close()
end
local callback_outbox = assert(facade_client:new_outbox({ namespace = "callback-guard" }))
for _, terminal in ipairs({ "commit_start", "rollback_start" }) do
  bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
    local callback_box = assert(callback_tx:outbox(callback_outbox))
    local state = assert(callback_box:acquire({ key = "guard-" .. terminal }))
    assert(state:update('{"value":42}'))
    local invoked_terminal, terminal_error = pcall(callback_box[terminal], callback_box)
    assert(not invoked_terminal and tostring(terminal_error):match("callback owns"))
    error("callback failed after rejected early terminal operation")
  end, completed)
  assert(bad == nil and result.applied and result.decision == 2)
  local verify = assert(facade_client:acquire({ key = "guard-" .. terminal, owner = "verify", ttl_seconds = 30 }))
  local _, metadata = verify:read()
  assert(metadata.no_content)
  verify:close()
end
callback_outbox:close()

local failed_id
bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
  failed_id = callback_tx:id()
  assert(callback_tx:acquire({ key = "callback-error", owner = "lua", ttl_seconds = 30 }))
  error("application failure")
end, completed)
assert(bad == nil and code == core.ERR_INVALID and result.applied and result.decision == 2)
op = assert(facade_coordinator:recover_start(failed_id))
assert(completed(op).decision == 2)
assert(op:close_blocking())

bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
  local state_lease = assert(callback_tx:acquire({ key = "caught-error", owner = "lua", ttl_seconds = 30 }))
  local caught = pcall(function() state_lease:update("payload", { if_version = "bad" }) end)
  assert(not caught)
end, completed)
assert(bad == nil and result.applied and result.decision == 2)

local entered = false
bad, err, code, result = facade_coordinator:transaction(function() entered = true end,
  function() return true end)
assert(bad == nil and not entered and code == core.ERR_INVALID and #result.txn_id == 20)

-- Even an await exception after durable COMMIT preserves its completion proof.
bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
  local state_lease = assert(callback_tx:acquire({ key = "after-commit", owner = "lua", ttl_seconds = 30 }))
  assert(state_lease:update('{"value":42}'))
end, function(operation)
  local state = completed(operation)
  if state.decision == 1 and state.applied then error("reporting failure") end
  return state
end)
assert(bad == nil and result.applied and result.decision == 1)

bad, err, code, result = facade_coordinator:transaction(function(callback_tx)
  local state_lease = assert(callback_tx:acquire({ key = "returned-await-error", owner = "lua", ttl_seconds = 30 }))
  assert(state_lease:update('{"value":42}'))
end, function(operation)
  local state = completed(operation)
  if state.decision == 1 and state.applied then
    return nil, { code = core.ERR_TIMEOUT, message = "reporting timed out" }, core.ERR_TIMEOUT
  end
  return state
end)
assert(bad == nil and code == core.ERR_TIMEOUT and result.applied and result.decision == 1)

local thread = coroutine.create(function()
  return facade_coordinator:transaction(function(callback_tx)
    local state_lease = assert(callback_tx:acquire({ key = "yielded", owner = "lua", ttl_seconds = 30 }))
    coroutine.yield("application")
    assert(state_lease:update('{"value":42}'))
  end, function(operation)
    local state = completed(operation)
    coroutine.yield("readiness")
    return state
  end)
end)
local resumed, value
repeat
  resumed, value = coroutine.resume(thread)
  assert(resumed, value)
  collectgarbage("collect")
until coroutine.status(thread) == "dead"
assert(value.applied and value.decision == 1)
assert(facade_coordinator:close_blocking())
facade_client:close()
collectgarbage("collect")
print("Lua XA registration, ownership, recovery and failure tests passed")
