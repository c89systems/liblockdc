local core = require("lockdc.core")
local lonejson = require("lonejson")

local M = {}

-- Process-wide startup selection; call before creating any SDK objects.
-- Custom allocator callbacks and Lua's own heap remain outside this tracker.
function M.allocator_tracking_configure(enabled)
  return core.allocator_tracking_configure(enabled)
end

local Client = {}
Client.__index = Client

local Lease = {}
Lease.__index = Lease

local Collection = {}
Collection.__index = Collection
local CollectionWriter = {}
CollectionWriter.__index = CollectionWriter

local Message = {}
Message.__index = Message

local Outbox = {}
Outbox.__index = Outbox

local OutboxDispatcher = {}
OutboxDispatcher.__index = OutboxDispatcher

local OutboxTransaction = {}
OutboxTransaction.__index = OutboxTransaction

local OutboxParticipant = {}
OutboxParticipant.__index = OutboxParticipant

local OutboxJob = {}
OutboxJob.__index = OutboxJob

local HistoryConsumer = {}
HistoryConsumer.__index = HistoryConsumer

local XACoordinator = {}
XACoordinator.__index = XACoordinator
local XATransaction = {}
XATransaction.__index = XATransaction
local XAOperation = {}
XAOperation.__index = XAOperation
local XABorrow = {}
XABorrow.__index = XABorrow
local XAStore = {}
XAStore.__index = XAStore
local XAResource = {}
XAResource.__index = XAResource
local XABranch = {}
XABranch.__index = XABranch
local XAResourceOperation = {}
XAResourceOperation.__index = XAResourceOperation

local Service = {}
Service.__index = Service

local JSON_NULL = lonejson.json_null
-- Native dispatcher bindings are shared by aliases. The state is weakly held
-- by the native binding id and strongly held by live facade wrappers, so an
-- adapter cannot survive the binding that accepted it.
local dispatcher_binding_states = setmetatable({}, { __mode = "v" })

local function release_dispatcher_handler_map(self)
  self._handler_core_map = nil
  self._handler_binding_state = nil
end

local function wrap_client(core_client)
  return setmetatable({ _core = core_client }, Client)
end

local function wrap_lease(core_lease)
  return setmetatable({ _core = core_lease, _closed = false }, Lease)
end

local function wrap_collection(core_collection)
  return setmetatable({ _core = core_collection }, Collection)
end

local function wrap_message(core_message)
  return setmetatable({ _core = core_message, _closed = false }, Message)
end

local function wrap_outbox(core_outbox)
  return setmetatable({ _core = core_outbox, _closed = false }, Outbox)
end

local function wrap_outbox_dispatcher(core_dispatcher)
  local binding_id = core_dispatcher:_binding_id()
  local state = dispatcher_binding_states[binding_id]

  if state == nil then
    state = {}
    dispatcher_binding_states[binding_id] = state
  end
  return setmetatable({
    _core = core_dispatcher,
    _closed = false,
    _handler_binding_state = state,
  }, OutboxDispatcher)
end

local function wrap_outbox_transaction(core_transaction)
  return setmetatable({
    _core = core_transaction,
    _closed = false,
    _terminal = false,
  }, OutboxTransaction)
end

local function wrap_outbox_participant(core_participant)
  return setmetatable({ _core = core_participant, _closed = false }, OutboxParticipant)
end

local function wrap_outbox_job(core_job)
  return setmetatable({
    _core = core_job,
    _closed = false,
    _terminal = false,
  }, OutboxJob)
end

local function wrap_history_consumer(core_consumer)
  return setmetatable({ _core = core_consumer, _closed = false }, HistoryConsumer)
end

local function wrap_xa_operation(operation, coordinator, transaction)
  return setmetatable({ _core = operation, _coordinator = coordinator,
    _transaction = transaction }, XAOperation)
end
local function wrap_xa_transaction(transaction, coordinator)
  return setmetatable({ _core = transaction, _coordinator = coordinator }, XATransaction)
end

-- Every start/advance/wait delegates to the same C coordinator. An application's
-- event loop owns readiness and coroutine scheduling; the facade never polls.
-- Start deadlines are absolute monotonic milliseconds: nil/0 uses 30 seconds,
-- -1 is infinite. A terminal ERR_PENDING still requires durable recovery;
-- inspect result().done rather than treating every pending result as progress.
function Client:xa_coordinator(config)
  local await = type(config) == "table" and rawget(config, "await") or nil
  local receiver, err, code = self._core:xa_coordinator(config)
  if receiver == nil then return nil, err, code end
  return setmetatable({ _core = receiver, _await = await }, XACoordinator)
end
function XACoordinator:register_start(name, config, deadline_ms)
  local op, err, code = self._core:register_start(name, config, deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self)
end
function XACoordinator:begin_start(deadline_ms)
  local op, err, code = self._core:begin_start(deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self)
end
function XACoordinator:recover_start(id, deadline_ms)
  local op, err, code = self._core:recover_start(id, deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self)
end
function XACoordinator:adopt_start(id, deadline_ms)
  local op, err, code = self._core:adopt_start(id, deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self)
end
function XACoordinator:retire_start(deadline_ms)
  local op, err, code = self._core:retire_start(deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self)
end
function XACoordinator:close_blocking() return self._core:close_blocking() end
-- Manual resource receivers retain their coordinator. Their close releases
-- that Lua view, never closes the coordinator-owned C resource independently.
function XACoordinator:resource(resource_id)
  local resource, err, code = self._core:resource(resource_id)
  if not resource then return nil, err, code end
  return setmetatable({ _core = resource, _owner = self }, XAResource)
end
function Client:xa_store()
  local store, err, code = self._core:xa_store()
  if not store then return nil, err, code end
  return setmetatable({ _core = store, _owner = self }, XAStore)
end
function XAStore:resource(name, configuration)
  local resource, err, code = self._core:resource(name, configuration)
  if not resource then return nil, err, code end
  return setmetatable({ _core = resource, _owner = self }, XAResource)
end
function XAStore:retain()
  local store, err, code = self._core:retain()
  if not store then return nil, err, code end
  return setmetatable({ _core = store, _owner = self }, XAStore)
end
for _, name in ipairs({ "register_blocking", "enroll_blocking", "progress_blocking", "read_blocking", "close" }) do
  XAStore[name] = function(self, ...) return self._core[name](self._core, ...) end
end
for _, name in ipairs({ "identity", "close_blocking" }) do
  XAResource[name] = function(self, ...) return self._core[name](self._core, ...) end
end
local function resource_operation(operation, owner)
  return setmetatable({ _core = operation, _owner = owner }, XAResourceOperation)
end
for _, name in ipairs({ "register_start", "branch_start", "recover_start", "status_start", "inventory_start" }) do
  XAResource[name] = function(self, ...)
    local operation, err, code = self._core[name](self._core, ...)
    if not operation then return nil, err, code end
    return resource_operation(operation, self)
  end
end
for _, name in ipairs({ "record", "close" }) do
  XABranch[name] = function(self, ...) return self._core[name](self._core, ...) end
end
for _, name in ipairs({ "prepare_start", "resolve_start" }) do
  XABranch[name] = function(self, ...)
    local operation, err, code = self._core[name](self._core, ...)
    if not operation then return nil, err, code end
    return resource_operation(operation, self)
  end
end
function XABranch:borrow()
  local borrow, err, code = self._core:borrow()
  if not borrow then return nil, err, code end
  return setmetatable({ _core = borrow }, XABorrow)
end
function XAResourceOperation:take_branch()
  local branch, err, code = self._core:take_branch()
  if not branch then return nil, err, code end
  return setmetatable({ _core = branch, _owner = self }, XABranch)
end
for _, name in ipairs({ "advance", "wait", "result", "cancel", "close_blocking" }) do
  XAResourceOperation[name] = function(self, ...) return self._core[name](self._core, ...) end
end
function XATransaction:id() return self._core:id() end
function XATransaction:enroll_start(resource_id, deadline_ms)
  local op, err, code = self._core:enroll_start(resource_id, deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self._coordinator)
end
function XATransaction:acquire_start(request, deadline_ms)
  local op, err, code = self._core:acquire_start(request, deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self._coordinator, self)
end
function XATransaction:renew_start(deadline_ms)
  local op, err, code = self._core:renew_start(deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self._coordinator)
end
function XATransaction:commit_start(deadline_ms)
  if self._callback then error("XA callback owns the terminal decision", 2) end
  local op, err, code = self._core:commit_start(deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self._coordinator)
end
function XATransaction:rollback_start(deadline_ms)
  if self._callback then error("XA callback owns the terminal decision", 2) end
  local op, err, code = self._core:rollback_start(deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, self._coordinator)
end
function XATransaction:borrow(resource_id)
  local borrow, err, code = self._core:borrow(resource_id)
  if borrow == nil then return nil, err, code end
  local wrapper = setmetatable({ _core = borrow }, XABorrow)
  if self._callback then
    self._borrows[#self._borrows + 1] = wrapper
  end
  return wrapper
end
function XATransaction:close_blocking()
  if self._callback then error("cannot close XA inside its callback", 2) end
  return self._core:close_blocking()
end
function XABorrow:view() return self._core:view() end
function XABorrow:close() return self._core:close() end
function XAOperation:advance() return self._core:advance() end
function XAOperation:wait() return self._core:wait() end
function XAOperation:result() return self._core:result() end
function XAOperation:cancel() return self._core:cancel() end
function XAOperation:close_blocking() return self._core:close_blocking() end
function XAOperation:take_transaction()
  local tx, err, code = self._core:take_transaction()
  if tx == nil then return nil, err, code end
  return wrap_xa_transaction(tx, self._coordinator)
end
local function xa_scope_lease(transaction, lease)
  if not transaction or not transaction._callback then return lease end
  local release = lease.release
  lease.release = function(receiver, ...)
    if transaction._callback then error("XA callback owns the lease terminal decision", 2) end
    return release(receiver, ...)
  end
  for _, name in ipairs({ "update", "update_json", "mutate", "mutate_local",
      "metadata", "remove", "attach", "delete_attachment", "delete_all_attachments" }) do
    local method = lease[name]
    lease[name] = function(receiver, ...)
      local result = table.pack(pcall(method, receiver, ...))
      if not result[1] or result[2] == nil then transaction._failed = true end
      if not result[1] then error(result[2], 0) end
      return table.unpack(result, 2, result.n)
    end
  end
  transaction._leases[#transaction._leases + 1] = lease
  return lease
end
function XAOperation:take_lease()
  local lease, err, code = self._core:take_lease()
  if lease == nil then return nil, err, code end
  return xa_scope_lease(self._transaction, wrap_lease(lease))
end

local function xa_error(message, cause)
  return { code = core.ERR_INVALID, message = message, cause = cause }
end

-- The supplied scheduler may yield. Observe the native terminal status again
-- rather than trusting its return value; preserve evidence even if it throws.
local function xa_drive(op, await, take)
  local called = table.pack(pcall(await, op))
  local state = op:result()
  local value, err, code
  if not called[1] then
    err, code = xa_error("XA await callback failed", called[2]), core.ERR_INVALID
  elseif called[2] == nil and called[3] ~= nil then
    err, code = called[3], called[4] or core.ERR_INVALID
  elseif not state or not state.done then
    err, code = xa_error("XA await returned before operation completion"), core.ERR_INVALID
  else
    local done
    done, err, code = op:advance()
    if done then
      if take == "transaction" then value, err, code = op:take_transaction()
      elseif take == "lease" then value, err, code = op:take_lease()
      else value = state end
    end
  end
  if not value then op:cancel() end
  local closed, close_error, close_code = op:close_blocking()
  if not closed and not err then err, code = close_error, close_code end
  if err then return nil, err, code, state end
  return value, nil, nil, state
end

local function xa_application_operation(tx, start, take)
  if not tx._callback then
    return nil, xa_error("XA conveniences require a coordinator callback"), core.ERR_INVALID
  end
  local op, err, code = start()
  if op == nil then tx._failed = true; return nil, err, code end
  local value
  value, err, code = xa_drive(op, tx._await, take)
  if value == nil then tx._failed = true end
  return value, err, code
end

function XATransaction:enroll(resource_id)
  return xa_application_operation(self, function() return self:enroll_start(resource_id) end)
end
function XATransaction:renew()
  return xa_application_operation(self, function() return self:renew_start() end)
end
function XATransaction:acquire(request)
  local lease, err, code = xa_application_operation(self,
    function() return self:acquire_start(request) end, "lease")
  if not lease then return nil, err, code end
  return lease
end
function XATransaction:outbox(producer)
  if not self._callback then
    return nil, xa_error("XA outbox convenience requires a coordinator callback"), core.ERR_INVALID
  end
  local box, err, code = producer:begin()
  if not box then self._failed = true; return nil, err, code end
  local bound
  bound, err, code = box:bind_xa(self)
  if not bound then box:close(); self._failed = true; return nil, err, code end
  self._boxes[#self._boxes + 1] = box
  return box
end
function XATransaction:with_connection(resource_id, adapter, callback)
  local borrow, err, code = self:borrow(resource_id)
  if not borrow then self._failed = true; return nil, err, code end
  local created = table.pack(pcall(adapter, borrow))
  local session = created[1] and created[2] or nil
  local result
  if session then
    result = table.pack(pcall(callback, session))
    local closed = table.pack(pcall(function() return session:close() end))
    if result[1] and (not closed[1] or closed[2] == nil or closed[2] == false) then
      result = { false, closed[1] and closed[3] or closed[2], n = 2 }
    end
  else result = { false, created[1] and created[3] or created[2], n = 2 } end
  local released, release_error = borrow:close()
  if result[1] and not released then result = { false, release_error, n = 2 } end
  if not result[1] then self._failed = true; error(result[2], 0) end
  return table.unpack(result, 2, result.n)
end
function XACoordinator:transaction(callback, await)
  await = await or self._await
  if type(callback) ~= "function" or type(await) ~= "function" then
    return nil, xa_error("XA transaction requires callback and await functions"), core.ERR_INVALID
  end
  local begin_op, err, code = self:begin_start()
  if not begin_op then return nil, err, code end
  local tx, state
  tx, err, code, state = xa_drive(begin_op, await, "transaction")
  if not tx then return nil, err, code, state end
  tx._callback, tx._await = true, await
  tx._leases, tx._boxes, tx._borrows = {}, {}, {}
  local invoked = table.pack(pcall(callback, tx))
  tx._callback = false
  if not invoked[1] then err, code = xa_error("XA callback failed", invoked[2]), core.ERR_INVALID end
  if tx._failed and not err then err, code = xa_error("XA callback has failed application work"), core.ERR_INVALID end
  for _, borrow in ipairs(tx._borrows) do
    local closed = table.pack(pcall(borrow.close, borrow))
    if not closed[1] or not closed[2] then
      if not err then err, code = xa_error("XA borrow cleanup failed", closed[2]), core.ERR_INVALID end
    end
  end
  local terminal_op, start_error, start_code
  if err then terminal_op, start_error, start_code = tx:rollback_start()
  else terminal_op, start_error, start_code = tx:commit_start() end
  if not terminal_op and not err then
    err, code = start_error, start_code
    terminal_op, start_error, start_code = tx:rollback_start()
  end
  local applied, terminal_error, terminal_code
  if terminal_op then applied, terminal_error, terminal_code, state = xa_drive(terminal_op, await)
  else terminal_error, terminal_code = start_error, start_code end
  if not err and terminal_error then err, code = terminal_error, terminal_code end
  local receipts = {}
  if applied and applied.applied and applied.decision == 1 then
    for _, box in ipairs(tx._boxes) do
      local receipt, receipt_error = box:commit()
      if receipt then receipts[#receipts + 1] = receipt
      elseif not err then err, code = receipt_error, receipt_error.code end
    end
  end
  for _, objects in ipairs({ tx._boxes, tx._leases }) do
    for _, object in ipairs(objects) do
      local closed, close_error = pcall(object.close, object)
      if not closed and not err then err, code = xa_error("XA application cleanup failed", close_error), core.ERR_INVALID end
    end
  end
  local closed, close_error, close_code = tx:close_blocking()
  if not closed and not err then err, code = close_error, close_code end
  if err then return nil, err, code, state end
  return { txn_id = applied.txn_id, decision = applied.decision,
    applied = applied.applied, outboxes = receipts,
    values = table.pack(table.unpack(invoked, 2, invoked.n)) }
end

local function normalize_result(a, b)
  if a == nil then
    return nil, b
  end
  return a, b
end

local function encode_json(value)
  return lonejson.encode_json(value == nil and JSON_NULL or value)
end

local function decode_json(payload)
  return lonejson.decode_json(payload)
end

local function require_sink(sink, method, materializer)
  if sink == nil then
    error(method .. " requires a sink; use " .. materializer .. " to materialize")
  end
  return sink
end

local function with_json_content_type(req)
  local next_req = {}
  local k, v

  if req ~= nil then
    for k, v in pairs(req) do
      next_req[k] = v
    end
  end
  if next_req.content_type == nil then
    next_req.content_type = "application/json"
  end
  return next_req
end

local function unwrap_lease_ref(value)
  if type(value) == "table" and getmetatable(value) == Lease then
    return value._core
  end
  return value
end

local function unwrap_message_ref(value)
  if type(value) == "table" and getmetatable(value) == Message then
    return value._core
  end
  return value
end

local function flatten_lease_request(req)
  local next_req = {}
  local k, v
  local lease_ref

  if type(req) == "table" and getmetatable(req) == Lease then
    return req:info()
  end
  req = req or {}
  for k, v in pairs(req) do
    next_req[k] = v
  end
  lease_ref = req.lease
  if lease_ref ~= nil and next_req.namespace == nil then
    if type(lease_ref) == "table" and getmetatable(lease_ref) == Lease then
      lease_ref = lease_ref:info()
    end
    if type(lease_ref) == "table" then
      for k, v in pairs(lease_ref) do
        if next_req[k] == nil then
          next_req[k] = v
        end
      end
    end
  end
  next_req.lease = nil
  return next_req
end

local function flatten_message_request(req)
  local next_req = {}
  local k, v
  local message_ref

  if type(req) == "table" and getmetatable(req) == Message then
    return req:info()
  end
  req = req or {}
  for k, v in pairs(req) do
    next_req[k] = v
  end
  message_ref = req.message
  if message_ref ~= nil and next_req.namespace == nil then
    if type(message_ref) == "table" and getmetatable(message_ref) == Message then
      message_ref = message_ref:info()
    end
    if type(message_ref) == "table" then
      for k, v in pairs(message_ref) do
        if next_req[k] == nil then
          next_req[k] = v
        end
      end
    end
  end
  next_req.message = nil
  return next_req
end

function M.encode_json(value)
  return encode_json(value)
end

function M.decode_json(payload)
  return decode_json(payload)
end

function M.version_string()
  return core.version_string()
end

function M.xid_new()
  return core.xid_new()
end

function M.pouch_crypto_generate_key()
  return core.pouch_crypto_generate_key()
end

function M.pouch_crypto_default_key_file()
  return core.pouch_crypto_default_key_file()
end

function M.pouch_crypto_generate_key_file(path, overwrite)
  return core.pouch_crypto_generate_key_file(path, overwrite)
end

M.json_null = JSON_NULL
M.OK = core.OK
M.ERR_INVALID = core.ERR_INVALID
M.ERR_NOMEM = core.ERR_NOMEM
M.ERR_TRANSPORT = core.ERR_TRANSPORT
M.ERR_PROTOCOL = core.ERR_PROTOCOL
M.ERR_SERVER = core.ERR_SERVER
M.ERR_TIMEOUT = core.ERR_TIMEOUT
M.ERR_PENDING = core.ERR_PENDING
M.NACK_FAILURE = core.NACK_FAILURE
M.NACK_DEFER = core.NACK_DEFER
M.COMMAND_PENDING = core.COMMAND_PENDING
M.COMMAND_COMPLETED = core.COMMAND_COMPLETED
M.COMMAND_FAILED = core.COMMAND_FAILED
for name, value in pairs(core) do
  if name:match("^XA_") then M[name] = value end
end

-- pouch.compression omission inherits an existing root (new roots use none).
-- Explicit none/zlib converts before open returns when exclusively owned;
-- competing shared writers cause a warning and use of the stored mode.
-- Conversion errors return the usual structured open error.
-- Conversion replaces one segment/snapshot at a time, with no second root.
-- Interrupted conversion resumes on the next open even when compression is nil.
-- Durable sync dispatches ready groups without a fill delay. Its process-local
-- fsync_batch_max_ops is an upper bound (0 is unbounded), not a minimum to await.
function M.open(config)
  local client, err = core.open(config)

  if client == nil then
    return nil, err
  end
  return wrap_client(client)
end

function Client:info()
  return self._core:info()
end

function Client:close()
  if self._core ~= nil then
    self._core:close()
    self._core = nil
  end
end

-- Creates a threadless producer. Delivery and terminal-receipt/dead-letter
-- reclamation require an explicit outbox:dispatcher(); options may attach one.
-- Ordinary state and collection expiry instead use Pouch's root maintenance.
-- Completed/inbox/command retention defaults independently to 30 days. Seconds
-- 0 select defaults, -1 disables that type (including its count cap); positive
-- seconds override. Their count fields are unbounded with nil, 0, or false.
-- Dead letters separately default to 30 days, 1024 records, and 64 MiB; false
-- disables an individual capacity bound. Reclamation ends deduplication and
-- command-status availability. Keep the same config across namespace users.
function Client:new_outbox(config, options)
  local core_options = options

  if options ~= nil and options.dispatcher ~= nil then
    core_options = { dispatcher = options.dispatcher._core }
  end
  local outbox, err = self._core:new_outbox(config, core_options)

  if outbox == nil then
    return nil, err
  end
  return wrap_outbox(outbox)
end

-- Collection entries retain their stored JSON bodies and permanent positions.
-- Retention metadata can change separately; omitted retention preserves it.
-- Collection retention drops the collection and all entries; entry retention
-- deletes only that entry. Both default to no expiry and are reclaimed lazily.
-- Optional max_entries is a canonical uint64 decimal string ("1000"). It caps
-- live entries by deleting oldest positions during maintenance, independently
-- of age expiry. "0" disables it; append may temporarily exceed the cap.
-- Counts do not include retry tombstones or bound physical disk usage.
-- Collections are Pouch-only and append independently of implicit/managed XA.
function Client:create_collection(config)
  local receiver = wrap_collection(false)
  local value, err, code = self._core:create_collection(config)
  if value == nil then return nil, err, code end
  receiver._core = value
  return receiver
end

function Client:open_collection(config)
  local receiver = wrap_collection(false)
  local value, err, code = self._core:open_collection(config)
  if value == nil then return nil, err, code end
  receiver._core = value
  return receiver
end

function Client:collection_capabilities()
  return self._core:collection_capabilities()
end

function Client:list_collections(options, visitor)
  return self._core:list_collections(options, visitor)
end

function Collection:describe() return self._core:describe() end
-- Acquire once before restoring a writable journal. Each append commits while
-- ownership remains held. Public reads are allowed unless private_read=true;
-- release/expiry restores public visibility. TTL renewal is separate from
-- retention. A stale owned receiver never falls back to multiwriter mutation.
function Collection:acquire(request)
  -- Reserve the facade table before publishing durable ownership.
  local receiver = wrap_collection(false)
  local value, err, code = self._core:acquire(request)
  if value == nil then return nil, err, code end
  receiver._core = value
  return receiver
end
-- Explicitly replace exactly the lease ID observed by describe().lease.
function Collection:takeover(request, observed_lease_id)
  local receiver = wrap_collection(false)
  local value, err, code = self._core:takeover(request, observed_lease_id)
  if value == nil then return nil, err, code end
  receiver._core = value
  return receiver
end
function Collection:keepalive(request) return self._core:keepalive(request) end
-- Success closes this receiver. Close/GC alone leaves durable ownership held.
function Collection:release() return self._core:release() end
-- nil/omitted retention_seconds preserves, 0 clears, positive integer seconds
-- reset expiry from this commit. Collection policy does not assign entry ages.
-- max_entries omission preserves; "0" disables; a positive decimal string
-- changes the cap for existing and future entries without restoring deletions.
-- describe/list return max_entries; collection_capabilities has count_retention.
function Collection:metadata(request) return self._core:metadata(request) end
-- Entry metadata accepts retention_seconds only; max_entries is collection-only.
function Collection:entry_metadata(position, request)
  return self._core:entry_metadata(position, request)
end
-- Atomic collection-local cut/replacement/marker. Observe head().etag first;
-- retain operation_id and the original request for uncertain-outcome retries.
-- Ranges are ordered {after=..., through=...} intervals. Optional replacement
-- uses append options and a borrowed streaming source. Recovery is <=1024 UTF-8
-- bytes; omission preserves and "" clears. recovery_anchor="replacement" binds
-- this edit's new entry without guessing its position. It does not pin entries.
function Collection:head() return self._core:head() end
-- Identical retries bypass a stale head, while still revalidating ownership
-- after source callbacks; rejection never rolls back the prior commit.
function Collection:edit(request, source) return self._core:edit(request, source) end
function Collection:edit_status(operation_id) return self._core:edit_status(operation_id) end

-- Exactly one JSON object. Keep append_id before a single-pass upload; retry
-- the same compact body/ID or inspect append_status after an ambiguous error.
-- An identical retry preserves the original position and retention deadline.
-- All append forms accept collection_retention_seconds for the parent:
-- omission preserves, 0 clears, positive seconds reset at new publication.
-- Only new membership renews. Results/status describe the original directive;
-- describe() reports the parent's current deadline.
-- Direct append retains append authority while consuming the source. Use
-- append_staged for sources that may wait; neither form resets/replays input.
function Collection:append(request, source) return self._core:append(request, source) end
-- Explicit staged pull consumes the source once through the same writer used
-- below, without holding publication locks while reading. It preserves append's
-- callback/retry rules and accepts the same sources and retention directives.
-- Useful for slow/concurrent input; direct append can cost less for fast input.
-- The Lua source adapter is closed on return, including errors. Source failure
-- publishes nothing; commit/result errors may need append_status reconciliation.
function Collection:append_staged(request, source)
  return self._core:append_staged(request, source)
end
-- Push raw JSON fragments; Pouch validates/encodes once, buffers up to 64 KiB
-- of encoded bytes, then uses an anonymous spool for larger entries. Commit
-- copies those bytes into a shared segment. Close after commit or failure.
-- collection_retention_seconds is atomic with a new append; omission preserves,
-- 0 clears, positive seconds reset from commit. Identical retries do not renew.
function Collection:append_begin(request)
  local writer, err, code = self._core:append_begin(request)
  if writer == nil then return nil, err, code end
  return setmetatable({ _core = writer }, CollectionWriter)
end
-- write borrows a raw JSON string for this call; an empty string is not EOF.
-- write/abort return true; commit returns the append result. Errors follow the
-- normal nil,error,code convention, including uncertain publication errors.
-- The owned writer survives public client/collection close. Commit is terminal;
-- abort cannot undo publication; close/GC abort unfinished work and never commit.
function CollectionWriter:write(bytes) return self._core:write(bytes) end
function CollectionWriter:commit() return self._core:commit() end
function CollectionWriter:abort() return self._core:abort() end
function CollectionWriter:close() return self._core:close() end
function Collection:append_status(append_id) return self._core:append_status(append_id) end
-- Requires a sink. Raw JSONL reaches it directly; cursor/retention facts arrive
-- separately through on_metadata. Success is nil, result; errors also return
-- the accepted partial result as the fourth value. All uint64 facts are strings.
-- after (exclusive), through (inclusive), and first_n are optional canonical
-- uint64 decimal strings. first_n selects live inputs before LQL; filtered-out
-- inputs consume N, deleted entries do not. Resume retains the input budget;
-- a completed window's checkpoint continues after its last examined input.
-- Repeat all selection options when supplying either cursor purpose.
-- Each call captures a new view: later deletion/expiry can remove entries from
-- a resumed window. Discard incomplete JSON bytes before replaying that entry.
function Collection:read(options, sink) return self._core:read(options, sink) end
function Collection:delete_entry(position) return self._core:delete_entry(position) end
function Collection:drop() return self._core:drop() end
function Collection:close() return self._core:close() end

function Client:new_history_consumer(config)
  local consumer, err = self._core:new_history_consumer(config)

  if consumer == nil then
    return nil, err
  end
  return wrap_history_consumer(consumer)
end

function HistoryConsumer:position()
  return self._core:position()
end

function HistoryConsumer:advance(acknowledged_index_seq)
  return self._core:advance(acknowledged_index_seq)
end

function HistoryConsumer:unregister()
  return self._core:unregister()
end

function HistoryConsumer:close()
  if self._core ~= nil then
    self._core:close()
    self._core = nil
    self._closed = true
  end
end

function Client:acquire(req)
  local lease, err = self._core:acquire(req)

  if lease == nil then
    return nil, err
  end
  return wrap_lease(lease)
end

-- Buffered callback convenience: C first spools the complete private snapshot
-- to a temporary file, then the binding copies it into the Lua state string.
-- Use acquire() plus lease:get(opts, sink) for transfer without staging a body.
-- Return nil or forward SDK results for success. Failure is nil,error, a string,
-- false, or a raised error; error tables belong in the second return value.
function Client:acquire_for_update(req, handler)
  return normalize_result(self._core:acquire_for_update(req, function(ctx)
    local lease = wrap_lease(ctx.lease)
    local af = {
      lease = lease,
      state = ctx.state,
      state_meta = ctx.state_meta,
    }

    function af:load_json()
      if self.state == nil then
        return nil, self.state_meta
      end
      return decode_json(self.state), self.state_meta
    end

    function af:update(body, opts)
      return lease:update(body, opts)
    end

    function af:update_json(value, opts)
      return lease:update_json(value, opts)
    end

    function af:mutate(update_req)
      return lease:mutate(update_req)
    end

    function af:mutate_local(update_req)
      return lease:mutate_local(update_req)
    end

    function af:metadata(update_req)
      return lease:metadata(update_req)
    end

    function af:remove(update_req)
      return lease:remove(update_req)
    end

    function af:keepalive(update_req)
      return lease:keepalive(update_req)
    end

    local ok, result, handler_err = pcall(handler, af)
    lease:close()
    if not ok then
      error(result)
    end
    if handler_err ~= nil then
      return nil, handler_err
    end
    if result == false then
      return nil, "acquire_for_update handler returned false"
    end
    if type(result) == "string" then
      return nil, result
    end
    return nil
  end))
end

function Client:describe(req)
  return self._core:describe(req)
end

function Client:get(req, sink)
  -- `req.namespace` selects an explicit namespace; omitted keeps the client
  -- default. This mirrors lc_get_in_namespace without exposing C maps.
  return self._core:get(req, require_sink(sink, "client:get", "client:read"))
end

function Client:read(req)
  return self._core:get(req)
end

function Client:read_json(req)
  local payload, meta_or_err = self:read(req)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function Client:update(req, body)
  return self._core:update(flatten_lease_request(req), body)
end

-- retention_seconds is forwarded unchanged: omission preserves the policy,
-- zero clears it, and positive seconds start at the transaction's commit.
function Client:update_json(req, value)
  return self:update(with_json_content_type(req), encode_json(value))
end

function Client:mutate(req)
  return self._core:mutate(flatten_lease_request(req))
end

function Client:metadata(req)
  return self._core:metadata(flatten_lease_request(req))
end

function Client:remove(req)
  return self._core:remove(flatten_lease_request(req))
end

function Client:keepalive(req)
  return self._core:keepalive(flatten_lease_request(req))
end

function Client:release(req)
  return self._core:release(flatten_lease_request(req))
end

function Client:attach(req, body)
  return self._core:attach(flatten_lease_request(req), body)
end

function Client:list_attachments(req)
  return self._core:list_attachments(flatten_lease_request(req))
end

function Client:get_attachment(req, sink)
  return self._core:get_attachment(flatten_lease_request(req),
    require_sink(sink, "client:get_attachment", "client:read_attachment"))
end

function Client:read_attachment(req)
  return self._core:get_attachment(flatten_lease_request(req))
end

function Client:delete_attachment(req)
  return self._core:delete_attachment(flatten_lease_request(req))
end

function Client:delete_all_attachments(req)
  return self._core:delete_all_attachments(flatten_lease_request(req))
end

function Client:queue_stats(req)
  return self._core:queue_stats(req)
end

function Client:queue_ack(message_or_req)
  return self._core:queue_ack(unwrap_message_ref(message_or_req))
end

function Client:queue_nack(req)
  return self._core:queue_nack(flatten_message_request(req))
end

function Client:queue_extend(req)
  return self._core:queue_extend(flatten_message_request(req))
end

function Client:query(req, sink)
  return self._core:query(req,
    require_sink(sink, "client:query", "client:read_query"))
end

function Client:read_query(req)
  return self._core:query(req)
end

function Client:query_keys(req, handler)
  return self._core:query_keys(req, handler)
end

function Client:get_namespace_config(req)
  return self._core:get_namespace_config(req)
end

function Client:update_namespace_config(req)
  return self._core:update_namespace_config(req)
end

function Client:flush_index(req)
  return self._core:flush_index(req)
end

function Client:txn_replay(req)
  return self._core:txn_replay(req)
end

function Client:txn_prepare(req)
  return self._core:txn_prepare(req)
end

function Client:txn_commit(req)
  return self._core:txn_commit(req)
end

function Client:txn_rollback(req)
  return self._core:txn_rollback(req)
end

function Client:tc_lease_acquire(req)
  return self._core:tc_lease_acquire(req)
end

function Client:tc_lease_renew(req)
  return self._core:tc_lease_renew(req)
end

function Client:tc_lease_release(req)
  return self._core:tc_lease_release(req)
end

function Client:tc_leader()
  return self._core:tc_leader()
end

function Client:tc_cluster_announce(req)
  return self._core:tc_cluster_announce(req)
end

function Client:tc_cluster_leave()
  return self._core:tc_cluster_leave()
end

function Client:tc_cluster_list()
  return self._core:tc_cluster_list()
end

function Client:tc_rm_register(req)
  return self._core:tc_rm_register(req)
end

function Client:tc_rm_unregister(req)
  return self._core:tc_rm_unregister(req)
end

function Client:tc_rm_list()
  return self._core:tc_rm_list()
end

function Client:enqueue(req, body)
  return self._core:enqueue(req, body)
end

function Client:dequeue(req)
  local message, err = self._core:dequeue(req)

  if message == nil then
    return nil, err
  end
  return wrap_message(message)
end

function Client:dequeue_batch(req)
  local batch, err = self._core:dequeue_batch(req)
  local i

  if batch == nil then
    return nil, err
  end
  for i = 1, #batch do
    batch[i] = wrap_message(batch[i])
  end
  return batch
end

function Client:dequeue_with_state(req)
  local message, err = self._core:dequeue_with_state(req)

  if message == nil then
    return nil, err
  end
  return wrap_message(message)
end

function Lease:info()
  return self._core:info()
end

function Lease:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function Lease:describe()
  return self._core:describe()
end

function Lease:get(opts, sink)
  return self._core:get(opts, require_sink(sink, "lease:get", "lease:read"))
end

function Lease:read(opts)
  return self._core:get(opts)
end

function Lease:read_json(opts)
  local payload, meta_or_err = self:read(opts)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function Lease:update(body, req)
  return self._core:update(body, req)
end

-- State retention uses the same request field as client and XA participant
-- writes. Returned metadata reports support and the observed private deadline.
function Lease:update_json(value, req)
  return self:update(encode_json(value), with_json_content_type(req))
end

function Lease:mutate(req)
  return self._core:mutate(req)
end

function Lease:mutate_local(req)
  return self._core:mutate_local(req)
end

function Lease:metadata(req)
  return self._core:metadata(req)
end

function Lease:remove(req)
  return self._core:remove(req)
end

function Lease:keepalive(req)
  return self._core:keepalive(req)
end

-- Pouch retires automatically completed single-key retention decisions here.
-- A cleanup error may follow publication; it is not proof of rollback.
-- Explicit/joined/managed XA retain their separate outcome lifecycle.
function Lease:release(req)
  local ok, err = normalize_result(self._core:release(req))

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function Lease:attach(req, body)
  return self._core:attach(req, body)
end

function Lease:list_attachments()
  return self._core:list_attachments()
end

function Lease:get_attachment(req, sink)
  return self._core:get_attachment(req,
    require_sink(sink, "lease:get_attachment", "lease:read_attachment"))
end

function Lease:read_attachment(req)
  return self._core:get_attachment(req)
end

function Lease:delete_attachment(selector)
  return self._core:delete_attachment(selector)
end

function Lease:delete_all_attachments()
  return self._core:delete_all_attachments()
end

function Message:info()
  return self._core:info()
end

function Message:is_open()
  return not self._closed
end

function Message:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function Message:ack()
  local ok, err = normalize_result(self._core:ack())

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function Message:nack(req)
  local ok, err = normalize_result(self._core:nack(req))

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function Message:extend(req)
  return self._core:extend(req)
end

function Message:state()
  local lease = self._core:state()

  if lease == nil then
    return nil
  end
  return wrap_lease(lease)
end

function Message:rewind_payload()
  return self._core:rewind_payload()
end

function Message:write_payload(sink)
  return self._core:payload(
    require_sink(sink, "message:write_payload", "message:read_payload"))
end

function Message:read_payload()
  return self._core:payload()
end

function Message:read_payload_json()
  local payload, written_or_err = self:read_payload()

  if payload == nil then
    return nil, written_or_err
  end
  return decode_json(payload), written_or_err
end

function Outbox:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function Outbox:begin()
  local transaction, err = self._core:begin()

  if transaction == nil then
    return nil, err
  end
  return wrap_outbox_transaction(transaction)
end

function Outbox:transaction(fn)
  return self._core:transaction(function(transaction)
    return fn(wrap_outbox_transaction(transaction))
  end)
end

-- Gets or starts the one compatible local dispatcher, including maintenance.
-- Closing a producer or dispatcher wrapper does not stop the shared worker.
function Outbox:dispatcher()
  local dispatcher, err = self._core:dispatcher()

  if dispatcher == nil then
    return nil, err
  end
  return wrap_outbox_dispatcher(dispatcher)
end

function Outbox:append(entry, payload)
  local transaction, receipt_or_err = self._core:append(entry, payload)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil and type(receipt_or_err) ~= "table" then
    return nil, receipt_or_err
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_outbox_transaction(transaction), receipt_or_err
end

function Outbox:accept_inbox(message)
  local transaction, result_or_err = self._core:accept_inbox(message)

  if transaction == nil and result_or_err == nil then
    return nil
  end
  if transaction == nil and type(result_or_err) ~= "table" then
    return nil, result_or_err
  end
  if transaction == nil then
    return nil, result_or_err
  end
  return wrap_outbox_transaction(transaction), result_or_err
end

function Outbox:accept_command(request)
  local transaction, receipt_or_err = self._core:accept_command(request)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil and type(receipt_or_err) ~= "table" then
    return nil, receipt_or_err
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_outbox_transaction(transaction), receipt_or_err
end

-- Reads committed status only. A reclaimed terminal receipt is no longer
-- available; it is not reconstructed or reopened by a status read.
function Outbox:get_command_receipt(identity)
  return self._core:get_command_receipt(identity)
end

-- Reads one durable receipt by the component-generated command id.
function Outbox:get_command_receipt_by_id(command_id)
  return self._core:get_command_receipt_by_id(command_id)
end

-- Waits only for a durable terminal receipt; it never performs dispatch.
-- 0 reads once, -1 waits indefinitely, and positive milliseconds bound the wait
-- and each remote read. Timeout returns pending_receipt, error; other failures
-- return nil, error, code. A reclaimed receipt is a normal missing-record error.
function Outbox:wait_command(command_id, timeout_ms)
  return self._core:wait_command(command_id, timeout_ms)
end

function Outbox:write_command_result(identity, sink)
  return self._core:write_command_result(identity, require_sink(sink,
    "outbox:write_command_result", "outbox:read_command_result"))
end

function Outbox:read_command_result(identity)
  return self._core:write_command_result(identity)
end

function Outbox:resume_command(identity)
  local transaction, receipt_or_err = self._core:resume_command(identity)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil and type(receipt_or_err) ~= "table" then
    return nil, receipt_or_err
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_outbox_transaction(transaction), receipt_or_err
end

-- Resumes a pending receipt by command id for a supervisor-owned finalizer.
-- A terminal receipt returns nil, receipt; a missing/invalid id returns nil,
-- error. Retention can remove terminal status; resume never recreates it.
function Outbox:resume_command_by_id(command_id)
  local transaction, receipt_or_err = self._core:resume_command_by_id(command_id)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_outbox_transaction(transaction), receipt_or_err
end

function OutboxDispatcher:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
  -- A Lua handler binding lasts while at least one dispatcher wrapper is
  -- reachable. This wrapper no longer needs to retain its adapter map once it
  -- has surrendered its receiver reference.
  release_dispatcher_handler_map(self)
end

function OutboxDispatcher:next(timeout_ms)
  local job, err = self._core:next(timeout_ms)

  if job == nil then
    return nil, err
  end
  return wrap_outbox_job(job)
end

-- Explicitly opts this pull into the paired outbox/checkpoint claim. The
-- native boundary rejects backends that cannot provide that durable invariant;
-- it never silently substitutes a stateless job.
function OutboxDispatcher:next_with_state(timeout_ms)
  local job, err = self._core:next_with_state(timeout_ms)

  if job == nil then
    return nil, err
  end
  return wrap_outbox_job(job)
end

local function dispatcher_handler_options(self, options)
  local handlers
  local key, handler
  local handler_count
  local core_options
  local core_handlers
  local cached_handlers
  local activation
  local activate_map
  local retain_map

  if type(options) ~= "table" then
    return options
  end
  handlers = options.handlers
  if type(handlers) ~= "table" then
    return options
  end
  handler_count = 0
  for key, handler in pairs(handlers) do
    if type(key) ~= "string" or key == "" or key:find("\0", 1, true) ~= nil or
        type(handler) ~= "function" then
      -- Let the native boundary report the standard structured argument error.
      return options
    end
    handler_count = handler_count + 1
  end
  if handler_count == 0 then
    return options
  end
  cached_handlers = self._handler_binding_state.cached_handlers
  if self._handler_binding_state.handlers ~= handlers then
    cached_handlers = nil
  end
  activation = { handlers = handlers, cached_handlers = cached_handlers }
  activate_map = function()
    local source = activation.handlers

    if source == nil then
      return
    end
    self._handler_binding_state.handlers = source
    self._handler_binding_state.cached_handlers = activation.cached_handlers
    -- The adapter callbacks retain this closure after binding. Drop the
    -- application table as soon as the adapter is published so a fully closed
    -- dispatcher does not keep the caller's handler map alive.
    activation.handlers = nil
    activation.cached_handlers = nil
  end
  retain_map = function()
    activate_map()
    if self._closed then
      return
    end
    self._handler_core_map = cached_handlers.core_handlers
  end
  if cached_handlers == nil then
    core_handlers = {}
    for key, handler in pairs(handlers) do
      local handler_function = handler

      core_handlers[key] = function(core_job)
        -- This wrapper is entered only after native code has accepted this
        -- exact map. Promote before application code can recurse through an
        -- alias while the first pump/run invocation remains active.
        activate_map()
        return handler_function(wrap_outbox_job(core_job))
      end
    end
    cached_handlers = { core_handlers = core_handlers }
    activation.cached_handlers = cached_handlers
  end
  core_options = {}
  for key, handler in pairs(options) do
    core_options[key] = handler
  end
  core_options.handlers = cached_handlers.core_handlers
  -- The native layer changes this only after it has accepted this exact
  -- adapter as the dispatcher's immutable handler map.
  core_options._lockdc_facade_handlers_bound = false
  return core_options, retain_map
end

local function dispatcher_handler_call(self, options, method)
  local core_options
  local retain_map
  local result
  local error_result
  local status

  core_options, retain_map = dispatcher_handler_options(self, options)
  if retain_map == nil then
    return method(self._core, core_options)
  end
  result, error_result, status = method(self._core, core_options)
  if core_options._lockdc_facade_handlers_bound then
    retain_map()
  end
  return result, error_result, status
end

function OutboxDispatcher:pump(options)
  return dispatcher_handler_call(self, options, self._core.pump)
end

function OutboxDispatcher:run(options)
  return dispatcher_handler_call(self, options, self._core.run)
end

function OutboxDispatcher:notify_outbox_key(outbox_key)
  return self._core:notify_outbox_key(outbox_key)
end

-- Copies process-local counters. terminal_reclaims and dead_letter_reclaims
-- count retired markers (including stale ones), not records deleted. Failure
-- counters and last_error expose retryable maintenance errors without a query.
function OutboxDispatcher:stats()
  return self._core:stats()
end

function OutboxDispatcher:reconcile()
  return self._core:reconcile()
end

function OutboxDispatcher:replay_dead_letter(outbox_key)
  return self._core:replay_dead_letter(outbox_key)
end

function OutboxDispatcher:delete_dead_letter(outbox_key)
  return self._core:delete_dead_letter(outbox_key)
end

function OutboxDispatcher:export_dead_letters(options, sink)
  return self._core:export_dead_letters(options, require_sink(sink,
    "dispatcher:export_dead_letters", "dispatcher:read_dead_letters"))
end

function OutboxDispatcher:read_dead_letters(options)
  return self._core:export_dead_letters(options)
end

-- Requests stop and waits for private work and handed-out jobs: nil/-1 waits
-- indefinitely, 0 polls, positive milliseconds bound the wait. Finish or close
-- caller-owned jobs before waiting indefinitely. Timeout leaves it stopping;
-- outstanding jobs remain usable for a terminal outcome or close.
-- Stop/wait are rejected inside a dispatcher handler, including zero-time polls.
function OutboxDispatcher:stop(deadline_ms)
  return self._core:stop(deadline_ms)
end

-- Observes an already requested stop using stop()'s deadline convention; it
-- rejects a dispatcher that has not been stopped and never restarts it.
function OutboxDispatcher:wait(deadline_ms)
  return self._core:wait(deadline_ms)
end

function OutboxTransaction:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function OutboxTransaction:id() return self._core:id() end
function OutboxTransaction:bind_xa(transaction)
  local bound, err, code = self._core:bind_xa(transaction._core)
  if bound then self._xa_transaction = transaction end
  return bound, err, code
end
function OutboxTransaction:commit_start(deadline_ms)
  if self._xa_transaction and self._xa_transaction._callback then
    error("XA callback owns the outbox terminal decision", 2)
  end
  local op, err, code = self._core:commit_start(deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, nil)
end
function OutboxTransaction:rollback_start(deadline_ms)
  if self._xa_transaction and self._xa_transaction._callback then
    error("XA callback owns the outbox terminal decision", 2)
  end
  local op, err, code = self._core:rollback_start(deadline_ms)
  if op == nil then return nil, err, code end
  return wrap_xa_operation(op, nil)
end

function OutboxTransaction:acquire(req)
  local participant, err = self._core:acquire(req)

  if participant == nil then
    return nil, err
  end
  return wrap_outbox_participant(participant)
end

function OutboxTransaction:append(entry, payload)
  return self._core:append(entry, payload)
end

function OutboxTransaction:accept_command(request)
  return self._core:accept_command(request)
end

function OutboxTransaction:accept_inbox(message)
  return self._core:accept_inbox(message)
end

function OutboxTransaction:complete_command(result)
  return self._core:complete_command(result)
end

function OutboxTransaction:fail_command(result)
  return self._core:fail_command(result)
end

function OutboxTransaction:commit()
  local ok, err = normalize_result(self._core:commit())

  if ok ~= nil then
    self._terminal = true
  end
  return ok, err
end

function OutboxTransaction:rollback()
  local ok, err = normalize_result(self._core:rollback())

  if ok ~= nil then
    self._terminal = true
  end
  return ok, err
end

function OutboxParticipant:info()
  return self._core:info()
end

function OutboxParticipant:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function OutboxParticipant:describe()
  return self._core:describe()
end

function OutboxParticipant:get(opts, sink)
  return self._core:get(opts, require_sink(sink,
    "outbox participant:get", "outbox participant:read"))
end

function OutboxParticipant:read(opts)
  return self._core:get(opts)
end

function OutboxParticipant:read_json(opts)
  local payload, meta_or_err = self:read(opts)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function OutboxParticipant:update(body, opts)
  return self._core:update(body, opts)
end

-- Retention belongs to the business state, independently of outbox receipts:
-- omitted/nil preserves, 0 clears, positive seconds start at global commit.
function OutboxParticipant:update_json(value, opts)
  return self:update(encode_json(value), with_json_content_type(opts))
end

function OutboxParticipant:mutate(req)
  return self._core:mutate(req)
end

function OutboxParticipant:mutate_local(req)
  return self._core:mutate_local(req)
end

function OutboxParticipant:metadata(req)
  return self._core:metadata(req)
end

function OutboxParticipant:remove(req)
  return self._core:remove(req)
end

function OutboxParticipant:keepalive(req)
  return self._core:keepalive(req)
end

function OutboxParticipant:attach(req, body)
  return self._core:attach(req, body)
end

function OutboxParticipant:list_attachments()
  return self._core:list_attachments()
end

function OutboxParticipant:get_attachment(req, sink)
  return self._core:get_attachment(req, require_sink(sink,
    "outbox participant:get_attachment", "outbox participant:read_attachment"))
end

function OutboxParticipant:read_attachment(req)
  return self._core:get_attachment(req)
end

function OutboxParticipant:delete_attachment(selector)
  return self._core:delete_attachment(selector)
end

function OutboxParticipant:delete_all_attachments()
  return self._core:delete_all_attachments()
end

function OutboxJob:info()
  return self._core:info()
end

function OutboxJob:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function OutboxJob:write_payload(sink)
  return self._core:write_payload(require_sink(sink,
    "outbox job:write_payload", "outbox job:read_payload"))
end

-- Returns a normal Lua lease facade for a stateful job's paired checkpoint.
-- The job owns the claim lifetime: do not close this borrowed wrapper or use
-- it after job completion, retry, dead-letter, or close.
function OutboxJob:state()
  local lease = self._core:state()

  if lease == nil then
    return nil
  end
  return wrap_lease(lease)
end

function OutboxJob:read_payload()
  return self._core:write_payload()
end

function OutboxJob:read_payload_json()
  local payload, written_or_err = self:read_payload()

  if payload == nil then
    return nil, written_or_err
  end
  return decode_json(payload), written_or_err
end

-- Renews both leases for a stateful job. Checkpoint writes may precede or follow
-- renewal; repeated renewal preserves the versions needed for terminalization.
function OutboxJob:renew(ttl_seconds)
  return self._core:renew(ttl_seconds)
end

function OutboxJob:complete(completion)
  local ok, err = normalize_result(self._core:complete(completion))

  if ok ~= nil then
    self._terminal = true
    self._closed = true
  end
  return ok, err
end

function OutboxJob:retry(req)
  local ok, err = normalize_result(self._core:retry(req))

  if ok ~= nil then
    self._terminal = true
    self._closed = true
  end
  return ok, err
end

function OutboxJob:dead_letter(diagnostic)
  local ok, err = normalize_result(self._core:dead_letter(diagnostic))

  if ok ~= nil then
    self._terminal = true
    self._closed = true
  end
  return ok, err
end

local function run_threadless_service(client, req, with_state, handler, should_stop)
  local dequeue_fn

  if with_state then
    dequeue_fn = client.dequeue_with_state
  else
    dequeue_fn = client.dequeue
  end

  while true do
    if should_stop ~= nil and should_stop() then
      return true
    end

    local message, err = dequeue_fn(client, req)
    local ok, handler_result, handler_err
    local state

    if should_stop ~= nil and should_stop() then
      return true
    end
    if message == nil then
      return nil, err
    end
    state = with_state and message:state() or nil
    ok, handler_result, handler_err = pcall(handler, message, state)
    if state ~= nil then
      state:close()
      state = nil
    end
    if ok and (handler_result == nil and handler_err == nil or
        handler_result == true) then
      if message:is_open() then
        local ack_ok, ack_err = message:ack()

        if ack_ok == nil then
          return nil, ack_err
        end
      end
    else
      local failure = handler_err or handler_result

      if not ok then
        failure = handler_result
      end
      if failure == nil or failure == false then
        failure = {
          code = core.ERR_INVALID,
          message = "Lua consumer handler returned false",
        }
      elseif type(failure) ~= "table" then
        failure = {
          code = core.ERR_INVALID,
          message = tostring(failure),
        }
      end
      if message:is_open() then
        local nack_ok, nack_err = message:nack({ intent = "failure" })

        if nack_ok == nil then
          return nil, nack_err
        end
      end
      return nil, failure
    end
  end
end

local function invoke_subscription_handler(handler, core_message, core_state)
  local message = wrap_message(core_message)
  local state = core_state ~= nil and wrap_lease(core_state) or nil
  local ok, first, second = pcall(handler, message, state)

  -- Subscription deliveries and their attached state leases are borrowed from
  -- the native callback frame. Keep the public wrapper's lifecycle truthful
  -- when an application retains either object after the callback returns.
  message._closed = true
  if state ~= nil then
    state._closed = true
  end
  if not ok then
    error(first, 0)
  end
  return first, second
end

function Client:subscribe(req, handler)
  if type(handler) ~= "function" then
    error("client:subscribe requires a handler function")
  end
  return self._core:subscribe(req, function(message)
    return invoke_subscription_handler(handler, message, nil)
  end)
end

function Client:subscribe_with_state(req, handler)
  if type(handler) ~= "function" then
    error("client:subscribe_with_state requires a handler function")
  end
  return self._core:subscribe_with_state(req, function(message, state)
    return invoke_subscription_handler(handler, message, state)
  end)
end

function Client:watch_queue(req, handler)
  return self._core:watch_queue(req, handler)
end

function Client:new_consumer_service(config)
  return setmetatable({
    _client = self,
    _config = config,
    _stop_requested = false,
  }, Service)
end

function Service:stop()
  self._stop_requested = true
  return true
end

function Service:wait()
  if self._completed or self._stop_requested then
    return true
  end
  return nil, {
    code = core.ERR_INVALID,
    message = "lockdc Lua consumer service wait() only becomes meaningful after run() completes",
  }
end

function Service:run()
  local config = self._config
  local req = {}
  local k, v

  if type(config) ~= "table" then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service requires one config table",
    }
  end
  if type(config.request) ~= "table" then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service config requires request",
    }
  end
  if type(config.handle) ~= "function" then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service config requires handle",
    }
  end
  for k, v in pairs(config.request) do
    req[k] = v
  end
  req.owner = req.owner or config.name or req.queue
  local ok, err = run_threadless_service(self._client, req,
    config.with_state == true, config.handle, function()
      return self._stop_requested
    end)
  if ok == nil then
    self._completed = true
    return nil, err
  end
  self._completed = true
  return true
end

function Service:close()
  self:stop()
  self._completed = true
end



return M
