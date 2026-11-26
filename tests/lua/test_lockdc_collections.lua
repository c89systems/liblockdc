local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
local client = assert(core.open({ endpoints = { "pouch://" .. lockdc_test_root },
  default_namespace = "journal", disable_mtls = true,
  pouch = { background_compaction = false, query_indexing = true } }))
local function rejected(value, err, code)
  assert(value == nil and err and err.message and code == core.ERR_INVALID,
    err and err.message or "expected structured validation failure")
end
local function stream(bytes, close)
  local offset = 1
  return { read = function(capacity)
    if offset > #bytes then return nil end
    local chunk = bytes:sub(offset, offset + math.min(capacity, 3) - 1)
    offset = offset + #chunk
    return chunk
  end, close = close }
end
local function read(collection, options)
  local chunks = {}
  local value, result, code, partial = collection:read(options, {
    write = function(chunk) chunks[#chunks + 1] = chunk end,
  })
  assert(value == nil and code == nil, result and result.message)
  assert(partial == nil)
  return table.concat(chunks), result
end
local capabilities = assert(client:collection_capabilities())
assert(capabilities.collections and capabilities.lql_filter and capabilities.resume)
assert(capabilities.checkpoint and capabilities.ranges and capabilities.first_n and not capabilities.last_n)
assert(capabilities.count_retention)
local collection = assert(client:create_collection({ name = "events", retention_seconds = 3600 }))
local info = assert(collection:describe())
assert(info.name == "events" and info.namespace == "journal" and info.live_count == "0")
assert(info.upper_position == "0" and info.retention_support == "supported")
assert(info.retention_expires_at_unix >= os.time())
local parent_deadline = info.retention_expires_at_unix
assert(info.max_entries == "0")

local closed = 0
local first = assert(collection:append({ append_id = "one", retention_seconds = 60 },
  stream(' { "n" : 1 } ', function() closed = closed + 1 end)))
assert(closed == 1 and first.position == "1" and first.bytes == "7")
assert(#first.etag == 64 and first.retention_support == "supported")
assert(first.retention_expires_at_unix > os.time())
local duplicate = assert(collection:append({ append_id = "one", retention_seconds = 600 }, '{"n":1}'))
assert(duplicate.duplicate and duplicate.position == "1")
assert(duplicate.retention_expires_at_unix == first.retention_expires_at_unix)
local second = assert(collection:append({ append_id = "two" }, stream('{"n":2}')))
assert(second.position == "2" and second.retention_expires_at_unix == 0)
assert(collection:describe().retention_expires_at_unix == parent_deadline)
local list = {}
assert(client:list_collections({}, function(item)
  list[#list + 1] = item.name
  assert(type(item.live_count) == "string" and item.retention_support == "supported")
end))
assert(#list == 1 and list[1] == "events")
rejected(client:list_collections({}, function() return false, "stop visitor" end))
local changed = assert(collection:entry_metadata("1", { retention_seconds = 120 }))
assert(changed.position == first.position and changed.etag == first.etag and changed.bytes == first.bytes)
assert(changed.retention_expires_at_unix >= first.retention_expires_at_unix)
assert(collection:entry_metadata("1", {}).retention_expires_at_unix == changed.retention_expires_at_unix)
assert(collection:metadata({}).retention_expires_at_unix == parent_deadline)
assert(collection:metadata({ retention_seconds = 0 }).retention_expires_at_unix == 0)
assert(collection:entry_metadata("1", { retention_seconds = 0 }).retention_expires_at_unix == 0)
local entries = 0
local payload, complete = read(collection, { on_metadata = function(meta)
  assert(type(meta.last_examined_position) == "string" and type(meta.resume_cursor) == "string")
  if meta.kind == "entry" then
    entries = entries + 1
    assert(meta.position == tostring(entries) and meta.bytes == "7")
    assert(meta.retention_support == "supported" and meta.retention_expires_at_unix == 0)
  else
    assert(meta.kind == "progress" and meta.position == nil and meta.retention_support == nil)
  end
end })
assert(payload == '{"n":1}\n{"n":2}\n' and entries == 2)
assert(complete.complete and complete.completed_records == "2" and complete.upper_position == "2")
assert(complete.last_examined_position == "2" and complete.resume_cursor == nil)
assert(type(complete.checkpoint_cursor) == "string")
local selected, selected_result = read(collection, { selector_lql = "eq{field=/n,value=2}" })
assert(selected == '{"n":2}\n' and selected_result.completed_records == "1")

do
  local bounded = assert(client:create_collection({ name = "bounded" }))
  for n = 1, 3 do
    assert(bounded:append({ append_id = "event-" .. n }, '{"n":' .. n .. '}'))
  end
  local bytes, window = read(bounded, { after = "0", through = "3", first_n = "1" })
  assert(bytes == '{"n":1}\n' and window.complete and window.upper_position == "3")
  assert(window.last_examined_position == "1" and window.completed_records == "1")
  bytes, window = read(bounded, { after = "0", through = "3", first_n = "1",
    cursor = window.checkpoint_cursor })
  assert(bytes == '{"n":2}\n' and window.last_examined_position == "2")
  bytes, window = read(bounded, { first_n = "1", selector_lql = "eq{field=/n,value=3}" })
  assert(bytes == "" and window.completed_records == "0" and window.complete)
  assert(window.last_examined_position == "1")
  bytes, window = read(bounded, { first_n = "1", selector_lql = "eq{field=/n,value=3}",
    cursor = window.checkpoint_cursor })
  assert(bytes == "" and window.completed_records == "0" and window.last_examined_position == "2")
  bytes, window = read(bounded, { first_n = "1", selector_lql = "eq{field=/n,value=3}",
    cursor = window.checkpoint_cursor })
  assert(bytes == '{"n":3}\n' and window.completed_records == "1" and window.last_examined_position == "3")
  local seen = 0
  local value, err, code, partial = bounded:read({ after = "0", through = "3", first_n = "2",
    on_metadata = function(meta)
      if meta.kind == "entry" then
        seen = seen + 1
        if seen == 2 then return false, "reject bounded entry" end
      end
    end,
  }, { write = function() end })
  rejected(value, err, code)
  assert(partial.completed_records == "1")
  local resume = partial.resume_cursor
  value, err, code = bounded:read({ after = "0", through = "3", first_n = "3", cursor = resume },
    { write = function() error("mismatched selection reached sink") end })
  rejected(value, err, code)
  assert(err.server_code == "cursor_invalid")
  bytes, window = read(bounded, { after = "0", through = "3", first_n = "2", cursor = resume })
  assert(bytes == '{"n":2}\n' and window.complete and window.completed_records == "1")
  bytes, window = read(bounded, { after = "0", through = "3", first_n = "2",
    cursor = window.checkpoint_cursor })
  assert(bytes == '{"n":3}\n')
  bytes, window = read(bounded, { first_n = "0" })
  assert(bytes == "" and window.complete and window.last_examined_position == "0")
  bytes, window = read(bounded, { after = "18446744073709551615" })
  assert(bytes == "" and window.complete and window.upper_position == "3")
  assert(window.last_examined_position == "18446744073709551615")
  bytes, window = read(bounded, { after = "18446744073709551615", cursor = window.checkpoint_cursor })
  assert(bytes == "" and window.complete)
  for _, field in ipairs({ "after", "through", "first_n" }) do
    for _, invalid in ipairs({ 0, true, {}, "", "01", "-1", "+1", "1.5", "18446744073709551616" }) do
      local options = {}; options[field] = invalid
      rejected(bounded:read(options, { write = function() error("invalid selection reached sink") end }))
    end
  end
  rejected(bounded:read({ after = "3", through = "2" },
    { write = function() error("reversed range reached sink") end }))
  local closing = assert(client:open_collection({ name = "bounded" }))
  rejected(closing:read(setmetatable({ first_n = "1" }, { __index = function(_, key)
    if key == "after" then closing:close() end
  end }), { write = function() error("closed receiver reached sink") end }))
  assert(bounded:drop())
  bounded:close()
end

-- A rejected metadata boundary leaves the preceding accepted cursor usable.
local delivered = 0
local value, err, code, partial = collection:read({ on_metadata = function(meta)
  if meta.kind == "entry" then
    delivered = delivered + 1
    if delivered == 2 then return false, "metadata rejection" end
  end
end }, { write = function() end })
rejected(value, err, code)
assert(partial.completed_records == "1" and partial.last_examined_position == "1")
assert(not partial.complete and partial.stop_reason == "error")
payload, complete = read(collection, { cursor = partial.resume_cursor })
assert(payload == '{"n":2}\n' and complete.completed_records == "1")
value, err, code, partial = collection:read({ cancelled = function() return true end },
  { write = function() error("cancelled output must not be invoked") end })
assert(value == nil and code == nil and not err.complete and err.stop_reason == "cancelled")
assert(err.completed_records == "0" and type(err.resume_cursor) == "string")
value, err, code, partial = collection:read({ cancelled = function() error("cancel failure") end },
  { write = function() end })
rejected(value, err, code)
assert(partial.stop_reason == "error" and partial.completed_records == "0")
value, err, code, partial = collection:read({}, { write = function() return false, "sink failure" end })
rejected(value, err, code)
assert(partial.completed_records == "0" and partial.stop_reason == "error")
payload = read(collection)
assert(payload == '{"n":1}\n{"n":2}\n')

local reads = 0
for _, invalid in ipairs({ -1, 0.5, true, false, "30", {}, math.huge }) do
  rejected(collection:append({ append_id = "invalid", retention_seconds = invalid },
    { read = function() reads = reads + 1; return "{}" end }))
  rejected(collection:entry_metadata("1", { retention_seconds = invalid }))
  rejected(collection:metadata({ retention_seconds = invalid }))
end
assert(reads == 0)
for _, invalid in ipairs({ 1, 1.5, "", "01", "-1", "+1", "1.0", "18446744073709551616" }) do
  rejected(collection:delete_entry(invalid))
  rejected(collection:entry_metadata(invalid, {}))
end
value, err, code = collection:delete_entry("18446744073709551615")
rejected(value, err, code)
assert(err.server_code == "entry_not_found")
rejected(client:open_collection({ name = "events", retention_seconds = 0 }))
rejected(collection:append({ append_id = "one", future_option = true }, "{}"))
value, err, code, partial = collection:read({ last_n = "1" }, { write = function() reads = reads + 1 end })
rejected(value, err, code)
assert(reads == 0 and partial.completed_records == "0")
value, err, code, partial = collection:read({})
rejected(value, err, code)
assert(partial.completed_records == "0")

-- Accessor exceptions are status errors, with owned adapters cleaned up.
rejected(collection:read({}, setmetatable({}, { __index = function() error("sink accessor") end })))
rejected(collection:append({ append_id = "accessor" },
  setmetatable({}, { __index = function() error("source accessor") end })))
assert(collection:append_status("accessor").state == "absent")
local source_closed = 0
rejected(collection:append({ append_id = "upload-failure" }, {
  read = function() return nil, "source failure" end,
  close = function() source_closed = source_closed + 1 end,
}))
assert(source_closed == 1 and collection:append_status("upload-failure").state == "absent")
local deleted = assert(collection:delete_entry("1"))
assert(deleted.position == "1" and deleted.deleted and not deleted.already_deleted)
assert(collection:delete_entry("1").already_deleted)
duplicate = assert(collection:append({ append_id = "one" }, '{"n":1}'))
assert(duplicate.deleted and duplicate.duplicate and duplicate.retention_expires_at_unix == nil)
assert(collection:append_status("one").state == "deleted")
rejected(collection:entry_metadata("1", { retention_seconds = 1 }))
payload = read(collection)
assert(payload == '{"n":2}\n')

local called = false
payload, complete = read(collection, { on_metadata = function(meta)
  if meta.kind == "entry" then
    called = true
    collection:close()
    rejected(collection:describe())
  end
end })
assert(called and complete.complete and payload == '{"n":2}\n')
rejected(collection:describe())
collection = assert(client:open_collection({ name = "events" }))
local stale = assert(client:open_collection({ name = "events" }))
local prior_id = collection:describe().collection_id
assert(collection:drop())
rejected(stale:describe())
stale:close()
collection:close()
collection = assert(client:create_collection({ name = "events" }))
assert(collection:describe().collection_id ~= prior_id)
collection:close()

-- A producer and consumer with bounded chunks; neither stores the whole body.
local large = assert(client:create_collection({ name = "large" }))
local remaining, produced, consumed = 4 * 1024 * 1024, 0, 0
local stage = 0
lockdc_test_lua_heap_limit(128 * 1024)
local large_entry = assert(large:append({ append_id = "large" }, {
  read = function(capacity)
    if stage == 0 then stage = 1; return '{"large":"' end
    if remaining ~= 0 then
      local count = math.min(capacity, remaining, 2048)
      remaining = remaining - count
      produced = produced + 1
      if produced % 100 == 0 then collectgarbage("collect") end
      return string.rep("x", count)
    end
    if stage == 1 then stage = 2; return '"}' end
    return nil
  end,
}))
assert(large_entry.position == "1" and produced >= 2048)
local _, large_result, large_code = large:read({}, { write = function(chunk)
  assert(#chunk <= 128 * 1024)
  consumed = consumed + #chunk
  collectgarbage("step")
end })
assert(large_code == nil and large_result.complete)
assert(tostring(consumed - 1) == large_entry.bytes)
lockdc_test_lua_heap_limit(0)
local fail_next_chunk = false
value, err, code, partial = large:read({}, { write = function()
  if not fail_next_chunk then
    fail_next_chunk = true
    lockdc_test_lua_heap_fail(2)
  end
end })
assert(value == nil and code == core.ERR_NOMEM and err.message)
assert(not partial.complete and partial.completed_records == "0")
assert(type(partial.resume_cursor) == "string")
-- Root locks and receiver busy state must have unwound after callback OOM.
assert(large:describe().live_count == "1")
local recovery_bytes = 0
local _, recovered, recovery_code = large:read({}, { write = function(chunk)
  recovery_bytes = recovery_bytes + #chunk
end })
assert(recovery_code == nil and recovered.complete and recovery_bytes == consumed)
value, err, code = large:append({ append_id = "source-oom" }, {
  read = function()
    lockdc_test_lua_heap_fail(2)
    return string.rep("y", 4096)
  end,
})
assert(value == nil and code == core.ERR_NOMEM and err.message)
assert(large:append_status("source-oom").state == "absent")
large:close()
local co = coroutine.create(function()
  local receiver = assert(client:open_collection({ name = "large" }))
  local _, err, code, partial_result = receiver:read({ on_metadata = function()
    receiver:close()
    error("coroutine metadata failure")
  end }, { write = function() end })
  rejected(nil, err, code)
  assert(partial_result.completed_records == "0")
end)
assert(coroutine.resume(co))
co = nil
collectgarbage("collect")
client:close()

-- Use the actual facade with JSON conversion disabled: collection bytes are
-- already JSON and must never pass through Lua JSON decoding/encoding.
package.preload.lonejson = function()
  return { json_null = {}, encode_json = function() error("unexpected JSON encoding") end,
    decode_json = function() error("unexpected JSON decoding") end }
end
local source_path = debug.getinfo(1, "S").source:sub(2)
local repo = assert(source_path:match("^(.*)/tests/lua/[^/]+$"))
package.path = repo .. "/lua/?.lua;" .. repo .. "/lua/?/init.lua;" .. package.path
local lockdc = require("lockdc")
local facade = assert(lockdc.open({ endpoints = { "pouch://" .. lockdc_test_root .. "-facade" },
  pouch = { background_compaction = false } }))
local journal = assert(facade:create_collection({ name = "facade", max_entries = "1000" }))
assert(facade:collection_capabilities().collections)
assert(facade:collection_capabilities().count_retention)
assert(journal:describe().max_entries == "1000")
assert(journal:append({ append_id = "first" }, stream("{ \"raw\" : true }")).position == "1")
assert(journal:entry_metadata("1", { retention_seconds = 60 }).retention_support == "supported")
assert(journal:metadata({ retention_seconds = 0 }).retention_expires_at_unix == 0)
assert(journal:metadata({ max_entries = "2" }).max_entries == "2")
payload, complete = read(journal)
assert(payload == '{"raw":true}\n' and complete.completed_records == "1")
assert(facade:list_collections({}, function(item) assert(item.name == "facade") end))
local reopened = assert(facade:open_collection({ name = "facade" }))
assert(reopened:append_status("first").entry.position == "1")
assert(reopened:delete_entry("1").deleted)
reopened:close()
assert(facade:collection_capabilities().append_writer)
assert(facade:collection_capabilities().append_collection_retention)
local pushed = assert(journal:append_begin({ append_id = "pushed", collection_retention_seconds = 7776000 }))
assert(pushed:write('{ "raw" : '))
assert(pushed:write('true }'))
local pushed_result = assert(pushed:commit())
assert(pushed_result.position == "2" and pushed_result.collection_retention_expires_at_unix > os.time())
pushed:close()
payload, complete = read(journal)
assert(payload == '{"raw":true}\n' and complete.completed_records == "1")
local aborted = assert(journal:append_begin({ append_id = "aborted-shared" }))
assert(aborted:write("{}") and aborted:abort())
aborted:close()
assert(journal:append_status("aborted-shared").state == "absent")
assert(journal:drop())
journal:close()
facade:close()
collectgarbage("collect")
print("Lua collections: public workflow, streaming boundaries, metadata, ownership and failure cases passed")
