local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
local function rejected(value, message, code)
  assert(value == nil and type(message) == "table" and message.message and code ~= nil, tostring(value))
end
local client = assert(core.open({endpoints={"pouch://"..lockdc_test_root}, default_namespace="journal",
  pouch={background_compaction=false, single_writer=false}}))
assert(client:collection_capabilities().atomic_edit)
local c = assert(client:create_collection({name="events"}))
for i=1,8 do assert(c:append({append_id="entry-"..i}, '{"n":'..i..'}')) end
local head = assert(c:head())
local req = {operation_id="cut", expected_head=head.etag, ranges={{after="1",through="2"},{after="4",through="8"}},
  replacement={append_id="baseline",retention_seconds=3600}, recovery='{"checkpoint":true}', recovery_anchor="replacement"}
local result = assert(c:edit(req,' { "context" : 1 } '))
assert(result.deleted_count=="5" and result.live_count=="4" and result.replacement.position=="9")
head=assert(c:head())
assert(head.etag==result.head_etag and head.recovery_anchor=="9" and head.recovery_anchor_available)
assert(head.recovery=='{"checkpoint":true}')
assert(c:edit(req,'{"context":1}').duplicate)
rejected(c:edit(req,'{"context":2}'))
assert(c:append_status("entry-2").state=="deleted")
local delivered=0
rejected(c:read({expected_head=req.expected_head},{write=function(bytes) delivered=delivered+#bytes end}))
assert(delivered==0)
local output={}
local _, progress, code=c:read({expected_head=head.etag},{write=function(bytes) output[#output+1]=bytes end})
assert(code==nil and progress.complete and table.concat(output)=='{"n":1}\n{"n":3}\n{"n":4}\n{"context":1}\n')
assert(c:edit_status("cut").found)
assert(not c:edit_status("absent").found)
rejected(c:edit({operation_id="bad",expected_head=head.etag,ranges={{after="3",through="2"}}}))
rejected(c:edit({operation_id="bad",expected_head=head.etag,ranges={[2]={after="0",through="1"}}}))
rejected(c:edit({operation_id="bad",expected_head=head.etag,recovery=string.rep("x",1025)}))
assert(c:delete_entry("9"))
assert(not c:head().recovery_anchor_available)
assert(c:edit(req,'{"context":1}').duplicate)
if lockdc_test_maintain_collections then assert(lockdc_test_maintain_collections(client,true)) end
assert(c:edit_status("cut").result.head_etag==result.head_etag)
assert(c:edit(req,'{"context":1}').duplicate)
local ack={operation_id="ack-loss",expected_head=c:head().etag,recovery="published marker"}
if lockdc_test_fail_collection_result then
  lockdc_test_fail_collection_result(true)
  local value,err,code=c:edit(ack)
  assert(value==nil and code==core.ERR_NOMEM and err.uncertain and err.result.published)
else
  assert(c:edit(ack).published)
end
assert(c:edit_status("ack-loss").found)
assert(c:edit(ack).duplicate)
local _,conflict=c:edit({operation_id="stale",expected_head=ack.expected_head})
assert(conflict.current_head.head_etag==c:head().etag and not conflict.current_head.published)
local failed={operation_id="source-error",expected_head=c:head().etag,replacement={append_id="never"}}
local failed_value,failed_error=c:edit(failed,{read=function() error("source stopped") end})
assert(failed_value==nil and not failed_error.uncertain and not c:edit_status("source-error").found)
c:close(); client:close()
-- Exercise the shipped facade without materializing streamed values through JSON helpers.
package.preload.lonejson=function() return {json_null={},encode_json=function() error("unexpected encoding") end,
  decode_json=function() error("unexpected decoding") end} end
local file=debug.getinfo(1,"S").source:sub(2)
local repo=assert(file:match("^(.*)/tests/lua/[^/]+$"))
package.path=repo.."/lua/?.lua;"..repo.."/lua/?/init.lua;"..package.path
local facade=require("lockdc")
client=assert(facade.open({endpoints={"pouch://"..lockdc_test_root},default_namespace="journal"}))
c=assert(client:open_collection({name="events"}))
head=assert(c:head())
assert(c:edit({operation_id="clear",expected_head=head.etag,recovery=""}))
assert(c:head().recovery=="")
assert(c:edit(req,'{"context":1}').duplicate)
assert(c:head().recovery=="")
assert(c:edit_status("clear").found)
local empty_head=assert(c:head())
local all=assert(c:edit({operation_id="remove-all",expected_head=empty_head.etag,
  ranges={{after="0",through=empty_head.upper_position}}}))
assert(all.live_count=="0" and all.upper_position==empty_head.upper_position)
empty_head=assert(c:head())
local again=assert(c:edit({operation_id="already-deleted",expected_head=empty_head.etag,
  ranges={{after="0",through=empty_head.upper_position}}}))
assert(again.deleted_count=="0" and again.live_count=="0")
assert(c:append({append_id="after-empty"},"{}").position=="10")
c:close();client:close();collectgarbage("collect")
-- Core and facade retries must enforce ownership changes at the EOF callback.
local function retry_fencing(api, label)
  local client=assert(api.open({endpoints={"pouch://"..lockdc_test_root},default_namespace="journal",
    pouch={background_compaction=false,single_writer=false}}))
  for _,transient in ipairs({false,true}) do
    local name="retry-"..label..(transient and "-transient" or "-takeover")
    local public=assert(client:create_collection({name=name}))
    local peer=assert(client:open_collection({name=name}))
    local owner=transient and public or assert(public:acquire({owner="initial",private_read=true}))
    local req={operation_id="retry",expected_head=owner:head().etag,
      replacement={append_id="body"},recovery="committed marker"}
    local original=assert(owner:edit(req,"{}"))
    assert(owner:append({append_id="later"},'{"later":true}'))
    local current=assert(owner:head())
    local calls,next_owner=0,nil
    local value,err,code=owner:edit(req,{read=function()
      calls=calls+1
      if calls==1 then return "{}" end
      assert(calls==2)
      if transient then
        local temporary=assert(peer:acquire({owner="temporary",private_read=true}))
        assert(temporary:release())
      else
        next_owner=assert(peer:takeover({owner="replacement",private_read=true},peer:describe().lease.lease_id))
      end
      return nil
    end})
    assert(value==nil and code==core.ERR_INVALID and calls==2)
    assert(err.server_code==(transient and "lease_changed" or "lease_lost"))
    assert(not err.uncertain and not err.result)
    local authorized=next_owner or public
    assert(authorized:edit_status("retry").result.head_etag==original.head_etag)
    local retried=assert(authorized:edit(req,"{}"))
    assert(retried.duplicate and retried.head_etag==original.head_etag)
    assert(retried.upper_position==original.upper_position and retried.live_count==original.live_count)
    local observed=assert(authorized:head())
    assert(observed.etag==current.etag and observed.live_count=="2" and observed.recovery=="committed marker")
    if next_owner then assert(next_owner:release()) end
    if owner~=public then owner:close() end
    peer:close();public:close()
  end
  client:close()
end
retry_fencing(core,"core")
retry_fencing(facade,"facade")
collectgarbage("collect")
print("atomic collection edits C/Lua parity passed")
