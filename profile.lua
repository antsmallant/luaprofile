local c = require "luaprofilecore"

local M = {}

local old_co_create = coroutine.create
local old_co_wrap = coroutine.wrap

local function my_coroutine_create(f)
    return old_co_create(function (...)
            c.mark()
            return f(...)
        end)
end

local function my_coroutine_wrap(f)
    return old_co_wrap(function (...)
            c.mark()
            return f(...)
        end)
end

local g_profile_started = false

-- opts = { mem_profile = "off|on" }
function M.start(opts)
    if g_profile_started then
        print("profile start fail, already started")
        return
    end
    g_profile_started = true
    c.start(opts)
    c.mark_all()
    coroutine.create = my_coroutine_create
    coroutine.wrap = my_coroutine_wrap
end

function M.stop()
    if not g_profile_started then
        print("profile stop fail, not started")
        return
    end
    coroutine.create = old_co_create
    coroutine.wrap = old_co_wrap    
    local record_time, nodes = c.dump()
    c.unmark_all()
    c.stop()
    g_profile_started = false
    return {time = record_time, nodes = nodes}
end

return M
