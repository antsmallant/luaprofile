local build_dir = assert(arg[1], "usage: lua normal_profile.lua <build_dir>")
package.cpath = build_dir .. "/?.so;" .. package.cpath

local c = require "luaprofilecore"

local function walk(node, totals)
    totals.nodes = totals.nodes + 1
    totals.call_count = totals.call_count + (node.call_count or 0)
    totals.alloc_times = totals.alloc_times + (node.alloc_times or 0)
    totals.alloc_bytes = totals.alloc_bytes + (node.alloc_bytes or 0)

    local children = node.children
    if children then
        for _, child in pairs(children) do
            walk(child, totals)
        end
    end
end

local function allocate_work()
    local t = {}
    for i = 1, 128 do
        t[i] = ("item-%d"):format(i)
    end
    return #t
end

c.start({ mem_profile = "on" })
c.mark()

local result = allocate_work()
local duration_seconds, nodes = c.dump()

c.unmark()
c.stop()

assert(result == 128)
assert(type(duration_seconds) == "number")
assert(duration_seconds >= 0)
assert(type(nodes) == "table")
assert(type(nodes.children) == "table")

local totals = { nodes = 0, call_count = 0, alloc_times = 0, alloc_bytes = 0 }
walk(nodes, totals)

assert(totals.nodes > 1, "profile should include child call nodes")
assert(totals.call_count > 0, "normal call path should record call counts")
assert(totals.alloc_times > 0, "mem profile should record successful allocations")
assert(totals.alloc_bytes > 0, "mem profile should record allocated bytes")
