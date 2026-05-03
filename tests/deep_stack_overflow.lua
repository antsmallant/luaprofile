local build_dir = assert(arg[1], "usage: lua deep_stack_overflow.lua <build_dir>")
package.cpath = build_dir .. "/?.so;" .. package.cpath

local c = require "luaprofilecore"

c.start()
c.mark()

local function recurse(n)
    if n <= 0 then
        return 0
    end
    return recurse(n - 1) + 1
end

local ok, err = pcall(recurse, 2300)
local duration_seconds, nodes = c.dump()
c.unmark()
c.stop()

assert(ok, err)
assert(type(duration_seconds) == "number")
assert(type(nodes) == "table")
