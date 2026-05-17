local filepath = assert(arg[1], "usage: lua tests/validate_profile_schema.lua <profile_json> [off|on]")
local mem_profile = arg[2] or "off"
assert(mem_profile == "off" or mem_profile == "on", "mem_profile must be off or on")

local source = debug.getinfo(1, "S").source:match("^@(.+)$") or ""
local root_dir = source:match("^(.*)/tests/[^/]+$") or "."
if root_dir == "" then
    root_dir = "."
end

package.path = root_dir .. "/?.lua;" .. root_dir .. "/example/?.lua;" .. package.path

local json = require "json"
local schema = require "tests.assertions.profile_schema"

local file = assert(io.open(filepath, "r"))
local content = file:read("*a")
file:close()

local result = json.decode(content)
schema.assert_result(result, { mem_profile = mem_profile })

io.stdout:write("profile schema validation passed: ", filepath, " (mem_profile=", mem_profile, ")\n")
