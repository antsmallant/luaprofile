root="./"
package.path = package.path .. ";" .. root .. "?.lua"
package.cpath = package.cpath .. ";" .. root .. "?.so"

local profile = require "profile"
local json = require "json"
local c = require "luaprofilec"

local function test1()
    for i = 1, 1000000 do
        local a = tonumber("123")
    end
end

local function test_non_profile()
    local t1 = c.getnanoseconds()
    for i = 1, 10 do
        test1()
    end
    local t2 = c.getnanoseconds()
    print("test_non_profile cost:", t2 - t1)
end

local function test_profile()
    local opts = { cpu = "profile", mem = "off", sample_period = 10 }
    profile.start(opts)
    local t1 = c.getnanoseconds()
    for i = 1, 10 do
        test1()
    end
    local t2 = c.getnanoseconds()
    print("test_profile cost:", t2 - t1)
    profile.stop()
end

test_profile()
test_non_profile()
