root="./"
package.path = package.path .. ";" .. root .. "?.lua"
package.cpath = package.cpath .. ";" .. root .. "?.so"

local profile = require "profile"
local json = require "json"
local c = require "luaprofilec"

local g_storage = {}

local function test3()
    local t = {}
    local s = 0
    for i = 1, 10000 do
        s = s + i
        table.insert(t, i)
    end
end

local function test2()
    for i = 1, 100 do
        test3()
    end    
end

-- 触发 LUA_VCCL：string.gmatch 返回 C 闭包迭代器（带 upvalues）
local function test_vccl()
    local acc = 0
    for w in string.gmatch("foo bar baz", "%S+") do
        acc = acc + #w
    end
    return acc
end

local function test_storage1()
    for i = 1, 100 do
        table.insert(g_storage, i)
    end
end

local function test_storage2()
    for i = 1, 10 do
        table.insert(g_storage, i)
    end
    g_storage = {} 
    collectgarbage("collect")
    for i = 1, 100 do
        table.insert(g_storage, i)
    end
end

local function do_test()
    test_storage1()
    test_storage2()
    tonumber("123")    
    print("111")
    tonumber("234")
    print("222")
    test2()
    test_vccl()    
end

local function test_with_profile()
    local opts = { cpu = "profile", mem = "off", sample_period = 1 }
    profile.start(opts)
    local t1 = c.getmonons()
    do_test()
    local t2 = c.getmonons()
    local result = profile.stop()
    local strResult = json.encode(result)
    print(strResult)
    return t2 - t1
end

local function test_without_profile()
    local t1 = c.getmonons()
    do_test()
    local t2 = c.getmonons()
    return t2 - t1
end

local cost_without_profile = test_without_profile()
local cost_with_profile = test_with_profile()
print("test_with_profile cost:", cost_with_profile)
print("test_without_profile cost:", cost_without_profile)
print("test_with_profile cost / test_without_profile cost:", cost_with_profile / cost_without_profile)