root="./"
package.path = package.path .. ";" .. root .. "?.lua"
package.cpath = package.cpath .. ";" .. root .. "?.so"

local profile = require "profile"
local json = require "json"

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

local function test1()
    profile.start()
    test_storage1()
    test_storage2()
    tonumber("123")    
    print("111")
    tonumber("234")
    print("222")
    test2()
    test_vccl()
    local result = profile.stop()
    local strResult = json.encode(result)
    print(strResult)
end

local function test()
    test1()
end

test()