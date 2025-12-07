root="../"
package.path = package.path .. ";" .. root .. "?.lua" .. ";" .. root .. "example/?.lua"
package.cpath = package.cpath .. ";" .. root .. "?.so"

local profile = require "profile"
local json = require "json"
local c = require "luaprofilecore"

-- 带时间戳的打印：[YYYY-MM-DD HH:MM:SS] 后跟原始内容
local function print_ts(...)
    local ts = os.date("%Y-%m-%d %H:%M:%S")
    local parts = {}
    for i = 1, select("#", ...) do
        parts[i] = tostring(select(i, ...))
    end
    io.stdout:write("[" .. ts .. "] ", table.concat(parts, "\t"), "\n")
end

local function write_profile_result(str)
    local fp = "./example_result.json"
    local file, err = io.open(fp, "w")
    if not file then
        io.stderr:write("open file failed: " .. tostring(err) .. "\n")
        return false
    end
    file:write(str, "\n")
    file:close()
    print_ts("write profile result to " .. fp)
    return true
end

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

local g_storage = {}

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

local function do_test1()
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
    print_ts("test_with_profile start")
    local opts = { mem_profile = "on" } -- 控制是否开启内存 profile
    profile.start(opts)
    local t1 = c.getnanosec()
    do_test1()
    local t2 = c.getnanosec()
    local result = profile.stop()
    local strResult = json.encode(result)
    write_profile_result(strResult)
    print_ts("test_with_profile stop")
    return t2 - t1
end

test_with_profile()