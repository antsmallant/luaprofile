local root = "../"
package.path = package.path .. ";" .. root .. "?.lua" .. ";" .. root .. "example/?.lua"
package.cpath = package.cpath .. ";" .. root .. "?.so"

local lpaux = require "luaprofileaux"
local json = require "json"

local OUTPUT_FILE = "./output_of_example.json"
local PROFILE_OPTS = { mem_profile = "off" }

local function print_ts(...)
    local ts = os.date("%Y-%m-%d %H:%M:%S")
    local parts = {}
    for i = 1, select("#", ...) do
        parts[i] = tostring(select(i, ...))
    end
    io.stdout:write("[" .. ts .. "] ", table.concat(parts, "\t"), "\n")
end

local function write_profile_result(filepath, content)
    local file, err = io.open(filepath, "w")
    if not file then
        io.stderr:write("open file failed: " .. tostring(err) .. "\n")
        return false
    end
    file:write(content, "\n")
    file:close()
    print_ts("write profile result to " .. filepath)
    return true
end

-- 场景1：Lua 函数调用和 table.insert 热点
local function scenario_lua_call_and_insert()
    local function test3()
        local t = {}
        local s = 0
        for i = 1, 10000 do
            s = s + i
            table.insert(t, i)
        end
    end

    local function test2()
        for _ = 1, 100 do
            test3()
        end
    end

    test2()
end

-- 场景2：触发 LUA_VCCL（string.gmatch 返回的 C 闭包迭代器）
local function scenario_c_closure_iterator()
    local acc = 0
    for w in string.gmatch("foo bar baz", "%S+") do
        acc = acc + #w
    end
    return acc
end

-- 场景3：触发常见内建 C 函数（LUA_VLCF）
local function scenario_builtin_c_functions()
    tonumber("123")
    tonumber("234")
    string.byte("abcdef", 3)
    string.len("abcdef")
end

-- 场景4：协程切换（yield/resume）
local function scenario_coroutine_switch()
    local co = coroutine.create(function(n)
        local sum = 0
        for i = 1, n do
            sum = sum + i
            if i % 50 == 0 then
                coroutine.yield(sum)
            end
        end
        return sum
    end)

    local ok, result = coroutine.resume(co, 500)
    while ok and coroutine.status(co) ~= "dead" do
        ok, result = coroutine.resume(co)
    end
    return result
end

-- 场景5：尾调用链
local function scenario_tailcall()
    local function tail_worker(n, acc)
        if n == 0 then
            return acc
        end
        return tail_worker(n - 1, acc + n)
    end
    return tail_worker(1200, 0)
end

-- 场景6：f -> g -> h 尾调用链，验证不会误合并
local function scenario_tailcall_fgh()
    local function h(n)
        if n <= 0 then
            return 0
        end
        return h(n - 1)
    end

    local function g(n)
        return h(n)
    end

    local function f(n)
        return g(n)
    end

    return f(800)
end

-- 场景7：内存分配/释放
local g_storage = {}
local function scenario_memory_activity()
    for i = 1, 200 do
        table.insert(g_storage, i)
    end
    g_storage = {}
    collectgarbage("collect")
    for i = 1, 200 do
        table.insert(g_storage, i)
    end
end

local function run_scenarios()
    scenario_lua_call_and_insert()
    scenario_c_closure_iterator()
    scenario_builtin_c_functions()
    scenario_coroutine_switch()
    scenario_tailcall()
    scenario_tailcall_fgh()
    scenario_memory_activity()
end

local function run_example_with_profile()
    print_ts("profile start")
    lpaux.start(PROFILE_OPTS)
    run_scenarios()
    local result = lpaux.stop()
    local encoded = json.encode(result)
    write_profile_result(OUTPUT_FILE, encoded)
    print_ts("profile stop")
end

run_example_with_profile()