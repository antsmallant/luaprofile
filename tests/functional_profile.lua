local build_dir = assert(arg[1], "usage: lua functional_profile.lua <build_dir> [root_dir]")
local root_dir = arg[2] or build_dir:match("^(.-)/tests/%.build")
assert(root_dir, "cannot infer root_dir from build_dir: " .. tostring(build_dir))

package.path = root_dir .. "/?.lua;" .. root_dir .. "/example/?.lua;" .. package.path
package.cpath = build_dir .. "/?.so;" .. package.cpath

local lpaux = require "luaprofileaux"

local original_create = coroutine.create
local original_wrap = coroutine.wrap

local function fail(message, ...)
    error(message:format(...), 2)
end

local function assertf(condition, message, ...)
    if not condition then
        fail(message, ...)
    end
end

local function walk(node, visitor)
    visitor(node)
    local children = node.children
    if children then
        for _, child in pairs(children) do
            walk(child, visitor)
        end
    end
end

local function find_node(root, pattern)
    local found
    walk(root, function(node)
        if not found and type(node.name) == "string" and node.name:find(pattern, 1, true) then
            found = node
        end
    end)
    return found
end

local function count_nodes(root, pattern)
    local count = 0
    walk(root, function(node)
        if type(node.name) == "string" and node.name:find(pattern, 1, true) then
            count = count + 1
        end
    end)
    return count
end

local function child_count(node)
    local count = 0
    local children = node.children
    if children then
        for _ in pairs(children) do
            count = count + 1
        end
    end
    return count
end

local function collect_totals(root)
    local result = {
        nodes = 0,
        call_count = 0,
        call_count_incl = 0,
        alloc_times = 0,
        alloc_bytes = 0,
        free_times = 0,
        free_bytes = 0,
        realloc_times = 0,
        cpu_cost_raw = 0,
        cpu_cost_real = 0,
    }

    walk(root, function(node)
        result.nodes = result.nodes + 1
        result.call_count = result.call_count + (node.call_count or 0)
        result.call_count_incl = result.call_count_incl + (node.call_count_incl or 0)
        result.alloc_times = result.alloc_times + (node.alloc_times or 0)
        result.alloc_bytes = result.alloc_bytes + (node.alloc_bytes or 0)
        result.free_times = result.free_times + (node.free_times or 0)
        result.free_bytes = result.free_bytes + (node.free_bytes or 0)
        result.realloc_times = result.realloc_times + (node.realloc_times or 0)
        result.cpu_cost_raw = result.cpu_cost_raw + (node["cpu_cost_raw(ns)"] or 0)
        result.cpu_cost_real = result.cpu_cost_real + (node["cpu_cost_real(ns)"] or 0)
    end)

    return result
end

local function assert_result_shape(result)
    assertf(type(result) == "table", "stop should return profile result table")
    assertf(type(result.start_time) == "string", "result should include start_time")
    assertf(result.start_time:match("^%d%d%d%d%-%d%d%-%d%d %d%d:%d%d:%d%d$") ~= nil,
        "start_time should use YYYY-MM-DD HH:MM:SS format, got %s", tostring(result.start_time))
    assertf(type(result.duration_seconds) == "number", "result should include duration_seconds")
    assertf(result.duration_seconds >= 0, "duration_seconds should be non-negative")
    assertf(type(result.nodes) == "table", "result should include root nodes table")
    assertf(result.nodes.name == "root root:0", "root node name should be stable, got %s", tostring(result.nodes.name))
    assertf(type(result.nodes.children) == "table", "root node should include children")
    assertf(type(result.nodes["cpu_cost_raw(ns)"]) == "number", "root should expose raw cpu cost")
    assertf(type(result.nodes["cpu_cost_real(ns)"]) == "number", "root should expose real cpu cost")
    assertf(type(result.nodes["cpu_cost_raw(%)"]) == "string", "root should expose raw cpu percent")
    assertf(type(result.nodes["cpu_cost_real(%)"]) == "string", "root should expose real cpu percent")
    assertf(type(result.nodes.cpu_call_count_total) == "number", "root should expose total call count")
    assertf(type(result.nodes["profiler_cpu_cost_total(ns)"]) == "number", "root should expose profiler overhead")
    assertf(type(result.nodes["avg_profiler_cost_per_call(ns)"]) == "number", "root should expose average profiler overhead")
end

local function assert_mem_fields_absent(root)
    walk(root, function(node)
        assertf(node.alloc_times == nil, "mem_profile off should not export alloc_times on %s", tostring(node.name))
        assertf(node.alloc_bytes == nil, "mem_profile off should not export alloc_bytes on %s", tostring(node.name))
        assertf(node.free_times == nil, "mem_profile off should not export free_times on %s", tostring(node.name))
        assertf(node.free_bytes == nil, "mem_profile off should not export free_bytes on %s", tostring(node.name))
        assertf(node.inuse_bytes == nil, "mem_profile off should not export inuse_bytes on %s", tostring(node.name))
    end)
end

local function assert_mem_fields_present(root)
    walk(root, function(node)
        assertf(type(node.alloc_times) == "number", "mem_profile on should export alloc_times on %s", tostring(node.name))
        assertf(type(node.alloc_bytes) == "number", "mem_profile on should export alloc_bytes on %s", tostring(node.name))
        assertf(type(node.free_times) == "number", "mem_profile on should export free_times on %s", tostring(node.name))
        assertf(type(node.free_bytes) == "number", "mem_profile on should export free_bytes on %s", tostring(node.name))
        assertf(type(node.realloc_times) == "number", "mem_profile on should export realloc_times on %s", tostring(node.name))
        assertf(type(node.inuse_bytes) == "number", "mem_profile on should export inuse_bytes on %s", tostring(node.name))
    end)
end

local function run_profile(opts, body)
    assertf(coroutine.create == original_create, "coroutine.create should be restored before start")
    assertf(coroutine.wrap == original_wrap, "coroutine.wrap should be restored before start")

    lpaux.start(opts)
    assertf(coroutine.create ~= original_create, "lpaux.start should wrap coroutine.create")
    assertf(coroutine.wrap ~= original_wrap, "lpaux.start should wrap coroutine.wrap")

    local ok, body_result = pcall(body)
    local result = lpaux.stop()

    assertf(coroutine.create == original_create, "lpaux.stop should restore coroutine.create")
    assertf(coroutine.wrap == original_wrap, "lpaux.stop should restore coroutine.wrap")

    if not ok then
        error(body_result, 0)
    end

    assert_result_shape(result)
    return result, body_result
end

local function scenario_lua_call_and_insert()
    local function test3()
        local t = {}
        local s = 0
        for i = 1, 1000 do
            s = s + i
            table.insert(t, i)
        end
        return s + #t
    end

    local function test2()
        local sum = 0
        for _ = 1, 20 do
            sum = sum + test3()
        end
        return sum
    end

    return test2()
end

local function scenario_c_closure_iterator()
    local acc = 0
    for w in string.gmatch("foo bar baz", "%S+") do
        acc = acc + #w
    end
    return acc
end

local function scenario_builtin_c_functions()
    return tonumber("123") + tonumber("234") + string.byte("abcdef", 3) + string.len("abcdef")
end

local function scenario_coroutine_switch()
    local function co_inner(n)
        local sum = 0
        for i = 1, n do
            sum = sum + i
            if i % 20 == 0 then
                coroutine.yield(sum)
            end
        end
        return sum
    end

    local co = coroutine.create(function(n)
        return co_inner(n)
    end)

    local ok, result = coroutine.resume(co, 120)
    while ok and coroutine.status(co) ~= "dead" do
        ok, result = coroutine.resume(co)
    end

    assert(ok)
    return result
end

local function scenario_coroutine_wrap()
    local function wrapped_inner(n)
        local sum = 0
        for i = 1, n do
            sum = sum + i
        end
        return sum
    end

    local wrapped = coroutine.wrap(function(n)
        return wrapped_inner(n)
    end)

    return wrapped(50)
end

local function scenario_tailcall()
    local function tail_worker(n, acc)
        if n == 0 then
            return acc
        end
        return tail_worker(n - 1, acc + n)
    end
    return tail_worker(200, 0)
end

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

    return f(100)
end

local g_storage = {}
local function scenario_memory_activity()
    for i = 1, 300 do
        table.insert(g_storage, ("item-%d"):format(i))
    end
    g_storage = {}
    collectgarbage("collect")
    for i = 1, 300 do
        table.insert(g_storage, { i, tostring(i), string.rep("x", 32) })
    end
end

local tests = {}

function tests.cpu_profile_without_mem_fields()
    local result = run_profile({ mem_profile = "off" }, function()
        assert(scenario_lua_call_and_insert() > 0)
        assert(scenario_c_closure_iterator() == 9)
        assert(scenario_builtin_c_functions() > 0)
    end)

    local root = result.nodes
    local stat = collect_totals(root)

    assert_mem_fields_absent(root)
    assertf(stat.nodes >= 6, "cpu profile should contain multiple nodes, got %d", stat.nodes)
    assertf(stat.call_count > 20, "cpu profile should record call counts, got %d", stat.call_count)
    assertf(stat.cpu_cost_raw > 0, "cpu profile should record raw cost")
    assertf(root.cpu_call_count_total > 20, "root total call count should be populated")

    local lua_scenario = find_node(root, "scenario_lua_call_and_insert")
    assertf(lua_scenario, "missing Lua scenario node")
    assertf(child_count(lua_scenario) > 0, "Lua scenario should have child call nodes")
    assertf(find_node(root, "scenario_c_closure_iterator"), "missing C closure iterator scenario node")
    assertf(find_node(root, "scenario_builtin_c_functions"), "missing builtin C function scenario node")
end

function tests.mem_profile_tracks_alloc_free_and_exports_fields()
    local result = run_profile({ mem_profile = "on" }, function()
        scenario_memory_activity()
    end)

    local root = result.nodes
    local stat = collect_totals(root)

    assert_mem_fields_present(root)
    assertf(stat.alloc_times > 0, "mem profile should record allocation count")
    assertf(stat.alloc_bytes > 0, "mem profile should record allocation bytes")
    assertf(stat.free_times > 0, "mem profile should record free count after GC")
    assertf(stat.free_bytes > 0, "mem profile should record freed bytes after GC")
    assertf(root.alloc_times > 0, "root should include aggregate alloc count")
    assertf(root.alloc_bytes > 0, "root should include aggregate alloc bytes")
    assertf(find_node(root, "scenario_memory_activity"), "missing memory activity scenario node")
end

function tests.aux_marks_coroutines_created_after_start()
    local result = run_profile({ mem_profile = "off" }, function()
        assert(scenario_coroutine_switch() == 7260)
        assert(scenario_coroutine_wrap() == 1275)
    end)

    local root = result.nodes
    assert_mem_fields_absent(root)
    assertf(find_node(root, "scenario_coroutine_switch"), "missing coroutine.create scenario node")
    assertf(find_node(root, "scenario_coroutine_wrap"), "missing coroutine.wrap scenario node")
    assertf(find_node(root, "yield"), "coroutine.create scenario should record yielded coroutine work")
    assertf(find_node(root, "wrapped"), "coroutine.wrap scenario should record wrapped coroutine work")
    assertf(count_nodes(root, "mark @") >= 2, "aux coroutine wrappers should call c.mark for new coroutines")
end

function tests.tailcall_paths_are_profiled()
    local result = run_profile({ mem_profile = "off" }, function()
        assert(scenario_tailcall() == 20100)
        assert(scenario_tailcall_fgh() == 0)
    end)

    local root = result.nodes
    local stat = collect_totals(root)

    assert_mem_fields_absent(root)
    assertf(stat.call_count > 100, "tailcall profile should record repeated calls, got %d", stat.call_count)
    assertf(find_node(root, "scenario_tailcall"), "missing self-tailcall scenario node")
    assertf(find_node(root, "scenario_tailcall_fgh"), "missing f/g/h tailcall scenario node")
end

function tests.mem_profile_can_run_with_mixed_example_scenarios()
    local result = run_profile({ mem_profile = "on" }, function()
        assert(scenario_lua_call_and_insert() > 0)
        assert(scenario_c_closure_iterator() == 9)
        assert(scenario_builtin_c_functions() > 0)
        assert(scenario_coroutine_switch() == 7260)
        assert(scenario_coroutine_wrap() == 1275)
        assert(scenario_tailcall() == 20100)
        assert(scenario_tailcall_fgh() == 0)
        scenario_memory_activity()
    end)

    local root = result.nodes
    local stat = collect_totals(root)

    assert_mem_fields_present(root)
    assertf(stat.nodes > 10, "mixed profile should contain many call nodes, got %d", stat.nodes)
    assertf(stat.call_count > 100, "mixed profile should record many calls, got %d", stat.call_count)
    assertf(stat.alloc_times > 0, "mixed mem profile should record allocation count")
    assertf(stat.alloc_bytes > 0, "mixed mem profile should record allocation bytes")

    assertf(find_node(root, "scenario_lua_call_and_insert"), "missing Lua scenario node")
    assertf(find_node(root, "scenario_c_closure_iterator"), "missing C closure iterator scenario node")
    assertf(find_node(root, "scenario_builtin_c_functions"), "missing builtin C function scenario node")
    assertf(find_node(root, "scenario_coroutine_switch"), "missing coroutine.create scenario node")
    assertf(find_node(root, "scenario_coroutine_wrap"), "missing coroutine.wrap scenario node")
    assertf(find_node(root, "scenario_tailcall"), "missing tailcall scenario node")
    assertf(find_node(root, "scenario_tailcall_fgh"), "missing f/g/h tailcall scenario node")
    assertf(find_node(root, "scenario_memory_activity"), "missing memory activity scenario node")
end

local order = {
    "cpu_profile_without_mem_fields",
    "mem_profile_tracks_alloc_free_and_exports_fields",
    "aux_marks_coroutines_created_after_start",
    "tailcall_paths_are_profiled",
    "mem_profile_can_run_with_mixed_example_scenarios",
}

for _, name in ipairs(order) do
    local ok, err = pcall(tests[name])
    if not ok then
        fail("functional test failed: %s: %s", name, tostring(err))
    end
    io.stdout:write("ok - ", name, "\n")
end
