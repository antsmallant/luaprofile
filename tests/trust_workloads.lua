local build_dir = assert(arg[1], "usage: lua trust_workloads.lua <build_dir> <root_dir> <report_dir>")
local root_dir = assert(arg[2], "usage: lua trust_workloads.lua <build_dir> <root_dir> <report_dir>")
local report_dir = assert(arg[3], "usage: lua trust_workloads.lua <build_dir> <root_dir> <report_dir>")

package.path = root_dir .. "/?.lua;" .. root_dir .. "/example/?.lua;" .. package.path
package.cpath = build_dir .. "/?.so;" .. package.cpath

local lpaux = require "luaprofileaux"
local json = require "json"
local schema = require "tests.assertions.profile_schema"

local original_create = coroutine.create
local original_wrap = coroutine.wrap
local summaries = {}

local function write_file(path, content)
    local file = assert(io.open(path, "w"))
    file:write(content)
    file:close()
end

local function encode_json(value)
    return json.encode(value)
end

local function sanitize_filename(value)
    return (value:gsub("[^%w_.-]", "_"))
end

local function fail(ctx, invariant, expected, actual)
    error(table.concat({
        "trust workload invariant failed",
        "workload=" .. ctx.workload,
        "invariant=" .. invariant,
        "expected=" .. tostring(expected),
        "actual=" .. tostring(actual),
        "profile_path=" .. ctx.report_path,
        "mem_profile=" .. ctx.mem_profile,
        "build_dir=" .. ctx.build_dir,
        "lua_bin=" .. ctx.lua_bin,
    }, "\n"), 2)
end

local function check(ctx, invariant, condition, expected, actual)
    if not condition then
        fail(ctx, invariant, expected, actual)
    end
end

local function walk(node, visitor, path)
    path = path or { node.name or "<unnamed>" }
    visitor(node, path)
    if node.children then
        for i, child in ipairs(node.children) do
            local next_path = { table.unpack(path) }
            next_path[#next_path + 1] = child.name or ("<child-" .. i .. ">")
            walk(child, visitor, next_path)
        end
    end
end

local function node_path_to_string(path)
    return table.concat(path, " -> ")
end

local function name_matches(node, pattern)
    return type(node.name) == "string" and node.name:find(pattern, 1, true) ~= nil
end

local function find_node(root, pattern)
    local found, found_path
    walk(root, function(node, path)
        if not found and name_matches(node, pattern) then
            found = node
            found_path = path
        end
    end)
    return found, found_path
end

local function find_child(parent, pattern)
    if not parent.children then
        return nil
    end
    for _, child in ipairs(parent.children) do
        if name_matches(child, pattern) then
            return child
        end
    end
    return nil
end

local function count_nodes(root, pattern)
    local count = 0
    walk(root, function(node)
        if name_matches(node, pattern) then
            count = count + 1
        end
    end)
    return count
end

local function observed_nodes(root, limit)
    local names = {}
    walk(root, function(node, path)
        if #names < limit then
            names[#names + 1] = node_path_to_string(path)
        end
    end)
    return table.concat(names, " | ")
end

local function observed_children(parent)
    local names = {}
    if parent.children then
        for _, child in ipairs(parent.children) do
            names[#names + 1] = child.name or "<unnamed>"
        end
    end
    if #names == 0 then
        return "<no children>"
    end
    return table.concat(names, " | ")
end

local function require_node(ctx, root, pattern)
    local node, path = find_node(root, pattern)
    check(ctx, "node present: " .. pattern, node ~= nil, "node matching " .. pattern, observed_nodes(root, 12))
    return node, path
end

local function require_direct_child(ctx, parent, child_pattern, parent_path)
    local child = find_child(parent, child_pattern)
    check(ctx,
        "direct child present: " .. child_pattern,
        child ~= nil,
        "child matching " .. child_pattern .. " under " .. node_path_to_string(parent_path),
        observed_children(parent))
    return child
end

local function line_pattern(func)
    return ":" .. tostring(debug.getinfo(func, "S").linedefined)
end

local function assert_metric_at_least(ctx, node, field, minimum)
    local value = node[field]
    check(ctx,
        "metric lower bound: " .. tostring(node.name) .. "." .. field,
        type(value) == "number" and value >= minimum,
        ">=" .. tostring(minimum),
        tostring(value))
end

local function assert_metric_between(ctx, node, field, minimum, maximum)
    local value = node[field]
    check(ctx,
        "metric range: " .. tostring(node.name) .. "." .. field,
        type(value) == "number" and value >= minimum and value <= maximum,
        tostring(minimum) .. ".." .. tostring(maximum),
        tostring(value))
end

local function run_profile(workload, mem_profile, body, assertions)
    local report_path = ("%s/%s-%s.json"):format(report_dir, sanitize_filename(workload), mem_profile)
    local ctx = {
        workload = workload,
        mem_profile = mem_profile,
        report_path = report_path,
        build_dir = build_dir,
        lua_bin = arg[-1] or "lua",
    }

    check(ctx, "coroutine.create restored before start", coroutine.create == original_create, "original coroutine.create", "wrapped")
    check(ctx, "coroutine.wrap restored before start", coroutine.wrap == original_wrap, "original coroutine.wrap", "wrapped")

    lpaux.start({ mem_profile = mem_profile })
    local ok, body_result = pcall(body)
    local result = lpaux.stop()

    check(ctx, "coroutine.create restored after stop", coroutine.create == original_create, "original coroutine.create", "wrapped")
    check(ctx, "coroutine.wrap restored after stop", coroutine.wrap == original_wrap, "original coroutine.wrap", "wrapped")

    write_file(report_path, encode_json(result) .. "\n")

    if not ok then
        fail(ctx, "workload body completed", "no Lua error", body_result)
    end

    schema.assert_result(result, { mem_profile = mem_profile })
    assertions(ctx, result, body_result)

    summaries[#summaries + 1] = {
        workload = workload,
        mem_profile = mem_profile,
        report_path = report_path,
        status = "pass",
    }
    io.stdout:write("ok - ", workload, " (mem_profile=", mem_profile, ")\n")
end

local function known_lua_leaf(iterations)
    local t = {}
    local sum = 0
    for i = 1, iterations do
        sum = sum + i
        table.insert(t, i)
    end
    return sum + #t
end

local function known_lua_middle(rounds, iterations)
    local total = 0
    for _ = 1, rounds do
        total = total + known_lua_leaf(iterations)
    end
    return total
end

local function known_lua_root()
    local result = known_lua_middle(6, 25)
    return result
end

local function workload_lua_calls()
    local result = known_lua_root()
    return result
end

local function known_c_closure_root()
    local total = 0
    for word in string.gmatch("alpha beta gamma", "%S+") do
        total = total + #word
    end
    return total
end

local function known_light_c_root()
    return tonumber("123") + string.len("abcdef") + string.byte("abcdef", 2) + math.abs(-5)
end

local function workload_c_functions()
    return known_c_closure_root() + known_light_c_root()
end

local function known_self_tail_worker(n, acc)
    if n == 0 then
        return acc
    end
    return known_self_tail_worker(n - 1, acc + n)
end

local function known_tail_h(n)
    if n <= 0 then
        return 0
    end
    return known_tail_h(n - 1)
end

local function known_tail_g(n)
    return known_tail_h(n)
end

local function known_tail_f(n)
    return known_tail_g(n)
end

local function workload_tailcalls()
    return known_self_tail_worker(120, 0) + known_tail_f(30)
end

local function known_coroutine_inner(n)
    local sum = 0
    for i = 1, n do
        sum = sum + i
        if i % 10 == 0 then
            coroutine.yield(sum)
        end
    end
    return sum
end

local function known_coroutine_create_root()
    local co = coroutine.create(function(n)
        return known_coroutine_inner(n)
    end)
    local ok, result = coroutine.resume(co, 40)
    while ok and coroutine.status(co) ~= "dead" do
        ok, result = coroutine.resume(co)
    end
    assert(ok)
    return result
end

local function known_coroutine_wrap_root()
    local wrapped = coroutine.wrap(function(n)
        local total = 0
        for i = 1, n do
            total = total + i
        end
        return total
    end)
    return wrapped(25)
end

local function workload_coroutines()
    return known_coroutine_create_root() + known_coroutine_wrap_root()
end

local memory_storage = {}
local function known_memory_root()
    for i = 1, 256 do
        memory_storage[i] = ("item-%04d"):format(i)
    end
    for i = 257, 512 do
        table.insert(memory_storage, { i, tostring(i), string.rep("x", 48) })
    end
    memory_storage = {}
    collectgarbage("collect")
    for i = 1, 128 do
        memory_storage[i] = string.rep("y", i % 17)
    end
    return #memory_storage
end

local function workload_memory()
    local result = known_memory_root()
    return result
end

local function known_mixed_root()
    local total = known_lua_middle(3, 10)
    total = total + known_light_c_root()
    total = total + known_memory_root()
    return total
end

local function workload_mixed()
    local result = known_mixed_root()
    return result
end

run_profile("lua-call-tree", "off", workload_lua_calls, function(ctx, result, body_result)
    check(ctx, "workload return", body_result == known_lua_middle(6, 25), "known lua total", body_result)
    local root_node, root_path = require_node(ctx, result.nodes, "known_lua_root")
    local middle = require_direct_child(ctx, root_node, "known_lua_middle", root_path)
    local leaf = require_direct_child(ctx, middle, "known_lua_leaf", { table.unpack(root_path), middle.name })
    assert_metric_between(ctx, root_node, "call_count", 1, 1)
    assert_metric_between(ctx, middle, "call_count", 1, 1)
    assert_metric_between(ctx, leaf, "call_count", 6, 6)
    check(ctx, "leaf inclusive call count includes table.insert", leaf.call_count_incl > leaf.call_count, "> self call_count", leaf.call_count_incl)
end)

run_profile("c-function-separation", "off", workload_c_functions, function(ctx, result, body_result)
    check(ctx, "workload return", body_result == 246, "246", body_result)
    local closure_root = require_node(ctx, result.nodes, "known_c_closure_root")
    require_direct_child(ctx, closure_root, "gmatch", { closure_root.name })
    require_direct_child(ctx, closure_root, "for iterator", { closure_root.name })

    local light_root = require_node(ctx, result.nodes, "known_light_c_root")
    require_direct_child(ctx, light_root, "tonumber", { light_root.name })
    require_direct_child(ctx, light_root, "len", { light_root.name })
    require_direct_child(ctx, light_root, "byte", { light_root.name })
    require_direct_child(ctx, light_root, "abs", { light_root.name })
    check(ctx, "light C functions stay distinct", count_nodes(light_root, "tonumber") == 1, "one tonumber node", count_nodes(light_root, "tonumber"))
    check(ctx, "light C len stays distinct", count_nodes(light_root, "len") == 1, "one len node", count_nodes(light_root, "len"))
    check(ctx, "light C byte stays distinct", count_nodes(light_root, "byte") == 1, "one byte node", count_nodes(light_root, "byte"))
    check(ctx, "light C abs stays distinct", count_nodes(light_root, "abs") == 1, "one abs node", count_nodes(light_root, "abs"))
end)

run_profile("tailcall-semantics", "off", workload_tailcalls, function(ctx, result, body_result)
    check(ctx, "workload return", body_result == 7260, "7260", body_result)
    local self_node = require_node(ctx, result.nodes, "known_self_tail_worker")
    assert_metric_at_least(ctx, self_node, "call_count", 120)
    local f_node = require_node(ctx, result.nodes, "known_tail_f")
    local g_node = require_node(ctx, f_node, line_pattern(known_tail_g))
    local h_node = require_node(ctx, g_node, line_pattern(known_tail_h))
    check(ctx, "tail f/g/h call-path nodes are distinct", f_node ~= g_node and g_node ~= h_node and f_node ~= h_node, "three distinct nodes", "node alias")
    assert_metric_at_least(ctx, h_node, "call_count", 30)
end)

run_profile("coroutine-attribution", "off", workload_coroutines, function(ctx, result, body_result)
    check(ctx, "workload return", body_result == 1145, "1145", body_result)
    require_node(ctx, result.nodes, "known_coroutine_create_root")
    require_node(ctx, result.nodes, "known_coroutine_wrap_root")
    require_node(ctx, result.nodes, line_pattern(known_coroutine_inner))
    require_node(ctx, result.nodes, "yield")
    check(ctx, "coroutine wrapper marks new coroutines", count_nodes(result.nodes, "mark @") >= 2, "at least two mark nodes", count_nodes(result.nodes, "mark @"))
end)

run_profile("memory-ownership", "on", workload_memory, function(ctx, result, body_result)
    check(ctx, "workload return", body_result == 128, "128", body_result)
    local node = require_node(ctx, result.nodes, "known_memory_root")
    assert_metric_at_least(ctx, node, "alloc_times", 1)
    assert_metric_at_least(ctx, node, "alloc_bytes", 1)
    assert_metric_at_least(ctx, node, "free_times", 1)
    assert_metric_at_least(ctx, node, "free_bytes", 1)
    assert_metric_at_least(ctx, node, "realloc_times", 1)
    check(ctx, "memory inuse within allocated bytes", node.inuse_bytes <= node.alloc_bytes, "<= alloc_bytes", node.inuse_bytes)
    check(ctx, "root memory aggregates workload", result.nodes.alloc_bytes >= node.alloc_bytes, "root alloc >= workload alloc", result.nodes.alloc_bytes)
end)

run_profile("mixed-cpu-memory", "on", workload_mixed, function(ctx, result, body_result)
    check(ctx, "workload return type", type(body_result) == "number" and body_result > 0, "positive number", body_result)
    local mixed = require_node(ctx, result.nodes, "known_mixed_root")
    require_node(ctx, result.nodes, "known_lua_middle")
    require_node(ctx, result.nodes, "known_light_c_root")
    require_node(ctx, result.nodes, "known_memory_root")
    assert_metric_at_least(ctx, mixed, "call_count_incl", 3)
    assert_metric_at_least(ctx, mixed, "alloc_bytes", 1)
    assert_metric_at_least(ctx, result.nodes, "cpu_call_count_total", mixed.call_count_incl)
end)

write_file(report_dir .. "/summary.json", encode_json({ tests = summaries }) .. "\n")
