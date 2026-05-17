local M = {}

local function fail(path, message)
    error(("%s: %s"):format(path, message), 2)
end

local function assert_type(value, expected, path)
    if type(value) ~= expected then
        fail(path, ("expected %s, got %s"):format(expected, type(value)))
    end
end

local function assert_non_negative_number(value, path)
    assert_type(value, "number", path)
    if value < 0 then
        fail(path, "expected non-negative number")
    end
end

local function assert_non_negative_integer(value, path)
    assert_type(value, "number", path)
    if value < 0 or value % 1 ~= 0 then
        fail(path, "expected non-negative integer")
    end
end

local function assert_percent_string(value, path)
    assert_type(value, "string", path)
    if not value:match("^%-?%d+%.%d%d$") then
        fail(path, "expected percent string with two decimal places")
    end
end

local memory_fields = {
    "alloc_times",
    "alloc_bytes",
    "free_times",
    "free_bytes",
    "realloc_times",
    "inuse_bytes",
}

local root_only_fields = {
    "profiler_cpu_cost_total(ns)",
    "cpu_call_count_total",
    "avg_profiler_cost_per_call(ns)",
}

local function assert_memory_fields(node, opts, path)
    if opts.mem_profile == "on" then
        for _, field in ipairs(memory_fields) do
            assert_non_negative_integer(node[field], path .. "." .. field)
        end
    else
        for _, field in ipairs(memory_fields) do
            if node[field] ~= nil then
                fail(path .. "." .. field, "must be absent when mem_profile is off")
            end
        end
    end
end

local function assert_root_only_fields(node, is_root, path)
    if is_root then
        assert_non_negative_integer(node["profiler_cpu_cost_total(ns)"], path .. "[profiler_cpu_cost_total(ns)]")
        assert_non_negative_integer(node.cpu_call_count_total, path .. ".cpu_call_count_total")
        assert_non_negative_number(node["avg_profiler_cost_per_call(ns)"], path .. "[avg_profiler_cost_per_call(ns)]")
    else
        for _, field in ipairs(root_only_fields) do
            if node[field] ~= nil then
                fail(path .. "." .. field, "root-only field must not appear on child node")
            end
        end
    end
end

local function assert_node(node, opts, path, is_root)
    assert_type(node, "table", path)
    assert_type(node.name, "string", path .. ".name")
    assert_non_negative_integer(node.last_ret_time, path .. ".last_ret_time")
    assert_non_negative_integer(node.call_count, path .. ".call_count")
    assert_non_negative_integer(node.call_count_incl, path .. ".call_count_incl")
    assert_non_negative_integer(node["cpu_cost_raw(ns)"], path .. "[cpu_cost_raw(ns)]")
    assert_non_negative_integer(node["cpu_cost_real(ns)"], path .. "[cpu_cost_real(ns)]")
    assert_percent_string(node["cpu_cost_raw(%)"], path .. "[cpu_cost_raw(%)]")
    assert_percent_string(node["cpu_cost_real(%)"], path .. "[cpu_cost_real(%)]")

    assert_memory_fields(node, opts, path)
    assert_root_only_fields(node, is_root, path)

    if node.children ~= nil then
        assert_type(node.children, "table", path .. ".children")
        for i, child in ipairs(node.children) do
            assert_node(child, opts, ("%s.children[%d]"):format(path, i), false)
        end
    end
end

function M.assert_result(result, opts)
    opts = opts or {}
    local mem_profile = opts.mem_profile or "off"
    if mem_profile ~= "on" and mem_profile ~= "off" then
        error("opts.mem_profile must be 'on' or 'off'", 2)
    end
    opts = { mem_profile = mem_profile }

    assert_type(result, "table", "result")
    assert_type(result.start_time, "string", "result.start_time")
    if not result.start_time:match("^%d%d%d%d%-%d%d%-%d%d %d%d:%d%d:%d%d$") then
        fail("result.start_time", "expected YYYY-MM-DD HH:MM:SS")
    end
    assert_non_negative_number(result.duration_seconds, "result.duration_seconds")
    assert_node(result.nodes, opts, "result.nodes", true)

    if result.nodes.name ~= "root root:0" then
        fail("result.nodes.name", "expected root node name 'root root:0'")
    end
end

return M
