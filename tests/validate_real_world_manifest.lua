local manifest_file = assert(arg[1], "usage: lua tests/validate_real_world_manifest.lua <manifest_json>")

local source = debug.getinfo(1, "S").source:match("^@(.+)$") or ""
local root_dir = source:match("^(.*)/tests/[^/]+$") or "."
if root_dir == "" then
    root_dir = "."
end

package.path = root_dir .. "/?.lua;" .. root_dir .. "/example/?.lua;" .. package.path

local json = require "json"

local function fail(path, message)
    error(("%s: %s"):format(path, message), 2)
end

local function assert_type(value, expected, path)
    if type(value) ~= expected then
        fail(path, ("expected %s, got %s"):format(expected, type(value)))
    end
end

local function assert_field(table_value, field, expected_type, path)
    local value = table_value[field]
    assert_type(value, expected_type, path .. "." .. field)
    return value
end

local file = assert(io.open(manifest_file, "r"))
local content = file:read("*a")
file:close()

local manifest = json.decode(content)
assert_type(manifest, "table", "manifest")
assert_field(manifest, "schema_version", "number", "manifest")
assert_field(manifest, "kind", "string", "manifest")
assert_field(manifest, "profile", "string", "manifest")
assert_field(manifest, "generated_at", "string", "manifest")
assert_field(manifest, "environment", "table", "manifest")
assert_field(manifest, "workloads", "table", "manifest")

local required_modes = {
    no_profiler = true,
    cpu_profiler = true,
    memory_profiler = true,
}

if #manifest.workloads < 5 then
    fail("manifest.workloads", "expected at least five workloads")
end

for wi, workload in ipairs(manifest.workloads) do
    local workload_path = ("manifest.workloads[%d]"):format(wi)
    assert_field(workload, "name", "string", workload_path)
    assert_field(workload, "description", "string", workload_path)
    assert_field(workload, "scale", "table", workload_path)
    local expected = assert_field(workload, "expected_bottleneck", "table", workload_path)
    assert_field(expected, "pattern", "string", workload_path .. ".expected_bottleneck")
    assert_field(expected, "metric", "string", workload_path .. ".expected_bottleneck")
    assert_field(expected, "top_n", "number", workload_path .. ".expected_bottleneck")
    assert_field(expected, "reason", "string", workload_path .. ".expected_bottleneck")
    local modes = assert_field(workload, "modes", "table", workload_path)

    local seen = {}
    for mi, mode in ipairs(modes) do
        local mode_path = ("%s.modes[%d]"):format(workload_path, mi)
        local name = assert_field(mode, "name", "string", mode_path)
        seen[name] = true
        assert_field(mode, "mem_profile", "string", mode_path)
        assert_field(mode, "wall_duration_ns", "number", mode_path)
        assert_type(mode.workload_result, "number", mode_path .. ".workload_result")
        if name == "cpu_profiler" or name == "memory_profiler" then
            assert_field(mode, "report_path", "string", mode_path)
            assert_field(mode, "top_nodes", "table", mode_path)
            assert_field(mode, "expected_bottleneck_rank", "number", mode_path)
            assert_field(mode, "expected_bottleneck_node", "table", mode_path)
            assert_field(mode, "profiler", "table", mode_path)
        end
    end

    for mode_name in pairs(required_modes) do
        if not seen[mode_name] then
            fail(workload_path .. ".modes", "missing mode " .. mode_name)
        end
    end
end

io.stdout:write("real-world manifest validation passed: ", manifest_file, "\n")
