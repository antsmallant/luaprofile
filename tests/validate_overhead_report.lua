local report_file = assert(arg[1], "usage: lua tests/validate_overhead_report.lua <report_json>")

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

local function assert_number(value, path)
    assert_type(value, "number", path)
    if value < 0 then
        fail(path, "expected non-negative number")
    end
end

local function assert_field(table_value, field, expected_type, path)
    local value = table_value[field]
    assert_type(value, expected_type, path .. "." .. field)
    return value
end

local file = assert(io.open(report_file, "r"))
local content = file:read("*a")
file:close()

local report = json.decode(content)
assert_type(report, "table", "report")
assert_field(report, "schema_version", "number", "report")
assert_field(report, "kind", "string", "report")
assert_field(report, "profile", "string", "report")
assert_field(report, "generated_at", "string", "report")
assert_field(report, "config", "table", "report")
assert_field(report, "environment", "table", "report")
assert_field(report, "workloads", "table", "report")
assert_field(report, "warnings", "table", "report")

for i, warning in ipairs(report.warnings) do
    local warning_path = ("report.warnings[%d]"):format(i)
    assert_type(warning, "table", warning_path)
    assert_field(warning, "workload", "string", warning_path)
    assert_field(warning, "mode", "string", warning_path)
    assert_field(warning, "code", "string", warning_path)
    assert_field(warning, "message", "string", warning_path)
    if warning.data ~= nil then
        assert_type(warning.data, "table", warning_path .. ".data")
    end
end

local required_modes = {
    no_profiler = true,
    cpu_profiler = true,
    memory_profiler = true,
}

local required_stats = {
    "count",
    "min_ns",
    "max_ns",
    "mean_ns",
    "p50_ns",
    "p95_ns",
    "variance_ns2",
    "stddev_ns",
    "coefficient_of_variation",
}

if #report.workloads == 0 then
    fail("report.workloads", "expected at least one workload")
end

for wi, workload in ipairs(report.workloads) do
    local workload_path = ("report.workloads[%d]"):format(wi)
    assert_field(workload, "name", "string", workload_path)
    assert_number(workload.scale, workload_path .. ".scale")
    assert_field(workload, "modes", "table", workload_path)

    local seen_modes = {}
    for mi, mode in ipairs(workload.modes) do
        local mode_path = ("%s.modes[%d]"):format(workload_path, mi)
        local name = assert_field(mode, "name", "string", mode_path)
        seen_modes[name] = true
        assert_field(mode, "mem_profile", "string", mode_path)
        local stats = assert_field(mode, "stats", "table", mode_path)
        local runs = assert_field(mode, "runs", "table", mode_path)

        for _, field in ipairs(required_stats) do
            assert_number(stats[field], mode_path .. ".stats." .. field)
        end
        if name ~= "no_profiler" then
            assert_number(stats.relative_slowdown_vs_no_profiler, mode_path .. ".stats.relative_slowdown_vs_no_profiler")
        end
        if #runs ~= stats.count then
            fail(mode_path .. ".runs", "run count must match stats.count")
        end
        for ri, run in ipairs(runs) do
            local run_path = ("%s.runs[%d]"):format(mode_path, ri)
            assert_number(run.wall_duration_ns, run_path .. ".wall_duration_ns")
            assert_number(run.iteration, run_path .. ".iteration")
            if name == "no_profiler" then
                if run.profiler ~= nil then
                    fail(run_path .. ".profiler", "no_profiler mode must not include profiler metrics")
                end
            else
                local profiler = assert_field(run, "profiler", "table", run_path)
                assert_number(profiler.cpu_cost_raw_ns, run_path .. ".profiler.cpu_cost_raw_ns")
                assert_number(profiler.cpu_cost_real_ns, run_path .. ".profiler.cpu_cost_real_ns")
                assert_number(profiler.profiler_cpu_cost_total_ns, run_path .. ".profiler.profiler_cpu_cost_total_ns")
                assert_number(profiler.avg_profiler_cost_per_call_ns, run_path .. ".profiler.avg_profiler_cost_per_call_ns")
                assert_number(profiler.cpu_call_count_total, run_path .. ".profiler.cpu_call_count_total")
            end
        end
    end

    for mode_name in pairs(required_modes) do
        if not seen_modes[mode_name] then
            fail(workload_path .. ".modes", "missing mode " .. mode_name)
        end
    end
end

io.stdout:write("overhead report validation passed: ", report_file, "\n")
