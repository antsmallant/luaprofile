local build_dir = assert(arg[1], "usage: lua overhead_benchmark.lua <build_dir> <root_dir> <report_file> [smoke|extended]")
local root_dir = assert(arg[2], "usage: lua overhead_benchmark.lua <build_dir> <root_dir> <report_file> [smoke|extended]")
local report_file = assert(arg[3], "usage: lua overhead_benchmark.lua <build_dir> <root_dir> <report_file> [smoke|extended]")
local profile = arg[4] or "smoke"
assert(profile == "smoke" or profile == "extended", "profile must be smoke or extended")

package.path = root_dir .. "/?.lua;" .. root_dir .. "/example/?.lua;" .. package.path
package.cpath = build_dir .. "/?.so;" .. package.cpath

local lpaux = require "luaprofileaux"
local core = require "luaprofilecore"
local json = require "json"
local schema = require "tests.assertions.profile_schema"

local profiles = {
    smoke = {
        warmup = 1,
        iterations = 3,
        variance_warning_cv = 0.60,
        scale = {
            function_call_rate = 220,
            coroutine_switch = 90,
            table_allocation = 180,
            serialization_traversal = 90,
        },
    },
    extended = {
        warmup = 3,
        iterations = 12,
        variance_warning_cv = 0.35,
        scale = {
            function_call_rate = 1200,
            coroutine_switch = 450,
            table_allocation = 900,
            serialization_traversal = 420,
        },
    },
}

local config = profiles[profile]

local modes = {
    { name = "no_profiler", mem_profile = nil },
    { name = "cpu_profiler", mem_profile = "off" },
    { name = "memory_profiler", mem_profile = "on" },
}

local function write_file(path, content)
    local file = assert(io.open(path, "w"))
    file:write(content)
    file:close()
end

local function now_ns()
    return core.getnanosec()
end

local function percentile(sorted, pct)
    if #sorted == 0 then
        return 0
    end
    local rank = math.ceil((pct / 100) * #sorted)
    if rank < 1 then
        rank = 1
    elseif rank > #sorted then
        rank = #sorted
    end
    return sorted[rank]
end

local function stats(values)
    local sorted = { table.unpack(values) }
    table.sort(sorted)

    local sum = 0
    for _, value in ipairs(values) do
        sum = sum + value
    end

    local mean = #values > 0 and (sum / #values) or 0
    local variance_sum = 0
    for _, value in ipairs(values) do
        local delta = value - mean
        variance_sum = variance_sum + delta * delta
    end
    local variance = #values > 0 and (variance_sum / #values) or 0
    local stddev = math.sqrt(variance)

    return {
        count = #values,
        min_ns = sorted[1] or 0,
        max_ns = sorted[#sorted] or 0,
        mean_ns = mean,
        p50_ns = percentile(sorted, 50),
        p95_ns = percentile(sorted, 95),
        variance_ns2 = variance,
        stddev_ns = stddev,
        coefficient_of_variation = mean > 0 and (stddev / mean) or 0,
    }
end

local function function_leaf(v)
    return v + 1
end

local function function_mid(v)
    return function_leaf(v) + function_leaf(v + 1)
end

local function workload_function_call_rate(n)
    local total = 0
    for i = 1, n do
        total = total + function_mid(i)
    end
    return total
end

local function workload_coroutine_switch(n)
    local co = coroutine.create(function(limit)
        local total = 0
        for i = 1, limit do
            total = total + i
            if i % 3 == 0 then
                coroutine.yield(total)
            end
        end
        return total
    end)

    local ok, result = coroutine.resume(co, n)
    while ok and coroutine.status(co) ~= "dead" do
        ok, result = coroutine.resume(co)
    end
    assert(ok)
    return result
end

local function workload_table_allocation(n)
    local rows = {}
    for i = 1, n do
        rows[i] = {
            id = i,
            name = ("entity-%04d"):format(i),
            attrs = { i % 7, i % 11, i % 13 },
        }
    end
    local total = 0
    for i = 1, #rows do
        total = total + rows[i].attrs[1] + #rows[i].name
    end
    return total
end

local function make_serialized_rows(n)
    local rows = {}
    for i = 1, n do
        rows[i] = {
            id = i,
            pos = { x = i * 2, y = i * 3 },
            flags = { active = i % 2 == 0, dirty = i % 5 == 0 },
            tags = { "a", "b", tostring(i % 9) },
        }
    end
    return rows
end

local function traverse(value)
    local t = type(value)
    if t == "number" then
        return value
    elseif t == "string" then
        return #value
    elseif t == "boolean" then
        return value and 1 or 0
    elseif t == "table" then
        local total = 0
        for k, v in pairs(value) do
            total = total + traverse(k) + traverse(v)
        end
        return total
    end
    return 0
end

local function workload_serialization_traversal(n)
    return traverse(make_serialized_rows(n))
end

local workloads = {
    {
        name = "function_call_rate",
        scale_key = "function_call_rate",
        run = workload_function_call_rate,
    },
    {
        name = "coroutine_switch",
        scale_key = "coroutine_switch",
        run = workload_coroutine_switch,
    },
    {
        name = "table_allocation",
        scale_key = "table_allocation",
        run = workload_table_allocation,
    },
    {
        name = "serialization_traversal",
        scale_key = "serialization_traversal",
        run = workload_serialization_traversal,
    },
}

local function run_once(workload, mode)
    collectgarbage("collect")

    local profile_result
    local started = false
    if mode.mem_profile then
        lpaux.start({ mem_profile = mode.mem_profile })
        started = true
    end

    local begin_ns = now_ns()
    local ok, workload_result = pcall(workload.run, config.scale[workload.scale_key])
    local end_ns = now_ns()

    if started then
        profile_result = lpaux.stop()
        schema.assert_result(profile_result, { mem_profile = mode.mem_profile })
    end

    if not ok then
        error(("workload failed: %s mode=%s: %s"):format(workload.name, mode.name, tostring(workload_result)), 0)
    end

    local root = profile_result and profile_result.nodes or nil
    return {
        wall_duration_ns = end_ns - begin_ns,
        workload_result = workload_result,
        profiler = root and {
            cpu_cost_raw_ns = root["cpu_cost_raw(ns)"],
            cpu_cost_real_ns = root["cpu_cost_real(ns)"],
            profiler_cpu_cost_total_ns = root["profiler_cpu_cost_total(ns)"],
            avg_profiler_cost_per_call_ns = root["avg_profiler_cost_per_call(ns)"],
            cpu_call_count_total = root.cpu_call_count_total,
        } or nil,
    }
end

local function summarize_runs(runs)
    local durations = {}
    for _, run in ipairs(runs) do
        durations[#durations + 1] = run.wall_duration_ns
    end
    return stats(durations)
end

local function add_warning(warnings, workload_name, mode_name, code, message, data)
    warnings[#warnings + 1] = {
        workload = workload_name,
        mode = mode_name,
        code = code,
        message = message,
        data = data,
    }
end

local function benchmark()
    local report = {
        schema_version = 1,
        kind = "luaprofile-overhead-benchmark",
        profile = profile,
        generated_at = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        config = {
            warmup = config.warmup,
            iterations = config.iterations,
            variance_warning_cv = config.variance_warning_cv,
            scale = config.scale,
        },
        environment = {
            lua_version = _VERSION,
            build_dir = build_dir,
            root_dir = root_dir,
            lua_bin = arg[-1] or "lua",
            report_file = report_file,
        },
        workloads = {},
        warnings = {},
    }

    for _, workload in ipairs(workloads) do
        local workload_report = {
            name = workload.name,
            scale = config.scale[workload.scale_key],
            modes = {},
        }

        local baseline_mean
        local baseline_result
        for _, mode in ipairs(modes) do
            for _ = 1, config.warmup do
                run_once(workload, mode)
            end

            local runs = {}
            for i = 1, config.iterations do
                local run = run_once(workload, mode)
                run.iteration = i
                runs[#runs + 1] = run
            end

            local mode_stats = summarize_runs(runs)
            if mode.name == "no_profiler" then
                baseline_mean = mode_stats.mean_ns
                baseline_result = runs[1] and runs[1].workload_result
            elseif baseline_mean and baseline_mean > 0 then
                mode_stats.relative_slowdown_vs_no_profiler = mode_stats.mean_ns / baseline_mean
            end

            if baseline_result ~= nil then
                for _, run in ipairs(runs) do
                    if run.workload_result ~= baseline_result then
                        add_warning(report.warnings, workload.name, mode.name, "workload-result-drift",
                            "workload result differed from no-profiler baseline",
                            { expected = baseline_result, actual = run.workload_result })
                    end
                end
            end

            if mode_stats.coefficient_of_variation > config.variance_warning_cv then
                add_warning(report.warnings, workload.name, mode.name, "high-variance",
                    "wall duration coefficient of variation exceeded configured warning threshold",
                    {
                        coefficient_of_variation = mode_stats.coefficient_of_variation,
                        threshold = config.variance_warning_cv,
                    })
            end

            workload_report.modes[#workload_report.modes + 1] = {
                name = mode.name,
                mem_profile = mode.mem_profile or "none",
                stats = mode_stats,
                runs = runs,
            }
        end

        report.workloads[#report.workloads + 1] = workload_report
    end

    return report
end

local report = benchmark()
write_file(report_file, json.encode(report) .. "\n")

io.stdout:write("overhead benchmark report written: ", report_file, "\n")
io.stdout:write("warnings: ", tostring(#report.warnings), "\n")
