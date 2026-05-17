local build_dir = assert(arg[1], "usage: lua real_world_workloads.lua <build_dir> <root_dir> <report_dir> [smoke|extended]")
local root_dir = assert(arg[2], "usage: lua real_world_workloads.lua <build_dir> <root_dir> <report_dir> [smoke|extended]")
local report_dir = assert(arg[3], "usage: lua real_world_workloads.lua <build_dir> <root_dir> <report_dir> [smoke|extended]")
local profile = arg[4] or "smoke"
assert(profile == "smoke" or profile == "extended", "profile must be smoke or extended")

package.path = root_dir .. "/?.lua;" .. root_dir .. "/example/?.lua;" .. package.path
package.cpath = build_dir .. "/?.so;" .. package.cpath

local lpaux = require "luaprofileaux"
local core = require "luaprofilecore"
local json = require "json"
local schema = require "tests.assertions.profile_schema"
local real_world = require "tests.workloads.real_world"

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

local function sanitize(value)
    return value:gsub("[^%w_.-]", "_")
end

local function now_ns()
    return core.getnanosec()
end

local function fail(workload, mode, message, data)
    error(table.concat({
        "real-world workload invariant failed",
        "workload=" .. workload.name,
        "mode=" .. mode.name,
        "message=" .. message,
        "data=" .. (data and json.encode(data) or "<none>"),
        "report_dir=" .. report_dir,
        "profile=" .. profile,
        "build_dir=" .. build_dir,
        "lua_bin=" .. (arg[-1] or "lua"),
    }, "\n"), 2)
end

local function walk(node, visitor)
    visitor(node)
    if node.children then
        for _, child in ipairs(node.children) do
            walk(child, visitor)
        end
    end
end

local function metric_value(node, metric)
    if metric == "cpu_cost_raw(ns)" then
        return node["cpu_cost_raw(ns)"] or 0
    end
    return node[metric] or 0
end

local function top_nodes(root, metric, count)
    local nodes = {}
    walk(root, function(node)
        nodes[#nodes + 1] = {
            name = node.name,
            value = metric_value(node, metric),
        }
    end)
    table.sort(nodes, function(a, b)
        return a.value > b.value
    end)
    local result = {}
    for i = 1, math.min(count, #nodes) do
        result[i] = nodes[i]
    end
    return result
end

local function contains_pattern(nodes, pattern)
    for index, node in ipairs(nodes) do
        if type(node.name) == "string" and node.name:find(pattern, 1, true) then
            return true, index, node
        end
    end
    return false
end

local function run_once(workload, mode, scale)
    collectgarbage("collect")

    local result
    local started = false
    if mode.mem_profile then
        lpaux.start({ mem_profile = mode.mem_profile })
        started = true
    end

    local begin_ns = now_ns()
    local ok, workload_result = pcall(workload.run, scale)
    local end_ns = now_ns()

    if started then
        result = lpaux.stop()
        schema.assert_result(result, { mem_profile = mode.mem_profile })
    end

    if not ok then
        fail(workload, mode, "workload raised error", { error = tostring(workload_result) })
    end

    return {
        wall_duration_ns = end_ns - begin_ns,
        workload_result = workload_result,
        profile_result = result,
    }
end

local function run()
    local scales = assert(real_world.profiles[profile], "missing profile config: " .. profile)
    local manifest = {
        schema_version = 1,
        kind = "luaprofile-real-world-workloads",
        profile = profile,
        generated_at = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        environment = {
            lua_version = _VERSION,
            build_dir = build_dir,
            root_dir = root_dir,
            report_dir = report_dir,
            lua_bin = arg[-1] or "lua",
        },
        workloads = {},
    }

    for _, workload in ipairs(real_world.workloads) do
        local scale = assert(scales[workload.name], "missing scale for workload " .. workload.name)
        local workload_entry = {
            name = workload.name,
            description = workload.description,
            scale = scale,
            expected_bottleneck = workload.expected_bottleneck,
            modes = {},
        }

        local baseline_result
        for _, mode in ipairs(modes) do
            local run_result = run_once(workload, mode, scale)
            if mode.name == "no_profiler" then
                baseline_result = run_result.workload_result
            elseif run_result.workload_result ~= baseline_result then
                fail(workload, mode, "workload result differs from no-profiler baseline",
                    { expected = baseline_result, actual = run_result.workload_result })
            end

            local mode_entry = {
                name = mode.name,
                mem_profile = mode.mem_profile or "none",
                wall_duration_ns = run_result.wall_duration_ns,
                workload_result = run_result.workload_result,
            }

            if run_result.profile_result then
                local report_path = ("%s/%s-%s.json"):format(report_dir, sanitize(workload.name), mode.name)
                write_file(report_path, json.encode(run_result.profile_result) .. "\n")
                local metric = workload.expected_bottleneck.metric
                local top = top_nodes(run_result.profile_result.nodes, metric, workload.expected_bottleneck.top_n)
                local found, rank, node = contains_pattern(top, workload.expected_bottleneck.pattern)
                if not found then
                    fail(workload, mode, "expected bottleneck missing from top nodes", {
                        pattern = workload.expected_bottleneck.pattern,
                        metric = metric,
                        top_n = workload.expected_bottleneck.top_n,
                        top_nodes = top,
                        report_path = report_path,
                    })
                end
                mode_entry.report_path = report_path
                mode_entry.top_nodes = top
                mode_entry.expected_bottleneck_rank = rank
                mode_entry.expected_bottleneck_node = node
                mode_entry.profiler = {
                    cpu_cost_raw_ns = run_result.profile_result.nodes["cpu_cost_raw(ns)"],
                    cpu_cost_real_ns = run_result.profile_result.nodes["cpu_cost_real(ns)"],
                    profiler_cpu_cost_total_ns = run_result.profile_result.nodes["profiler_cpu_cost_total(ns)"],
                    avg_profiler_cost_per_call_ns = run_result.profile_result.nodes["avg_profiler_cost_per_call(ns)"],
                    cpu_call_count_total = run_result.profile_result.nodes.cpu_call_count_total,
                }
            end

            workload_entry.modes[#workload_entry.modes + 1] = mode_entry
        end

        manifest.workloads[#manifest.workloads + 1] = workload_entry
        io.stdout:write("ok - ", workload.name, " (profile=", profile, ")\n")
    end

    write_file(report_dir .. "/manifest.json", json.encode(manifest) .. "\n")
end

run()
