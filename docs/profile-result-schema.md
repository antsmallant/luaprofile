# Profile Result Schema

`luaprofileaux.stop()` returns a Lua table with profiling metadata and a nested call tree. `example/example_profile.lua` serializes the same table to JSON.

This document describes the current exported contract. It is intentionally descriptive: it freezes the observable shape used by tests, report tooling, and later profiler-core work, without redesigning profiler internals.

## Top-Level Result

| Field | Type | Scope | Description |
| --- | --- | --- | --- |
| `start_time` | string | result | Wall-clock start time formatted as `YYYY-MM-DD HH:MM:SS`. |
| `duration_seconds` | number | result | Monotonic elapsed time between profiler start and dump, in seconds. |
| `nodes` | table | result | Root call tree node. |

## Node Fields

Every node may contain these fields.

| Field | Type | Scope | Units | Description |
| --- | --- | --- | --- | --- |
| `name` | string | all nodes | none | Display label formatted as `<function-name> <source>:<line>`. Treat it as display text, not a stable structured identifier. |
| `children` | array table or absent | non-leaf nodes | none | Child call-path nodes. Leaf nodes omit this field. |
| `last_ret_time` | integer | all nodes | ns | Last monotonic return timestamp recorded for this node. This is diagnostic metadata, not a duration. |
| `call_count` | integer | all nodes | calls | Self call count recorded for this call-path node. Self-recursive tail calls may be aggregated into the same node. |
| `call_count_incl` | integer | all nodes | calls | Inclusive call count used for profiler overhead attribution. For non-root nodes this is `call_count` plus descendant `call_count_incl`; for the root node it is overwritten with `cpu_call_count_total`. |
| `cpu_cost_raw(ns)` | integer | all nodes | ns | Raw elapsed CPU-attribution duration recorded for this node's call frame. This value is self/frame-attributed, not summed from children during dump. |
| `cpu_cost_real(ns)` | integer | all nodes | ns | Raw CPU cost after subtracting estimated profiler overhead for `call_count_incl`. The subtraction saturates at zero. |
| `cpu_cost_raw(%)` | string | all nodes | percent | Raw CPU cost divided by the parent's raw CPU cost, formatted with two decimal places. Root uses `100.00` because it has no parent denominator. |
| `cpu_cost_real(%)` | string | all nodes | percent | Real CPU cost divided by the parent's real CPU cost, formatted with two decimal places. Root uses `100.00` because it has no parent denominator. |

### Memory Fields

Memory fields are exported only when the profiler is started with `{ mem_profile = "on" }`. They must be absent when `mem_profile` is `"off"` or omitted.

| Field | Type | Scope | Units | Description |
| --- | --- | --- | --- | --- |
| `alloc_bytes` | integer | all nodes when memory profiling is on | bytes | Inclusive allocated bytes for this node and descendants. |
| `free_bytes` | integer | all nodes when memory profiling is on | bytes | Inclusive freed bytes for this node and descendants. |
| `alloc_times` | integer | all nodes when memory profiling is on | allocations | Inclusive allocation count for this node and descendants. |
| `free_times` | integer | all nodes when memory profiling is on | frees | Inclusive free count for this node and descendants. |
| `realloc_times` | integer | all nodes when memory profiling is on | reallocations | Inclusive reallocation count for this node and descendants. |
| `inuse_bytes` | integer | all nodes when memory profiling is on | bytes | `alloc_bytes - free_bytes` when allocation bytes are greater than or equal to free bytes. Current overflow/error sentinel behavior is not yet a stable API and should be treated as an implementation limitation. |

### Root-Only Fields

The root node is currently identified by `name == "root root:0"`. Only the root node exports these fields.

| Field | Type | Units | Description |
| --- | --- | --- | --- |
| `profiler_cpu_cost_total(ns)` | integer | ns | Total time spent inside profiler hook handling. |
| `cpu_call_count_total` | integer | calls | Total counted profiler CPU call events. |
| `avg_profiler_cost_per_call(ns)` | number | ns/call | `profiler_cpu_cost_total(ns) / cpu_call_count_total` when the call count is non-zero; otherwise zero. |

## CPU Cost Semantics

`cpu_cost_raw(ns)` is recorded from the current call frame's elapsed time, excluding coroutine yield time tracked by the profiler. It is not currently recomputed as an inclusive sum of descendants during dump.

`cpu_cost_real(ns)` subtracts estimated profiler overhead:

```text
cpu_cost_real = max(0, cpu_cost_raw - avg_profiler_cost_per_call * call_count_incl)
```

This means `cpu_cost_real(ns)` depends on both the raw duration and the inclusive call count chosen for overhead attribution.

## Tailcall Semantics

The current profiler handles tail calls in the hook path:

- Self-recursive tail calls are aggregated into the same call-path node by incrementing `call_count`.
- Non-self tail calls keep separate call paths where the hook can distinguish prototypes.
- The schema does not yet promise a final public API for every Lua tailcall edge case. Later correctness workloads should pin down additional behavior before core refactors.

## Coroutine Semantics

`luaprofileaux.start()` wraps `coroutine.create` and `coroutine.wrap` so coroutines created after profiler start call `luaprofilecore.mark()`. `luaprofileaux.stop()` restores the original coroutine functions.

Coroutine yield/resume attribution is represented in the same call tree. Current tests assert that coroutine scenarios and wrapper mark calls appear, but the schema does not yet define a stable cross-coroutine parent/child attribution model beyond the exported node fields above.

## Memory Ownership Semantics

When memory profiling is enabled, allocation events are attributed to the current leaf call-path node at allocation time. Dump output exports inclusive memory totals by adding descendant memory counters. Frees are attributed through the allocation map when possible.

Allocator failure should not add allocation counters. More exhaustive allocator failure behavior belongs to stress/failure harness work.

## CPU-Only Example

```lua
{
  start_time = "2026-05-17 23:00:00",
  duration_seconds = 0.001,
  nodes = {
    name = "root root:0",
    call_count = 1,
    call_count_incl = 42,
    ["cpu_cost_raw(ns)"] = 1000000,
    ["cpu_cost_real(ns)"] = 950000,
    ["cpu_cost_raw(%)"] = "100.00",
    ["cpu_cost_real(%)"] = "100.00",
    cpu_call_count_total = 42,
    ["profiler_cpu_cost_total(ns)"] = 50000,
    ["avg_profiler_cost_per_call(ns)"] = 1190.4761904762,
    children = {
      {
        name = "worker @script.lua:10",
        call_count = 2,
        call_count_incl = 4,
        ["cpu_cost_raw(ns)"] = 400000,
        ["cpu_cost_real(ns)"] = 395238,
        ["cpu_cost_raw(%)"] = "40.00",
        ["cpu_cost_real(%)"] = "41.60",
      },
    },
  },
}
```

No memory fields are present in CPU-only output.

## Memory Profile Example

```lua
{
  start_time = "2026-05-17 23:00:00",
  duration_seconds = 0.002,
  nodes = {
    name = "root root:0",
    call_count = 1,
    call_count_incl = 30,
    ["cpu_cost_raw(ns)"] = 2000000,
    ["cpu_cost_real(ns)"] = 1900000,
    ["cpu_cost_raw(%)"] = "100.00",
    ["cpu_cost_real(%)"] = "100.00",
    alloc_bytes = 4096,
    free_bytes = 1024,
    alloc_times = 4,
    free_times = 1,
    realloc_times = 0,
    inuse_bytes = 3072,
    cpu_call_count_total = 30,
    ["profiler_cpu_cost_total(ns)"] = 100000,
    ["avg_profiler_cost_per_call(ns)"] = 3333.3333333333,
  },
}
```

Memory fields are numeric on every node when memory profiling is enabled.

## Validation Entry Point

Reusable schema checks live in `tests/assertions/profile_schema.lua`.

Functional tests load this helper directly. Later tests can reuse:

```lua
local schema = require "tests.assertions.profile_schema"
schema.assert_result(result, { mem_profile = "off" })
schema.assert_result(result, { mem_profile = "on" })
```

The helper reports field-specific failures for missing fields, wrong types, invalid root-only field placement, and incorrect memory field behavior.

Generated JSON can be validated with:

```bash
3rd/lua-5.4.8/src/lua tests/validate_profile_schema.lua example/output_of_example.json off
```
