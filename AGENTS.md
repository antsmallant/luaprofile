# Agent Guide

This repository uses GitHub issues and pull requests as the source of truth. Slock is used for coordination, status updates, and handoffs.

## Project Map

- `luaprofilecore.c` - Lua C module that implements hook handling, call path tracking, memory profiling, and profile dumping.
- `luaprofileaux.lua` - Lua wrapper around `luaprofilecore`, including coroutine hook setup.
- `tests/` - current automated safety and functional tests.
- `example/` - runnable profiling example and local JSON output.
- `3rd/lua-5.4.8/` - vendored Lua source submodule used for builds and tests.
- `WORKFLOW.md` - issue-to-PR workflow for humans and agents.

## Required Workflow

1. Start from a GitHub issue.
2. Create a focused branch named `issue-N-short-name`.
3. Keep changes scoped to the issue's goal and non-goals.
4. Run the relevant verification commands before opening a PR.
5. Open a PR that links the issue, includes verification output, and states remaining risk.
6. Report progress and blockers in Slock only as coordination notes; keep canonical task details in GitHub.

See `WORKFLOW.md` for the full flow.

## Build And Test

Initialize dependencies and build the module:

```bash
git submodule update --init
sh build.sh
```

Run the full current test suite:

```bash
bash tests/run_all_tests.sh
```

Run only deterministic trust workloads:

```bash
bash tests/run_trust_workloads.sh
```

Run the example after building:

```bash
cd example
sh run_example.sh
```

Expected example output: `example/output_of_example.json`.

Validate generated profile JSON against the documented schema:

```bash
3rd/lua-5.4.8/src/lua tests/validate_profile_schema.lua example/output_of_example.json off
```

## Current Harness Priorities

The first project phase is to make profiler results trustworthy before doing profiler core refactors. Prioritize:

- Repository workflow and CI.
- Profile result schema and metric semantics.
- Deterministic correctness workloads.
- Profiler overhead benchmark harness.
- Local result reporting.
- Real-world style Lua workloads.

Do not start high-risk profiler core rewrites until schema, correctness, and overhead harnesses are in place.
