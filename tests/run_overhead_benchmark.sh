#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${1:-smoke}"
WORKLOAD_SET="${2:-focused}"
BUILD_DIR="$ROOT_DIR/tests/.build-overhead-benchmark"
REPORT_DIR="$BUILD_DIR/reports"
REPORT_FILE="$REPORT_DIR/overhead-$PROFILE-$WORKLOAD_SET.json"
LUA_SRC_DIR="$ROOT_DIR/3rd/lua-5.4.8/src"
LUA_BIN="$LUA_SRC_DIR/lua"

if [[ "$PROFILE" != "smoke" && "$PROFILE" != "extended" ]]; then
    echo "usage: bash tests/run_overhead_benchmark.sh [smoke|extended] [focused|real_world]" >&2
    exit 2
fi

if [[ "$WORKLOAD_SET" != "focused" && "$WORKLOAD_SET" != "real_world" ]]; then
    echo "usage: bash tests/run_overhead_benchmark.sh [smoke|extended] [focused|real_world]" >&2
    exit 2
fi

mkdir -p "$BUILD_DIR" "$REPORT_DIR"

make -C "$ROOT_DIR/3rd/lua-5.4.8" linux >/dev/null

gcc -DNDEBUG -shared -fPIC -Wall -Wextra -g -O2 \
    -I"$LUA_SRC_DIR" \
    -o "$BUILD_DIR/luaprofilecore.so" \
    "$ROOT_DIR/luaprofilecore.c"

"$LUA_BIN" "$ROOT_DIR/tests/overhead_benchmark.lua" "$BUILD_DIR" "$ROOT_DIR" "$REPORT_FILE" "$PROFILE" "$WORKLOAD_SET"
"$LUA_BIN" "$ROOT_DIR/tests/validate_overhead_report.lua" "$REPORT_FILE"

echo "overhead benchmark $PROFILE/$WORKLOAD_SET passed: $REPORT_FILE"
