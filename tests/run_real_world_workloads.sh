#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${1:-smoke}"
BUILD_DIR="$ROOT_DIR/tests/.build-real-world-workloads"
REPORT_DIR="$BUILD_DIR/reports/$PROFILE"
LUA_SRC_DIR="$ROOT_DIR/3rd/lua-5.4.8/src"
LUA_BIN="$LUA_SRC_DIR/lua"

if [[ "$PROFILE" != "smoke" && "$PROFILE" != "extended" ]]; then
    echo "usage: bash tests/run_real_world_workloads.sh [smoke|extended]" >&2
    exit 2
fi

mkdir -p "$BUILD_DIR" "$REPORT_DIR"

make -C "$ROOT_DIR/3rd/lua-5.4.8" linux >/dev/null

gcc -DNDEBUG -shared -fPIC -Wall -Wextra -g -O2 \
    -I"$LUA_SRC_DIR" \
    -o "$BUILD_DIR/luaprofilecore.so" \
    "$ROOT_DIR/luaprofilecore.c"

"$LUA_BIN" "$ROOT_DIR/tests/real_world_workloads.lua" "$BUILD_DIR" "$ROOT_DIR" "$REPORT_DIR" "$PROFILE"
"$LUA_BIN" "$ROOT_DIR/tests/validate_real_world_manifest.lua" "$REPORT_DIR/manifest.json"

echo "real-world workload $PROFILE passed: $REPORT_DIR/manifest.json"
