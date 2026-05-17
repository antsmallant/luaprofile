#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/tests/.build-trust-workloads"
REPORT_DIR="$BUILD_DIR/reports"
LUA_SRC_DIR="$ROOT_DIR/3rd/lua-5.4.8/src"
LUA_BIN="$LUA_SRC_DIR/lua"

mkdir -p "$BUILD_DIR" "$REPORT_DIR"

make -C "$ROOT_DIR/3rd/lua-5.4.8" linux >/dev/null

gcc -DNDEBUG -shared -fPIC -Wall -Wextra -g -O2 \
    -I"$LUA_SRC_DIR" \
    -o "$BUILD_DIR/luaprofilecore.so" \
    "$ROOT_DIR/luaprofilecore.c"

"$LUA_BIN" "$ROOT_DIR/tests/trust_workloads.lua" "$BUILD_DIR" "$ROOT_DIR" "$REPORT_DIR"

echo "trust workload tests passed"
