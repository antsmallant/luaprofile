#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

bash "$ROOT_DIR/tests/test_core_safety.sh"
bash "$ROOT_DIR/tests/test_functional_profile.sh"

echo "all tests passed"
