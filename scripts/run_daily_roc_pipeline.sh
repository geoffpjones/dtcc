#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -x "$ROOT/gradlew" ]]; then
  "$ROOT/gradlew" -q run --args="$*"
else
  gradle -q run --args="$*"
fi
