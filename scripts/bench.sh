#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build="$(realpath -m "${1:-$root/build}")"

if [[ $# -ge 2 ]]; then
    output="$(realpath -m "$2")"
else
    parent="$(mktemp -d "${TMPDIR:-/tmp}/frontierloom-bench.XXXXXX")"
    output="$parent/measurements"
fi

echo "benchmark_output=$output"
exec "$build/frontier-bench" "$output" "${3:-4096}" "${4:-3}"
