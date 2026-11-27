#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build="$(realpath -m "${1:-$root/build}")"
if [[ $# -ge 2 ]]; then
    output="$(realpath -m "$2")"
    mkdir "$output"
else
    output="$(mktemp -d "${TMPDIR:-/tmp}/frontierloom-demo.XXXXXX")"
fi

cli="$build/frontierloom"
campus="$root/samples/campus.v1.graph"
dangling="$root/samples/dangling.v1.graph"
config="$root/config/baseline.conf"
echo "demo_output=$output"

"$cli" canonicalize --graph "$campus" --out "$output/campus.canonical.graph"
cmp "$campus" "$output/campus.canonical.graph"

"$cli" compute --graph "$campus" --checkpoint "$output/bfs.flc" --config "$config" \
    --algorithm bfs --source 10 --fault after-send:0:1 --stop-after 2
"$cli" inspect --graph "$campus" --checkpoint "$output/bfs.flc" --details --audit
"$cli" plan --graph "$campus" --checkpoint "$output/bfs.flc" --workers 4
"$cli" scale --graph "$campus" --checkpoint "$output/bfs.flc" --workers 4
"$cli" resume --graph "$campus" --checkpoint "$output/bfs.flc" --config "$config" \
    --fault before-commit:2 --fault before-replace:3:0 --fault after-replace:3:1 \
    --scale-at 4:2 --verify --vector "$output/bfs.csv"
cmp "$root/samples/campus.bfs.csv" "$output/bfs.csv"
"$cli" inspect --graph "$campus" --checkpoint "$output/bfs.flc" --audit

cp "$root/samples/campus.epoch1.v1.flc" "$output/fixture.flc"
"$cli" resume --graph "$campus" --checkpoint "$output/fixture.flc" \
    --verify --vector "$output/fixture.csv"
cmp "$output/bfs.csv" "$output/fixture.csv"

cp "$root/samples/campus.epoch1.v1.flc" "$output/legacy.flc"
"$cli" convert --graph "$campus" --checkpoint "$output/legacy.flc"
"$cli" resume --graph "$campus" --checkpoint "$output/legacy.flc" \
    --verify --vector "$output/legacy.csv"
cmp "$output/bfs.csv" "$output/legacy.csv"
"$cli" convert --graph "$campus" --checkpoint "$output/legacy.flc"

"$cli" compute --graph "$dangling" --checkpoint "$output/rank.flc" --config "$config" \
    --algorithm pagerank --workers 3 --stop-after 5
"$cli" plan --graph "$dangling" --checkpoint "$output/rank.flc" --workers 1
"$cli" resume --graph "$dangling" --checkpoint "$output/rank.flc" --scale-at 5:1 \
    --verify --vector "$output/rank.csv"
"$cli" inspect --graph "$dangling" --checkpoint "$output/rank.flc" --audit
cat "$output/rank.csv"
echo "demo=pass"
