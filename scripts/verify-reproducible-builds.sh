#!/usr/bin/env bash
set -euo pipefail

ai_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
gate_tmp=$(mktemp -d)
trap 'rm -rf -- "$gate_tmp"' EXIT

targets=(
  tos-ai-cli
  tos-ai-edge
  tos-ai-worker
)

build_set() {
  local destination=$1
  mkdir -p "$destination"
  for target in "${targets[@]}"; do
    (
      cd "$ai_repo"
      GOWORK=off go build -mod=readonly -trimpath -buildvcs=false \
        -ldflags=-buildid= -o "$destination/$target" "./cmd/$target"
    )
  done
}

build_set "$gate_tmp/first"
build_set "$gate_tmp/second"

for target in "${targets[@]}"; do
  cmp "$gate_tmp/first/$target" "$gate_tmp/second/$target"
  sha256sum "$gate_tmp/first/$target"
done
