#!/bin/sh
set -eu

required='TOS_AI_CONTAINERD_TEST_SOCKET
TOS_AI_CONTAINERD_TEST_NAMESPACE
TOS_AI_CONTAINERD_TEST_FIFO_DIR
TOS_AI_CONTAINERD_TEST_IMAGE_REFERENCE
TOS_AI_CONTAINERD_TEST_IMAGE_DIGEST
TOS_AI_CONTAINERD_TEST_NVIDIA_CDI_DEVICE'

for name in $required; do
  eval "value=\${$name-}"
  if [ -z "$value" ]; then
    echo "missing required environment: $name" >&2
    exit 2
  fi
done

if ! command -v nvidia-smi >/dev/null 2>&1; then
  echo "nvidia-smi is unavailable" >&2
  exit 2
fi

# Fail before the container test when the driver cannot enumerate a device.
# The query emits only a count and does not persist UUIDs or serial numbers.
count=$(nvidia-smi --query-gpu=index --format=csv,noheader,nounits | wc -l)
if [ "$count" -lt 1 ]; then
  echo "no NVIDIA GPU is visible to the certification user" >&2
  exit 2
fi

GOWORK=off go test -race -count=1 -v \
  -run '^TestContainerdBackendLiveNVIDIAConformance$' \
  ./pkg/executor/containerdbackend
