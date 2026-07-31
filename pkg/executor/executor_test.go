package executor

import (
	"strings"
	"testing"
	"time"
)

func TestPolicyRequiresPinnedAllowlistedBoundedWorkload(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	ceiling := Limits{
		CPUMillis:      1000,
		MemoryBytes:    1 << 30,
		DiskBytes:      1 << 30,
		PIDs:           64,
		ExecutionTime:  time.Minute,
		OutputBytes:    1 << 20,
		GPUDeviceCount: 1,
	}
	policy := Policy{
		AllowedImages:       map[string]struct{}{digest: {}},
		MaxEnvironment:      8,
		MaxArguments:        8,
		MaxAllowedHosts:     4,
		Ceiling:             ceiling,
		PermitGPU:           true,
		RequireReadOnlyRoot: true,
	}
	spec := Spec{
		ImageDigest:  digest,
		Entrypoint:   []string{"/worker"},
		ReadOnlyRoot: true,
		Network:      NetworkNone,
		AllowGPU:     true,
		Limits:       ceiling,
	}
	if err := policy.Validate(spec); err != nil {
		t.Fatal(err)
	}
	spec.ReadOnlyRoot = false
	if err := policy.Validate(spec); err == nil {
		t.Fatal("writable root accepted")
	}
	spec.ReadOnlyRoot = true
	spec.Limits.MemoryBytes++
	if err := policy.Validate(spec); err == nil {
		t.Fatal("memory above policy accepted")
	}
}
