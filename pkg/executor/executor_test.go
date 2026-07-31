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
		MaxAllowedImages:    8,
		MaxEnvironment:      8,
		MaxArguments:        8,
		MaxAllowedHosts:     4,
		MaxStringBytes:      256,
		MaxInputBytes:       1024,
		Ceiling:             ceiling,
		PermitGPU:           true,
		RequireReadOnlyRoot: true,
	}
	spec := Spec{
		ImageDigest:     digest,
		Entrypoint:      []string{"/worker"},
		ReadOnlyRoot:    true,
		Network:         NetworkNone,
		AllowGPU:        true,
		UserID:          65532,
		GroupID:         65532,
		NoNewPrivileges: true,
		Limits:          ceiling,
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

func TestPolicyRejectsUnsafeContainerCapabilities(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	ceiling := Limits{
		CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 1 << 30,
		PIDs: 32, ExecutionTime: time.Minute, OutputBytes: 1 << 20,
	}
	policy := Policy{
		AllowedImages: map[string]struct{}{digest: {}}, MaxEnvironment: 4,
		MaxAllowedImages: 8,
		MaxArguments:     4, MaxAllowedHosts: 2, MaxStringBytes: 128,
		MaxInputBytes: 1024, Ceiling: ceiling, RequireReadOnlyRoot: true,
	}
	base := Spec{
		ImageDigest: digest, Entrypoint: []string{"/worker"}, ReadOnlyRoot: true,
		Network: NetworkNone, UserID: 1000, GroupID: 1000,
		NoNewPrivileges: true, Limits: ceiling,
	}
	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{"privileged", func(value *Spec) { value.Privileged = true }},
		{"host mount", func(value *Spec) { value.HostMounts = []Mount{{Source: "/etc", Target: "/host"}} }},
		{"network", func(value *Spec) { value.Network = NetworkAllowlist; value.AllowedHosts = []string{"example.com"} }},
		{"writable root", func(value *Spec) { value.ReadOnlyRoot = false }},
		{"unpinned image", func(value *Spec) { value.ImageDigest = "latest" }},
		{"root user", func(value *Spec) { value.UserID = 0 }},
		{"runtime socket", func(value *Spec) { value.ExposeRuntimeSocket = true }},
		{"new privileges", func(value *Spec) { value.NoNewPrivileges = false }},
		{"resource overflow", func(value *Spec) { value.Limits.MemoryBytes++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.edit(&value)
			if err := policy.Validate(value); err == nil {
				t.Fatal("unsafe spec accepted")
			}
		})
	}
}
