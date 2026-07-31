// Package executor defines the isolation contract for future containerd-backed
// execution. No arbitrary workload executor is enabled by the bootstrap.
package executor

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type NetworkMode string

const (
	NetworkNone      NetworkMode = "none"
	NetworkAllowlist NetworkMode = "allowlist"
)

type Limits struct {
	CPUMillis      uint64
	MemoryBytes    uint64
	DiskBytes      uint64
	PIDs           uint32
	ExecutionTime  time.Duration
	OutputBytes    uint64
	GPUDeviceCount uint32
}

type Spec struct {
	ImageDigest  string
	Entrypoint   []string
	Environment  map[string]string
	ReadOnlyRoot bool
	Network      NetworkMode
	AllowedHosts []string
	AllowGPU     bool
	Limits       Limits
}

type Result struct {
	ExitCode int
	Output   []byte
	Usage    Usage
}

type Usage struct {
	CPUMillis   uint64
	PeakMemory  uint64
	DiskWritten uint64
	Duration    time.Duration
}

type Executor interface {
	Execute(context.Context, Spec, []byte) (Result, error)
}

// Policy is a terminal-owned ceiling. A remote task may request less, never
// more, and may not enable capabilities absent from this policy.
type Policy struct {
	AllowedImages       map[string]struct{}
	MaxEnvironment      int
	MaxArguments        int
	MaxAllowedHosts     int
	Ceiling             Limits
	PermitGPU           bool
	PermitNetwork       bool
	RequireReadOnlyRoot bool
}

func (p Policy) Validate(spec Spec) error {
	if !digestPattern.MatchString(spec.ImageDigest) {
		return errors.New("container image must be pinned by sha256 digest")
	}
	if _, allowed := p.AllowedImages[spec.ImageDigest]; !allowed {
		return errors.New("container image is not allowlisted")
	}
	if len(spec.Entrypoint) == 0 || len(spec.Entrypoint) > p.MaxArguments ||
		len(spec.Environment) > p.MaxEnvironment || len(spec.AllowedHosts) > p.MaxAllowedHosts {
		return errors.New("executor argument or environment bounds exceeded")
	}
	if p.RequireReadOnlyRoot && !spec.ReadOnlyRoot {
		return errors.New("read-only root filesystem is required")
	}
	switch spec.Network {
	case NetworkNone:
		if len(spec.AllowedHosts) != 0 {
			return errors.New("allowed hosts require allowlist network mode")
		}
	case NetworkAllowlist:
		if !p.PermitNetwork || len(spec.AllowedHosts) == 0 {
			return errors.New("network access is not authorized")
		}
	default:
		return errors.New("invalid network mode")
	}
	if spec.AllowGPU && !p.PermitGPU {
		return errors.New("GPU device access is not authorized")
	}
	if spec.AllowGPU {
		if spec.Limits.GPUDeviceCount == 0 || spec.Limits.GPUDeviceCount > p.Ceiling.GPUDeviceCount {
			return errors.New("GPU device limit is zero or exceeds policy")
		}
	} else if spec.Limits.GPUDeviceCount != 0 {
		return errors.New("GPU device count requires GPU authorization")
	}
	if err := withinCeiling(spec.Limits, p.Ceiling); err != nil {
		return err
	}
	return nil
}

func withinCeiling(request, ceiling Limits) error {
	checks := []struct {
		name      string
		requested uint64
		maximum   uint64
	}{
		{"cpu", request.CPUMillis, ceiling.CPUMillis},
		{"memory", request.MemoryBytes, ceiling.MemoryBytes},
		{"disk", request.DiskBytes, ceiling.DiskBytes},
		{"output", request.OutputBytes, ceiling.OutputBytes},
		{"execution time", uint64(request.ExecutionTime), uint64(ceiling.ExecutionTime)},
		{"pids", uint64(request.PIDs), uint64(ceiling.PIDs)},
	}
	for _, check := range checks {
		if check.requested == 0 || check.maximum == 0 || check.requested > check.maximum {
			return fmt.Errorf("%s limit is zero or exceeds policy", check.name)
		}
	}
	return nil
}

// DenyAll makes the bootstrap fail closed until a concrete isolated executor
// and operator policy are explicitly configured.
type DenyAll struct{}

func (DenyAll) Execute(context.Context, Spec, []byte) (Result, error) {
	return Result{}, errors.New("arbitrary workload execution is disabled")
}
