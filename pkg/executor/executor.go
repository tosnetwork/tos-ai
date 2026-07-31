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

const MaxAllowedImagesHard = 1024

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
	ImageDigest         string
	Entrypoint          []string
	Environment         map[string]string
	ReadOnlyRoot        bool
	Network             NetworkMode
	AllowedHosts        []string
	AllowGPU            bool
	UserID              uint32
	GroupID             uint32
	Privileged          bool
	NoNewPrivileges     bool
	HostMounts          []Mount
	ExposeRuntimeSocket bool
	Limits              Limits
}

type Mount struct {
	Source string
	Target string
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

// ContainerdClient is the narrow client a future audited backend must
// implement. This package intentionally does not provide a concrete
// containerd implementation yet.
type ContainerdClient interface {
	RunIsolated(context.Context, ContainerRequest, []byte) (Result, error)
}

type ContainerRequest struct {
	ImageDigest     string
	Entrypoint      []string
	Environment     map[string]string
	UserID          uint32
	GroupID         uint32
	ReadOnlyRoot    bool
	NoNewPrivileges bool
	Network         NetworkMode
	AllowedHosts    []string
	AllowGPU        bool
	Limits          Limits
}

// Policy is a terminal-owned ceiling. A remote task may request less, never
// more, and may not enable capabilities absent from this policy.
type Policy struct {
	AllowedImages       map[string]struct{}
	MaxAllowedImages    int
	MaxEnvironment      int
	MaxArguments        int
	MaxAllowedHosts     int
	MaxStringBytes      int
	MaxInputBytes       uint64
	Ceiling             Limits
	PermitGPU           bool
	PermitNetwork       bool
	RequireReadOnlyRoot bool
}

func (p Policy) Validate(spec Spec) error {
	if p.MaxAllowedImages <= 0 || p.MaxAllowedImages > MaxAllowedImagesHard ||
		len(p.AllowedImages) > p.MaxAllowedImages ||
		p.MaxEnvironment < 0 || p.MaxArguments <= 0 || p.MaxAllowedHosts < 0 ||
		p.MaxStringBytes <= 0 || p.MaxInputBytes == 0 {
		return errors.New("invalid executor policy")
	}
	for digest := range p.AllowedImages {
		if !digestPattern.MatchString(digest) {
			return errors.New("executor policy contains an unpinned image")
		}
	}
	if !digestPattern.MatchString(spec.ImageDigest) {
		return errors.New("container image must be pinned by sha256 digest")
	}
	if _, allowed := p.AllowedImages[spec.ImageDigest]; !allowed {
		return errors.New("container image is not allowlisted")
	}
	if len(spec.Entrypoint) == 0 || len(spec.Entrypoint) > p.MaxArguments ||
		len(spec.Environment) > p.MaxEnvironment || len(spec.AllowedHosts) > p.MaxAllowedHosts ||
		len(spec.HostMounts) != 0 {
		return errors.New("executor argument or environment bounds exceeded")
	}
	for _, argument := range spec.Entrypoint {
		if len(argument) == 0 || len(argument) > p.MaxStringBytes {
			return errors.New("executor argument is invalid")
		}
	}
	for key, value := range spec.Environment {
		if len(key) == 0 || len(key) > p.MaxStringBytes || len(value) > p.MaxStringBytes {
			return errors.New("executor environment is invalid")
		}
	}
	for _, host := range spec.AllowedHosts {
		if len(host) == 0 || len(host) > p.MaxStringBytes {
			return errors.New("executor allowed host is invalid")
		}
	}
	if spec.Privileged {
		return errors.New("privileged execution is forbidden")
	}
	if spec.UserID == 0 || spec.GroupID == 0 {
		return errors.New("container must run as a non-root user and group")
	}
	if !spec.NoNewPrivileges {
		return errors.New("no-new-privileges is required")
	}
	if spec.ExposeRuntimeSocket {
		return errors.New("container runtime socket exposure is forbidden")
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

func (p Policy) ValidateInput(input []byte) error {
	if uint64(len(input)) > p.MaxInputBytes {
		return errors.New("executor input exceeds policy")
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
