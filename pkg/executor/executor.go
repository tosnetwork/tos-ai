// Package executor defines the isolation contract for future containerd-backed
// execution. No arbitrary workload executor is enabled by the bootstrap.
package executor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

const (
	MaxAllowedImagesHard       = 1024
	MaxAllowedNetworkHostsHard = 1024
)

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
// implement. Implementations must honor context cancellation by stopping and
// cleaning up the workload before returning. Result and Usage are treated as
// untrusted input and validated by PolicyExecutor. This package intentionally
// does not provide a concrete containerd implementation yet.
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
	AllowedNetworkHosts []string
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

func (p Policy) validate() error {
	if p.MaxAllowedImages <= 0 || p.MaxAllowedImages > MaxAllowedImagesHard ||
		len(p.AllowedImages) > p.MaxAllowedImages ||
		len(p.AllowedNetworkHosts) > MaxAllowedNetworkHostsHard ||
		p.MaxEnvironment < 0 || p.MaxArguments <= 0 || p.MaxAllowedHosts < 0 ||
		p.MaxAllowedHosts > MaxAllowedNetworkHostsHard ||
		p.MaxStringBytes <= 0 || p.MaxInputBytes == 0 {
		return errors.New("invalid executor policy")
	}
	if err := validatePolicyCeiling(p.Ceiling, p.PermitGPU); err != nil {
		return err
	}
	if p.PermitNetwork {
		if p.MaxAllowedHosts == 0 || len(p.AllowedNetworkHosts) == 0 {
			return errors.New("network policy requires an operator host allowlist")
		}
	} else if len(p.AllowedNetworkHosts) != 0 {
		return errors.New("network host allowlist requires network permission")
	}
	if err := validateNetworkHosts(
		p.AllowedNetworkHosts,
		p.MaxStringBytes,
	); err != nil {
		return fmt.Errorf("invalid executor network policy: %w", err)
	}
	for digest := range p.AllowedImages {
		if !digestPattern.MatchString(digest) {
			return errors.New("executor policy contains an unpinned image")
		}
	}
	return nil
}

func (p Policy) Validate(spec Spec) error {
	if err := p.validate(); err != nil {
		return err
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
		if invalidContainerString(argument, p.MaxStringBytes, false) {
			return errors.New("executor argument is invalid")
		}
	}
	for key, value := range spec.Environment {
		if invalidContainerString(key, p.MaxStringBytes, false) ||
			strings.Contains(key, "=") ||
			invalidContainerString(value, p.MaxStringBytes, true) {
			return errors.New("executor environment is invalid")
		}
	}
	if err := validateNetworkHosts(spec.AllowedHosts, p.MaxStringBytes); err != nil {
		return fmt.Errorf("executor allowed host is invalid: %w", err)
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
		for _, host := range spec.AllowedHosts {
			if !containsExactString(p.AllowedNetworkHosts, host) {
				return errors.New("network host is not authorized")
			}
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

// PolicyExecutor is a bounded validation and defensive-copy boundary around
// an audited container runtime client. It does not implement isolation by
// itself: the supplied client must enforce ContainerRequest in its runtime.
// The policy is cloned at construction and is safe for concurrent use.
type PolicyExecutor struct {
	policy Policy
	client ContainerdClient
}

func NewPolicyExecutor(
	policy Policy,
	client ContainerdClient,
) (*PolicyExecutor, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	if nilContainerdClient(client) {
		return nil, errors.New("nil container runtime client")
	}
	return &PolicyExecutor{
		policy: clonePolicy(policy),
		client: client,
	}, nil
}

func (e *PolicyExecutor) Execute(
	ctx context.Context,
	spec Spec,
	input []byte,
) (Result, error) {
	if e == nil || e.client == nil {
		return Result{}, errors.New("invalid policy executor")
	}
	if ctx == nil {
		return Result{}, errors.New("nil execution context")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := e.policy.Validate(spec); err != nil {
		return Result{}, err
	}
	if err := e.policy.ValidateInput(input); err != nil {
		return Result{}, err
	}
	request := ContainerRequest{
		ImageDigest:     spec.ImageDigest,
		Entrypoint:      append([]string(nil), spec.Entrypoint...),
		Environment:     cloneStringsMap(spec.Environment),
		UserID:          spec.UserID,
		GroupID:         spec.GroupID,
		ReadOnlyRoot:    spec.ReadOnlyRoot,
		NoNewPrivileges: spec.NoNewPrivileges,
		Network:         spec.Network,
		AllowedHosts:    append([]string(nil), spec.AllowedHosts...),
		AllowGPU:        spec.AllowGPU,
		Limits:          spec.Limits,
	}
	// The runtime client receives a deadline even when the caller did not set
	// one. A concrete backend must honor context cancellation while stopping
	// and cleaning up the isolated workload; this boundary deliberately does
	// not launch an unbounded goroutine to emulate cancellation for a broken
	// backend.
	executionCtx, cancel := context.WithTimeout(ctx, spec.Limits.ExecutionTime)
	defer cancel()
	result, err := callContainerdClient(
		executionCtx,
		e.client,
		request,
		append([]byte(nil), input...),
	)
	if contextErr := executionContextError(executionCtx); contextErr != nil {
		return Result{}, contextErr
	}
	if err != nil {
		return Result{}, errors.New("isolated execution backend failed")
	}
	if err := validateResult(spec.Limits, result); err != nil {
		return Result{}, err
	}
	result.Output = append([]byte(nil), result.Output...)
	return result, nil
}

func executionContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// A backend may return just after the deadline before the context timer's
	// goroutine has published DeadlineExceeded. Check the absolute deadline so
	// such a result cannot be accepted due to scheduler timing.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func callContainerdClient(
	ctx context.Context,
	client ContainerdClient,
	request ContainerRequest,
	input []byte,
) (result Result, err error) {
	defer func() {
		if recover() != nil {
			result = Result{}
			err = errors.New("container runtime client panicked")
		}
	}()
	return client.RunIsolated(ctx, request, input)
}

func validateResult(limits Limits, result Result) error {
	if result.ExitCode < 0 || result.ExitCode > 255 ||
		uint64(len(result.Output)) > limits.OutputBytes ||
		result.Usage.CPUMillis > limits.CPUMillis ||
		result.Usage.PeakMemory > limits.MemoryBytes ||
		result.Usage.DiskWritten > limits.DiskBytes ||
		result.Usage.Duration < 0 ||
		result.Usage.Duration > limits.ExecutionTime {
		return errors.New("isolated execution result exceeds requested limits")
	}
	return nil
}

func clonePolicy(policy Policy) Policy {
	cloned := policy
	cloned.AllowedNetworkHosts = append(
		[]string(nil),
		policy.AllowedNetworkHosts...,
	)
	cloned.AllowedImages = make(
		map[string]struct{},
		len(policy.AllowedImages),
	)
	for digest := range policy.AllowedImages {
		cloned.AllowedImages[digest] = struct{}{}
	}
	return cloned
}

func cloneStringsMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func invalidContainerString(value string, maximum int, allowEmpty bool) bool {
	return (!allowEmpty && len(value) == 0) ||
		len(value) > maximum || strings.IndexByte(value, 0) >= 0
}

func validateNetworkHosts(hosts []string, maximum int) error {
	seen := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		if invalidNetworkHost(host, maximum) {
			return errors.New("host is empty, malformed, or exceeds the string bound")
		}
		if _, duplicate := seen[host]; duplicate {
			return errors.New("duplicate host")
		}
		seen[host] = struct{}{}
	}
	return nil
}

func invalidNetworkHost(host string, maximum int) bool {
	if invalidContainerString(host, maximum, false) {
		return true
	}
	if net.ParseIP(host) != nil {
		return false
	}

	hostname := host
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return net.ParseIP(host[1:len(host)-1]) == nil
	}
	if strings.Contains(host, ":") {
		var port string
		var err error
		hostname, port, err = net.SplitHostPort(host)
		if err != nil || invalidNetworkPort(port) {
			return true
		}
		if net.ParseIP(hostname) != nil {
			return false
		}
	}
	return invalidDNSHostname(hostname)
}

func invalidNetworkPort(port string) bool {
	parsed, err := strconv.ParseUint(port, 10, 16)
	return err != nil || parsed == 0
}

func invalidDNSHostname(hostname string) bool {
	if len(hostname) == 0 || len(hostname) > 253 {
		return true
	}
	for _, label := range strings.Split(hostname, ".") {
		if len(label) == 0 || len(label) > 63 ||
			label[0] == '-' || label[len(label)-1] == '-' {
			return true
		}
		for index := 0; index < len(label); index++ {
			character := label[index]
			if character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' ||
				character >= '0' && character <= '9' ||
				character == '-' {
				continue
			}
			return true
		}
	}
	return false
}

func containsExactString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func validatePolicyCeiling(ceiling Limits, permitGPU bool) error {
	if ceiling.CPUMillis == 0 || ceiling.MemoryBytes == 0 ||
		ceiling.DiskBytes == 0 || ceiling.PIDs == 0 ||
		ceiling.ExecutionTime <= 0 || ceiling.OutputBytes == 0 {
		return errors.New("executor policy contains a zero or invalid resource ceiling")
	}
	if permitGPU && ceiling.GPUDeviceCount == 0 {
		return errors.New("GPU policy requires a positive device ceiling")
	}
	return nil
}

func nilContainerdClient(client ContainerdClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface,
		reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (p Policy) ValidateInput(input []byte) error {
	if uint64(len(input)) > p.MaxInputBytes {
		return errors.New("executor input exceeds policy")
	}
	return nil
}

func withinCeiling(request, ceiling Limits) error {
	if request.ExecutionTime <= 0 || ceiling.ExecutionTime <= 0 {
		return errors.New("execution time limit is zero or exceeds policy")
	}
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
