package containerdbackend

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/contrib/seccomp"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

func validateRequest(request executor.ContainerRequest, input []byte) error {
	if executor.ValidateExecutionDigest(request.ExecutionDigest) != nil ||
		executor.ValidateExecutionDigest(request.ImageDigest) != nil ||
		len(input) > MaxInputBytesHard ||
		len(request.Entrypoint) == 0 || len(request.Entrypoint) > MaxArgumentsHard ||
		len(request.Environment) > MaxEnvironmentHard ||
		request.UserID == 0 || request.GroupID == 0 ||
		!request.ReadOnlyRoot || !request.NoNewPrivileges ||
		request.Network != executor.NetworkNone || len(request.AllowedHosts) != 0 ||
		request.AllowGPU || request.Limits.GPUDeviceCount != 0 ||
		validateDriverLimits(request.Limits) != nil {
		return errors.New("unsupported or invalid containerd execution request")
	}
	for _, argument := range request.Entrypoint {
		if invalidRuntimeString(argument, false) {
			return errors.New("invalid containerd process argument")
		}
	}
	for key, value := range request.Environment {
		if invalidRuntimeString(key, false) || strings.Contains(key, "=") ||
			invalidRuntimeString(value, true) {
			return errors.New("invalid containerd process environment")
		}
	}
	return nil
}

func validateDriverLimits(limits executor.Limits) error {
	if limits.CPUMillis == 0 || limits.MemoryBytes == 0 ||
		limits.DiskBytes < minimumTmpfsBytes || limits.PIDs == 0 ||
		limits.ExecutionTime <= 0 || limits.OutputBytes == 0 ||
		limits.CPUMillis > MaxCPUMillisHard ||
		limits.MemoryBytes > MaxMemoryBytesHard ||
		limits.DiskBytes > MaxDiskBytesHard || limits.PIDs > MaxPIDsHard ||
		limits.ExecutionTime > MaxExecutionTimeHard ||
		limits.OutputBytes > MaxOutputBytesHard || limits.GPUDeviceCount != 0 {
		return errors.New("containerd resource limits are unsupported or invalid")
	}
	_, _, err := cpuQuota(limits)
	return err
}

func invalidRuntimeString(value string, allowEmpty bool) bool {
	return (!allowEmpty && value == "") || len(value) > MaxStringBytesHard ||
		strings.IndexByte(value, 0) >= 0
}

func fixedIsolationSpec(request executor.ContainerRequest) oci.SpecOpts {
	return func(
		_ context.Context,
		_ oci.Client,
		_ *containers.Container,
		spec *oci.Spec,
	) error {
		if spec == nil || spec.Process == nil || spec.Root == nil || spec.Linux == nil {
			return errors.New("containerd generated an incomplete OCI specification")
		}
		period, quota, err := cpuQuota(request.Limits)
		if err != nil {
			return err
		}
		spec.Process.Args = append([]string(nil), request.Entrypoint...)
		spec.Process.Env = environment(request.Environment)
		spec.Process.Cwd = "/"
		spec.Process.Terminal = false
		spec.Process.User = specs.User{UID: request.UserID, GID: request.GroupID}
		spec.Process.NoNewPrivileges = true
		spec.Process.Capabilities = &specs.LinuxCapabilities{}
		spec.Process.Rlimits = []specs.POSIXRlimit{{
			Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024,
		}}
		spec.Root.Readonly = true
		if spec.Linux.Resources == nil {
			spec.Linux.Resources = &specs.LinuxResources{}
		}
		memory := int64(request.Limits.MemoryBytes)
		pids := int64(request.Limits.PIDs)
		spec.Linux.Resources.Memory = &specs.LinuxMemory{Limit: &memory}
		spec.Linux.Resources.Pids = &specs.LinuxPids{Limit: pids}
		spec.Linux.Resources.CPU = &specs.LinuxCPU{Period: &period, Quota: &quota}
		spec.Linux.Resources.Devices = []specs.LinuxDeviceCgroup{{
			Allow: false, Access: "rwm",
		}}
		profile := seccomp.DefaultProfile(spec)
		if profile == nil || profile.DefaultAction != specs.ActErrno ||
			len(profile.Syscalls) == 0 {
			return errors.New("containerd default seccomp profile is unavailable")
		}
		spec.Linux.Seccomp = profile
		if !hasPrivateNetworkNamespace(spec.Linux.Namespaces) {
			return errors.New("containerd OCI specification lacks a private network namespace")
		}
		if err := boundTmpfsMounts(spec, request.Limits.DiskBytes); err != nil {
			return err
		}
		return nil
	}
}

func environment(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func cpuQuota(limits executor.Limits) (uint64, int64, error) {
	const period = uint64(100_000)
	wallMillis := uint64(limits.ExecutionTime / time.Millisecond)
	if wallMillis == 0 || limits.CPUMillis > uint64(^uint64(0)/period) {
		return 0, 0, errors.New("invalid containerd CPU limit")
	}
	quota := (limits.CPUMillis*period + wallMillis - 1) / wallMillis
	if quota < 1000 || quota > uint64(^uint64(0)>>1) {
		return 0, 0, errors.New("containerd CPU limit cannot be represented safely")
	}
	return period, int64(quota), nil
}

func hasPrivateNetworkNamespace(namespaces []specs.LinuxNamespace) bool {
	for _, namespace := range namespaces {
		if namespace.Type == specs.NetworkNamespace && namespace.Path == "" {
			return true
		}
	}
	return false
}

func boundTmpfsMounts(spec *oci.Spec, total uint64) error {
	const expected = 3
	perMount := total / expected
	if perMount < 4096 {
		return errors.New("containerd disk limit is too small")
	}
	found := 0
	for index := range spec.Mounts {
		mount := &spec.Mounts[index]
		if mount.Type != "tmpfs" ||
			(mount.Destination != "/dev" && mount.Destination != "/dev/shm" &&
				mount.Destination != "/run") {
			continue
		}
		options := make([]string, 0, len(mount.Options)+2)
		for _, option := range mount.Options {
			if !strings.HasPrefix(option, "size=") {
				options = append(options, option)
			}
		}
		if !contains(options, "noexec") {
			options = append(options, "noexec")
		}
		options = append(options, "size="+strconv.FormatUint(perMount, 10))
		mount.Options = options
		found++
	}
	if found != expected {
		return errors.New("containerd OCI specification has unexpected tmpfs mounts")
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
