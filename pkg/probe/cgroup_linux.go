//go:build linux

package probe

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

const (
	maxCgroupMembershipBytes = 64 << 10
	maxMountInfoBytes        = 1 << 20
	maxCgroupScalarBytes     = 4 << 10
	maxCgroupLines           = 4096
	maxCgroupMounts          = 64
	maxControllersPerLine    = 32
	maxCgroupDepth           = 128
	maxResourcePathBytes     = 4096
)

var errLinuxResourceObservation = errors.New("invalid Linux resource observation")

type resourceReadFunc func(string, int64) ([]byte, error)

type cgroupMemberships struct {
	unified    string
	unifiedSet bool
	memory     string
	memorySet  bool
	cpu        string
	cpuSet     bool
	cpuset     string
	cpusetSet  bool
}

type cgroupMount struct {
	version int
	root    string
	point   string
	memory  bool
	cpu     bool
	cpuset  bool
}

func readLimitedResourceFile(path string, maximum int64) ([]byte, error) {
	if maximum <= 0 {
		return nil, errLinuxResourceObservation
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, errLinuxResourceObservation
	}
	return data, nil
}

func effectiveLinuxResources(
	systemMemory uint64,
	systemCPUs int,
	read resourceReadFunc,
) (uint64, int, error) {
	if systemMemory == 0 || systemCPUs <= 0 || read == nil {
		return 0, 0, errLinuxResourceObservation
	}
	membershipData, err := read("/proc/self/cgroup", maxCgroupMembershipBytes)
	if err != nil {
		return 0, 0, errLinuxResourceObservation
	}
	mountData, err := read("/proc/self/mountinfo", maxMountInfoBytes)
	if err != nil {
		return 0, 0, errLinuxResourceObservation
	}
	memberships, err := parseCgroupMemberships(membershipData)
	if err != nil {
		return 0, 0, errLinuxResourceObservation
	}
	mounts, err := parseCgroupMounts(mountData)
	if err != nil {
		return 0, 0, errLinuxResourceObservation
	}

	memory := systemMemory
	if memberships.memorySet {
		mount, current, selectErr := selectCgroupMount(
			mounts, 1, "memory", memberships.memory,
		)
		if selectErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		limit, limited, limitErr := cgroupMemoryLimit(
			mount, current, "memory.limit_in_bytes", false, true, read,
		)
		if limitErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		if limited && limit < memory {
			memory = limit
		}
	} else if memberships.unifiedSet {
		mount, current, selectErr := selectCgroupMount(
			mounts, 2, "", memberships.unified,
		)
		if selectErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		limit, limited, limitErr := cgroupMemoryLimit(
			mount, current, "memory.max", true, false, read,
		)
		if limitErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		if limited && limit < memory {
			memory = limit
		}
	}

	cpus := systemCPUs
	if cpus > MaxLogicalCPUs {
		cpus = MaxLogicalCPUs
	}
	if memberships.cpuSet {
		mount, current, selectErr := selectCgroupMount(
			mounts, 1, "cpu", memberships.cpu,
		)
		if selectErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		quotaCPUs, limited, quotaErr := cgroupV1CPUQuota(
			mount, current, read,
		)
		if quotaErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		if limited && quotaCPUs < cpus {
			cpus = quotaCPUs
		}
	} else if memberships.unifiedSet {
		mount, current, selectErr := selectCgroupMount(
			mounts, 2, "", memberships.unified,
		)
		if selectErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		quotaCPUs, limited, quotaErr := cgroupV2CPUQuota(
			mount, current, read,
		)
		if quotaErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		if limited && quotaCPUs < cpus {
			cpus = quotaCPUs
		}
	}

	if memberships.cpusetSet {
		mount, current, selectErr := selectCgroupMount(
			mounts, 1, "cpuset", memberships.cpuset,
		)
		if selectErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		cpusetCPUs, limited, cpusetErr := cgroupV1CPUSet(
			mount, current, read,
		)
		if cpusetErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		if limited && cpusetCPUs < cpus {
			cpus = cpusetCPUs
		}
	} else if memberships.unifiedSet {
		_, current, selectErr := selectCgroupMount(
			mounts, 2, "", memberships.unified,
		)
		if selectErr != nil {
			return 0, 0, errLinuxResourceObservation
		}
		data, readErr := read(
			filepath.Join(current, "cpuset.cpus.effective"),
			maxCgroupScalarBytes,
		)
		if readErr == nil {
			if len(strings.TrimSpace(string(data))) == 0 {
				return 0, 0, errLinuxResourceObservation
			}
			cpusetCPUs, parseErr := parseCPUSet(data)
			if parseErr != nil {
				return 0, 0, errLinuxResourceObservation
			}
			if cpusetCPUs < cpus {
				cpus = cpusetCPUs
			}
		} else if readErr != nil && !errors.Is(readErr, fs.ErrNotExist) {
			return 0, 0, errLinuxResourceObservation
		}
	}
	if memory == 0 || cpus <= 0 {
		return 0, 0, errLinuxResourceObservation
	}
	return memory, cpus, nil
}

func parseCgroupMemberships(data []byte) (cgroupMemberships, error) {
	var result cgroupMemberships
	lines, err := boundedLines(data, maxCgroupLines)
	if err != nil {
		return result, err
	}
	for _, line := range lines {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 || !decimalString(parts[0]) {
			return cgroupMemberships{}, errLinuxResourceObservation
		}
		path, pathErr := cleanResourcePath(parts[2])
		if pathErr != nil {
			return cgroupMemberships{}, pathErr
		}
		if parts[1] == "" {
			if parts[0] != "0" || result.unifiedSet {
				return cgroupMemberships{}, errLinuxResourceObservation
			}
			result.unified, result.unifiedSet = path, true
			continue
		}
		controllers := strings.Split(parts[1], ",")
		if len(controllers) == 0 || len(controllers) > maxControllersPerLine {
			return cgroupMemberships{}, errLinuxResourceObservation
		}
		for _, controller := range controllers {
			switch controller {
			case "memory":
				if result.memorySet && result.memory != path {
					return cgroupMemberships{}, errLinuxResourceObservation
				}
				result.memory, result.memorySet = path, true
			case "cpu":
				if result.cpuSet && result.cpu != path {
					return cgroupMemberships{}, errLinuxResourceObservation
				}
				result.cpu, result.cpuSet = path, true
			case "cpuset":
				if result.cpusetSet && result.cpuset != path {
					return cgroupMemberships{}, errLinuxResourceObservation
				}
				result.cpuset, result.cpusetSet = path, true
			case "cpuacct":
			default:
				if controller == "" || len(controller) > 64 {
					return cgroupMemberships{}, errLinuxResourceObservation
				}
			}
		}
	}
	return result, nil
}

func parseCgroupMounts(data []byte) ([]cgroupMount, error) {
	lines, err := boundedLines(data, maxCgroupLines)
	if err != nil {
		return nil, err
	}
	mounts := make([]cgroupMount, 0, 4)
	for _, line := range lines {
		separator := strings.Index(line, " - ")
		if separator < 0 {
			return nil, errLinuxResourceObservation
		}
		left := strings.Fields(line[:separator])
		right := strings.Fields(line[separator+3:])
		if len(left) < 6 || len(right) < 3 {
			return nil, errLinuxResourceObservation
		}
		version := 0
		switch right[0] {
		case "cgroup2":
			version = 2
		case "cgroup":
			version = 1
		default:
			continue
		}
		if len(mounts) >= maxCgroupMounts {
			return nil, errLinuxResourceObservation
		}
		root, rootErr := decodeMountPath(left[3])
		point, pointErr := decodeMountPath(left[4])
		if rootErr != nil || pointErr != nil {
			return nil, errLinuxResourceObservation
		}
		mount := cgroupMount{version: version, root: root, point: point}
		if version == 1 {
			setMountControllers(&mount, left[5])
			setMountControllers(&mount, right[2])
		}
		mounts = append(mounts, mount)
	}
	return mounts, nil
}

func setMountControllers(mount *cgroupMount, options string) {
	for _, option := range strings.Split(options, ",") {
		switch option {
		case "memory":
			mount.memory = true
		case "cpu":
			mount.cpu = true
		case "cpuset":
			mount.cpuset = true
		}
	}
}

func selectCgroupMount(
	mounts []cgroupMount,
	version int,
	controller string,
	membership string,
) (cgroupMount, string, error) {
	selected := -1
	selectedPath := ""
	for index := range mounts {
		mount := mounts[index]
		if mount.version != version || !mountHasController(mount, controller) {
			continue
		}
		current, ok := resolveCgroupPath(mount, membership)
		if !ok {
			continue
		}
		if selected < 0 || len(mount.root) > len(mounts[selected].root) {
			selected, selectedPath = index, current
			continue
		}
		if len(mount.root) == len(mounts[selected].root) &&
			(mount.point != mounts[selected].point || current != selectedPath) {
			return cgroupMount{}, "", errLinuxResourceObservation
		}
	}
	if selected < 0 {
		return cgroupMount{}, "", errLinuxResourceObservation
	}
	return mounts[selected], selectedPath, nil
}

func mountHasController(mount cgroupMount, controller string) bool {
	if mount.version == 2 {
		return controller == ""
	}
	switch controller {
	case "memory":
		return mount.memory
	case "cpu":
		return mount.cpu
	case "cpuset":
		return mount.cpuset
	default:
		return false
	}
}

func resolveCgroupPath(mount cgroupMount, membership string) (string, bool) {
	root, rootErr := cleanResourcePath(mount.root)
	point, pointErr := cleanResourcePath(mount.point)
	member, memberErr := cleanResourcePath(membership)
	if rootErr != nil || pointErr != nil || memberErr != nil {
		return "", false
	}
	var relative string
	switch {
	case member == root:
		relative = "."
	case root == "/":
		relative = strings.TrimPrefix(member, "/")
	case strings.HasPrefix(member, root+"/"):
		relative = strings.TrimPrefix(strings.TrimPrefix(member, root), "/")
	case member == "/":
		// A cgroup namespace reports its namespace root as "/" even when the
		// host-side mountinfo root names a deeper hierarchy.
		relative = "."
	default:
		return "", false
	}
	current := filepath.Clean(filepath.Join(point, relative))
	if !pathWithin(point, current) || len(current) > maxResourcePathBytes {
		return "", false
	}
	return current, true
}

func cgroupMemoryLimit(
	mount cgroupMount,
	current string,
	filename string,
	allowMax bool,
	requireFile bool,
	read resourceReadFunc,
) (uint64, bool, error) {
	var minimum uint64
	foundFile := false
	err := walkCgroupAncestors(current, mount.point, func(directory string) error {
		data, readErr := read(
			filepath.Join(directory, filename), maxCgroupScalarBytes,
		)
		if errors.Is(readErr, fs.ErrNotExist) && !requireFile {
			return nil
		}
		if readErr != nil {
			return errLinuxResourceObservation
		}
		foundFile = true
		limit, limited, parseErr := parseMemoryLimit(data, allowMax)
		if parseErr != nil {
			return parseErr
		}
		if limited && (minimum == 0 || limit < minimum) {
			minimum = limit
		}
		return nil
	})
	if err != nil || requireFile && !foundFile {
		return 0, false, errLinuxResourceObservation
	}
	return minimum, minimum != 0, nil
}

func parseMemoryLimit(data []byte, allowMax bool) (uint64, bool, error) {
	fields := strings.Fields(string(data))
	if len(fields) != 1 {
		return 0, false, errLinuxResourceObservation
	}
	if allowMax && fields[0] == "max" {
		return 0, false, nil
	}
	value, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil || value == 0 {
		return 0, false, errLinuxResourceObservation
	}
	return value, true, nil
}

func cgroupV2CPUQuota(
	mount cgroupMount,
	current string,
	read resourceReadFunc,
) (int, bool, error) {
	minimum := 0
	err := walkCgroupAncestors(current, mount.point, func(directory string) error {
		data, readErr := read(
			filepath.Join(directory, "cpu.max"), maxCgroupScalarBytes,
		)
		if errors.Is(readErr, fs.ErrNotExist) {
			return nil
		}
		if readErr != nil {
			return errLinuxResourceObservation
		}
		fields := strings.Fields(string(data))
		if len(fields) != 2 {
			return errLinuxResourceObservation
		}
		period, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil || period == 0 {
			return errLinuxResourceObservation
		}
		if fields[0] == "max" {
			return nil
		}
		quota, parseErr := strconv.ParseUint(fields[0], 10, 64)
		if parseErr != nil || quota == 0 {
			return errLinuxResourceObservation
		}
		cpus := quotaCPUCount(quota, period)
		if minimum == 0 || cpus < minimum {
			minimum = cpus
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return minimum, minimum != 0, nil
}

func cgroupV1CPUQuota(
	mount cgroupMount,
	current string,
	read resourceReadFunc,
) (int, bool, error) {
	minimum := 0
	err := walkCgroupAncestors(current, mount.point, func(directory string) error {
		quotaData, quotaErr := read(
			filepath.Join(directory, "cpu.cfs_quota_us"), maxCgroupScalarBytes,
		)
		periodData, periodErr := read(
			filepath.Join(directory, "cpu.cfs_period_us"), maxCgroupScalarBytes,
		)
		if quotaErr != nil || periodErr != nil {
			return errLinuxResourceObservation
		}
		quotaFields := strings.Fields(string(quotaData))
		periodFields := strings.Fields(string(periodData))
		if len(quotaFields) != 1 || len(periodFields) != 1 {
			return errLinuxResourceObservation
		}
		quota, parseErr := strconv.ParseInt(quotaFields[0], 10, 64)
		if parseErr != nil || quota == 0 || quota < -1 {
			return errLinuxResourceObservation
		}
		period, parseErr := strconv.ParseUint(periodFields[0], 10, 64)
		if parseErr != nil || period == 0 {
			return errLinuxResourceObservation
		}
		if quota == -1 {
			return nil
		}
		cpus := quotaCPUCount(uint64(quota), period)
		if minimum == 0 || cpus < minimum {
			minimum = cpus
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return minimum, minimum != 0, nil
}

func quotaCPUCount(quota uint64, period uint64) int {
	count := quota / period
	if quota%period != 0 {
		count++
	}
	if count == 0 {
		count = 1
	}
	if count > MaxLogicalCPUs {
		return MaxLogicalCPUs
	}
	return int(count)
}

func cgroupV1CPUSet(
	mount cgroupMount,
	current string,
	read resourceReadFunc,
) (int, bool, error) {
	count := 0
	found := false
	err := walkCgroupAncestors(current, mount.point, func(directory string) error {
		if found {
			return nil
		}
		data, readErr := read(
			filepath.Join(directory, "cpuset.cpus"), maxCgroupScalarBytes,
		)
		if readErr != nil {
			return errLinuxResourceObservation
		}
		if len(strings.TrimSpace(string(data))) == 0 {
			return nil
		}
		parsed, parseErr := parseCPUSet(data)
		if parseErr != nil {
			return parseErr
		}
		count, found = parsed, true
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return count, found, nil
}

func parseCPUSet(data []byte) (int, error) {
	value := strings.TrimSpace(string(data))
	if value == "" || len(value) > maxCgroupScalarBytes {
		return 0, errLinuxResourceObservation
	}
	var observed [MaxLogicalCPUs]bool
	count := 0
	items := strings.Split(value, ",")
	if len(items) > MaxLogicalCPUs {
		return 0, errLinuxResourceObservation
	}
	for _, item := range items {
		bounds := strings.Split(item, "-")
		if len(bounds) < 1 || len(bounds) > 2 || bounds[0] == "" {
			return 0, errLinuxResourceObservation
		}
		start, err := strconv.Atoi(bounds[0])
		if err != nil || start < 0 || start >= MaxLogicalCPUs {
			return 0, errLinuxResourceObservation
		}
		end := start
		if len(bounds) == 2 {
			if bounds[1] == "" {
				return 0, errLinuxResourceObservation
			}
			end, err = strconv.Atoi(bounds[1])
			if err != nil || end < start || end >= MaxLogicalCPUs {
				return 0, errLinuxResourceObservation
			}
		}
		for cpu := start; cpu <= end; cpu++ {
			if observed[cpu] {
				return 0, errLinuxResourceObservation
			}
			observed[cpu] = true
			count++
		}
	}
	if count == 0 {
		return 0, errLinuxResourceObservation
	}
	return count, nil
}

func walkCgroupAncestors(
	current string,
	root string,
	visit func(string) error,
) error {
	current = filepath.Clean(current)
	root = filepath.Clean(root)
	if visit == nil || !pathWithin(root, current) {
		return errLinuxResourceObservation
	}
	for depth := 0; depth <= maxCgroupDepth; depth++ {
		if err := visit(current); err != nil {
			return err
		}
		if current == root {
			return nil
		}
		parent := filepath.Dir(current)
		if parent == current || !pathWithin(root, parent) {
			return errLinuxResourceObservation
		}
		current = parent
	}
	return errLinuxResourceObservation
}

func boundedLines(data []byte, maximum int) ([]string, error) {
	if len(data) == 0 {
		return []string{}, nil
	}
	if strings.IndexByte(string(data), 0) >= 0 {
		return nil, errLinuxResourceObservation
	}
	value := strings.TrimSuffix(string(data), "\n")
	lines := strings.Split(value, "\n")
	if len(lines) > maximum {
		return nil, errLinuxResourceObservation
	}
	for _, line := range lines {
		if line == "" || len(line) > maxResourcePathBytes*2 {
			return nil, errLinuxResourceObservation
		}
	}
	return lines, nil
}

func decodeMountPath(value string) (string, error) {
	if value == "" || len(value) > maxResourcePathBytes*2 {
		return "", errLinuxResourceObservation
	}
	var decoded strings.Builder
	decoded.Grow(len(value))
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' {
			decoded.WriteByte(value[index])
			continue
		}
		if index+3 >= len(value) {
			return "", errLinuxResourceObservation
		}
		escape := value[index+1 : index+4]
		switch escape {
		case "011":
			decoded.WriteByte('\t')
		case "012":
			decoded.WriteByte('\n')
		case "040":
			decoded.WriteByte(' ')
		case "134":
			decoded.WriteByte('\\')
		default:
			return "", errLinuxResourceObservation
		}
		index += 3
	}
	return cleanResourcePath(decoded.String())
}

func cleanResourcePath(value string) (string, error) {
	if value == "" || len(value) > maxResourcePathBytes ||
		!filepath.IsAbs(value) ||
		strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", errLinuxResourceObservation
	}
	cleaned := filepath.Clean(value)
	if cleaned != value {
		return "", errLinuxResourceObservation
	}
	return cleaned, nil
}

func pathWithin(root string, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && (relative == "." ||
		(relative != ".." && !strings.HasPrefix(
			relative, ".."+string(filepath.Separator),
		)))
}

func decimalString(value string) bool {
	if value == "" || len(value) > 20 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
