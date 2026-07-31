//go:build linux

package probe

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
)

const (
	testGiB = uint64(1 << 30)
	v2Mount = "35 25 0:30 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n"
)

type fakeResourceFiles map[string]string

func (files fakeResourceFiles) read(path string, maximum int64) ([]byte, error) {
	value, ok := files[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	if int64(len(value)) > maximum {
		return nil, errLinuxResourceObservation
	}
	return []byte(value), nil
}

func TestEffectiveLinuxResourcesV2UsesAncestorMinimums(t *testing.T) {
	files := fakeResourceFiles{
		"/proc/self/cgroup":                               "0::/tenant/job\n",
		"/proc/self/mountinfo":                            v2Mount,
		"/sys/fs/cgroup/tenant/job/memory.max":            "34359738368\n",
		"/sys/fs/cgroup/tenant/memory.max":                "8589934592\n",
		"/sys/fs/cgroup/memory.max":                       "max\n",
		"/sys/fs/cgroup/tenant/job/cpu.max":               "350000 100000\n",
		"/sys/fs/cgroup/tenant/cpu.max":                   "200000 100000\n",
		"/sys/fs/cgroup/cpu.max":                          "max 100000\n",
		"/sys/fs/cgroup/tenant/job/cpuset.cpus.effective": "0-7\n",
	}

	memory, cpus, err := effectiveLinuxResources(64*testGiB, 32, files.read)
	if err != nil {
		t.Fatal(err)
	}
	if memory != 8*testGiB || cpus != 2 {
		t.Fatalf("effective resources = %d bytes, %d CPUs", memory, cpus)
	}
}

func TestEffectiveLinuxResourcesV2UsesCPUSetAndHostMinimums(t *testing.T) {
	files := fakeResourceFiles{
		"/proc/self/cgroup":                               "0::/tenant/job\n",
		"/proc/self/mountinfo":                            v2Mount,
		"/sys/fs/cgroup/tenant/job/memory.max":            "68719476736\n",
		"/sys/fs/cgroup/tenant/memory.max":                "max\n",
		"/sys/fs/cgroup/memory.max":                       "max\n",
		"/sys/fs/cgroup/tenant/job/cpu.max":               "500000 100000\n",
		"/sys/fs/cgroup/tenant/cpu.max":                   "max 100000\n",
		"/sys/fs/cgroup/cpu.max":                          "max 100000\n",
		"/sys/fs/cgroup/tenant/job/cpuset.cpus.effective": "4-5\n",
	}

	memory, cpus, err := effectiveLinuxResources(4*testGiB, 3, files.read)
	if err != nil {
		t.Fatal(err)
	}
	if memory != 4*testGiB || cpus != 2 {
		t.Fatalf("effective resources = %d bytes, %d CPUs", memory, cpus)
	}
}

func TestEffectiveLinuxResourcesV1(t *testing.T) {
	files := fakeResourceFiles{
		"/proc/self/cgroup": "5:memory:/group/task\n" +
			"4:cpu,cpuacct:/group/task\n" +
			"3:cpuset:/group/task\n",
		"/proc/self/mountinfo": "31 25 0:27 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n" +
			"32 25 0:28 / /sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu,cpuacct\n" +
			"33 25 0:29 / /sys/fs/cgroup/cpuset rw - cgroup cgroup rw,cpuset\n",
		"/sys/fs/cgroup/memory/group/task/memory.limit_in_bytes": "17179869184\n",
		"/sys/fs/cgroup/memory/group/memory.limit_in_bytes":      "12884901888\n",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes":            "9223372036854771712\n",
		"/sys/fs/cgroup/cpu/group/task/cpu.cfs_quota_us":         "-1\n",
		"/sys/fs/cgroup/cpu/group/task/cpu.cfs_period_us":        "100000\n",
		"/sys/fs/cgroup/cpu/group/cpu.cfs_quota_us":              "150000\n",
		"/sys/fs/cgroup/cpu/group/cpu.cfs_period_us":             "100000\n",
		"/sys/fs/cgroup/cpu/cpu.cfs_quota_us":                    "-1\n",
		"/sys/fs/cgroup/cpu/cpu.cfs_period_us":                   "100000\n",
		"/sys/fs/cgroup/cpuset/group/task/cpuset.cpus":           "\n",
		"/sys/fs/cgroup/cpuset/group/cpuset.cpus":                "0-3\n",
	}

	memory, cpus, err := effectiveLinuxResources(64*testGiB, 32, files.read)
	if err != nil {
		t.Fatal(err)
	}
	if memory != 12*testGiB || cpus != 2 {
		t.Fatalf("effective resources = %d bytes, %d CPUs", memory, cpus)
	}
}

func TestEffectiveLinuxResourcesWithoutResourceCgroups(t *testing.T) {
	files := fakeResourceFiles{
		"/proc/self/cgroup":    "1:name=systemd:/user.slice\n",
		"/proc/self/mountinfo": "1 0 0:1 / / rw - tmpfs tmpfs rw\n",
	}
	memory, cpus, err := effectiveLinuxResources(16*testGiB, 8, files.read)
	if err != nil {
		t.Fatal(err)
	}
	if memory != 16*testGiB || cpus != 8 {
		t.Fatalf("effective resources = %d bytes, %d CPUs", memory, cpus)
	}
}

func TestEffectiveLinuxResourcesRejectsMalformedOrAmbiguousInput(t *testing.T) {
	valid := fakeResourceFiles{
		"/proc/self/cgroup":                               "0::/tenant/job\n",
		"/proc/self/mountinfo":                            v2Mount,
		"/sys/fs/cgroup/tenant/job/memory.max":            "max\n",
		"/sys/fs/cgroup/tenant/memory.max":                "max\n",
		"/sys/fs/cgroup/memory.max":                       "max\n",
		"/sys/fs/cgroup/tenant/job/cpu.max":               "max 100000\n",
		"/sys/fs/cgroup/tenant/cpu.max":                   "max 100000\n",
		"/sys/fs/cgroup/cpu.max":                          "max 100000\n",
		"/sys/fs/cgroup/tenant/job/cpuset.cpus.effective": "0-3\n",
	}
	tests := []struct {
		name   string
		mutate func(fakeResourceFiles)
	}{
		{name: "missing mount", mutate: func(files fakeResourceFiles) {
			files["/proc/self/mountinfo"] = "1 0 0:1 / / rw - tmpfs tmpfs rw\n"
		}},
		{name: "path traversal", mutate: func(files fakeResourceFiles) {
			files["/proc/self/cgroup"] = "0::/tenant/../escape\n"
		}},
		{name: "zero memory", mutate: func(files fakeResourceFiles) {
			files["/sys/fs/cgroup/tenant/job/memory.max"] = "0\n"
		}},
		{name: "malformed quota", mutate: func(files fakeResourceFiles) {
			files["/sys/fs/cgroup/tenant/job/cpu.max"] = "100000 0\n"
		}},
		{name: "overlapping cpuset", mutate: func(files fakeResourceFiles) {
			files["/sys/fs/cgroup/tenant/job/cpuset.cpus.effective"] = "0-2,2-3\n"
		}},
		{name: "empty effective cpuset", mutate: func(files fakeResourceFiles) {
			files["/sys/fs/cgroup/tenant/job/cpuset.cpus.effective"] = "\n"
		}},
		{name: "ambiguous mount", mutate: func(files fakeResourceFiles) {
			files["/proc/self/mountinfo"] = v2Mount +
				"36 25 0:31 / /other/cgroup rw - cgroup2 cgroup2 rw\n"
		}},
		{name: "oversized membership", mutate: func(files fakeResourceFiles) {
			files["/proc/self/cgroup"] = strings.Repeat("x", maxCgroupMembershipBytes+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := cloneResourceFiles(valid)
			test.mutate(files)
			_, _, err := effectiveLinuxResources(16*testGiB, 8, files.read)
			if !errors.Is(err, errLinuxResourceObservation) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestEffectiveLinuxResourcesRedactsReaderFailures(t *testing.T) {
	read := func(path string, _ int64) ([]byte, error) {
		return nil, fmt.Errorf("secret path %q: denied", path)
	}
	_, _, err := effectiveLinuxResources(16*testGiB, 8, read)
	if !errors.Is(err, errLinuxResourceObservation) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("reader failure was not redacted: %v", err)
	}
}

func TestParseCPUSet(t *testing.T) {
	tests := []struct {
		value string
		want  int
		ok    bool
	}{
		{value: "0", want: 1, ok: true},
		{value: "0-2,4,7-8", want: 6, ok: true},
		{value: "", ok: false},
		{value: "0-", ok: false},
		{value: "2-1", ok: false},
		{value: "0,0", ok: false},
		{value: "4096", ok: false},
	}
	for _, test := range tests {
		got, err := parseCPUSet([]byte(test.value))
		if (err == nil) != test.ok || got != test.want {
			t.Errorf("parseCPUSet(%q) = %d, %v", test.value, got, err)
		}
	}
}

func TestResolveCgroupPathSupportsNamespaceRoot(t *testing.T) {
	mount := cgroupMount{
		version: 2,
		root:    "/host/tenant",
		point:   "/sys/fs/cgroup",
	}
	current, ok := resolveCgroupPath(mount, "/")
	if !ok || current != "/sys/fs/cgroup" {
		t.Fatalf("resolved path = %q, %v", current, ok)
	}
}

func TestCgroupParsersEnforceStructuralBounds(t *testing.T) {
	var mounts strings.Builder
	for index := 0; index <= maxCgroupMounts; index++ {
		fmt.Fprintf(
			&mounts,
			"%d 25 0:%d / /sys/fs/cgroup/%d rw - cgroup2 cgroup2 rw\n",
			index+100, index+100, index,
		)
	}
	if _, err := parseCgroupMounts([]byte(mounts.String())); err == nil {
		t.Fatal("too many cgroup mounts accepted")
	}

	current := "/sys/fs/cgroup/" + strings.Repeat("a/", maxCgroupDepth+1) + "leaf"
	if err := walkCgroupAncestors(current, "/sys/fs/cgroup", func(string) error {
		return nil
	}); err == nil {
		t.Fatal("over-deep cgroup hierarchy accepted")
	}
}

func cloneResourceFiles(source fakeResourceFiles) fakeResourceFiles {
	clone := make(fakeResourceFiles, len(source))
	for path, value := range source {
		clone[path] = value
	}
	return clone
}
