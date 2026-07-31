package operatorconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadTerminalPolicy(t *testing.T) {
	policy, err := LoadTerminalPolicy(writePrivate(
		t, "terminal-policy.json", validTerminalPolicyJSON(), 0o600,
	))
	if err != nil {
		t.Fatal(err)
	}
	if policy.Workers != 2 || policy.MaxQueue != 8 ||
		policy.MaxConnections != 32 || policy.QuoteTTL != 30*time.Second ||
		policy.MaxQuotes != 128 || policy.MaxInvocations != 64 ||
		policy.MaxDeadline != 15*time.Minute ||
		policy.PreflightTimeout != 5*time.Second ||
		policy.PreflightTTL != 2*time.Minute ||
		policy.FailureTTL != 2*time.Second ||
		policy.RefreshInterval != 5*time.Second ||
		policy.PreflightWorkers != 4 {
		t.Fatalf("terminal policy=%#v", policy)
	}
	if policy.Admission.MaxConcurrent != policy.Workers ||
		policy.Admission.MaxQueue != policy.MaxQueue ||
		policy.Admission.Capacity.RAMBytes != 8<<30 ||
		policy.Admission.OwnerReserved.RAMBytes != 2<<30 ||
		policy.Admission.PerRequestMax.ExecutionTime != 5*time.Minute {
		t.Fatalf("admission policy=%#v", policy.Admission)
	}
}

func TestLoadTerminalPolicyRejectsInvalidAndAmbiguousValues(t *testing.T) {
	valid := validTerminalPolicyJSON()
	tests := []struct {
		name string
		data string
	}{
		{"version", strings.Replace(valid, `"version":1`, `"version":2`, 1)},
		{"workers", strings.Replace(valid, `"workers":2`, `"workers":0`, 1)},
		{"queue", strings.Replace(valid, `"maxQueue":8`, `"maxQueue":4097`, 1)},
		{"connections", strings.Replace(valid, `"maxConnections":32`, `"maxConnections":4097`, 1)},
		{"quote ttl", strings.Replace(valid, `"quoteTtlMillis":30000`, `"quoteTtlMillis":300001`, 1)},
		{"quotes", strings.Replace(valid, `"maxQuotes":128`, `"maxQuotes":65537`, 1)},
		{"deadline", strings.Replace(valid, `"maxDeadlineMillis":900000`, `"maxDeadlineMillis":3600001`, 1)},
		{"preflight timeout", strings.Replace(valid, `"timeoutMillis":5000`, `"timeoutMillis":30001`, 1)},
		{"failure ttl", strings.Replace(valid, `"failureTtlMillis":2000`, `"failureTtlMillis":120001`, 1)},
		{"refresh", strings.Replace(valid, `"refreshMillis":5000`, `"refreshMillis":249`, 1)},
		{"preflight workers", strings.Replace(valid, `"workers":4`, `"workers":17`, 1)},
		{"owner ram", strings.Replace(valid, `"ramBytes":2147483648`, `"ramBytes":0`, 1)},
		{"owner vram", strings.Replace(valid, `"vramBytes":1073741824`, `"vramBytes":0`, 1)},
		{"owner overflow", strings.Replace(valid, `"ramBytes":2147483648`, `"ramBytes":17179869184`, 1)},
		{"request overflow", strings.Replace(valid, `"ramBytes":4294967296`, `"ramBytes":17179869184`, 1)},
		{"execution", strings.Replace(valid, `"executionMillis":300000`, `"executionMillis":-1`, 1)},
		{"unknown", strings.Replace(valid, `"version":1`, `"version":1,"endpoint":"bad"`, 1)},
		{"duplicate", strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writePrivate(t, "invalid-policy.json", test.data, 0o600)
			if _, err := LoadTerminalPolicy(path); err == nil {
				t.Fatal("invalid terminal policy was accepted")
			}
		})
	}
}

func TestLoadTerminalPolicyRejectsInsecureAndOversizedFiles(t *testing.T) {
	insecure := writePrivate(
		t, "insecure-policy.json", validTerminalPolicyJSON(), 0o644,
	)
	if _, err := LoadTerminalPolicy(insecure); err == nil ||
		strings.Contains(err.Error(), insecure) {
		t.Fatal("insecure terminal policy was accepted")
	}
	if _, err := LoadTerminalPolicy(writePrivate(
		t, "oversized-policy.json",
		strings.Repeat(" ", int(MaxTerminalPolicyConfigBytes)+1), 0o600,
	)); err == nil {
		t.Fatal("oversized terminal policy was accepted")
	}
	target := writePrivate(
		t, "target-policy.json", validTerminalPolicyJSON(), 0o600,
	)
	link := filepath.Join(t.TempDir(), "linked-policy.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadTerminalPolicy(link); err == nil {
		t.Fatal("symlinked terminal policy was accepted")
	}
}

func validTerminalPolicyJSON() string {
	return `{
  "version":1,
  "workers":2,
  "maxQueue":8,
  "maxConnections":32,
  "quoteTtlMillis":30000,
  "maxQuotes":128,
  "maxInvocations":64,
  "maxDeadlineMillis":900000,
  "preflight":{
    "timeoutMillis":5000,
    "successTtlMillis":120000,
    "failureTtlMillis":2000,
    "refreshMillis":5000,
    "workers":4
  },
  "admission":{
    "capacity":{
      "ramBytes":8589934592,
      "vramBytes":4294967296,
      "kvCacheBytes":4294967296,
      "contextTokens":262144,
      "batchSize":64,
      "outputBytes":67108864,
      "executionMillis":900000
    },
    "ownerReserved":{
      "ramBytes":2147483648,
      "vramBytes":1073741824,
      "kvCacheBytes":1073741824,
      "contextTokens":32768,
      "batchSize":8,
      "outputBytes":8388608,
      "executionMillis":0
    },
    "perRequestMax":{
      "ramBytes":4294967296,
      "vramBytes":4294967296,
      "kvCacheBytes":2147483648,
      "contextTokens":32768,
      "batchSize":8,
      "outputBytes":8388608,
      "executionMillis":300000
    }
  }
}`
}
