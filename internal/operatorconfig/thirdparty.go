package operatorconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/tosnetwork/tos-ai/internal/runtimehttp"
)

// This file loads the bounded, administrator-owned allowlist of third-party
// provider bindings a tos-ai worker's ThirdPartyExecutionService is
// permitted to dial. It exists to enforce the same "task payloads cannot
// select the endpoint" invariant this package's other adapters already
// enforce (see the package doc comment): an inbound
// tos.edge.v1.ThirdPartyExecutionService request names a (transport,
// endpoint_ref, capability_id) triple, but it can only ever *reference* an
// entry an operator already approved here -- it can never introduce a new
// one. See atos-spec docs/THIRD_PARTY_EXECUTION_PLANE.md for the
// cross-repository design this implements.

const (
	ThirdPartyBindingsVersion         = 1
	MaxThirdPartyBindingsConfigBytes  = int64(1 << 20)
	MaxThirdPartyBindings             = 256
	defaultThirdPartyTimeoutMillis    = int64((30 * time.Second) / time.Millisecond)
	defaultThirdPartyMaxRequestBytes  = uint64(1 << 20)
	defaultThirdPartyMaxResponseBytes = uint64(1 << 20)
	maxThirdPartyTimeoutMillis        = int64((5 * time.Minute) / time.Millisecond)
	maxThirdPartyBodyBytesHard        = uint64(16 << 20)
)

var validThirdPartyTransports = map[string]bool{"http": true, "mcp": true, "a2a": true}

type thirdPartyBindingsFileConfig struct {
	Version  int                       `json:"version"`
	Bindings []thirdPartyBindingConfig `json:"bindings"`
}

type thirdPartyBindingConfig struct {
	Transport             string   `json:"transport"`
	EndpointRef           string   `json:"endpointRef"`
	CapabilityID          string   `json:"capabilityId"`
	CapabilityVersion     string   `json:"capabilityVersion,omitempty"`
	TimeoutMillis         int64    `json:"timeoutMillis,omitempty"`
	MaxRequestBytes       uint64   `json:"maxRequestBytes,omitempty"`
	MaxResponseBytes      uint64   `json:"maxResponseBytes,omitempty"`
	AllowedPlaintextCIDRs []string `json:"allowedPlaintextCidrs,omitempty"`
}

// ThirdPartyBinding is one operator-approved (transport, endpoint_ref,
// capability_id[, capability_version]) entry -- the durable trust unit an
// inbound ThirdPartyExecutionService request is checked against.
type ThirdPartyBinding struct {
	Transport    string
	EndpointRef  string
	CapabilityID string
	// CapabilityVersion "" or "*" matches any version -- health/certification
	// probing in particular may run before a specific version is pinned.
	CapabilityVersion     string
	Timeout               time.Duration
	MaxRequestBytes       uint64
	MaxResponseBytes      uint64
	AllowedPlaintextCIDRs []string
}

// ThirdPartyBindings is the full operator-approved allowlist for one
// worker.
type ThirdPartyBindings struct {
	entries []ThirdPartyBinding
}

// Allowed reports whether (transport, endpointRef, capabilityID,
// capabilityVersion) matches an operator-approved entry, and returns it.
// This is the sole authorization check ThirdPartyExecutionService's RPCs
// must pass before any outbound dial -- see this file's package-level
// comment.
func (b ThirdPartyBindings) Allowed(transport, endpointRef, capabilityID, capabilityVersion string) (ThirdPartyBinding, bool) {
	for _, entry := range b.entries {
		if entry.Transport != transport || entry.EndpointRef != endpointRef || entry.CapabilityID != capabilityID {
			continue
		}
		if entry.CapabilityVersion != "" && entry.CapabilityVersion != "*" && entry.CapabilityVersion != capabilityVersion {
			continue
		}
		return entry, true
	}
	return ThirdPartyBinding{}, false
}

func (b ThirdPartyBindings) Len() int { return len(b.entries) }

// Entries returns a defensive copy of every approved binding, for building
// one bound transport per entry at startup.
func (b ThirdPartyBindings) Entries() []ThirdPartyBinding {
	return append([]ThirdPartyBinding(nil), b.entries...)
}

// LoadThirdPartyBindings reads a private regular JSON file and constructs
// only the third-party bindings explicitly approved in that file. An empty/
// absent configuration is valid (Len() == 0): a worker that does not serve
// any third-party capability simply never approves any binding, and every
// ThirdPartyExecutionService RPC then fails closed.
func LoadThirdPartyBindings(path string) (ThirdPartyBindings, error) {
	data, err := readPrivateFile(path, MaxThirdPartyBindingsConfigBytes, false)
	if err != nil {
		return ThirdPartyBindings{}, errors.New("load third-party bindings configuration")
	}
	if err := validateJSON(data); err != nil {
		return ThirdPartyBindings{}, errors.New("invalid third-party bindings configuration")
	}
	var config thirdPartyBindingsFileConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return ThirdPartyBindings{}, errors.New("invalid third-party bindings configuration")
	}
	if config.Version != ThirdPartyBindingsVersion || len(config.Bindings) > MaxThirdPartyBindings {
		return ThirdPartyBindings{}, errors.New("third-party bindings configuration exceeds hard limits")
	}
	entries := make([]ThirdPartyBinding, 0, len(config.Bindings))
	seen := make(map[string]struct{}, len(config.Bindings))
	for _, value := range config.Bindings {
		if !validThirdPartyTransports[value.Transport] {
			return ThirdPartyBindings{}, errors.New("third-party binding has an invalid transport")
		}
		if value.EndpointRef == "" || len(value.EndpointRef) > runtimehttp.MaxEndpointBytes {
			return ThirdPartyBindings{}, errors.New("third-party binding has an invalid endpoint_ref")
		}
		if value.CapabilityID == "" {
			return ThirdPartyBindings{}, errors.New("third-party binding requires a capability_id")
		}
		if len(value.AllowedPlaintextCIDRs) > runtimehttp.MaxPlaintextCIDRs {
			return ThirdPartyBindings{}, errors.New("third-party binding exceeds the plaintext CIDR limit")
		}
		key := value.Transport + "\x00" + value.EndpointRef + "\x00" + value.CapabilityID + "\x00" + value.CapabilityVersion
		if _, duplicate := seen[key]; duplicate {
			return ThirdPartyBindings{}, errors.New("duplicate third-party binding")
		}
		seen[key] = struct{}{}

		timeoutMillis := value.TimeoutMillis
		if timeoutMillis == 0 {
			timeoutMillis = defaultThirdPartyTimeoutMillis
		}
		if timeoutMillis <= 0 || timeoutMillis > maxThirdPartyTimeoutMillis {
			return ThirdPartyBindings{}, errors.New("third-party binding has an invalid timeout")
		}
		maxRequestBytes := value.MaxRequestBytes
		if maxRequestBytes == 0 {
			maxRequestBytes = defaultThirdPartyMaxRequestBytes
		}
		maxResponseBytes := value.MaxResponseBytes
		if maxResponseBytes == 0 {
			maxResponseBytes = defaultThirdPartyMaxResponseBytes
		}
		if maxRequestBytes > maxThirdPartyBodyBytesHard || maxResponseBytes > maxThirdPartyBodyBytesHard {
			return ThirdPartyBindings{}, errors.New("third-party binding exceeds the body byte hard limit")
		}
		entries = append(entries, ThirdPartyBinding{
			Transport: value.Transport, EndpointRef: value.EndpointRef,
			CapabilityID: value.CapabilityID, CapabilityVersion: value.CapabilityVersion,
			Timeout:               time.Duration(timeoutMillis) * time.Millisecond,
			MaxRequestBytes:       maxRequestBytes,
			MaxResponseBytes:      maxResponseBytes,
			AllowedPlaintextCIDRs: append([]string(nil), value.AllowedPlaintextCIDRs...),
		})
	}
	return ThirdPartyBindings{entries: entries}, nil
}
