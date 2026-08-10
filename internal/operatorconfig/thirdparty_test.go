package operatorconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func writeThirdPartyConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "thirdparty.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadThirdPartyBindings_Valid(t *testing.T) {
	path := writeThirdPartyConfig(t, `{
		"version": 1,
		"bindings": [
			{"transport": "http", "endpointRef": "https://provider.example.com/invoke", "capabilityId": "cap_1"},
			{"transport": "mcp", "endpointRef": "https://provider.example.com/mcp#analyze", "capabilityId": "cap_2", "capabilityVersion": "1.0.0"}
		]
	}`)
	bindings, err := LoadThirdPartyBindings(path)
	if err != nil {
		t.Fatalf("LoadThirdPartyBindings: %v", err)
	}
	if bindings.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", bindings.Len())
	}
	if _, ok := bindings.Allowed("http", "https://provider.example.com/invoke", "cap_1", ""); !ok {
		t.Fatal("expected the http binding to be allowed")
	}
	if _, ok := bindings.Allowed("mcp", "https://provider.example.com/mcp#analyze", "cap_2", "2.0.0"); ok {
		t.Fatal("expected a version-pinned binding to reject a different capability_version")
	}
	if _, ok := bindings.Allowed("mcp", "https://provider.example.com/mcp#analyze", "cap_2", "1.0.0"); !ok {
		t.Fatal("expected the exact quoted capability_version to be allowed")
	}
}

func TestLoadThirdPartyBindings_RejectsUnknownTransport(t *testing.T) {
	path := writeThirdPartyConfig(t, `{"version":1,"bindings":[{"transport":"ftp","endpointRef":"https://x.example.com","capabilityId":"cap_1"}]}`)
	if _, err := LoadThirdPartyBindings(path); err == nil {
		t.Fatal("expected an unknown transport to be rejected")
	}
}

func TestLoadThirdPartyBindings_RejectsDuplicateEntries(t *testing.T) {
	path := writeThirdPartyConfig(t, `{"version":1,"bindings":[
		{"transport":"http","endpointRef":"https://x.example.com","capabilityId":"cap_1"},
		{"transport":"http","endpointRef":"https://x.example.com","capabilityId":"cap_1"}
	]}`)
	if _, err := LoadThirdPartyBindings(path); err == nil {
		t.Fatal("expected a duplicate binding to be rejected")
	}
}

func TestLoadThirdPartyBindings_RejectsUnknownFields(t *testing.T) {
	path := writeThirdPartyConfig(t, `{"version":1,"bindings":[{"transport":"http","endpointRef":"https://x.example.com","capabilityId":"cap_1","unknownField":true}]}`)
	if _, err := LoadThirdPartyBindings(path); err == nil {
		t.Fatal("expected an unknown field to be rejected")
	}
}

func TestLoadThirdPartyBindings_EmptyIsValid(t *testing.T) {
	path := writeThirdPartyConfig(t, `{"version":1,"bindings":[]}`)
	bindings, err := LoadThirdPartyBindings(path)
	if err != nil {
		t.Fatalf("LoadThirdPartyBindings: %v", err)
	}
	if bindings.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", bindings.Len())
	}
	if _, ok := bindings.Allowed("http", "https://x.example.com", "cap_1", ""); ok {
		t.Fatal("an empty allowlist must reject everything")
	}
}
