package ollamabinding

import (
	"fmt"
	"strings"
	"testing"
)

func TestRuntimeModelRoundTrip(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	model, err := RuntimeModel("primary-1", digest)
	if err != nil {
		t.Fatal(err)
	}
	if model != "tos-ai/primary-1:"+strings.Repeat("a", 64) {
		t.Fatalf("runtime model=%q", model)
	}
	recovered, err := ParseRuntimeModel("primary-1", model)
	if err != nil || recovered != digest {
		t.Fatalf("recovered digest=%q err=%v", recovered, err)
	}
	for _, invalid := range []string{
		"", "Primary", "primary_", "-primary", "primary-",
		strings.Repeat("a", MaxSlotIDBytes+1),
	} {
		if ValidSlotID(invalid) {
			t.Fatalf("invalid slot accepted: %q", invalid)
		}
	}
	if _, err := ParseRuntimeModel("other", model); err == nil {
		t.Fatal("cross-slot runtime model accepted")
	}
	if !ValidRuntimeModel(model, digest) ||
		ValidRuntimeModel(model, "sha256:"+strings.Repeat("b", 64)) ||
		ValidRuntimeModel(
			"other/primary-1:"+strings.Repeat("a", 64), digest,
		) {
		t.Fatal("runtime model/digest validation is not exact")
	}
}

func TestVerifyShowResponseRequiresExactGGUFSource(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	valid := fmt.Sprintf(
		`{"modelfile":"# generated\nFROM /models/blobs/sha256-%s\nTEMPLATE x",`+
			`"details":{"format":"gguf","family":"test"},"other":true}`,
		strings.TrimPrefix(digest, "sha256:"),
	)
	if err := VerifyShowResponse([]byte(valid), digest); err != nil {
		t.Fatal(err)
	}
	tests := []string{
		strings.Replace(valid, `"gguf"`, `"safetensors"`, 1),
		strings.Replace(valid, strings.Repeat("b", 64), strings.Repeat("c", 64), 1),
		strings.Replace(valid, `"modelfile":`, `"modelfile":"FROM bad","modelfile":`, 1),
		valid + `{"trailing":true}`,
		`{"modelfile":"TEMPLATE x\nFROM sha256-` + strings.Repeat("b", 64) +
			`","details":{"format":"gguf"}}`,
	}
	for index, value := range tests {
		if err := VerifyShowResponse([]byte(value), digest); err == nil {
			t.Fatalf("invalid response %d accepted", index)
		}
	}
}
