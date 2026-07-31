// Package ollamabinding defines the deterministic, operator-owned Ollama
// model names and source-blob verification shared by inference and activation.
package ollamabinding

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const (
	MaxSlotIDBytes       = 48
	MaxRuntimeModelBytes = 128
	MaxShowFields        = 64
	MaxDetailsFields     = 32
	MaxModelfileBytes    = 64 << 10
	runtimeNamespace     = "tos-ai/"
)

func RuntimeModel(slotID string, digest string) (string, error) {
	if !ValidSlotID(slotID) || !ValidDigest(digest) {
		return "", errors.New("invalid Ollama binding identity")
	}
	prefix, _ := RuntimePrefix(slotID)
	result := prefix +
		strings.TrimPrefix(digest, "sha256:")
	if len(result) > MaxRuntimeModelBytes {
		return "", errors.New("Ollama runtime model name exceeds hard limit")
	}
	return result, nil
}

func RuntimePrefix(slotID string) (string, error) {
	if !ValidSlotID(slotID) {
		return "", errors.New("invalid Ollama activation slot")
	}
	return runtimeNamespace + slotID + ":", nil
}

func ValidRuntimeModel(value string, digest string) bool {
	if len(value) == 0 || len(value) > MaxRuntimeModelBytes ||
		!ValidDigest(digest) || !strings.HasPrefix(value, runtimeNamespace) {
		return false
	}
	remainder := strings.TrimPrefix(value, runtimeNamespace)
	separator := strings.IndexByte(remainder, ':')
	if separator <= 0 {
		return false
	}
	slotID := remainder[:separator]
	expected, err := RuntimeModel(slotID, digest)
	return err == nil && expected == value
}

func ParseRuntimeModel(slotID string, value string) (string, error) {
	if !ValidSlotID(slotID) || len(value) == 0 ||
		len(value) > MaxRuntimeModelBytes {
		return "", errors.New("invalid Ollama runtime model name")
	}
	prefix, _ := RuntimePrefix(slotID)
	if !strings.HasPrefix(value, prefix) {
		return "", errors.New("Ollama runtime model is outside its slot")
	}
	digest := "sha256:" + strings.TrimPrefix(value, prefix)
	if !ValidDigest(digest) {
		return "", errors.New("invalid Ollama runtime model digest")
	}
	expected, err := RuntimeModel(slotID, digest)
	if err != nil || expected != value {
		return "", errors.New("non-canonical Ollama runtime model name")
	}
	return digest, nil
}

func ValidSlotID(value string) bool {
	if len(value) == 0 || len(value) > MaxSlotIDBytes ||
		value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, current := range []byte(value) {
		if (current >= 'a' && current <= 'z') ||
			(current >= '0' && current <= '9') || current == '-' {
			continue
		}
		return false
	}
	return value[len(value)-1] != '-'
}

func ValidDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 ||
		!strings.HasPrefix(value, "sha256:") {
		return false
	}
	encoded := strings.TrimPrefix(value, "sha256:")
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == sha256.Size &&
		hex.EncodeToString(decoded) == encoded
}

// VerifyShowResponse requires the bounded /api/show response to describe a
// GGUF whose first Modelfile instruction references the exact approved blob.
// Paths and other response fields are neither returned nor exposed.
func VerifyShowResponse(data []byte, digest string) error {
	if len(data) == 0 || !ValidDigest(digest) {
		return errors.New("invalid Ollama model source response")
	}
	fields, err := decodeObject(
		data, MaxShowFields, map[string]struct{}{
			"modelfile": {}, "details": {},
		},
	)
	if err != nil {
		return errors.New("invalid Ollama model source response")
	}
	var modelfile string
	if err := json.Unmarshal(fields["modelfile"], &modelfile); err != nil ||
		len(modelfile) == 0 || len(modelfile) > MaxModelfileBytes {
		return errors.New("invalid Ollama Modelfile")
	}
	details, err := decodeObject(
		fields["details"], MaxDetailsFields,
		map[string]struct{}{"format": {}},
	)
	if err != nil {
		return errors.New("invalid Ollama model details")
	}
	var format string
	if err := json.Unmarshal(details["format"], &format); err != nil ||
		format != "gguf" {
		return errors.New("Ollama model is not GGUF")
	}
	source, err := firstSourceDigest(modelfile)
	if err != nil || source != digest {
		return errors.New("Ollama model source digest mismatch")
	}
	return nil
}

func decodeObject(
	data []byte,
	maxFields int,
	wanted map[string]struct{},
) (map[string]json.RawMessage, error) {
	if len(data) == 0 || maxFields <= 0 {
		return nil, errors.New("invalid JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("invalid JSON object")
	}
	seen := make(map[string]struct{}, min(maxFields, len(wanted)+8))
	result := make(map[string]json.RawMessage, len(wanted))
	count := 0
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok || len(key) == 0 || len(key) > 256 {
			return nil, errors.New("invalid JSON object key")
		}
		count++
		if count > maxFields {
			return nil, errors.New("JSON object field limit")
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("duplicate JSON object field")
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if _, keep := wanted[key]; keep {
			result[key] = append(json.RawMessage(nil), value...)
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return nil, errors.New("invalid JSON object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing JSON value")
	}
	for key := range wanted {
		if len(result[key]) == 0 {
			return nil, errors.New("missing JSON object field")
		}
	}
	return result, nil
}

func firstSourceDigest(modelfile string) (string, error) {
	for _, line := range strings.Split(modelfile, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.EqualFold(fields[0], "FROM") {
			return "", errors.New("invalid first Modelfile instruction")
		}
		source := strings.ReplaceAll(fields[1], "\\", "/")
		if index := strings.LastIndexByte(source, '/'); index >= 0 {
			source = source[index+1:]
		}
		switch {
		case strings.HasPrefix(source, "sha256-"):
			source = "sha256:" + strings.TrimPrefix(source, "sha256-")
		case strings.HasPrefix(source, "sha256:"):
		default:
			return "", errors.New("Modelfile source is not content addressed")
		}
		if !ValidDigest(source) {
			return "", errors.New("invalid Modelfile source digest")
		}
		return source, nil
	}
	return "", errors.New("missing Modelfile source")
}
