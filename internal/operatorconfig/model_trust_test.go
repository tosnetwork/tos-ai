package operatorconfig

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
	"github.com/tosnetwork/tos-ai/pkg/update"
)

func modelTrustJSON(
	cacheDir string,
	keyID string,
	publicKey ed25519.PublicKey,
	revision uint64,
) string {
	return fmt.Sprintf(`{
		"version":1,
		"cacheDir":%q,
		"target":"linux/amd64/cuda",
		"currentSecurityRevision":%d,
		"maxModels":8,
		"maxTotalBytes":1048576,
		"signers":[{"keyId":%q,"publicKey":%q}]
	}`, cacheDir, revision, keyID, base64.StdEncoding.EncodeToString(publicKey))
}

func TestLoadModelTrustRecoversSignedApprovedArtifact(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(t.TempDir(), "models")
	path := writePrivate(
		t, "model-trust.json",
		modelTrustJSON(cacheDir, "models", publicKey, 7), 0o600,
	)
	trust, err := LoadModelTrust(path)
	if err != nil {
		t.Fatal(err)
	}
	if trust.VerificationTimeout !=
		time.Duration(defaultVerifyTimeoutMillis)*time.Millisecond {
		t.Fatalf("verification timeout=%v", trust.VerificationTimeout)
	}
	data := []byte("approved-model")
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	now := time.Now()
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: "approved.gguf",
		Digest: digest, SizeBytes: uint64(len(data)),
		Target: "linux/amd64/cuda", SecurityRevision: 7,
		IssuedAt:  now.Add(-time.Minute).UnixMilli(),
		ExpiresAt: now.Add(time.Hour).UnixMilli(), KeyID: "models",
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := trust.Manager.Import(
		context.Background(), manifest, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	recovered, err := LoadModelTrust(path)
	if err != nil {
		t.Fatal(err)
	}
	if model := recovered.Manager.Status(digest); model.State !=
		modelmanager.StateReady || model.Digest != digest {
		t.Fatalf("recovered approved model=%#v", model)
	}
	info, err := os.Lstat(cacheDir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("model cache permissions info=%v err=%v", info, err)
	}
}

func TestLoadModelTrustRejectsConfigAndKeyBounds(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(t.TempDir(), "models")
	valid := modelTrustJSON(cacheDir, "models", publicKey, 7)
	tests := []struct {
		name   string
		config string
		mode   os.FileMode
	}{
		{
			name: "unknown",
			config: strings.Replace(
				valid, `"version":1`, `"version":1,"unknown":true`, 1,
			),
			mode: 0o600,
		},
		{
			name: "duplicate",
			config: strings.Replace(
				valid, `"version":1`, `"version":1,"version":1`, 1,
			),
			mode: 0o600,
		},
		{
			name:   "relative cache",
			config: strings.Replace(valid, cacheDir, "relative", 1),
			mode:   0o600,
		},
		{
			name: "zero revision",
			config: strings.Replace(
				valid, `"currentSecurityRevision":7`,
				`"currentSecurityRevision":0`, 1,
			),
			mode: 0o600,
		},
		{
			name: "noncanonical key",
			config: strings.Replace(
				valid, base64.StdEncoding.EncodeToString(publicKey),
				base64.RawStdEncoding.EncodeToString(publicKey), 1,
			),
			mode: 0o600,
		},
		{
			name: "duplicate signer",
			config: strings.Replace(
				valid,
				`"signers":[`,
				`"signers":[{"keyId":"models","publicKey":"`+
					base64.StdEncoding.EncodeToString(publicKey)+`"},`,
				1,
			),
			mode: 0o600,
		},
		{name: "public file", config: valid, mode: 0o644},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writePrivate(
				t, "model-trust.json", test.config, test.mode,
			)
			if _, err := LoadModelTrust(path); err == nil {
				t.Fatal("invalid model trust configuration accepted")
			}
		})
	}
	oversized := strings.Repeat(" ", int(MaxModelTrustConfigBytes)+1)
	if _, err := LoadModelTrust(writePrivate(
		t, "oversized.json", oversized, 0o600,
	)); err == nil {
		t.Fatal("oversized model trust configuration accepted")
	}
}

func TestLoadModelTrustRejectsSignerAndRevisionRollback(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(t.TempDir(), "models")
	path := writePrivate(
		t, "trust.json",
		modelTrustJSON(cacheDir, "models", publicKey, 5), 0o600,
	)
	trust, err := LoadModelTrust(path)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("approved")
	sum := sha256.Sum256(data)
	now := time.Now()
	manifest, err := update.Sign(update.Manifest{
		Version: update.ManifestVersion, Artifact: "approved.gguf",
		Digest:    "sha256:" + hex.EncodeToString(sum[:]),
		SizeBytes: uint64(len(data)), Target: "linux/amd64/cuda",
		SecurityRevision: 5, IssuedAt: now.Add(-time.Minute).UnixMilli(),
		ExpiresAt: now.Add(time.Hour).UnixMilli(), KeyID: "models",
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := trust.Manager.Import(
		context.Background(), manifest, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	_, otherPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongSigner := writePrivate(
		t, "wrong.json",
		modelTrustJSON(
			cacheDir, "models", otherPrivate.Public().(ed25519.PublicKey), 5,
		), 0o600,
	)
	if _, err := LoadModelTrust(wrongSigner); err == nil ||
		strings.Contains(err.Error(), cacheDir) {
		t.Fatalf("wrong signer recovery error=%v", err)
	}
	rollback := writePrivate(
		t, "revision.json",
		modelTrustJSON(cacheDir, "models", publicKey, 6), 0o600,
	)
	if _, err := LoadModelTrust(rollback); err == nil {
		t.Fatal("recovered lower security revision accepted")
	}
}
