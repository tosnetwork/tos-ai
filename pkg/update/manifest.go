// Package update verifies content-addressed release manifests. Download and
// fleet rollout remain deployment layers; pkg/softwareupdate owns the local
// staged active/known-good slot state machine.
package update

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	ManifestVersion  = 1
	MaxArtifactBytes = 64 << 30
)

type Manifest struct {
	Version          uint8  `json:"version"`
	Artifact         string `json:"artifact"`
	Digest           string `json:"digest"`
	SizeBytes        uint64 `json:"sizeBytes"`
	Target           string `json:"target"`
	SecurityRevision uint64 `json:"securityRevision"`
	IssuedAt         int64  `json:"issuedAt"`
	ExpiresAt        int64  `json:"expiresAt"`
	KeyID            string `json:"keyId"`
	Signature        string `json:"signature"`
}

func Sign(manifest Manifest, privateKey ed25519.PrivateKey) (Manifest, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return Manifest{}, errors.New("invalid Ed25519 private key")
	}
	if err := manifest.validateStructure(); err != nil {
		return Manifest{}, err
	}
	manifest.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, manifest.signingMessage()))
	return manifest, nil
}

func (m Manifest) Verify(publicKey ed25519.PublicKey, target string, currentSecurityRevision uint64, now time.Time) error {
	if err := m.VerifyInstalled(publicKey, target, currentSecurityRevision); err != nil {
		return err
	}
	if time.UnixMilli(m.IssuedAt).After(now.Add(2*time.Minute)) || !time.UnixMilli(m.ExpiresAt).After(now) {
		return errors.New("manifest is outside its validity window")
	}
	return nil
}

// VerifyInstalled revalidates the authenticity and security policy of an
// artifact that was accepted while its manifest was valid. Expiry gates new
// imports; it does not make already-installed content invalid on restart.
func (m Manifest) VerifyInstalled(publicKey ed25519.PublicKey, target string, currentSecurityRevision uint64) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("invalid Ed25519 public key")
	}
	if err := m.validateStructure(); err != nil {
		return err
	}
	if m.Target != target {
		return errors.New("artifact target mismatch")
	}
	if m.SecurityRevision < currentSecurityRevision {
		return errors.New("security revision rollback rejected")
	}
	signature, err := base64.RawURLEncoding.DecodeString(m.Signature)
	if err != nil || !ed25519.Verify(publicKey, m.signingMessage(), signature) {
		return errors.New("manifest signature verification failed")
	}
	return nil
}

func (m Manifest) VerifyArtifact(reader io.Reader) error {
	if err := m.validateStructure(); err != nil {
		return err
	}
	if m.SizeBytes > MaxArtifactBytes {
		return errors.New("artifact exceeds hard size limit")
	}
	hasher := sha256.New()
	written, err := io.Copy(hasher, io.LimitReader(reader, int64(m.SizeBytes)+1))
	if err != nil {
		return err
	}
	if written != int64(m.SizeBytes) {
		return errors.New("artifact size mismatch")
	}
	digest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if digest != m.Digest {
		return errors.New("artifact digest mismatch")
	}
	return nil
}

func (m Manifest) validateStructure() error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	if len(m.Artifact) == 0 || len(m.Artifact) > 512 || len(m.Target) == 0 ||
		len(m.Target) > 256 || len(m.KeyID) == 0 || len(m.KeyID) > 512 {
		return errors.New("invalid manifest identity fields")
	}
	if !strings.HasPrefix(m.Digest, "sha256:") || len(m.Digest) != 71 {
		return errors.New("invalid artifact digest")
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(m.Digest, "sha256:")); err != nil {
		return errors.New("invalid artifact digest")
	}
	if m.SizeBytes == 0 || m.SizeBytes > MaxArtifactBytes || m.ExpiresAt <= m.IssuedAt {
		return errors.New("invalid artifact size or validity interval")
	}
	return nil
}

func (m Manifest) signingMessage() []byte {
	var output bytes.Buffer
	output.WriteString("TOS-AI-UPDATE-MANIFEST")
	output.WriteByte(0)
	output.WriteByte(m.Version)
	writeString(&output, m.Artifact)
	writeString(&output, m.Digest)
	_ = binary.Write(&output, binary.BigEndian, m.SizeBytes)
	writeString(&output, m.Target)
	_ = binary.Write(&output, binary.BigEndian, m.SecurityRevision)
	_ = binary.Write(&output, binary.BigEndian, m.IssuedAt)
	_ = binary.Write(&output, binary.BigEndian, m.ExpiresAt)
	writeString(&output, m.KeyID)
	return output.Bytes()
}

func writeString(output *bytes.Buffer, value string) {
	_ = binary.Write(output, binary.BigEndian, uint32(len(value)))
	output.WriteString(value)
}
