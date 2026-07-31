package operatorconfig

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/tosnetwork/tos-ai/pkg/modelapproval"
	"github.com/tosnetwork/tos-ai/pkg/modelmanager"
)

const (
	ModelTrustVersion          = 1
	MaxModelTrustConfigBytes   = int64(64 << 10)
	defaultVerifyTimeoutMillis = int64((2 * time.Minute) / time.Millisecond)
)

type modelTrustFileConfig struct {
	Version                 int                 `json:"version"`
	CacheDir                string              `json:"cacheDir"`
	Target                  string              `json:"target"`
	CurrentSecurityRevision uint64              `json:"currentSecurityRevision"`
	MaxModels               int                 `json:"maxModels"`
	MaxTotalBytes           uint64              `json:"maxTotalBytes"`
	VerifyTimeoutMillis     int64               `json:"verifyTimeoutMillis,omitempty"`
	Signers                 []modelSignerConfig `json:"signers"`
}

type modelSignerConfig struct {
	KeyID     string `json:"keyId"`
	PublicKey string `json:"publicKey"`
}

type ModelTrust struct {
	Manager             *modelmanager.Manager
	VerificationTimeout time.Duration
	CacheDir            string
}

// LoadModelTrust opens a signed model cache using only the bounded,
// administrator-owned trust configuration. Public keys are canonical standard
// base64 Ed25519 keys; private signing keys are never loaded by the worker.
func LoadModelTrust(path string) (ModelTrust, error) {
	data, err := readPrivateFile(path, MaxModelTrustConfigBytes, false)
	if err != nil {
		return ModelTrust{}, errors.New("load model trust configuration")
	}
	if err := validateJSON(data); err != nil {
		return ModelTrust{}, errors.New("invalid model trust configuration")
	}
	var config modelTrustFileConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return ModelTrust{}, errors.New("invalid model trust configuration")
	}
	if config.VerifyTimeoutMillis == 0 {
		config.VerifyTimeoutMillis = defaultVerifyTimeoutMillis
	}
	if config.Version != ModelTrustVersion ||
		!filepath.IsAbs(config.CacheDir) ||
		!validModelTrustIdentity(config.Target) ||
		config.CurrentSecurityRevision == 0 ||
		config.MaxModels <= 0 ||
		config.MaxModels > modelmanager.MaxModelsHard ||
		config.MaxTotalBytes == 0 ||
		config.MaxTotalBytes > modelmanager.MaxTotalBytesHard ||
		config.VerifyTimeoutMillis <= 0 ||
		config.VerifyTimeoutMillis >
			int64(modelapproval.MaxVerificationTimeoutHard/time.Millisecond) ||
		len(config.Signers) == 0 ||
		len(config.Signers) > modelmanager.MaxSignersHard {
		return ModelTrust{}, errors.New("model trust configuration exceeds hard limits")
	}
	signers := make(map[string]ed25519.PublicKey, len(config.Signers))
	for _, signer := range config.Signers {
		if !validModelTrustIdentity(signer.KeyID) {
			return ModelTrust{}, errors.New("invalid model trust signer")
		}
		if _, duplicate := signers[signer.KeyID]; duplicate {
			return ModelTrust{}, errors.New("duplicate model trust signer")
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(signer.PublicKey)
		if err != nil || len(decoded) != ed25519.PublicKeySize ||
			base64.StdEncoding.EncodeToString(decoded) != signer.PublicKey {
			return ModelTrust{}, errors.New("invalid model trust public key")
		}
		signers[signer.KeyID] = ed25519.PublicKey(
			append([]byte(nil), decoded...),
		)
	}
	manager, err := modelmanager.New(modelmanager.Config{
		RootDir: config.CacheDir, Target: config.Target,
		CurrentSecurityRevision: config.CurrentSecurityRevision,
		MaxModels:               config.MaxModels, MaxTotalBytes: config.MaxTotalBytes,
		Signers: signers,
	})
	if err != nil {
		return ModelTrust{}, errors.New("open approved model cache")
	}
	return ModelTrust{
		Manager:             manager,
		VerificationTimeout: time.Duration(config.VerifyTimeoutMillis) * time.Millisecond,
		CacheDir:            filepath.Clean(config.CacheDir),
	}, nil
}

func validModelTrustIdentity(value string) bool {
	return value != "" && len(value) <= 256 &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}
