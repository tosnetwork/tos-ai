package operatorconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/ard"
	"github.com/tosnetwork/tos-protocol/pkg/authorization"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
	"github.com/tosnetwork/tos-protocol/pkg/protocol"
	"github.com/tosnetwork/tos-protocol/pkg/toschain"
)

const (
	EdgeGatewayConfigVersion  = 1
	MaxEdgeGatewayConfigBytes = int64(64 << 10)
	maxDeploymentDocument     = int64(2 << 20)
	maxGatewayDuration        = uint64((24 * time.Hour) / time.Millisecond)
)

// EdgeGatewayConfig is the complete fail-closed production composition for
// the public tos-ai-edge process. It contains no private key material: signing
// remains behind the named private Unix socket.
type EdgeGatewayConfig struct {
	Version                       int    `json:"version"`
	ListenAddress                 string `json:"listenAddress"`
	DescriptorFile                string `json:"descriptorFile"`
	CatalogFile                   string `json:"catalogFile"`
	ManifestEnvelopeFile          string `json:"manifestEnvelopeFile"`
	ChainConfigFile               string `json:"chainConfigFile"`
	WorkerSocket                  string `json:"workerSocket"`
	RequestJournalFile            string `json:"requestJournalFile"`
	ReceiptSignerSocket           string `json:"receiptSignerSocket"`
	ReceiptSignerKeyID            string `json:"receiptSignerKeyId"`
	ReceiptSignerPublicKey        string `json:"receiptSignerPublicKey"`
	RequiredDelegationScope       string `json:"requiredDelegationScope"`
	MinimumMasterSeqno            uint64 `json:"minimumMasterSeqno,omitempty"`
	StartupTimeoutMillis          uint64 `json:"startupTimeoutMillis,omitempty"`
	CleanupIntervalMillis         uint64 `json:"cleanupIntervalMillis,omitempty"`
	PaidActionRetentionMillis     uint64 `json:"paidActionRetentionMillis,omitempty"`
	ReceiptLifetimeMillis         uint64 `json:"receiptLifetimeMillis,omitempty"`
	PaidActionMaxConcurrent       int    `json:"paidActionMaxConcurrent,omitempty"`
	PaymentReconcileMillis        uint64 `json:"paymentReconcileMillis,omitempty"`
	PaymentReconcileMaxMillis     uint64 `json:"paymentReconcileMaxMillis,omitempty"`
	PaymentReconcileTimeoutMillis uint64 `json:"paymentReconcileTimeoutMillis,omitempty"`
	PaymentReconcileBatch         int    `json:"paymentReconcileBatch,omitempty"`
}

// EdgeGatewayDocuments contains validated, defensively-owned deployment
// documents. Their cryptographic and chain bindings are repeated at request
// admission; loading them never makes discovery data authoritative.
type EdgeGatewayDocuments struct {
	Descriptor       protocol.ServiceDescriptor
	Catalog          ard.Catalog
	ManifestEnvelope identity.Envelope
	Chain            toschain.StartupConfig
}

func LoadEdgeGatewayConfig(path string) (EdgeGatewayConfig, error) {
	data, err := readPrivateFile(path, MaxEdgeGatewayConfigBytes, false)
	if err != nil {
		return EdgeGatewayConfig{}, err
	}
	if err := validateJSON(data); err != nil {
		return EdgeGatewayConfig{}, errors.New("invalid AI Edge gateway JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config EdgeGatewayConfig
	if err := decoder.Decode(&config); err != nil {
		return EdgeGatewayConfig{}, errors.New("invalid AI Edge gateway configuration")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return EdgeGatewayConfig{}, errors.New("multiple AI Edge gateway JSON values")
	}
	if err := config.validate(); err != nil {
		return EdgeGatewayConfig{}, err
	}
	return config, nil
}

func (config EdgeGatewayConfig) validate() error {
	if config.Version != EdgeGatewayConfigVersion ||
		len(config.ListenAddress) > 512 || strings.ContainsRune(config.ListenAddress, '\x00') {
		return errors.New("invalid AI Edge gateway version or listen address")
	}
	host, port, err := net.SplitHostPort(config.ListenAddress)
	ip := net.ParseIP(host)
	if err != nil || port == "" || strings.ContainsRune(host, '\x00') ||
		ip == nil || !ip.IsLoopback() {
		return errors.New("invalid AI Edge gateway listen address")
	}
	for _, path := range []string{
		config.DescriptorFile, config.CatalogFile, config.ManifestEnvelopeFile,
		config.ChainConfigFile, config.WorkerSocket, config.RequestJournalFile,
		config.ReceiptSignerSocket,
	} {
		if path == "" || len(path) > 4096 || !filepath.IsAbs(path) ||
			strings.ContainsRune(path, '\x00') {
			return errors.New("AI Edge gateway paths must be absolute and bounded")
		}
	}
	if config.ReceiptSignerKeyID == "" || len(config.ReceiptSignerKeyID) > 512 ||
		strings.ContainsRune(config.ReceiptSignerKeyID, '\x00') ||
		config.ReceiptSignerPublicKey == "" || len(config.ReceiptSignerPublicKey) > 512 ||
		strings.ContainsRune(config.ReceiptSignerPublicKey, '\x00') ||
		config.RequiredDelegationScope == "" || len(config.RequiredDelegationScope) > 256 ||
		strings.ContainsRune(config.RequiredDelegationScope, '\x00') {
		return errors.New("invalid AI Edge gateway signer or delegation policy")
	}
	for _, value := range []uint64{
		config.StartupTimeoutMillis, config.CleanupIntervalMillis,
		config.PaidActionRetentionMillis, config.ReceiptLifetimeMillis,
		config.PaymentReconcileMillis, config.PaymentReconcileMaxMillis,
		config.PaymentReconcileTimeoutMillis,
	} {
		if value > maxGatewayDuration {
			return errors.New("AI Edge gateway duration is outside bounds")
		}
	}
	if config.PaidActionMaxConcurrent < 0 || config.PaidActionMaxConcurrent > 128 ||
		config.PaymentReconcileBatch < 0 || config.PaymentReconcileBatch > 1024 {
		return errors.New("AI Edge gateway concurrency or batch is outside bounds")
	}
	return nil
}

func (config EdgeGatewayConfig) LoadDocuments(now time.Time) (EdgeGatewayDocuments, error) {
	if now.IsZero() {
		return EdgeGatewayDocuments{}, errors.New("AI Edge deployment validation time is required")
	}
	descriptorData, err := readPrivateFile(config.DescriptorFile, 256<<10, false)
	if err != nil {
		return EdgeGatewayDocuments{}, errors.New("load AI Edge descriptor")
	}
	descriptor, err := protocol.DecodeServiceDescriptorJSON(descriptorData, now.UTC())
	if err != nil {
		return EdgeGatewayDocuments{}, err
	}
	catalogData, err := readPrivateFile(config.CatalogFile, maxDeploymentDocument, false)
	if err != nil {
		return EdgeGatewayDocuments{}, errors.New("load AI Edge ARD catalog")
	}
	catalog, err := ard.DecodeCatalog(bytes.NewReader(catalogData), ard.DefaultLimits())
	if err != nil {
		return EdgeGatewayDocuments{}, err
	}
	manifestData, err := readPrivateFile(config.ManifestEnvelopeFile, maxDeploymentDocument, false)
	if err != nil {
		return EdgeGatewayDocuments{}, errors.New("load AI Edge manifest envelope")
	}
	manifest, err := identity.DecodeEnvelopeJSON(manifestData, protocol.ServiceManifestDomain)
	if err != nil {
		return EdgeGatewayDocuments{}, err
	}
	chainData, err := readPrivateFile(config.ChainConfigFile, toschain.MaxStartupConfigBytes, false)
	if err != nil {
		return EdgeGatewayDocuments{}, errors.New("load AI Edge TOS chain configuration")
	}
	chain, err := toschain.DecodeStartupConfigJSON(chainData)
	if err != nil {
		return EdgeGatewayDocuments{}, err
	}
	if chain.Network != descriptor.Network {
		return EdgeGatewayDocuments{}, errors.New("TOS chain network does not match AI Edge descriptor")
	}
	return EdgeGatewayDocuments{
		Descriptor: descriptor, Catalog: catalog,
		ManifestEnvelope: manifest, Chain: chain,
	}, nil
}

func (config EdgeGatewayConfig) Reference(descriptor protocol.ServiceDescriptor) authorization.Reference {
	return authorization.Reference{
		Network: descriptor.Network, Address: descriptor.Controller,
		ServiceID: descriptor.ServiceID, MinimumMasterSeqno: config.MinimumMasterSeqno,
	}
}

func (config EdgeGatewayConfig) CoreConfig() edge.CoreConfig {
	result := edge.DefaultCoreConfig(config.RequestJournalFile)
	if config.CleanupIntervalMillis != 0 {
		result.CleanupInterval = time.Duration(config.CleanupIntervalMillis) * time.Millisecond
	}
	if config.PaymentReconcileMillis != 0 {
		result.PaymentReconciliationInterval = time.Duration(config.PaymentReconcileMillis) * time.Millisecond
	}
	if config.PaymentReconcileMaxMillis != 0 {
		result.PaymentReconciliationMaxInterval = time.Duration(config.PaymentReconcileMaxMillis) * time.Millisecond
	}
	if config.PaymentReconcileTimeoutMillis != 0 {
		result.PaymentReconciliationTimeout = time.Duration(config.PaymentReconcileTimeoutMillis) * time.Millisecond
	}
	if config.PaymentReconcileBatch != 0 {
		result.PaymentReconciliationBatch = config.PaymentReconcileBatch
	}
	return result
}

func configuredMillis(value uint64, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}

func (config EdgeGatewayConfig) StartupTimeout() time.Duration {
	return configuredMillis(config.StartupTimeoutMillis, 10*time.Second)
}

func (config EdgeGatewayConfig) PaidActionRetention() time.Duration {
	return configuredMillis(config.PaidActionRetentionMillis, 24*time.Hour)
}

func (config EdgeGatewayConfig) ReceiptLifetime() time.Duration {
	return configuredMillis(config.ReceiptLifetimeMillis, 5*time.Minute)
}
