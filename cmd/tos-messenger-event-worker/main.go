// Command tos-messenger-event-worker composes the private Messenger A2A/MCP
// consumers, finalized execution Gate, bounded software-work runner, durable
// result outbox, and daemon-owned result publisher into one operator process.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/tosnetwork/tos-ai/internal/unixserver"
	"github.com/tosnetwork/tos-ai/pkg/a2aadapter"
	"github.com/tosnetwork/tos-ai/pkg/artifacthttp"
	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/executor"
	"github.com/tosnetwork/tos-ai/pkg/executor/containerdbackend"
	"github.com/tosnetwork/tos-ai/pkg/mcpadapter"
	"github.com/tosnetwork/tos-ai/pkg/messengereventbridge"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	"github.com/tosnetwork/tos-messenger/pkg/localapi"
	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
	"github.com/tosnetwork/tos-service-protocol/pkg/executiongate"
	"github.com/tosnetwork/tos-service-protocol/pkg/nativecore"
	"github.com/tosnetwork/tos-service-protocol/pkg/toschain"
)

const (
	configSchema   = "tos.service.messenger-event-worker.v1"
	maxConfigBytes = 128 << 10
)

type config struct {
	Schema                      string                             `json:"schema"`
	StateRoot                   string                             `json:"state_root"`
	A2ASocket                   string                             `json:"a2a_socket"`
	MCPSocket                   string                             `json:"mcp_socket"`
	ArtifactSocket              string                             `json:"artifact_socket"`
	ArtifactHTTPSOrigin         string                             `json:"artifact_https_origin"`
	MessengerRuntimeSocket      string                             `json:"messenger_runtime_socket"`
	MessengerCallTimeoutSeconds uint32                             `json:"messenger_call_timeout_seconds"`
	PublishIntervalMilliseconds uint32                             `json:"publish_interval_milliseconds"`
	ResultLifetimeSeconds       uint32                             `json:"result_lifetime_seconds"`
	AllowedSenders              []string                           `json:"allowed_senders"`
	AllowedConversations        []string                           `json:"allowed_conversations"`
	ResultRoutes                []messengereventbridge.ResultRoute `json:"result_routes"`
	Network                     networkConfig                      `json:"network"`
	Chain                       chainConfig                        `json:"chain"`
	Provider                    providerConfig                     `json:"provider"`
	ContainerdSocket            string                             `json:"containerd_socket"`
	ContainerdFIFODirectory     string                             `json:"containerd_fifo_directory"`
}

type networkConfig struct {
	ID              string `json:"id"`
	GenesisRootHash string `json:"genesis_root_hash"`
	GenesisFileHash string `json:"genesis_file_hash"`
}

type chainConfig struct {
	Endpoints             []string `json:"endpoints"`
	Quorum                int      `json:"quorum"`
	RegistryWorkchain     int32    `json:"registry_workchain"`
	RegistryCodeBOCBase64 string   `json:"registry_code_boc_base64"`
	RegistryCodeHash      string   `json:"registry_code_hash"`
	EscrowCodeHash        string   `json:"escrow_code_hash"`
}

type providerConfig struct {
	AgentID                      string `json:"agent_id"`
	Address                      string `json:"address"`
	TransportDigest              string `json:"transport_digest"`
	ExecutionSignerAuthorization string `json:"execution_signer_authorization"`
}

type worker struct {
	events       *messengereventbridge.UnixService
	publisher    *messengereventbridge.ResultPublisher
	interval     time.Duration
	artifacts    *http.Server
	artifactUnix *unixserver.Listener
	backend      *containerdbackend.Backend
	journal      *softwarework.Journal
	outbox       *messengereventbridge.ResultOutbox
	inboundMCP   *messengereventbridge.MCPResultJournal
}

type a2aLocator struct{ inner *artifacthttp.Locator }

func (l a2aLocator) URL(descriptor artifactstore.Descriptor) (a2a.URL, error) {
	value, err := l.inner.ArtifactURL(descriptor)
	return a2a.URL(value), err
}

type mcpLocator struct{ inner *artifacthttp.Locator }

func (l mcpLocator) URL(descriptor artifactstore.Descriptor) (string, error) {
	return l.inner.ArtifactURL(descriptor)
}

func main() {
	configPath := flag.String("config", "", "private mode-0600 operator configuration")
	check := flag.Bool("check", false, "validate configuration and private paths without opening runtimes")
	flag.Parse()
	if *configPath == "" || flag.NArg() != 0 {
		fail(errors.New("exactly one -config path is required"))
	}
	configuration, err := readConfig(*configPath)
	if err != nil {
		fail(err)
	}
	if err := validateConfig(configuration); err != nil {
		fail(err)
	}
	if *check {
		fmt.Println("configuration valid")
		return
	}
	process, err := openWorker(configuration)
	if err != nil {
		fail(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := process.Run(ctx); err != nil {
		fail(err)
	}
}

func readConfig(path string) (config, error) {
	var value config
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return value, errors.New("configuration path must be canonical and absolute")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return value, errors.New("configuration path contains a symlink")
	}
	before, err := os.Lstat(path)
	stat, ok := fileOwner(before)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 ||
		before.Mode().Perm() != 0o600 || before.Size() <= 0 || before.Size() > maxConfigBytes ||
		!ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return value, errors.New("configuration must be a bounded private owned regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return value, errors.New("open configuration")
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || after.Mode().Perm() != 0o600 {
		return value, errors.New("configuration changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil || int64(len(raw)) != after.Size() {
		return config{}, errors.New("read complete configuration")
	}
	if err := rejectDuplicateJSON(raw); err != nil {
		return config{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return config{}, errors.New("decode configuration")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return config{}, errors.New("configuration has trailing data")
	}
	return value, nil
}

func rejectDuplicateJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var scanValue func() error
	scanValue = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			return nil
		}
		switch delimiter {
		case '{':
			keys := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				key, ok := keyToken.(string)
				if err != nil || !ok {
					return errors.New("invalid configuration object")
				}
				if _, duplicate := keys[key]; duplicate {
					return errors.New("configuration contains a duplicate field")
				}
				keys[key] = struct{}{}
				if err := scanValue(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid configuration object ending")
			}
		case '[':
			for decoder.More() {
				if err := scanValue(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid configuration array ending")
			}
		default:
			return errors.New("invalid configuration delimiter")
		}
		return nil
	}
	if err := scanValue(); err != nil {
		return errors.New("invalid configuration JSON: " + err.Error())
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("configuration JSON has trailing data")
	}
	return nil
}

func validateConfig(c config) error {
	if c.Schema != configSchema || c.StateRoot == "" || c.A2ASocket == "" || c.MCPSocket == "" ||
		c.ArtifactSocket == "" || c.ArtifactHTTPSOrigin == "" || c.MessengerRuntimeSocket == "" ||
		c.ContainerdSocket == "" || c.ContainerdFIFODirectory == "" ||
		c.PublishIntervalMilliseconds < 100 || c.PublishIntervalMilliseconds > 60_000 ||
		c.ResultLifetimeSeconds < 60 || c.ResultLifetimeSeconds > 7*24*60*60 ||
		c.MessengerCallTimeoutSeconds < 1 || c.MessengerCallTimeoutSeconds > 60 ||
		len(c.Chain.Endpoints) == 0 || c.Chain.Quorum <= 0 || c.Chain.Quorum > len(c.Chain.Endpoints) ||
		c.Chain.RegistryCodeBOCBase64 == "" || c.Chain.RegistryCodeHash == "" || c.Chain.EscrowCodeHash == "" ||
		c.Provider.AgentID == "" || c.Provider.Address == "" || c.Provider.TransportDigest == "" ||
		c.Provider.ExecutionSignerAuthorization == "" {
		return errors.New("invalid Messenger event worker configuration")
	}
	paths := []string{c.StateRoot, c.A2ASocket, c.MCPSocket, c.ArtifactSocket,
		c.MessengerRuntimeSocket, c.ContainerdSocket, c.ContainerdFIFODirectory}
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("worker paths must be canonical and absolute")
		}
	}
	sockets := []string{c.A2ASocket, c.MCPSocket, c.ArtifactSocket, c.MessengerRuntimeSocket, c.ContainerdSocket}
	seenSockets := make(map[string]struct{}, len(sockets))
	for _, socket := range sockets {
		if _, exists := seenSockets[socket]; exists {
			return errors.New("worker authority sockets must be distinct")
		}
		seenSockets[socket] = struct{}{}
	}
	if err := requirePrivateDirectory(c.StateRoot); err != nil {
		return err
	}
	network := networkDomain(c.Network)
	if _, err := messengereventbridge.NewStaticPolicy(network, c.AllowedSenders, c.AllowedConversations); err != nil {
		return err
	}
	if _, err := messengereventbridge.NewResultPublisher(messengereventbridge.PublisherConfig{
		Outbox: &messengereventbridge.ResultOutbox{}, Client: validationCaller{}, Routes: c.ResultRoutes,
		Lifetime: time.Duration(c.ResultLifetimeSeconds) * time.Second,
	}); err != nil {
		return err
	}
	if len(c.AllowedSenders) != len(c.ResultRoutes) {
		return errors.New("every allowed sender must have exactly one result route")
	}
	for index := range c.AllowedSenders {
		if c.AllowedSenders[index] != c.ResultRoutes[index].SenderAgentID {
			return errors.New("result routes must exactly cover allowed senders")
		}
	}
	if _, err := toschain.New(toschain.Config{Network: c.Network.ID, Endpoints: c.Chain.Endpoints, Quorum: c.Chain.Quorum}); err != nil {
		return err
	}
	if _, err := nativecore.NewLocator(network, c.Chain.RegistryWorkchain,
		c.Chain.RegistryCodeBOCBase64, c.Chain.RegistryCodeHash); err != nil {
		return err
	}
	if _, err := artifacthttp.New(validationStore{}, c.ArtifactHTTPSOrigin); err != nil {
		return err
	}
	if _, err := localapi.NewClient(c.MessengerRuntimeSocket, time.Duration(c.MessengerCallTimeoutSeconds)*time.Second); err != nil {
		return err
	}
	return nil
}

type validationCaller struct{}

func (validationCaller) Call(context.Context, localapi.Request) (localapi.Response, error) {
	return localapi.Response{}, errors.New("validation-only caller")
}

type validationStore struct{}

func (validationStore) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("validation-only store")
}

func openWorker(c config) (_ *worker, result error) {
	stateDirectories := []string{"gate", "native-checkpoint", "escrow-checkpoint", "artifacts", "journal", "result-outbox", "mcp-result-journal"}
	for _, name := range stateDirectories {
		if err := ensurePrivateDirectory(filepath.Join(c.StateRoot, name)); err != nil {
			return nil, err
		}
	}
	network := networkDomain(c.Network)
	gate, err := executiongate.NewFromChain(executiongate.ChainConfig{
		Gate: executiongate.Config{Directory: filepath.Join(c.StateRoot, "gate"), Network: network,
			RegistryCodeHash: c.Chain.RegistryCodeHash, ProviderAgentID: c.Provider.AgentID,
			ProviderAddress: c.Provider.Address, ManifestDigest: softwarework.FrozenV1ManifestDigest,
			TransportDigest:              c.Provider.TransportDigest,
			ExecutionSignerAuthorization: c.Provider.ExecutionSignerAuthorization},
		Endpoints: c.Chain.Endpoints, Quorum: c.Chain.Quorum, RegistryWorkchain: c.Chain.RegistryWorkchain,
		RegistryCodeBOCBase64: c.Chain.RegistryCodeBOCBase64, EscrowCodeHash: c.Chain.EscrowCodeHash,
		NativeCheckpointPath: filepath.Join(c.StateRoot, "native-checkpoint", "checkpoint.json"),
		EscrowCheckpointPath: filepath.Join(c.StateRoot, "escrow-checkpoint", "checkpoint.json"),
	})
	if err != nil {
		return nil, err
	}
	limits := softwarework.FrozenV1Limits()
	backend, err := containerdbackend.Open(context.Background(), containerdbackend.Config{
		SocketPath: c.ContainerdSocket, Namespace: "tos-service-paid-work", Snapshotter: "overlayfs",
		Runtime: "io.containerd.runc.v2", FIFODir: c.ContainerdFIFODirectory, MaxActive: 1,
		PolicyLimits: limits, ImageReference: softwarework.FrozenV1ImageReference,
		ImageDigest: softwarework.FrozenV1ImageDigest, ImagePlatform: "linux/amd64",
	})
	if err != nil {
		return nil, err
	}
	process := &worker{backend: backend}
	defer func() {
		if result != nil {
			_ = process.Close()
		}
	}()
	bound, err := executor.NewPolicyExecutor(softwarework.FrozenV1Policy(), backend)
	if err != nil {
		return nil, err
	}
	store, err := artifactstore.Open(filepath.Join(c.StateRoot, "artifacts"), artifactstore.MaxObjectBytesHard)
	if err != nil {
		return nil, err
	}
	journal, err := softwarework.OpenJournal(filepath.Join(c.StateRoot, "journal"))
	if err != nil {
		return nil, err
	}
	process.journal = journal
	runner, err := softwarework.NewRunner(bound, store, journal, softwarework.FrozenV1Contract())
	if err != nil {
		return nil, err
	}
	locator, err := artifacthttp.OpenPersistent(store, c.ArtifactHTTPSOrigin, filepath.Join(c.StateRoot, "artifact-publications.json"))
	if err != nil {
		return nil, err
	}
	outbox, err := messengereventbridge.OpenResultOutbox(filepath.Join(c.StateRoot, "result-outbox"))
	if err != nil {
		return nil, err
	}
	process.outbox = outbox
	inboundMCP, err := messengereventbridge.OpenMCPResultJournal(filepath.Join(c.StateRoot, "mcp-result-journal"))
	if err != nil {
		return nil, err
	}
	process.inboundMCP = inboundMCP
	a2aAdapter, err := a2aadapter.New(gate, runner, a2aLocator{locator})
	if err != nil {
		return nil, err
	}
	mcpAdapter, err := mcpadapter.New(gate, runner, mcpLocator{locator})
	if err != nil {
		return nil, err
	}
	a2aHandler, err := messengereventbridge.NewA2AExecutionHandler(a2aAdapter, outbox)
	if err != nil {
		return nil, err
	}
	mcpHandler, err := messengereventbridge.NewMCPExecutionHandlerSplit(mcpAdapter, outbox, inboundMCP)
	if err != nil {
		return nil, err
	}
	policy, err := messengereventbridge.NewStaticPolicy(network, c.AllowedSenders, c.AllowedConversations)
	if err != nil {
		return nil, err
	}
	eventServer, err := messengereventbridge.New(messengereventbridge.Config{Authorizer: policy, A2A: a2aHandler, MCP: mcpHandler})
	if err != nil {
		return nil, err
	}
	events, err := messengereventbridge.ListenUnix(eventServer, c.A2ASocket, c.MCPSocket)
	if err != nil {
		return nil, err
	}
	process.events = events
	client, err := localapi.NewClient(c.MessengerRuntimeSocket, time.Duration(c.MessengerCallTimeoutSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	publisher, err := messengereventbridge.NewResultPublisher(messengereventbridge.PublisherConfig{Outbox: outbox,
		Client: client, Routes: c.ResultRoutes, Lifetime: time.Duration(c.ResultLifetimeSeconds) * time.Second})
	if err != nil {
		return nil, err
	}
	process.publisher = publisher
	process.interval = time.Duration(c.PublishIntervalMilliseconds) * time.Millisecond
	artifactListener, err := unixserver.ListenLimited(c.ArtifactSocket, 128)
	if err != nil {
		return nil, err
	}
	process.artifactUnix = artifactListener
	process.artifacts = &http.Server{Handler: locator.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 16 << 10}
	return process, nil
}

func (w *worker) Run(ctx context.Context) error {
	if w == nil || ctx == nil || w.events == nil || w.publisher == nil || w.artifacts == nil || w.artifactUnix == nil {
		return errors.New("invalid Messenger event worker")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsOut := make(chan error, 3)
	go func() { errorsOut <- w.events.Run(runCtx) }()
	go func() {
		err := w.artifacts.Serve(w.artifactUnix)
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, io.ErrClosedPipe) {
			err = nil
		}
		errorsOut <- err
	}()
	go func() { errorsOut <- w.publishLoop(runCtx) }()
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errorsOut:
		if runErr == nil && ctx.Err() == nil {
			runErr = errors.New("Messenger event worker component exited unexpectedly")
		}
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	_ = w.artifacts.Shutdown(shutdown)
	stop()
	_ = w.events.Close()
	_ = w.artifactUnix.Close()
	for range 2 {
		if err := <-errorsOut; runErr == nil && err != nil && !errors.Is(err, context.Canceled) {
			runErr = err
		}
	}
	return errors.Join(runErr, w.Close())
}

func (w *worker) publishLoop(ctx context.Context) error {
	publish := func() {
		summary, err := w.publisher.PublishPending(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Messenger result publish retained=%d: %v\n", summary.Retained, err)
		}
	}
	publish()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			publish()
		}
	}
}

func (w *worker) Close() error {
	if w == nil {
		return nil
	}
	var eventErr, artifactErr, backendErr, journalErr, outboxErr, inboundMCPErr error
	if w.events != nil {
		eventErr = w.events.Close()
	}
	if w.artifactUnix != nil {
		artifactErr = w.artifactUnix.Close()
	}
	if w.backend != nil {
		backendErr = w.backend.Close()
	}
	if w.journal != nil {
		journalErr = w.journal.Close()
	}
	if w.outbox != nil {
		outboxErr = w.outbox.Close()
	}
	if w.inboundMCP != nil {
		inboundMCPErr = w.inboundMCP.Close()
	}
	return errors.Join(eventErr, artifactErr, backendErr, journalErr, outboxErr, inboundMCPErr)
}

func networkDomain(c networkConfig) *nativev1.NetworkDomain {
	return &nativev1.NetworkDomain{NetworkId: c.ID, GenesisRootHash: c.GenesisRootHash, GenesisFileHash: c.GenesisFileHash}
}

func ensurePrivateDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return errors.New("create private worker state directory")
	}
	return requirePrivateDirectory(path)
}

func requirePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("worker state directory must be canonical and absolute")
	}
	info, err := os.Lstat(path)
	stat, ok := fileOwner(info)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 ||
		!ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("worker state directory must be private and owned")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("worker state directory contains a symlink")
	}
	return nil
}

func fileOwner(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
