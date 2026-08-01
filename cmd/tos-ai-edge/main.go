package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	"github.com/tosnetwork/tos-ai/pkg/edgegateway"
	"github.com/tosnetwork/tos-protocol/pkg/authorization"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
	"github.com/tosnetwork/tos-protocol/pkg/toschain"
)

type tosReadiness struct {
	runtime   *toschain.Runtime
	reference authorization.Reference
}

type paidActionLogger struct{}

func (paidActionLogger) ReportPaidActionError(_ context.Context, stage string, err error) {
	log.Printf("paid action failed: stage=%s error=%v", stage, err)
}

func (readiness *tosReadiness) CheckReady(ctx context.Context) error {
	if readiness == nil || readiness.runtime == nil {
		return errors.New("nil TOS chain readiness")
	}
	_, err := readiness.runtime.CheckServiceReady(ctx, readiness.reference, time.Now().UTC())
	return err
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("tos-ai-edge", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var configPath string
	flags.StringVar(&configPath, "config", "", "private absolute AI Edge gateway configuration")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if configPath == "" || len(flags.Args()) != 0 {
		return errors.New("exactly -config is required")
	}
	config, err := operatorconfig.LoadEdgeGatewayConfig(configPath)
	if err != nil {
		return fmt.Errorf("load AI Edge gateway config: %w", err)
	}
	now := time.Now().UTC()
	documents, err := config.LoadDocuments(now)
	if err != nil {
		return fmt.Errorf("load AI Edge deployment documents: %w", err)
	}
	reference := config.Reference(documents.Descriptor)
	chainRuntime, err := documents.Chain.BuildRuntime()
	if err != nil {
		return fmt.Errorf("configure TOS chain runtime: %w", err)
	}
	startupContext, cancelStartup := context.WithTimeout(context.Background(), config.StartupTimeout())
	defer cancelStartup()
	readiness, err := chainRuntime.CheckServiceReady(startupContext, reference, now)
	if err != nil {
		return fmt.Errorf("TOS chain startup preflight: %w", err)
	}

	workerConfig := localrpc.DefaultWorkerClientConfig(config.WorkerSocket)
	workerClient, err := localrpc.NewWorkerClient(workerConfig)
	if err != nil {
		return fmt.Errorf("configure private AI Worker client: %w", err)
	}
	if err := workerClient.CheckReady(startupContext); err != nil {
		return fmt.Errorf("AI Worker startup preflight: %w", err)
	}
	signerConfig := localrpc.DefaultReceiptSignerClientConfig(config.ReceiptSignerSocket)
	signerConfig.ExpectedKeyID = config.ReceiptSignerKeyID
	signerConfig.ExpectedPublicKey = config.ReceiptSignerPublicKey
	signerClient, err := localrpc.NewReceiptSignerClient(signerConfig)
	if err != nil {
		return fmt.Errorf("configure private receipt signer: %w", err)
	}
	defer signerClient.Close()
	if err := signerClient.CheckReady(startupContext); err != nil {
		return fmt.Errorf("receipt signer startup preflight: %w", err)
	}

	gateway, err := edgegateway.Open(
		startupContext,
		edgegateway.Config{
			Descriptor: documents.Descriptor, Catalog: documents.Catalog,
			ManifestEnvelope: documents.ManifestEnvelope, Reference: reference,
			RequiredDelegationScope: config.RequiredDelegationScope,
			CoreConfig:              config.CoreConfig(),
			PaidActionRetention:     config.PaidActionRetention(),
			ReceiptLifetime:         config.ReceiptLifetime(),
			PaidActionMaxConcurrent: config.PaidActionMaxConcurrent,
		},
		edgegateway.Dependencies{
			AuthorityResolver: chainRuntime.Authority,
			ClientKeyResolver: chainRuntime.ClientKeys,
			PaymentObserver:   chainRuntime.Payments,
			ChainReadiness: &tosReadiness{
				runtime: chainRuntime, reference: reference,
			},
			Worker: workerClient, ReceiptSigner: signerClient,
			ReceiptReadiness:        signerClient,
			PaidActionErrorReporter: paidActionLogger{},
		},
	)
	if err != nil {
		return err
	}
	defer gateway.Close()
	handler, err := gateway.Handler()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for AI Edge: %w", err)
	}
	server := edge.NewHTTPServer(config.ListenAddress, handler)
	log.Printf(
		"tos-ai-edge ready: listen=%s network=%s master_seqno=%d quorum=%d service=%s",
		config.ListenAddress, readiness.Network, readiness.ObservedMasterSeqno,
		readiness.QuorumEndpoints, documents.Descriptor.ServiceID,
	)
	return serveUntilSignal(server, listener)
}

func serveUntilSignal(server *http.Server, listener net.Listener) error {
	if server == nil || listener == nil {
		return errors.New("invalid AI Edge server")
	}
	stopContext, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stop()
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()
	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-stopContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			_ = server.Close()
			return fmt.Errorf("shutdown AI Edge HTTP server: %w", err)
		}
		err := <-serveErrors
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
