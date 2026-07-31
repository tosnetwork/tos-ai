package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	"github.com/tosnetwork/tos-ai/internal/unixserver"
	"github.com/tosnetwork/tos-ai/internal/worker"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/modelactivation"
	"github.com/tosnetwork/tos-ai/pkg/modelapproval"
	"github.com/tosnetwork/tos-ai/pkg/probe"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	var socketPath string
	var workers int
	var maxQueue int
	var maxConnections int
	var mockDelay time.Duration
	var runtimeConfigPath string
	var modelTrustConfigPath string
	flag.StringVar(&socketPath, "socket", defaultSocket(), "private Unix socket")
	flag.IntVar(&workers, "workers", 1, "concurrent runtime workers")
	flag.IntVar(&maxQueue, "max-queue", 64, "maximum queued work items")
	flag.IntVar(&maxConnections, "max-connections", 128, "maximum private socket connections")
	flag.DurationVar(&mockDelay, "mock-delay", 0, "development mock execution delay")
	flag.StringVar(&runtimeConfigPath, "runtime-config", "", "private administrator runtime configuration")
	flag.StringVar(
		&modelTrustConfigPath, "model-trust-config", "",
		"private signed-model trust configuration",
	)
	flag.Parse()

	report, err := probe.Collect(probe.NewNVMLBackend())
	if err != nil {
		return err
	}
	admissionConfig, err := defaultAdmissionConfig(report, workers, maxQueue)
	if err != nil {
		return err
	}
	admissionController, err := admission.New(admissionConfig)
	if err != nil {
		return err
	}
	taskScheduler, err := scheduler.New(scheduler.Config{Workers: workers, MaxQueue: maxQueue})
	if err != nil {
		return err
	}
	runtimes, err := configuredRuntimes(
		runtimeConfigPath, modelTrustConfigPath, mockDelay,
	)
	if err != nil {
		return err
	}
	service, err := worker.NewService(worker.Config{
		Version:             "0.1.0-dev",
		QuoteTTL:            30 * time.Second,
		MaxQuotes:           4096,
		MaxInvocations:      4096,
		MaxDeadline:         15 * time.Minute,
		PreflightTimeout:    5 * time.Second,
		PreflightTTL:        15 * time.Second,
		PreflightFailureTTL: 2 * time.Second,
		MaxPreflightWaiters: preflightWaiters(maxConnections),
		PriceNanoTOS:        0,
		GPUStatus:           report.NVIDIA.Status,
	}, taskScheduler, admissionController, runtimes.adapters)
	if err != nil {
		closeRuntimeAdapters(runtimes.adapters)
		shutdownContext, cancel := context.WithTimeout(
			context.Background(), runtimes.activationCleanupTimeout(),
		)
		defer cancel()
		return errors.Join(err, runtimes.closeActivation(shutdownContext))
	}
	preflightContext, cancelPreflight := context.WithTimeout(context.Background(), 10*time.Second)
	readiness := service.RefreshRuntimes(preflightContext)
	cancelPreflight()
	if readiness.RuntimeReady != readiness.RuntimeTotal {
		log.Printf("runtime preflight degraded: ready=%d total=%d",
			readiness.RuntimeReady, readiness.RuntimeTotal)
	}
	path, handler := edgev1connect.NewWorkerServiceHandler(
		service,
		connect.WithReadMaxBytes(2<<20),
		connect.WithSendMaxBytes(2<<20),
	)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       20 * time.Minute,
		WriteTimeout:      20 * time.Minute,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    16 << 10,
	}
	listener, err := unixserver.ListenLimited(socketPath, maxConnections)
	if err != nil {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = service.Shutdown(shutdownContext)
		cancel()
		activationContext, cancelActivation := context.WithTimeout(
			context.Background(), runtimes.activationCleanupTimeout(),
		)
		activationErr := runtimes.closeActivation(activationContext)
		cancelActivation()
		return errors.Join(err, activationErr)
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()
	log.Printf("tos-ai-worker private socket: %s", socketPath)
	var serveErr error
	select {
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			serveErr = err
		}
	case <-ctx.Done():
	}
	service.BeginDrain()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := server.Shutdown(shutdownContext)
	serviceErr := service.Shutdown(shutdownContext)
	cancel()
	activationContext, cancelActivation := context.WithTimeout(
		context.Background(), runtimes.activationCleanupTimeout(),
	)
	activationErr := runtimes.closeActivation(activationContext)
	cancelActivation()
	return errors.Join(serveErr, shutdownErr, serviceErr, activationErr)
}

type runtimeResources struct {
	adapters      []airuntime.Adapter
	activation    *modelactivation.Controller
	configuration *operatorconfig.Configuration
}

func configuredRuntimes(
	configPath string,
	modelTrustPath string,
	mockDelay time.Duration,
) (*runtimeResources, error) {
	if mockDelay < 0 || mockDelay > time.Minute {
		return nil, fmt.Errorf("mock delay exceeds hard limits")
	}
	if configPath == "" {
		if modelTrustPath != "" {
			return nil, fmt.Errorf("model trust requires a runtime configuration")
		}
		return &runtimeResources{
			adapters: []airuntime.Adapter{mock.New(mockDelay)},
		}, nil
	}
	if mockDelay != 0 {
		return nil, fmt.Errorf("mock delay cannot be used with a runtime configuration")
	}
	configuration, err := operatorconfig.Load(configPath)
	if err != nil {
		return nil, err
	}
	if modelTrustPath == "" {
		if configuration.Activation != nil {
			closeRuntimeAdapters(configuration.Adapters)
			_ = configuration.CloseBackends()
			return nil, fmt.Errorf("model activation requires signed model trust")
		}
		return &runtimeResources{
			adapters: configuration.Adapters, configuration: &configuration,
		}, nil
	}
	trust, err := operatorconfig.LoadModelTrust(modelTrustPath)
	if err != nil {
		closeRuntimeAdapters(configuration.Adapters)
		_ = configuration.CloseBackends()
		return nil, err
	}
	resources := &runtimeResources{
		adapters: configuration.Adapters, configuration: &configuration,
	}
	if configuration.Activation != nil {
		if directoriesOverlap(
			trust.CacheDir, configuration.Activation.Controller.StateDir,
		) {
			closeRuntimeAdapters(configuration.Adapters)
			_ = configuration.CloseBackends()
			return nil, fmt.Errorf(
				"model cache and activation state must be separate",
			)
		}
		controller, err := modelactivation.New(
			trust.Manager, configuration.Activation.Controller,
		)
		if err != nil {
			closeRuntimeAdapters(configuration.Adapters)
			_ = configuration.CloseBackends()
			return nil, fmt.Errorf("configure model activation")
		}
		resources.activation = controller
		activationContext, cancelActivation := context.WithTimeout(
			context.Background(),
			activationStartupTimeout(configuration.Activation),
		)
		err = controller.Recover(activationContext)
		if err == nil {
			for _, desired := range configuration.Activation.Desired {
				if activationContext.Err() != nil {
					err = activationContext.Err()
					break
				}
				if _, activateErr := controller.Activate(
					activationContext, desired.SlotID, desired.Digest,
				); activateErr != nil {
					err = activateErr
					break
				}
			}
		}
		cancelActivation()
		if err != nil {
			closeRuntimeAdapters(configuration.Adapters)
			cleanupContext, cancelCleanup := context.WithTimeout(
				context.Background(), resources.activationCleanupTimeout(),
			)
			_ = resources.closeActivation(cleanupContext)
			cancelCleanup()
			return nil, fmt.Errorf("activate configured runtime models")
		}
	}
	verifyContext, cancelVerify := context.WithTimeout(
		context.Background(), trust.VerificationTimeout,
	)
	defer cancelVerify()
	guarded, err := modelapproval.WrapAll(
		verifyContext, trust.Manager, configuration.Adapters,
		trust.VerificationTimeout,
	)
	if err != nil {
		cleanupContext, cancelCleanup := context.WithTimeout(
			context.Background(), resources.activationCleanupTimeout(),
		)
		_ = resources.closeActivation(cleanupContext)
		cancelCleanup()
		return nil, fmt.Errorf("approve configured runtime models")
	}
	resources.adapters = guarded
	return resources, nil
}

func (r *runtimeResources) closeActivation(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var activationErr error
	if r.activation != nil {
		activationErr = r.activation.Close(ctx)
	}
	var backendErr error
	if r.configuration != nil {
		backendErr = r.configuration.CloseBackends()
	}
	return errors.Join(activationErr, backendErr)
}

func (r *runtimeResources) activationCleanupTimeout() time.Duration {
	const hardLimit = 30 * time.Minute
	if r == nil || r.configuration == nil ||
		r.configuration.Activation == nil {
		return time.Second
	}
	activation := r.configuration.Activation
	operations := len(activation.Desired)
	perOperation := activation.Controller.CleanupTimeout
	if operations <= 0 || perOperation <= 0 ||
		perOperation > hardLimit/time.Duration(operations) {
		return hardLimit
	}
	result := perOperation * time.Duration(operations)
	if result <= 0 || result > hardLimit {
		return hardLimit
	}
	return result
}

func activationStartupTimeout(
	activation *operatorconfig.ActivationConfiguration,
) time.Duration {
	const hardLimit = 30 * time.Minute
	if activation == nil {
		return time.Second
	}
	operations := len(activation.Desired)*6 + 1
	perOperation := activation.Controller.OperationTimeout
	if operations <= 0 || perOperation <= 0 ||
		perOperation > hardLimit/time.Duration(operations) {
		return hardLimit
	}
	result := perOperation * time.Duration(operations)
	if result > hardLimit {
		return hardLimit
	}
	return result
}

func directoriesOverlap(left string, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	return pathContains(left, right) || pathContains(right, left)
}

func pathContains(parent string, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && (relative == "." ||
		(relative != ".." && !strings.HasPrefix(
			relative, ".."+string(filepath.Separator),
		)))
}

func closeRuntimeAdapters(adapters []airuntime.Adapter) {
	for _, adapter := range adapters {
		if closer, ok := adapter.(airuntime.AdapterCloser); ok {
			_ = closer.Close()
		}
	}
}

func defaultAdmissionConfig(report probe.Report, workers, maxQueue int) (admission.Config, error) {
	if workers <= 0 || workers > 128 || maxQueue <= 0 || maxQueue > 4096 {
		return admission.Config{}, fmt.Errorf("workers or queue exceed hard limits")
	}
	ramCapacity := report.Host.MemoryBytes / 2
	if ramCapacity > 64<<30 {
		ramCapacity = 64 << 30
	}
	if ramCapacity < 64<<20 {
		return admission.Config{}, fmt.Errorf("insufficient observed host RAM")
	}
	var vramCapacity uint64
	for _, device := range report.NVIDIA.Devices {
		available := device.VRAMBytes - device.VRAMUsedBytes
		if ^uint64(0)-vramCapacity < available {
			return admission.Config{}, fmt.Errorf("observed VRAM overflow")
		}
		vramCapacity += available
	}
	slots := uint64(workers + maxQueue)
	const (
		maxOutput  = uint64(1 << 20)
		maxContext = uint64(32768)
	)
	capacity := admission.Resources{
		RAMBytes: ramCapacity, VRAMBytes: vramCapacity, KVCacheBytes: 8 << 30,
		ContextTokens: slots * maxContext, BatchSize: uint32(slots * 8),
		OutputBytes: slots * maxOutput, ExecutionTime: 15 * time.Minute,
	}
	ownerReserved := admission.Resources{
		RAMBytes: ramCapacity / 4, VRAMBytes: vramCapacity / 4, KVCacheBytes: 2 << 30,
		ContextTokens: maxContext, BatchSize: 8, OutputBytes: maxOutput,
	}
	perRequest := admission.Resources{
		RAMBytes: 32 << 30, VRAMBytes: vramCapacity, KVCacheBytes: 4 << 30,
		ContextTokens: maxContext, BatchSize: 8, OutputBytes: maxOutput,
		ExecutionTime: 15 * time.Minute,
	}
	if perRequest.RAMBytes > capacity.RAMBytes {
		perRequest.RAMBytes = capacity.RAMBytes
	}
	if ownerReserved.KVCacheBytes > capacity.KVCacheBytes {
		ownerReserved.KVCacheBytes = capacity.KVCacheBytes / 4
	}
	return admission.Config{
		MaxConcurrent: workers, MaxQueue: maxQueue, Capacity: capacity,
		OwnerReserved: ownerReserved, PerRequestMax: perRequest,
	}, nil
}

func defaultSocket() string {
	if runtimeDirectory := os.Getenv("XDG_RUNTIME_DIR"); runtimeDirectory != "" {
		return filepath.Join(runtimeDirectory, "tos-ai", "worker.sock")
	}
	return fmt.Sprintf("/run/user/%d/tos-ai/worker.sock", os.Getuid())
}

func preflightWaiters(maxConnections int) int {
	if maxConnections <= 0 {
		return maxConnections
	}
	if maxConnections > worker.MaxPreflightWaitersHard {
		return worker.MaxPreflightWaitersHard
	}
	return maxConnections
}
