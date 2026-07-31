package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/tosnetwork/tos-ai/internal/operatorconfig"
	"github.com/tosnetwork/tos-ai/internal/unixserver"
	"github.com/tosnetwork/tos-ai/internal/worker"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	"github.com/tosnetwork/tos-ai/pkg/admission"
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
	flag.StringVar(&socketPath, "socket", defaultSocket(), "private Unix socket")
	flag.IntVar(&workers, "workers", 1, "concurrent runtime workers")
	flag.IntVar(&maxQueue, "max-queue", 64, "maximum queued work items")
	flag.IntVar(&maxConnections, "max-connections", 128, "maximum private socket connections")
	flag.DurationVar(&mockDelay, "mock-delay", 0, "development mock execution delay")
	flag.StringVar(&runtimeConfigPath, "runtime-config", "", "private administrator runtime configuration")
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
	adapters, err := configuredAdapters(runtimeConfigPath, mockDelay)
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
	}, taskScheduler, admissionController, adapters)
	if err != nil {
		return err
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
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Shutdown(shutdownContext)
		return err
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errors := make(chan error, 1)
	go func() { errors <- server.Serve(listener) }()
	log.Printf("tos-ai-worker private socket: %s", socketPath)
	var serveErr error
	select {
	case err := <-errors:
		if err != nil && err != http.ErrServerClosed {
			serveErr = err
		}
	case <-ctx.Done():
	}
	service.BeginDrain()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownContext)
	serviceErr := service.Shutdown(shutdownContext)
	if serveErr != nil {
		return serveErr
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	return serviceErr
}

func configuredAdapters(configPath string, mockDelay time.Duration) ([]airuntime.Adapter, error) {
	if mockDelay < 0 || mockDelay > time.Minute {
		return nil, fmt.Errorf("mock delay exceeds hard limits")
	}
	if configPath == "" {
		return []airuntime.Adapter{mock.New(mockDelay)}, nil
	}
	if mockDelay != 0 {
		return nil, fmt.Errorf("mock delay cannot be used with a runtime configuration")
	}
	return operatorconfig.Load(configPath)
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
