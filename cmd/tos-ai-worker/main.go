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
	"github.com/tosnetwork/tos-ai/internal/unixserver"
	"github.com/tosnetwork/tos-ai/internal/worker"
	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-ai/pkg/scheduler"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
)

func main() {
	var socketPath string
	var workers int
	var maxQueue int
	flag.StringVar(&socketPath, "socket", defaultSocket(), "private Unix socket")
	flag.IntVar(&workers, "workers", 1, "concurrent runtime workers")
	flag.IntVar(&maxQueue, "max-queue", 64, "maximum queued work items")
	flag.Parse()

	taskScheduler, err := scheduler.New(scheduler.Config{Workers: workers, MaxQueue: maxQueue})
	if err != nil {
		log.Fatal(err)
	}
	service, err := worker.NewService(worker.Config{
		Version:        "0.1.0-dev",
		QuoteTTL:       30 * time.Second,
		MaxQuotes:      4096,
		MaxInvocations: 4096,
		MaxDeadline:    15 * time.Minute,
		PriceNanoTOS:   0,
	}, taskScheduler, []airuntime.Adapter{mock.New(0)})
	if err != nil {
		log.Fatal(err)
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
	listener, err := unixserver.Listen(socketPath)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errors := make(chan error, 1)
	go func() { errors <- server.Serve(listener) }()
	log.Printf("tos-ai-worker private socket: %s", socketPath)
	select {
	case err := <-errors:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
		_ = taskScheduler.Shutdown(shutdownContext)
	}
}

func defaultSocket() string {
	if runtimeDirectory := os.Getenv("XDG_RUNTIME_DIR"); runtimeDirectory != "" {
		return filepath.Join(runtimeDirectory, "tos-ai", "worker.sock")
	}
	return fmt.Sprintf("/run/user/%d/tos-ai/worker.sock", os.Getuid())
}
