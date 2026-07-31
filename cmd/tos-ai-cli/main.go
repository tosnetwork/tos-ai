package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

func main() {
	var socketPath string
	var timeout time.Duration
	flag.StringVar(&socketPath, "socket", defaultSocket(), "private Unix socket")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "RPC timeout")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: tos-ai-cli [flags] capabilities|invoke [text]")
		os.Exit(2)
	}
	httpClient, err := localrpc.HTTPClient(socketPath, timeout)
	if err != nil {
		log.Fatal(err)
	}
	client := edgev1connect.NewWorkerServiceClient(httpClient, "http://unix")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch flag.Arg(0) {
	case "capabilities":
		response, err := client.GetCapabilities(ctx, connect.NewRequest(&edgev1.GetCapabilitiesRequest{}))
		if err != nil {
			log.Fatal(err)
		}
		printJSON(response.Msg)
	case "invoke":
		if flag.NArg() != 2 {
			log.Fatal("invoke requires one text argument")
		}
		invoke(ctx, client, []byte(flag.Arg(1)))
	default:
		log.Fatalf("unknown command %q", flag.Arg(0))
	}
}

func invoke(ctx context.Context, client edgev1connect.WorkerServiceClient, payload []byte) {
	requestID := randomID()
	deadline := time.Now().Add(25 * time.Second).UnixMilli()
	quote, err := client.Quote(ctx, connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          requestID,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         uint64(len(payload)),
		MaxOutputBytes:     1 << 20,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
	if err != nil {
		log.Fatal(err)
	}
	response, err := client.Invoke(ctx, connect.NewRequest(&edgev1.InvokeRequest{
		RequestId:          requestID,
		QuoteId:            quote.Msg.QuoteId,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		Payload:            payload,
		MaxOutputBytes:     1 << 20,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
	if err != nil {
		log.Fatal(err)
	}
	printJSON(response.Msg)
}

func randomID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		log.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(value)
}

func printJSON(value interface{}) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(encoded))
}

func defaultSocket() string {
	if runtimeDirectory := os.Getenv("XDG_RUNTIME_DIR"); runtimeDirectory != "" {
		return filepath.Join(runtimeDirectory, "tos-ai", "worker.sock")
	}
	return fmt.Sprintf("/run/user/%d/tos-ai/worker.sock", os.Getuid())
}
