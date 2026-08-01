package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/gen/tos/edge/v1/edgev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/ard"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

const maxMetricsResponseBytes = 16 << 10

func main() {
	var socketPath string
	var timeout time.Duration
	var requestID string
	var quoteID string
	var taskID string
	var requestDigest string
	var retainUntil int64
	var ardIdentifier string
	var ardServiceName string
	var ardHostName string
	var ardHostID string
	var ardServiceURL string
	var ardEntryVersion string
	var ardOutput string
	flag.StringVar(&socketPath, "socket", defaultSocket(), "private Unix socket")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "RPC timeout")
	flag.StringVar(&requestID, "request-id", "", "request ID (generated when omitted)")
	flag.StringVar(&quoteID, "quote-id", "", "existing quote ID for invoke")
	flag.StringVar(&taskID, "task-id", "", "durable Worker task ID")
	flag.StringVar(
		&requestDigest, "request-digest", "",
		"exact Worker invocation digest for get-task or cancel",
	)
	flag.Int64Var(
		&retainUntil, "retain-until", 0,
		"Worker task retention deadline in Unix milliseconds",
	)
	flag.StringVar(&ardIdentifier, "ard-identifier", "", "public ARD service identifier")
	flag.StringVar(&ardServiceName, "ard-service-name", "", "public ARD service display name")
	flag.StringVar(&ardHostName, "ard-host-name", "", "public ARD host display name")
	flag.StringVar(&ardHostID, "ard-host-id", "", "optional public ARD host identifier")
	flag.StringVar(&ardServiceURL, "ard-service-url", "", "public HTTPS TOS service descriptor URL")
	flag.StringVar(&ardEntryVersion, "ard-entry-version", "", "public ARD entry version")
	flag.StringVar(&ardOutput, "ard-output", "", "optional atomic mode-0600 catalog output file")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: tos-ai-cli [flags] health|capabilities|ard-catalog|metrics|quote|invoke|get-task|cancel [value]")
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
	case "health":
		response, err := client.Health(ctx, connect.NewRequest(&edgev1.HealthRequest{}))
		if err != nil {
			log.Fatal(err)
		}
		printJSON(response.Msg)
	case "capabilities":
		response, err := client.GetCapabilities(ctx, connect.NewRequest(&edgev1.GetCapabilitiesRequest{}))
		if err != nil {
			log.Fatal(err)
		}
		printJSON(response.Msg)
	case "ard-catalog":
		if flag.NArg() != 1 {
			log.Fatal("ard-catalog accepts no arguments")
		}
		response, err := client.GetCapabilities(
			ctx, connect.NewRequest(&edgev1.GetCapabilitiesRequest{}),
		)
		if err != nil {
			log.Fatal(err)
		}
		catalog, err := ard.BuildWorkerCatalog(ard.WorkerCatalogConfig{
			ServiceIdentifier: ardIdentifier, ServiceDisplayName: ardServiceName,
			HostDisplayName: ardHostName, HostIdentifier: ardHostID,
			ServiceURL: ardServiceURL, EntryVersion: ardEntryVersion,
		}, response.Msg, time.Now().UTC())
		if err != nil {
			log.Fatal(err)
		}
		if ardOutput == "" {
			printJSON(catalog)
			break
		}
		if err := ard.WriteCatalogFile(
			ardOutput, catalog, ard.DefaultLimits(),
		); err != nil {
			log.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, ardOutput)
	case "metrics":
		if flag.NArg() != 1 {
			log.Fatal("metrics accepts no arguments")
		}
		printMetrics(ctx, httpClient)
	case "quote":
		if flag.NArg() > 2 {
			log.Fatal("quote accepts at most one text argument")
		}
		payload := []byte("")
		if flag.NArg() == 2 {
			payload = []byte(flag.Arg(1))
		}
		if requestID == "" {
			requestID = randomID()
		}
		response, err := quote(ctx, client, requestID, payload)
		if err != nil {
			log.Fatal(err)
		}
		printJSON(response.Msg)
	case "invoke":
		if flag.NArg() != 2 {
			log.Fatal("invoke requires one text argument")
		}
		if requestID == "" {
			requestID = randomID()
		}
		invoke(ctx, client, requestID, quoteID, taskID, []byte(flag.Arg(1)))
	case "get-task":
		if requestID == "" || taskID == "" || requestDigest == "" ||
			retainUntil == 0 {
			log.Fatal("get-task requires request-id, task-id, request-digest, and retain-until")
		}
		response, err := client.GetTask(ctx, connect.NewRequest(&edgev1.GetTaskRequest{
			RequestId: requestID, TaskId: taskID,
			RequestDigest: requestDigest, RetainUntilUnixMillis: retainUntil,
		}))
		if err != nil {
			log.Fatal(err)
		}
		printJSON(response.Msg)
	case "cancel":
		if flag.NArg() == 2 {
			requestID = flag.Arg(1)
		}
		if requestID == "" || taskID == "" || requestDigest == "" {
			log.Fatal("cancel requires request-id, task-id, and request-digest")
		}
		response, err := client.Cancel(ctx, connect.NewRequest(&edgev1.CancelRequest{
			RequestId: requestID, TaskId: taskID, RequestDigest: requestDigest,
		}))
		if err != nil {
			log.Fatal(err)
		}
		printJSON(response.Msg)
	default:
		log.Fatalf("unknown command %q", flag.Arg(0))
	}
}

func quote(ctx context.Context, client edgev1connect.WorkerServiceClient, requestID string, payload []byte) (*connect.Response[edgev1.QuoteResponse], error) {
	deadline := time.Now().Add(25 * time.Second).UnixMilli()
	return client.Quote(ctx, connect.NewRequest(&edgev1.QuoteRequest{
		RequestId:          requestID,
		ServiceId:          "tos.ai.mock",
		Operation:          "generate",
		Model:              "deterministic-echo",
		InputBytes:         uint64(len(payload)),
		MaxOutputBytes:     1 << 20,
		DeadlineUnixMillis: deadline,
		Priority:           edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
	}))
}

func invoke(
	ctx context.Context,
	client edgev1connect.WorkerServiceClient,
	requestID string,
	quoteID string,
	taskID string,
	payload []byte,
) {
	deadline := time.Now().Add(25 * time.Second).UnixMilli()
	if quoteID == "" {
		quoted, err := quote(ctx, client, requestID, payload)
		if err != nil {
			log.Fatal(err)
		}
		quoteID = quoted.Msg.QuoteId
		deadline = quoted.Msg.ExpiresUnixMillis
	}
	if taskID == "" {
		taskID = randomID()
	}
	invocation, _, err := localrpc.BindInvocationRequest(&edgev1.InvokeRequest{
		RequestId:             requestID,
		QuoteId:               quoteID,
		TaskId:                taskID,
		ServiceId:             "tos.ai.mock",
		Operation:             "generate",
		Model:                 "deterministic-echo",
		Payload:               payload,
		MaxOutputBytes:        1 << 20,
		DeadlineUnixMillis:    deadline,
		Priority:              edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		RetainUntilUnixMillis: time.UnixMilli(deadline).Add(time.Hour).UnixMilli(),
	})
	if err != nil {
		log.Fatal(err)
	}
	response, err := client.Invoke(ctx, connect.NewRequest(invocation))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf(
		"task-id=%s request-digest=%s retain-until=%d",
		invocation.TaskId, invocation.RequestDigest,
		invocation.RetainUntilUnixMillis,
	)
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

func printMetrics(ctx context.Context, client *http.Client) {
	encoded, err := fetchMetrics(ctx, client)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stdout.Write(encoded); err != nil {
		log.Fatal("write metrics response")
	}
}

func fetchMetrics(ctx context.Context, client *http.Client) ([]byte, error) {
	if client == nil {
		return nil, errors.New("metrics client is unavailable")
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, "http://unix/metrics", nil,
	)
	if err != nil {
		return nil, errors.New("create metrics request")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("metrics request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK ||
		!strings.HasPrefix(response.Header.Get("Content-Type"), "text/plain;") ||
		response.ContentLength > maxMetricsResponseBytes {
		return nil, errors.New("metrics response rejected")
	}
	encoded, err := io.ReadAll(io.LimitReader(
		response.Body, maxMetricsResponseBytes+1,
	))
	if err != nil || len(encoded) == 0 || len(encoded) > maxMetricsResponseBytes ||
		encoded[len(encoded)-1] != '\n' || !safeMetricsText(encoded) {
		return nil, errors.New("metrics response rejected")
	}
	return encoded, nil
}

func safeMetricsText(encoded []byte) bool {
	for _, value := range encoded {
		if value == '\n' || value >= 0x20 && value <= 0x7e {
			continue
		}
		return false
	}
	return true
}

func defaultSocket() string {
	if runtimeDirectory := os.Getenv("XDG_RUNTIME_DIR"); runtimeDirectory != "" {
		return filepath.Join(runtimeDirectory, "tos-ai", "worker.sock")
	}
	return fmt.Sprintf("/run/user/%d/tos-ai/worker.sock", os.Getuid())
}
