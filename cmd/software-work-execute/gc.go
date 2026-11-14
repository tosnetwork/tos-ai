package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
)

// runArtifactsGC deletes, in one bounded call, artifacts whose every
// retaining job has expired, and prints the sweep report as JSON. A busy
// store, malformed retention metadata or an unsafe object refuses the
// sweep; rerunning it is always safe.
func runArtifactsGC(args []string, stdout, stderr io.Writer) int {
	return sweepArtifacts(args, stdout, stderr, time.Now)
}

func sweepArtifacts(args []string, stdout, stderr io.Writer, now func() time.Time) int {
	flags := flag.NewFlagSet("software-work-execute artifacts-gc", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("state-dir", "", "private execution state directory whose artifacts are swept")
	limit := flags.Int("limit", 64, fmt.Sprintf("expired jobs deleted per call (1-%d)", artifactstore.MaxSweepJobs))
	if err := flags.Parse(args); err != nil {
		return exitError
	}
	if flags.NArg() != 0 {
		return failLocal(stderr, fmt.Errorf("unexpected argument %q", flags.Arg(0)))
	}
	if *root == "" {
		return failLocal(stderr, errors.New("--state-dir is required"))
	}
	if *limit <= 0 || *limit > artifactstore.MaxSweepJobs {
		return failLocal(stderr, fmt.Errorf("--limit must be between 1 and %d", artifactstore.MaxSweepJobs))
	}
	if err := requirePrivateOwnedDirectory(*root); err != nil {
		return failLocal(stderr, err)
	}
	store, err := artifactstore.Open(filepath.Join(*root, "artifacts"), artifactstore.MaxObjectBytesHard)
	if err != nil {
		return failLocal(stderr, err)
	}
	report, err := store.Sweep(context.Background(), now(), *limit)
	if err != nil {
		return failLocal(stderr, err)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return failLocal(stderr, errors.New("encode sweep report"))
	}
	fmt.Fprintln(stdout, string(encoded))
	return exitOK
}
