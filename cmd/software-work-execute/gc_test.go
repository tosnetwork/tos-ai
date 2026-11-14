package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
)

func TestArtifactsGCDeletesOnlyExpiredRetention(t *testing.T) {
	root := privateDir(t)
	store, err := artifactstore.Open(filepath.Join(root, "artifacts"), artifactstore.MaxObjectBytesHard)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	old, err := store.PutFor(ctx, "sha256:"+strings.Repeat("a", 64), "application/octet-stream", strings.NewReader("old"))
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := store.PutFor(ctx, "sha256:"+strings.Repeat("b", 64), "application/octet-stream", strings.NewReader("fresh"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2_000_000_000, 0)
	if err := store.Retain(ctx, "sha256:"+strings.Repeat("a", 64), now.Add(-time.Minute), []string{old.Digest}); err != nil {
		t.Fatal(err)
	}
	if err := store.Retain(ctx, "sha256:"+strings.Repeat("b", 64), now.Add(time.Minute), []string{fresh.Digest}); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if code := sweepArtifacts([]string{"--state-dir", root, "--limit", "8"}, &stdout, io.Discard, func() time.Time { return now }); code != exitOK {
		t.Fatalf("exit = %d", code)
	}
	var report artifactstore.SweepReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || report.DeletedJobs != 1 || report.DeletedObjects != 1 || report.Live != 1 {
		t.Fatalf("report = %s %v", stdout.String(), err)
	}
	if _, err := store.Get(ctx, old.Digest); err == nil {
		t.Fatal("expired artifact survived")
	}
	if _, err := store.Get(ctx, fresh.Digest); err != nil {
		t.Fatalf("unexpired artifact deleted: %v", err)
	}
	// Malformed metadata refuses the sweep with a failing exit.
	if err := os.WriteFile(filepath.Join(root, "artifacts", "retention", strings.Repeat("c", 64)+".json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := sweepArtifacts([]string{"--state-dir", root}, io.Discard, io.Discard, func() time.Time { return now.Add(time.Hour) }); code != exitError {
		t.Fatalf("malformed metadata exit = %d", code)
	}
	if _, err := store.Get(ctx, fresh.Digest); err != nil {
		t.Fatal("a refused sweep deleted an artifact")
	}
}

func TestArtifactsGCRefusesBadInvocations(t *testing.T) {
	root := privateDir(t)
	for name, args := range map[string][]string{
		"no state dir":   nil,
		"relative":       {"--state-dir", "state"},
		"zero limit":     {"--state-dir", root, "--limit", "0"},
		"huge limit":     {"--state-dir", root, "--limit", "100000"},
		"extra argument": {"--state-dir", root, "now"},
	} {
		if code := run(append([]string{"artifacts-gc"}, args...), io.Discard, io.Discard); code != exitError {
			t.Errorf("%s: exit = %d", name, code)
		}
	}
	if code := run([]string{"artifacts-gc", "--state-dir", root, "--force"}, io.Discard, io.Discard); code != exitUsage {
		t.Errorf("unknown flag: exit = %d", code)
	}
	if code := run([]string{"artifacts-gc", "-h"}, io.Discard, io.Discard); code != exitOK {
		t.Errorf("help: exit = %d", code)
	}
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"artifacts-gc", "--state-dir", root}, io.Discard, io.Discard); code != exitError {
		t.Fatal("a group-accessible state directory was swept")
	}
}

func TestLocalModeRefusesUnexpectedArguments(t *testing.T) {
	for name, args := range map[string][]string{
		"positional":        {"payd"},
		"negative retain":   {"--retain-until", "-1"},
		"trailing argument": {"--state-dir", "/x", "extra"},
	} {
		if code := run(args, io.Discard, io.Discard); code != exitError {
			t.Errorf("%s: exit = %d", name, code)
		}
	}
	if code := run([]string{"--no-such-flag"}, io.Discard, io.Discard); code != exitUsage {
		t.Errorf("unknown flag: exit = %d", code)
	}
	if code := run([]string{"-help"}, io.Discard, io.Discard); code != exitOK {
		t.Errorf("help: exit = %d", code)
	}
}
