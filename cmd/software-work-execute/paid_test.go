package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
	"github.com/tosnetwork/tos-ai/pkg/artifactstore"
	"github.com/tosnetwork/tos-ai/pkg/railgate"
	"github.com/tosnetwork/tos-ai/pkg/softwarework"
	nativev1 "github.com/tosnetwork/tos-service-protocol/gen/tos/service/v1"
	"github.com/tosnetwork/tos-service-protocol/pkg/nativecore"
	"github.com/tosnetwork/tos-service-protocol/pkg/toschain"
)

var (
	testActor    = "0:" + strings.Repeat("77", 32)
	testBuyer    = "0:" + strings.Repeat("21", 32)
	testManifest = "sha256:" + strings.Repeat("ab", 32)
	testQuote    = "tvm-cell-sha256:" + strings.Repeat("cd", 32)
	testAgent    = "agent_" + strings.Repeat("44", 32)
)

func TestPaidExitCodesAreDistinctAndOnlyCompletionIsSuccess(t *testing.T) {
	// Unavailable shares exit 1 with a refused run: both mean nothing was
	// admitted and the run may be retried.
	codes := map[int]string{exitRefused: statusRefused, exitUsage: statusUsage}
	for _, status := range []railgate.Status{railgate.StatusCompleted, railgate.StatusAlreadyTaken, railgate.StatusNotPendingNow,
		railgate.StatusAmbiguous, railgate.StatusNotAdmitted, railgate.StatusExecutionFailed, railgate.StatusRefusedBeforeExecution} {
		code := exitCodeFor(status)
		if previous, taken := codes[code]; taken {
			t.Fatalf("%s and %s share exit code %d", status, previous, code)
		}
		codes[code] = string(status)
	}
	want := map[railgate.Status]int{railgate.StatusCompleted: 0, railgate.StatusAlreadyTaken: 3, railgate.StatusNotPendingNow: 4,
		railgate.StatusAmbiguous: 5, railgate.StatusNotAdmitted: 6, railgate.StatusExecutionFailed: 7,
		railgate.StatusRefusedBeforeExecution: 8, railgate.StatusUnavailable: 1}
	for status, code := range want {
		if got := exitCodeFor(status); got != code {
			t.Errorf("%s = %d, documented %d", status, got, code)
		}
	}
	if exitCodeFor("something-new") != exitAmbiguous {
		t.Fatal("an unknown status is not treated as ambiguous")
	}
}

func decodeReport(t *testing.T, output []byte) paidReport {
	t.Helper()
	var report paidReport
	if err := json.Unmarshal(output, &report); err != nil {
		t.Fatalf("status is not JSON: %v: %s", err, output)
	}
	return report
}

func validPaidArgs(directory string) []string {
	return []string{"--rail-config", filepath.Join(directory, "rail.json"), "--containerd-socket", filepath.Join(directory, "sock"),
		"--fifo-dir", filepath.Join(directory, "fifo"), "--state-dir", directory, "--source", filepath.Join(directory, "source.tar"),
		"--quote-package", filepath.Join(directory, "quote.pb"), "--buyer-wallet", testBuyer, "--actor", testActor,
		"--request-id", "0", "--call-lt", "10", "--call-hash", strings.Repeat("0a", 32),
		"--execution-id", "sha256:" + strings.Repeat("01", 32), "--input-digest", "sha256:" + strings.Repeat("02", 32)}
}

func TestRunPaidRefusesIncompleteOrMalformedInputs(t *testing.T) {
	directory := privateDir(t)
	base := validPaidArgs(directory)
	cases := map[string][]string{}
	for index := 0; index < len(base); index += 2 {
		args := append(append([]string(nil), base[:index]...), base[index+2:]...)
		cases["missing "+base[index]] = args
	}
	replace := func(flag, value string) []string {
		args := append([]string(nil), base...)
		for index := range args {
			if args[index] == flag {
				args[index+1] = value
			}
		}
		return args
	}
	cases["padded request id"] = replace("--request-id", "01")
	cases["negative request id"] = replace("--request-id", "-1")
	cases["overflowing request id"] = replace("--request-id", "18446744073709551616")
	cases["zero call lt"] = replace("--call-lt", "0")
	cases["upper-case call hash"] = replace("--call-hash", strings.Repeat("0A", 32))
	cases["bare execution id"] = replace("--execution-id", strings.Repeat("01", 32))
	cases["zero input digest"] = replace("--input-digest", "sha256:"+strings.Repeat("00", 32))
	cases["friendly actor"] = replace("--actor", "EQ"+strings.Repeat("A", 46))
	cases["extra argument"] = append(append([]string(nil), base...), "again")
	// Every input is well formed but the files do not exist: the run is
	// still refused before anything is admitted.
	cases["absent files"] = base
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var stdout bytes.Buffer
			if code := run(append([]string{"paid"}, args...), &stdout, io.Discard); code != exitRefused {
				t.Fatalf("exit = %d, want %d", code, exitRefused)
			}
			if report := decodeReport(t, stdout.Bytes()); report.Status != statusRefused || report.Error == "" {
				t.Fatalf("report = %+v", report)
			}
		})
	}
}

func TestRunPaidKeepsConventionalUsageExitCodes(t *testing.T) {
	var stdout bytes.Buffer
	if code := run([]string{"paid", "--no-such-flag"}, &stdout, io.Discard); code != exitUsage {
		t.Fatalf("unknown flag: exit = %d", code)
	}
	if report := decodeReport(t, stdout.Bytes()); report.Status != statusUsage || report.ExitCode != exitUsage {
		t.Fatalf("report = %+v", report)
	}
	if code := run([]string{"paid", "-h"}, io.Discard, io.Discard); code != exitOK {
		t.Fatalf("help: exit = %d", code)
	}
}

func TestParsePaidInputsAcceptsACompleteRequest(t *testing.T) {
	in, err := parsePaidInputs(validPaidArgs(privateDir(t)), io.Discard)
	if err != nil || in.requestID != 0 || in.callLT != 10 || in.actor != testActor {
		t.Fatalf("inputs = %+v %v", in, err)
	}
}

func TestParseCanonicalUint(t *testing.T) {
	for value, want := range map[string]uint64{"0": 0, "7": 7, "18446744073709551615": math.MaxUint64} {
		if got, err := parseCanonicalUint(value); err != nil || got != want {
			t.Errorf("%q = %d %v", value, got, err)
		}
	}
	for _, bad := range []string{"", "00", "01", "+1", "1e3", " 1", "18446744073709551616", "99999999999999999999"} {
		if _, err := parseCanonicalUint(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func writeConfig(t *testing.T, directory string, body string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(directory, "rail.json")
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func railConfigJSON(edit func(map[string]any)) string {
	config := map[string]any{
		"schema": railConfigSchema,
		"network": map[string]any{"network_id": "tos-testnet", "genesis_root_hash": "sha256:" + strings.Repeat("11", 32),
			"genesis_file_hash": "sha256:" + strings.Repeat("22", 32)},
		"rpc_endpoints":          []string{"https://a.example:1", "https://b.example:1", "https://c.example:1"},
		"quorum":                 2,
		"registry_code_boc_file": "/etc/tos-service/registry.boc",
		"registry_code_hash":     "tvm-cell-sha256:" + strings.Repeat("33", 32),
		"actor_code_hash":        "tvm-cell-sha256:" + strings.Repeat("88", 32),
		"provider_agent_id":      testAgent,
		"capability_id":          "cap_" + strings.Repeat("33", 32),
		"version":                "1.0.0",
		"manifest_file":          "/etc/tos-service/manifest.cbor",
		"respond_budget_seconds": 120,
	}
	if edit != nil {
		edit(config)
	}
	encoded, _ := json.Marshal(config)
	return string(encoded)
}

func TestLoadRailConfigFailsClosed(t *testing.T) {
	directory := privateDir(t)
	config, err := loadRailConfig(writeConfig(t, directory, railConfigJSON(nil), 0o600))
	if err != nil || config.Quorum != 2 || config.RespondBudgetSeconds != 120 {
		t.Fatalf("valid config = %+v %v", config, err)
	}
	for name, body := range map[string]string{
		"unknown field":      railConfigJSON(func(c map[string]any) { c["signer"] = "x" }),
		"wrong schema":       railConfigJSON(func(c map[string]any) { c["schema"] = "v0" }),
		"no endpoints":       railConfigJSON(func(c map[string]any) { c["rpc_endpoints"] = []string{} }),
		"quorum too large":   railConfigJSON(func(c map[string]any) { c["quorum"] = 4 }),
		"negative quorum":    railConfigJSON(func(c map[string]any) { c["quorum"] = -1 }),
		"relative manifest":  railConfigJSON(func(c map[string]any) { c["manifest_file"] = "manifest.cbor" }),
		"unclean registry":   railConfigJSON(func(c map[string]any) { c["registry_code_boc_file"] = "/etc/../registry.boc" }),
		"no respond budget":  railConfigJSON(func(c map[string]any) { delete(c, "respond_budget_seconds") }),
		"no actor code hash": railConfigJSON(func(c map[string]any) { delete(c, "actor_code_hash") }),
		"no provider":        railConfigJSON(func(c map[string]any) { delete(c, "provider_agent_id") }),
		"partial network":    railConfigJSON(func(c map[string]any) { c["network"] = map[string]any{"network_id": "tos-testnet"} }),
		"trailing data":      railConfigJSON(nil) + "{}",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadRailConfig(writeConfig(t, privateDir(t), body, 0o600)); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if _, err := loadRailConfig(writeConfig(t, privateDir(t), railConfigJSON(nil), 0o644)); err == nil {
		t.Fatal("a world-readable config was accepted")
	}
}

type failingReader struct{ opened int }

func (r *failingReader) BeginSession(context.Context) (railgate.Session, error) {
	r.opened++
	return nil, errors.New("no finalized endpoint answered")
}

func testGate(t *testing.T, reader railgate.Reader) *railgate.Gate {
	t.Helper()
	verifier, err := railgate.NewVerifier(railgate.VerifierConfig{Reader: reader, Manifests: staticManifest{digest: testManifest},
		Network:       &nativev1.NetworkDomain{NetworkId: "tos-testnet"},
		ActorCodeHash: "tvm-cell-sha256:" + strings.Repeat("88", 32), ProviderAgentID: testAgent,
		CapabilityID: "cap_" + strings.Repeat("33", 32), Version: "1.0.0", ExecutionBudget: 180 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := railgate.OpenGate(verifier, filepath.Join(privateDir(t), "slots"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	return gate
}

func testStore(t *testing.T, options ...artifactstore.Option) *artifactstore.Store {
	t.Helper()
	store, err := artifactstore.Open(filepath.Join(privateDir(t), "artifacts"), 1<<20, options...)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// At the record bound a paid run is refused before admission: no slot is
// taken and nothing runs, so the request stays claimable elsewhere.
func TestPaidIsRefusedBeforeAdmissionAtTheRecordBound(t *testing.T) {
	store := testStore(t, artifactstore.WithRecordBound(1))
	ctx := context.Background()
	retainedJob := "sha256:" + strings.Repeat("e", 64)
	object, err := store.PutFor(ctx, retainedJob, "application/octet-stream", strings.NewReader("retained"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Retain(ctx, retainedJob, time.Now().Add(time.Hour), []string{object.Digest}); err != nil {
		t.Fatal(err)
	}
	reader := &failingReader{}
	ran := false
	job := &paidJob{gate: testGate(t, reader), actor: testActor, manifestDigest: testManifest, store: store,
		claim: railgate.Claim{BuyerWallet: testBuyer, RequestID: 3},
		run: func(context.Context, softwarework.Request) (softwarework.Outcome, error) {
			ran = true
			return softwarework.Outcome{}, nil
		}}
	report := job.executeOnce(ctx)
	if report.Status != statusRefused || report.ExitCode != exitRefused || report.SlotState != "" || ran ||
		!strings.Contains(report.Error, "retain") {
		t.Fatalf("report = %+v ran=%v", report, ran)
	}
	// With room the same job reaches admission (and is refused there).
	job.store = testStore(t, artifactstore.WithRecordBound(1))
	if report := job.executeOnce(ctx); report.Status != string(railgate.StatusNotAdmitted) {
		t.Fatalf("with room = %+v", report)
	}
}

// Every retention lock wait in a paid run ends while the respond budget is
// still ahead of the response deadline, well before the store's own cap,
// and a wait that runs out never reads as success.
func TestPaidLockWaitsAreBoundedByTheDeadlineSlack(t *testing.T) {
	f := newPaidFixture(t)
	f.verified.ResponseDeadline = uint64(time.Now().Add(f.job.respondBudget + 400*time.Millisecond).Unix())
	holder, err := dirlock.Acquire(f.storeRoot, ".retention.lock")
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	started := time.Now()
	hash, err := f.job.execute(context.Background(), f.verified)
	elapsed := time.Since(started)
	if err == nil || hash != "" || f.job.receipt != nil {
		t.Fatalf("a run whose lock wait outlived the slack succeeded: %q %v", hash, err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the lock wait took %v; the slack was under 2 s", elapsed)
	}
}

func TestPaidJobNeverExecutesAClaimThatIsNotAdmitted(t *testing.T) {
	reader := &failingReader{}
	ran := false
	job := &paidJob{gate: testGate(t, reader), actor: testActor, manifestDigest: testManifest, store: testStore(t),
		claim: railgate.Claim{Package: &nativev1.QuoteProposalPackageV1{Proposal: &nativev1.QuoteProposalV1{}}, BuyerWallet: testBuyer,
			ExecutionID: [32]byte{1}, InputDigest: [32]byte{2}, Call: toschain.ActorTxID{LT: 10, Hash: strings.Repeat("0a", 32)}},
		run: func(context.Context, softwarework.Request) (softwarework.Outcome, error) {
			ran = true
			return softwarework.Outcome{}, nil
		}}
	report := job.executeOnce(context.Background())
	var stdout bytes.Buffer
	code := writeReport(&stdout, report)
	decoded := decodeReport(t, stdout.Bytes())
	if code != exitNotAdmitted || decoded.Status != string(railgate.StatusNotAdmitted) || decoded.ExitCode != exitNotAdmitted ||
		decoded.Error == "" || decoded.ResponseHash != "" || ran {
		t.Fatalf("exit=%d report=%+v ran=%v", code, decoded, ran)
	}
}

// paidFixture is a verified request and a store a fake runner writes to.
type paidFixture struct {
	job       *paidJob
	verified  *railgate.Verified
	store     *artifactstore.Store
	storeRoot string
	runs      int
	exitCode  int
}

func newPaidFixture(t *testing.T) *paidFixture {
	t.Helper()
	storeRoot := filepath.Join(privateDir(t), "artifacts")
	store, err := artifactstore.Open(storeRoot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	contract := softwarework.FrozenV1Contract()
	contract.ManifestDigest, contract.SandboxDigest = testManifest, testManifest
	archive := []byte("source archive")
	f := &paidFixture{store: store, storeRoot: storeRoot}
	f.verified = &railgate.Verified{Actor: testActor, RequestID: 5, QuoteCommitment: testQuote, ExecutionID: [32]byte{1},
		InputDigest: [32]byte{2}, ProviderAgentID: testAgent, ManifestDigest: testManifest,
		Manifest: nativecore.SoftwareWorkManifestV1{ToolchainDigest: contract.ToolchainDigest, RetentionSeconds: 86400},
		Price:    big.NewInt(300_000_000), ResponseDeadline: 2_000_003_600}
	f.job = &paidJob{actor: testActor, manifestDigest: testManifest, contract: contract, archive: archive,
		sourceDigest: sha256Digest(archive), store: store, claim: railgate.Claim{RequestID: 5}, respondBudget: 30 * time.Second}
	f.job.run = func(ctx context.Context, request softwarework.Request) (softwarework.Outcome, error) {
		f.runs++
		report, err := store.PutFor(ctx, request.ExecutionID, softwarework.ReportMediaType, strings.NewReader("report"))
		if err != nil {
			return softwarework.Outcome{}, err
		}
		artifact, err := store.PutFor(ctx, request.ExecutionID, softwarework.ArtifactMediaType, strings.NewReader("artifact"))
		if err != nil {
			return softwarework.Outcome{}, err
		}
		return softwarework.Outcome{QuoteCommitment: request.QuoteCommitment, ExecutionID: request.ExecutionID,
			InputDigest: request.InputDigest, ResultDigest: "sha256:" + strings.Repeat("ee", 32), Artifact: artifact, Report: report,
			SourceDigest: request.SourceDigest, ToolchainDigest: contract.ToolchainDigest, SandboxDigest: contract.SandboxDigest,
			ExitCode: f.exitCode, CompletedAtUnix: 2_000_000_100}, nil
	}
	return f
}

func TestPaidExecuteBindsTheReceiptAndRetentionToTheVerifiedRequest(t *testing.T) {
	f := newPaidFixture(t)
	hash, err := f.job.execute(context.Background(), f.verified)
	if err != nil || f.runs != 1 {
		t.Fatalf("execute = %q %v runs=%d", hash, err, f.runs)
	}
	outcome := f.job.outcome
	root, _, err := nativecore.BuildSoftwareWorkReceiptCellV1(nativecore.SoftwareWorkReceiptV1{
		QuoteCommitment: testQuote, ExecutionID: sha256Text([32]byte{1}), InputDigest: sha256Text([32]byte{2}),
		ResultDigest: outcome.ResultDigest, ArtifactDigest: outcome.Artifact.Digest, ReportDigest: outcome.Report.Digest,
		SourceDigest: f.job.sourceDigest, ToolchainDigest: f.job.contract.ToolchainDigest, SandboxDigest: testManifest,
		ChargedAtomicAmount: "300000000", ProviderAgentID: testAgent, CompletedAt: 2_000_000_100})
	if err != nil {
		t.Fatal(err)
	}
	if hash != hex.EncodeToString(root.Hash()) || !bytes.Equal(f.job.receipt, root.ToBOC()) {
		t.Fatal("the response hash is not the Receipt of the verified request")
	}
	if f.job.retainUntil != 2_000_003_600+86400 {
		t.Fatalf("retain until = %d", f.job.retainUntil)
	}
	// Retention is recorded: kept through its deadline, deleted after it.
	kept, err := f.store.Sweep(context.Background(), time.Unix(f.job.retainUntil, 0), 8)
	if err != nil || kept.Live != 1 || kept.DeletedJobs != 0 {
		t.Fatalf("before the deadline: %+v %v", kept, err)
	}
	swept, err := f.store.Sweep(context.Background(), time.Unix(f.job.retainUntil+1, 0), 8)
	if err != nil || swept.DeletedJobs != 1 || swept.DeletedObjects != 2 {
		t.Fatalf("after the deadline: %+v %v", swept, err)
	}
}

func TestPaidExecuteRefusesBeforeRunningAnythingItCannotBind(t *testing.T) {
	for name, edit := range map[string]func(*paidFixture){
		"other actor":       func(f *paidFixture) { f.verified.Actor = testBuyer },
		"other request":     func(f *paidFixture) { f.verified.RequestID = 6 },
		"other manifest":    func(f *paidFixture) { f.verified.ManifestDigest = "sha256:" + strings.Repeat("ba", 32) },
		"other toolchain":   func(f *paidFixture) { f.verified.Manifest.ToolchainDigest = "sha256:" + strings.Repeat("99", 32) },
		"no retention":      func(f *paidFixture) { f.verified.Manifest.RetentionSeconds = 0 },
		"no price":          func(f *paidFixture) { f.verified.Price = nil },
		"zero price":        func(f *paidFixture) { f.verified.Price = big.NewInt(0) },
		"retention overrun": func(f *paidFixture) { f.verified.ResponseDeadline = math.MaxInt64 },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPaidFixture(t)
			edit(f)
			// The pre-execution check refuses it, so the slot is abandoned
			// rather than started.
			if err := f.job.check(f.verified); err == nil {
				t.Fatal("the pre-execution check accepted it")
			}
			if _, err := f.job.execute(context.Background(), f.verified); err == nil || f.runs != 0 {
				t.Fatalf("err=%v runs=%d", err, f.runs)
			}
		})
	}
	if err := newPaidFixture(t).job.check(newPaidFixture(t).verified); err != nil {
		t.Fatalf("the check refused a servable request: %v", err)
	}
	if _, err := newPaidFixture(t).job.execute(context.Background(), nil); err == nil {
		t.Fatal("executed without a verified request")
	}
}

// A sweep that holds the retention lock while a paid run finishes, or runs
// concurrently with it, neither fails the run nor deletes the artifact it
// shares with an expired job.
func TestPaidExecuteSurvivesConcurrentSweeps(t *testing.T) {
	f := newPaidFixture(t)
	ctx := context.Background()
	expiredJob := "sha256:" + strings.Repeat("e", 64)
	shared, err := f.store.PutFor(ctx, expiredJob, softwarework.ArtifactMediaType, strings.NewReader("artifact"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.Retain(ctx, expiredJob, time.Unix(1000, 0), []string{shared.Digest}); err != nil {
		t.Fatal(err)
	}
	// Another process holds the lock when the run starts and releases it
	// shortly after.
	holder, err := dirlock.Acquire(f.storeRoot, ".retention.lock")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = holder.Close()
	}()
	stop := make(chan struct{})
	swept := make(chan error, 1)
	go func() {
		var sweepErr error
		for {
			select {
			case <-stop:
				swept <- sweepErr
				return
			default:
			}
			if _, err := f.store.Sweep(ctx, time.Now(), 4); err != nil && !errors.Is(err, artifactstore.ErrRetentionBusy) {
				sweepErr = err
			}
		}
	}()
	hash, err := f.job.execute(ctx, f.verified)
	close(stop)
	if sweepErr := <-swept; sweepErr != nil {
		t.Fatalf("sweep: %v", sweepErr)
	}
	if err != nil || hash == "" || f.job.outcome.Artifact.Digest != shared.Digest {
		t.Fatalf("a concurrent sweep failed the paid run: %q %v", hash, err)
	}
	if _, err := f.store.Sweep(ctx, time.Now(), 8); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Get(ctx, shared.Digest); err != nil {
		t.Fatalf("the paid run's artifact was deleted: %v", err)
	}
}

func TestPaidExecuteNeverHashesAnUnsuccessfulOrUnstoredResult(t *testing.T) {
	failed := newPaidFixture(t)
	failed.exitCode = 1
	hash, err := failed.job.execute(context.Background(), failed.verified)
	if err == nil || !strings.Contains(err.Error(), "not a successful run") || hash != "" || failed.job.receipt != nil {
		t.Fatalf("a failing run was hashed: %q %v", hash, err)
	}
	// Its objects still get the job's deadline, so they do not stay pending
	// (kept forever) after the slot failed.
	if report, err := failed.store.Sweep(context.Background(), time.Unix(2_000_003_600+86400+1, 0), 8); err != nil || report.DeletedJobs != 1 {
		t.Fatalf("a failing run's objects were left pending: %+v %v", report, err)
	}
	tampered := newPaidFixture(t)
	inner := tampered.job.run
	tampered.job.run = func(ctx context.Context, request softwarework.Request) (softwarework.Outcome, error) {
		outcome, err := inner(ctx, request)
		if err == nil {
			// The stored artifact keeps its name and mode but not its bytes.
			path := filepath.Join(tampered.storeRoot, "objects", strings.TrimPrefix(outcome.Artifact.Digest, "sha256:"))
			if chmodErr := os.Chmod(path, 0o600); chmodErr != nil {
				return outcome, chmodErr
			}
			if writeErr := os.WriteFile(path, []byte("swapped!"), 0o600); writeErr != nil {
				return outcome, writeErr
			}
			if chmodErr := os.Chmod(path, 0o400); chmodErr != nil {
				return outcome, chmodErr
			}
		}
		return outcome, err
	}
	if hash, err := tampered.job.execute(context.Background(), tampered.verified); err == nil || hash != "" {
		t.Fatalf("a tampered stored result was hashed: %q %v", hash, err)
	}
	missing := newPaidFixture(t)
	inner = missing.job.run
	missing.job.run = func(ctx context.Context, request softwarework.Request) (softwarework.Outcome, error) {
		outcome, err := inner(ctx, request)
		outcome.Artifact.Digest = "sha256:" + strings.Repeat("99", 32)
		return outcome, err
	}
	if hash, err := missing.job.execute(context.Background(), missing.verified); err == nil || hash != "" {
		t.Fatalf("an unstored result was hashed: %q %v", hash, err)
	}
	foreign := newPaidFixture(t)
	inner = foreign.job.run
	foreign.job.run = func(ctx context.Context, request softwarework.Request) (softwarework.Outcome, error) {
		outcome, err := inner(ctx, request)
		outcome.ExecutionID = "sha256:" + strings.Repeat("77", 32)
		return outcome, err
	}
	if _, err := foreign.job.execute(context.Background(), foreign.verified); err == nil {
		t.Fatal("an outcome of another execution was hashed")
	}
}

func TestRetentionDeadline(t *testing.T) {
	if got, err := retentionDeadline(100, 50); err != nil || got != 150 {
		t.Fatalf("= %d %v", got, err)
	}
	for _, bad := range [][2]uint64{{100, 0}, {0, 50}, {math.MaxInt64, 1}, {math.MaxUint64, 1}, {1, math.MaxInt64}} {
		if _, err := retentionDeadline(bad[0], bad[1]); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}

func TestStaticManifestServesOnlyTheConfiguredDigest(t *testing.T) {
	manifests := staticManifest{digest: testManifest, value: []byte("cbor")}
	if value, err := manifests.Manifest(context.Background(), testManifest); err != nil || string(value) != "cbor" {
		t.Fatalf("= %q %v", value, err)
	}
	if _, err := manifests.Manifest(context.Background(), "sha256:"+strings.Repeat("ba", 32)); err == nil {
		t.Fatal("served another manifest")
	}
}

func privateDir(t *testing.T) string {
	t.Helper()
	directory := resolvedTempDir(t)
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}
