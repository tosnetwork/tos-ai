package messengereventbridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"syscall"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/tosnetwork/tos-ai/internal/dirlock"
	"github.com/tosnetwork/tos-ai/pkg/mcpadapter"
	"github.com/tosnetwork/tos-messenger/pkg/envelope"
)

const (
	resultRecordSchema = "tos.service.messenger-result-outbox.v2"
	resultStatePending = "pending"
	resultStateDone    = "complete"
	maxResultBytes     = envelope.MaxContentBytes
	maxRecordBytes     = maxResultBytes + 32<<10
)

var (
	eventIDPattern = regexp.MustCompile(`^evt_[0-9a-f]{64}$`)
	digestPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type resultRecord struct {
	Schema              string `json:"schema"`
	SourceEventID       string `json:"source_event_id"`
	ConversationID      string `json:"conversation_id"`
	SenderAgentID       string `json:"sender_agent_id"`
	SourceCreatedAtUnix uint64 `json:"source_created_at_unix"`
	ResultKind          string `json:"result_kind"`
	ResultJSON          []byte `json:"result_json"`
	ResultDigest        string `json:"result_digest"`
	State               string `json:"state"`
}

type PendingResult struct {
	SourceEventID       string
	ConversationID      string
	SenderAgentID       string
	SourceCreatedAtUnix uint64
	ResultKind          string
	ResultJSON          []byte
	ResultDigest        string
}

// ResultOutbox makes the handler's result-before-202 promise durable. One
// process owns the directory, and one source Event ID can name only one exact
// result for its lifetime.
type ResultOutbox struct {
	root  string
	lock  *dirlock.Lock
	mutex sync.Mutex
}

// MCPResultJournal durably commits remotely returned MCP results without
// exposing them to ResultPublisher. It deliberately uses a distinct owned
// directory from the outbound result outbox.
type MCPResultJournal struct {
	store *ResultOutbox
}

func OpenMCPResultJournal(root string) (*MCPResultJournal, error) {
	store, err := OpenResultOutbox(root)
	if err != nil {
		return nil, err
	}
	return &MCPResultJournal{store: store}, nil
}

func (j *MCPResultJournal) ReceiveMCPResult(_ context.Context, event envelope.Event, output mcpadapter.Output) error {
	if j == nil || j.store == nil {
		return errors.New("invalid MCP result journal")
	}
	raw, err := json.Marshal(output)
	if err != nil {
		return errors.New("encode MCP result")
	}
	if err := j.store.enqueue(event, "mcp.result", raw); err != nil {
		return err
	}
	return j.store.Complete(event.EventID, resultDigest(raw))
}

func (j *MCPResultJournal) Close() error {
	if j == nil || j.store == nil {
		return nil
	}
	return j.store.Close()
}

func OpenResultOutbox(root string) (*ResultOutbox, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("invalid Messenger result outbox root")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, errors.New("create Messenger result outbox root")
	}
	info, err := os.Lstat(root)
	stat, valid := fileStat(info)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 ||
		!valid || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("Messenger result outbox root must be private and owned")
	}
	ownership, err := dirlock.Acquire(root, ".messenger-result-outbox.lock")
	if err != nil {
		return nil, errors.New("Messenger result outbox is already owned")
	}
	return &ResultOutbox{root: root, lock: ownership}, nil
}

func (o *ResultOutbox) ReceiveA2AResult(_ context.Context, event envelope.Event, task *a2a.Task) error {
	if task == nil {
		return errors.New("invalid A2A result")
	}
	raw, err := json.Marshal(task)
	if err != nil {
		return errors.New("encode A2A result")
	}
	return o.enqueue(event, "a2a.message", raw)
}

func (o *ResultOutbox) ReceiveMCPResult(_ context.Context, event envelope.Event, output mcpadapter.Output) error {
	raw, err := json.Marshal(output)
	if err != nil {
		return errors.New("encode MCP result")
	}
	return o.enqueue(event, "mcp.result", raw)
}

func (o *ResultOutbox) enqueue(event envelope.Event, kind string, result []byte) error {
	if o == nil || !eventIDPattern.MatchString(event.EventID) || event.ConversationID == "" ||
		event.SenderAgentID == "" || event.CreatedAtUnix == 0 || (kind != "a2a.message" && kind != "mcp.result") || len(result) == 0 || len(result) > maxResultBytes {
		return errors.New("invalid Messenger result outbox entry")
	}
	record := resultRecord{Schema: resultRecordSchema, SourceEventID: event.EventID,
		ConversationID: event.ConversationID, SenderAgentID: event.SenderAgentID, SourceCreatedAtUnix: event.CreatedAtUnix, ResultKind: kind,
		ResultJSON: append([]byte(nil), result...), ResultDigest: resultDigest(result), State: resultStatePending}
	encoded, err := json.Marshal(record)
	if err != nil || len(encoded) > maxRecordBytes {
		return errors.New("encode Messenger result outbox entry")
	}
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.lock == nil {
		return errors.New("Messenger result outbox is closed")
	}
	path := o.path(event.EventID)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if err := writeSyncClose(file, append(encoded, '\n')); err != nil {
			_ = os.Remove(path)
			return err
		}
		if err := syncResultDirectory(o.root); err != nil {
			_ = os.Remove(path)
			return err
		}
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return errors.New("create Messenger result outbox entry")
	}
	existing, err := readResultRecord(path)
	if err != nil {
		return err
	}
	if !sameResult(existing, record) {
		return errors.New("source Messenger Event conflicts with another result")
	}
	return nil
}

func (o *ResultOutbox) Pending() ([]PendingResult, error) {
	if o == nil {
		return nil, errors.New("invalid Messenger result outbox")
	}
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.lock == nil {
		return nil, errors.New("Messenger result outbox is closed")
	}
	entries, err := os.ReadDir(o.root)
	if err != nil {
		return nil, errors.New("list Messenger result outbox")
	}
	results := make([]PendingResult, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		record, err := readResultRecord(filepath.Join(o.root, entry.Name()))
		if err != nil {
			return nil, err
		}
		if record.State != resultStatePending {
			continue
		}
		results = append(results, pendingResult(record))
	}
	sort.Slice(results, func(i, j int) bool { return results[i].SourceEventID < results[j].SourceEventID })
	return results, nil
}

func (o *ResultOutbox) Complete(sourceEventID, resultDigestValue string) error {
	if o == nil || !eventIDPattern.MatchString(sourceEventID) || !digestPattern.MatchString(resultDigestValue) {
		return errors.New("invalid Messenger result completion")
	}
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.lock == nil {
		return errors.New("Messenger result outbox is closed")
	}
	path := o.path(sourceEventID)
	record, err := readResultRecord(path)
	if err != nil || record.ResultDigest != resultDigestValue {
		return errors.New("Messenger result completion does not match pending output")
	}
	if record.State == resultStateDone {
		return nil
	}
	record.State = resultStateDone
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(o.root, ".complete-")
	if err != nil {
		return errors.New("create Messenger result completion")
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return errors.New("protect Messenger result completion")
	}
	if err := writeSyncClose(temporary, append(encoded, '\n')); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return errors.New("commit Messenger result completion")
	}
	return syncResultDirectory(o.root)
}

func (o *ResultOutbox) Close() error {
	if o == nil {
		return nil
	}
	o.mutex.Lock()
	lock := o.lock
	o.lock = nil
	o.mutex.Unlock()
	if lock == nil {
		return nil
	}
	return lock.Close()
}

func (o *ResultOutbox) path(eventID string) string {
	return filepath.Join(o.root, eventID[len("evt_"):]+".json")
}

func pendingResult(record resultRecord) PendingResult {
	return PendingResult{SourceEventID: record.SourceEventID, ConversationID: record.ConversationID,
		SenderAgentID: record.SenderAgentID, SourceCreatedAtUnix: record.SourceCreatedAtUnix, ResultKind: record.ResultKind,
		ResultJSON: append([]byte(nil), record.ResultJSON...), ResultDigest: record.ResultDigest}
}

func readResultRecord(path string) (resultRecord, error) {
	info, err := os.Lstat(path)
	stat, valid := fileStat(info)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 ||
		info.Size() <= 0 || info.Size() > maxRecordBytes || !valid || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return resultRecord{}, errors.New("invalid Messenger result outbox record")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return resultRecord{}, errors.New("read Messenger result outbox record")
	}
	var record resultRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return resultRecord{}, errors.New("decode Messenger result outbox record")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) || !validResultRecord(record) {
		return resultRecord{}, errors.New("invalid Messenger result outbox record")
	}
	return record, nil
}

func validResultRecord(record resultRecord) bool {
	return record.Schema == resultRecordSchema && eventIDPattern.MatchString(record.SourceEventID) &&
		record.ConversationID != "" && record.SenderAgentID != "" && record.SourceCreatedAtUnix > 0 &&
		(record.ResultKind == "a2a.message" || record.ResultKind == "mcp.result") &&
		len(record.ResultJSON) > 0 && len(record.ResultJSON) <= maxResultBytes &&
		digestPattern.MatchString(record.ResultDigest) && record.ResultDigest == resultDigest(record.ResultJSON) &&
		(record.State == resultStatePending || record.State == resultStateDone)
}

func sameResult(left, right resultRecord) bool {
	return left.SourceEventID == right.SourceEventID && left.ConversationID == right.ConversationID &&
		left.SenderAgentID == right.SenderAgentID && left.SourceCreatedAtUnix == right.SourceCreatedAtUnix && left.ResultKind == right.ResultKind &&
		left.ResultDigest == right.ResultDigest && bytes.Equal(left.ResultJSON, right.ResultJSON)
}

func resultDigest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func writeSyncClose(file *os.File, raw []byte) error {
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return errors.New("write Messenger result record")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("sync Messenger result record")
	}
	if err := file.Close(); err != nil {
		return errors.New("close Messenger result record")
	}
	return nil
}

func syncResultDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("open Messenger result directory")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("sync Messenger result directory")
	}
	return nil
}

func fileStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

var _ A2AResultReceiver = (*ResultOutbox)(nil)
var _ MCPResultReceiver = (*ResultOutbox)(nil)
