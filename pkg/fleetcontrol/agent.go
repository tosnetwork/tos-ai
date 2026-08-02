// Package fleetcontrol provides the bounded terminal-side state machine used
// by a fleet controller. It never exposes a listener and never outranks local
// emergency, control, or real-time work.
package fleetcontrol

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
	bolt "go.etcd.io/bbolt"
)

const (
	CommandVersion = 1
	CommandDomain  = "tos.ai.fleet-command.v1"
	MaxQueueHard   = 4096
	MaxDrainHard   = 256
)

var (
	recordBucket      = []byte("fleet-record-v1")
	queueBucket       = []byte("fleet-queue-v1")
	metaBucket        = []byte("fleet-meta-v1")
	lastGenerationKey = []byte("last-generation")
	ErrRealtimeBusy   = errors.New("local real-time work has priority")
	ErrOffline        = errors.New("terminal is offline")
	ErrUncertain      = errors.New("fleet command outcome is uncertain")
)

type Command struct {
	Version       uint8  `json:"version" cbor:"version"`
	CommandID     string `json:"commandId" cbor:"commandId"`
	FleetID       string `json:"fleetId" cbor:"fleetId"`
	TerminalID    string `json:"terminalId" cbor:"terminalId"`
	Generation    uint64 `json:"generation" cbor:"generation"`
	Action        string `json:"action" cbor:"action"`
	ReleaseDigest string `json:"releaseDigest,omitempty" cbor:"releaseDigest,omitempty"`
	PolicyDigest  string `json:"policyDigest,omitempty" cbor:"policyDigest,omitempty"`
}

type Result struct {
	CommandID  string
	Generation uint64
	State      string
	Replay     bool
}

type Executor interface {
	Apply(context.Context, Command) error
}

type Config struct {
	DatabasePath   string
	FleetID        string
	TerminalID     string
	ControllerKeys map[string]ed25519.PublicKey
	Executor       Executor
	RealtimeBusy   func() bool
	Online         func() bool
	MaxQueued      int
	MaxRecords     int
	MaxDBBytes     int64
}

type Agent struct {
	db     *bolt.DB
	config Config
	keys   map[string]ed25519.PublicKey
	mu     sync.Mutex
	closed bool
}

type record struct {
	Fingerprint string  `json:"fingerprint"`
	Command     Command `json:"command"`
	State       string  `json:"state"`
	ExpiresAt   int64   `json:"expiresAt"`
	Sequence    uint64  `json:"sequence"`
}

func Open(config Config) (*Agent, error) {
	if config.MaxQueued == 0 {
		config.MaxQueued = 512
	}
	if config.MaxRecords == 0 {
		config.MaxRecords = 4096
	}
	if config.MaxDBBytes == 0 {
		config.MaxDBBytes = 64 << 20
	}
	if !filepath.IsAbs(config.DatabasePath) || config.DatabasePath != filepath.Clean(config.DatabasePath) ||
		!validID(config.FleetID) || !validID(config.TerminalID) || nilcheck.IsNil(config.Executor) ||
		config.RealtimeBusy == nil || config.Online == nil || config.MaxQueued <= 0 ||
		config.MaxQueued > MaxQueueHard || config.MaxDBBytes <= 0 || config.MaxDBBytes > 1<<30 ||
		config.MaxRecords <= 0 || config.MaxRecords > 65_536 || config.MaxRecords < config.MaxQueued ||
		len(config.ControllerKeys) == 0 || len(config.ControllerKeys) > 32 {
		return nil, errors.New("invalid fleet agent configuration")
	}
	keys := make(map[string]ed25519.PublicKey, len(config.ControllerKeys))
	for id, key := range config.ControllerKeys {
		if !validID(id) || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("invalid fleet trust root")
		}
		keys[id] = append(ed25519.PublicKey(nil), key...)
	}
	if err := os.MkdirAll(filepath.Dir(config.DatabasePath), 0o700); err != nil {
		return nil, err
	}
	parentInfo, err := os.Lstat(filepath.Dir(config.DatabasePath))
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 ||
		parentInfo.Mode().Perm() != 0o700 || !ownedByCurrentUser(parentInfo) {
		return nil, errors.New("fleet journal directory is not private")
	}
	if info, statErr := os.Lstat(config.DatabasePath); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
			info.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(info) {
			return nil, errors.New("fleet journal is not private")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, errors.New("inspect fleet journal")
	}
	db, err := bolt.Open(config.DatabasePath, 0o600, &bolt.Options{Timeout: time.Millisecond})
	if err != nil {
		return nil, errors.New("open fleet journal")
	}
	if err := os.Chmod(config.DatabasePath, 0o600); err != nil {
		_ = db.Close()
		return nil, errors.New("protect fleet journal")
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{recordBucket, queueBucket, metaBucket} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := reconcileInterrupted(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Agent{db: db, config: config, keys: keys}, nil
}

func (a *Agent) Submit(ctx context.Context, envelope identity.Envelope, now time.Time) (Result, error) {
	if a == nil || ctx == nil || now.IsZero() {
		return Result{}, errors.New("invalid fleet command")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return Result{}, errors.New("fleet agent is closed")
	}
	command, fingerprint, err := a.verify(envelope, now)
	if err != nil {
		return Result{}, err
	}
	if err := a.prune(now.UTC(), 64); err != nil {
		return Result{}, err
	}
	if result, found, err := a.replay(command, fingerprint); found || err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	queued, err := a.hasQueued()
	if err != nil {
		return Result{}, err
	}
	if !a.config.Online() || a.config.RealtimeBusy() || queued {
		if err := a.enqueue(command, fingerprint, envelope.ExpiresAt); err != nil {
			return Result{}, err
		}
		return Result{CommandID: command.CommandID, Generation: command.Generation, State: "queued"}, nil
	}
	if err := a.claim(command, fingerprint, envelope.ExpiresAt); err != nil {
		return Result{}, err
	}
	return a.execute(ctx, command, fingerprint, envelope.ExpiresAt, 0)
}

func (a *Agent) hasQueued() (bool, error) {
	queued := false
	err := a.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(queueBucket)
		if bucket == nil {
			return errors.New("fleet queue missing")
		}
		queued = bucket.Stats().KeyN != 0
		return nil
	})
	return queued, err
}

// Drain executes a bounded number of persisted commands after reconnect. It
// stops immediately if local real-time work appears.
func (a *Agent) Drain(ctx context.Context, now time.Time, maximum int) ([]Result, error) {
	if a == nil || ctx == nil || now.IsZero() || maximum <= 0 || maximum > MaxDrainHard {
		return nil, errors.New("invalid fleet drain")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, errors.New("fleet agent is closed")
	}
	if !a.config.Online() {
		return nil, ErrOffline
	}
	var queued []record
	err := a.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(queueBucket).Cursor()
		for key, value := cursor.First(); key != nil && len(queued) < maximum; key, value = cursor.Next() {
			var item record
			if json.Unmarshal(value, &item) != nil {
				return errors.New("corrupt fleet queue")
			}
			queued = append(queued, item)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	results := make([]Result, 0, len(queued))
	for _, item := range queued {
		if a.config.RealtimeBusy() {
			return results, ErrRealtimeBusy
		}
		if item.ExpiresAt <= now.UnixMilli() {
			if err := a.finish(item, "expired"); err != nil {
				return results, err
			}
			continue
		}
		claimed, err := a.claimQueued(item)
		if err != nil {
			return results, err
		}
		result, err := a.execute(ctx, claimed.Command, claimed.Fingerprint, claimed.ExpiresAt, 0)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (a *Agent) verify(envelope identity.Envelope, now time.Time) (Command, string, error) {
	key := a.keys[envelope.KeyID]
	var command Command
	if len(key) != ed25519.PublicKeySize || envelope.VerifyCanonical(key, CommandDomain, now.UTC(), &command) != nil ||
		command.Version != CommandVersion || command.FleetID != a.config.FleetID || command.TerminalID != a.config.TerminalID ||
		!validID(command.CommandID) || command.Generation == 0 || !validAction(command.Action) ||
		!validCommandDigests(command) {
		return Command{}, "", errors.New("fleet command is unauthorized")
	}
	fingerprint, err := envelope.Fingerprint()
	if err != nil {
		return Command{}, "", errors.New("fleet command is unauthorized")
	}
	return command, fingerprint, nil
}

func (a *Agent) replay(command Command, fingerprint string) (Result, bool, error) {
	var existing record
	var found bool
	err := a.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(recordBucket).Get([]byte(command.CommandID))
		if value == nil {
			return nil
		}
		found = true
		return json.Unmarshal(value, &existing)
	})
	if err != nil {
		return Result{}, true, errors.New("corrupt fleet journal")
	}
	if !found {
		return Result{}, false, nil
	}
	if existing.Fingerprint != fingerprint {
		return Result{}, true, errors.New("fleet command identifier conflict")
	}
	result := Result{CommandID: command.CommandID, Generation: command.Generation, State: existing.State, Replay: true}
	if existing.State == "uncertain" || existing.State == "executing" {
		result.State = "uncertain"
		return result, true, ErrUncertain
	}
	return result, true, nil
}

func (a *Agent) enqueue(command Command, fingerprint string, expiresAt int64) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		last := binary.BigEndian.Uint64(paddedGeneration(tx.Bucket(metaBucket).Get(lastGenerationKey)))
		if command.Generation <= last {
			return errors.New("fleet command generation is stale")
		}
		queue := tx.Bucket(queueBucket)
		if queue.Stats().KeyN >= a.config.MaxQueued {
			return errors.New("fleet command queue capacity exhausted")
		}
		if tx.Bucket(recordBucket).Stats().KeyN >= a.config.MaxRecords {
			return errors.New("fleet command journal capacity exhausted")
		}
		sequence, err := queue.NextSequence()
		if err != nil || sequence == 0 {
			return errors.New("fleet queue sequence exhausted")
		}
		item := record{Fingerprint: fingerprint, Command: command, State: "queued", ExpiresAt: expiresAt, Sequence: sequence}
		encoded, _ := json.Marshal(item)
		if err := queue.Put(sequenceKey(sequence), encoded); err != nil {
			return err
		}
		if err := tx.Bucket(recordBucket).Put([]byte(command.CommandID), encoded); err != nil {
			return err
		}
		var generation [8]byte
		binary.BigEndian.PutUint64(generation[:], command.Generation)
		if err := tx.Bucket(metaBucket).Put(lastGenerationKey, generation[:]); err != nil {
			return err
		}
		if tx.Size() > a.config.MaxDBBytes {
			return errors.New("fleet journal byte limit reached")
		}
		return nil
	})
}

func (a *Agent) claim(command Command, fingerprint string, expiresAt int64) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		last := binary.BigEndian.Uint64(paddedGeneration(tx.Bucket(metaBucket).Get(lastGenerationKey)))
		if command.Generation <= last {
			return errors.New("fleet command generation is stale")
		}
		if tx.Bucket(recordBucket).Stats().KeyN >= a.config.MaxRecords {
			return errors.New("fleet command journal capacity exhausted")
		}
		item := record{Fingerprint: fingerprint, Command: command, State: "executing", ExpiresAt: expiresAt}
		encoded, _ := json.Marshal(item)
		if err := tx.Bucket(recordBucket).Put([]byte(command.CommandID), encoded); err != nil {
			return err
		}
		var generation [8]byte
		binary.BigEndian.PutUint64(generation[:], command.Generation)
		if err := tx.Bucket(metaBucket).Put(lastGenerationKey, generation[:]); err != nil {
			return err
		}
		if tx.Size() > a.config.MaxDBBytes {
			return errors.New("fleet journal byte limit reached")
		}
		return nil
	})
}

// claimQueued durably removes a queued command before invoking its external
// side effect. A crash after this transaction is recovered as uncertain and
// never causes automatic re-execution.
func (a *Agent) claimQueued(item record) (record, error) {
	claimed := item
	err := a.db.Update(func(tx *bolt.Tx) error {
		queue := tx.Bucket(queueBucket)
		records := tx.Bucket(recordBucket)
		queuedValue := queue.Get(sequenceKey(item.Sequence))
		recordValue := records.Get([]byte(item.Command.CommandID))
		if queuedValue == nil || recordValue == nil {
			return errors.New("fleet queue claim is stale")
		}
		var queuedRecord, durableRecord record
		if json.Unmarshal(queuedValue, &queuedRecord) != nil || json.Unmarshal(recordValue, &durableRecord) != nil ||
			queuedRecord.Fingerprint != item.Fingerprint || durableRecord.Fingerprint != item.Fingerprint ||
			queuedRecord.Command.CommandID != item.Command.CommandID || durableRecord.Command.CommandID != item.Command.CommandID ||
			queuedRecord.Sequence != item.Sequence || durableRecord.Sequence != item.Sequence ||
			queuedRecord.State != "queued" || durableRecord.State != "queued" {
			return errors.New("fleet queue claim is inconsistent")
		}
		claimed.State = "executing"
		claimed.Sequence = 0
		encoded, err := json.Marshal(claimed)
		if err != nil {
			return err
		}
		if err := records.Put([]byte(claimed.Command.CommandID), encoded); err != nil {
			return err
		}
		return queue.Delete(sequenceKey(item.Sequence))
	})
	return claimed, err
}

func (a *Agent) execute(ctx context.Context, command Command, fingerprint string, expiresAt int64, sequence uint64) (Result, error) {
	err, panicked := callExecutor(ctx, a.config.Executor, command)
	if panicked || ctx.Err() != nil {
		item := record{Fingerprint: fingerprint, Command: command, State: "uncertain", ExpiresAt: expiresAt, Sequence: sequence}
		if finishErr := a.finish(item, "uncertain"); finishErr != nil {
			return Result{}, finishErr
		}
		return Result{CommandID: command.CommandID, Generation: command.Generation, State: "uncertain"}, ErrUncertain
	}
	if err != nil {
		item := record{Fingerprint: fingerprint, Command: command, State: "failed", ExpiresAt: expiresAt, Sequence: sequence}
		_ = a.finish(item, "failed")
		return Result{CommandID: command.CommandID, Generation: command.Generation, State: "failed"}, errors.New("fleet command failed")
	}
	item := record{Fingerprint: fingerprint, Command: command, State: "succeeded", ExpiresAt: expiresAt, Sequence: sequence}
	if err := a.finish(item, "succeeded"); err != nil {
		return Result{}, err
	}
	return Result{CommandID: command.CommandID, Generation: command.Generation, State: "succeeded"}, nil
}

func callExecutor(ctx context.Context, executor Executor, command Command) (resultErr error, panicked bool) {
	defer func() {
		if recover() != nil {
			resultErr = errors.New("fleet command executor panicked")
			panicked = true
		}
	}()
	return executor.Apply(ctx, command), false
}

func (a *Agent) finish(item record, state string) error {
	item.State = state
	return a.db.Update(func(tx *bolt.Tx) error {
		encoded, _ := json.Marshal(item)
		if err := tx.Bucket(recordBucket).Put([]byte(item.Command.CommandID), encoded); err != nil {
			return err
		}
		if item.Sequence != 0 {
			return tx.Bucket(queueBucket).Delete(sequenceKey(item.Sequence))
		}
		return nil
	})
}

func (a *Agent) prune(now time.Time, maximum int) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordBucket)
		queue := tx.Bucket(queueBucket)
		cursor := records.Cursor()
		removed := 0
		for key, value := cursor.First(); key != nil && removed < maximum; key, value = cursor.Next() {
			var item record
			if json.Unmarshal(value, &item) != nil {
				return errors.New("corrupt fleet journal")
			}
			if item.ExpiresAt > now.UnixMilli() {
				continue
			}
			if item.Sequence != 0 {
				if err := queue.Delete(sequenceKey(item.Sequence)); err != nil {
					return err
				}
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
}

func (a *Agent) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	return a.db.Close()
}

func sequenceKey(value uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], value)
	return key[:]
}
func paddedGeneration(value []byte) []byte {
	if len(value) == 8 {
		return value
	}
	return make([]byte, 8)
}

func reconcileInterrupted(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordBucket)
		queue := tx.Bucket(queueBucket)
		interrupted := make([]record, 0)
		if err := records.ForEach(func(_, value []byte) error {
			var item record
			if json.Unmarshal(value, &item) != nil {
				return errors.New("corrupt fleet journal")
			}
			if item.State == "executing" {
				interrupted = append(interrupted, item)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, item := range interrupted {
			if item.Sequence != 0 {
				if err := queue.Delete(sequenceKey(item.Sequence)); err != nil {
					return err
				}
				item.Sequence = 0
			}
			item.State = "uncertain"
			encoded, err := json.Marshal(item)
			if err != nil {
				return err
			}
			if err := records.Put([]byte(item.Command.CommandID), encoded); err != nil {
				return err
			}
		}
		return nil
	})
}
func validID(value string) bool {
	if len(value) < 3 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && !strings.ContainsRune("._:-", r) {
			return false
		}
	}
	return true
}
func validAction(value string) bool {
	return value == "install-release" || value == "rollback" || value == "drain" || value == "resume" ||
		value == "apply-policy" || value == "rollback-policy"
}
func validCommandDigests(command Command) bool {
	validDigest := func(value string) bool {
		if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
			return false
		}
		for _, character := range value[len("sha256:"):] {
			if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
				return false
			}
		}
		return true
	}
	switch command.Action {
	case "apply-policy", "rollback-policy":
		return command.ReleaseDigest == "" && validDigest(command.PolicyDigest)
	case "install-release":
		return command.PolicyDigest == "" && validDigest(command.ReleaseDigest)
	default:
		return command.PolicyDigest == "" &&
			(command.ReleaseDigest == "" || validDigest(command.ReleaseDigest))
	}
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

// SortTerminalIDs returns a stable, bounded cluster order for deterministic
// canary selection and audit logs.
func SortTerminalIDs(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > 4096 {
		return nil, errors.New("invalid fleet group size")
	}
	output := append([]string(nil), values...)
	for _, value := range output {
		if !validID(value) {
			return nil, errors.New("invalid terminal ID")
		}
	}
	sort.Strings(output)
	for index := 1; index < len(output); index++ {
		if output[index] == output[index-1] {
			return nil, errors.New("duplicate terminal ID")
		}
	}
	return output, nil
}
