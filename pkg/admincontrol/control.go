// Package admincontrol verifies signed, replay-safe lifecycle commands before
// invoking the local software update manager. It exposes no network listener;
// deployments choose a private Unix or mutually authenticated transport.
package admincontrol

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
	"github.com/tosnetwork/tos-ai/pkg/softwareupdate"
	"github.com/tosnetwork/tos-protocol/pkg/identity"
	bolt "go.etcd.io/bbolt"
)

const (
	CommandVersion = 1
	CommandDomain  = "tos.ai.admin-command.v1"

	ActionActivate = "activate-pending"
	ActionConfirm  = "confirm-healthy"
	ActionRollback = "rollback-known-good"

	DefaultMaxRecords       = 4096
	DefaultMaxDatabaseBytes = 64 << 20
	MaxRecordsHard          = 65_536
	MaxDatabaseBytesHard    = 1 << 30
	pruneBatch              = 64
	MaxHistoryEntries       = 256
)

var (
	commandBucket = []byte("admin-commands-v1")
	orderBucket   = []byte("admin-command-order-v1")
	expiryBucket  = []byte("admin-command-expiry-v1")
	ErrUncertain  = errors.New("administrator command outcome is uncertain")
)

type Command struct {
	Version            uint8  `json:"version"`
	CommandID          string `json:"commandId"`
	TerminalID         string `json:"terminalId"`
	Action             string `json:"action"`
	ExpectedActiveSlot string `json:"expectedActiveSlot,omitempty"`
}

type Result struct {
	CommandID  string
	Action     string
	ActiveSlot string
	Succeeded  bool
	Replay     bool
}

// HistoryEntry is a privacy-minimized durable operator event. It deliberately
// omits signatures, payloads, key identifiers, fingerprints, and raw errors.
type HistoryEntry struct {
	CommandID  string
	Action     string
	State      string
	ActiveSlot string
	Succeeded  bool
	Sequence   uint64
}

type Lifecycle interface {
	Status() (softwareupdate.Status, error)
	ActivatePending() error
	ConfirmHealthy() error
	Rollback() error
}

type Config struct {
	DatabasePath      string
	TerminalID        string
	AdministratorKeys map[string]ed25519.PublicKey
	Lifecycle         Lifecycle
	MaxRecords        int
	MaxDatabaseBytes  int64
	Retention         time.Duration
}

type Controller struct {
	database   *bolt.DB
	terminalID string
	keys       map[string]ed25519.PublicKey
	lifecycle  Lifecycle
	maximum    int
	maxBytes   int64
	retention  time.Duration

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

type commandRecord struct {
	Fingerprint string `json:"fingerprint"`
	Action      string `json:"action"`
	State       string `json:"state"`
	ActiveSlot  string `json:"activeSlot,omitempty"`
	Succeeded   bool   `json:"succeeded"`
	RetainUntil int64  `json:"retainUntil"`
	Sequence    uint64 `json:"sequence"`
}

func Open(config Config) (*Controller, error) {
	if config.MaxRecords == 0 {
		config.MaxRecords = DefaultMaxRecords
	}
	if config.MaxDatabaseBytes == 0 {
		config.MaxDatabaseBytes = DefaultMaxDatabaseBytes
	}
	if config.Retention == 0 {
		config.Retention = 24 * time.Hour
	}
	if !filepath.IsAbs(config.DatabasePath) || filepath.Clean(config.DatabasePath) != config.DatabasePath ||
		!validIdentifier(config.TerminalID, 128) || nilcheck.IsNil(config.Lifecycle) ||
		config.MaxRecords <= 0 || config.MaxRecords > MaxRecordsHard ||
		config.MaxDatabaseBytes <= 0 || config.MaxDatabaseBytes > MaxDatabaseBytesHard ||
		config.Retention <= 0 || config.Retention > 30*24*time.Hour ||
		len(config.AdministratorKeys) == 0 || len(config.AdministratorKeys) > 32 {
		return nil, errors.New("invalid administrator controller configuration")
	}
	keys := make(map[string]ed25519.PublicKey, len(config.AdministratorKeys))
	for keyID, key := range config.AdministratorKeys {
		if !validIdentifier(keyID, 512) || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("invalid administrator trust root")
		}
		keys[keyID] = append(ed25519.PublicKey(nil), key...)
	}
	parent := filepath.Dir(config.DatabasePath)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, errors.New("create administrator journal directory")
	}
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 ||
		info.Mode()&os.ModeSymlink != 0 || !ownedByCurrentUser(info) {
		return nil, errors.New("administrator journal directory is not private")
	}
	// Never wait indefinitely on an already-owned journal. The caller can
	// retry under its own bounded restart policy.
	database, err := bolt.Open(config.DatabasePath, 0o600, &bolt.Options{
		Timeout: time.Millisecond, NoGrowSync: false,
	})
	if err != nil {
		return nil, errors.New("open administrator command journal")
	}
	if err := os.Chmod(config.DatabasePath, 0o600); err != nil {
		_ = database.Close()
		return nil, errors.New("protect administrator command journal")
	}
	if err := database.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists(commandBucket)
		if createErr == nil {
			_, createErr = tx.CreateBucketIfNotExists(orderBucket)
		}
		if createErr == nil {
			_, createErr = tx.CreateBucketIfNotExists(expiryBucket)
		}
		if createErr == nil && tx.Size() > config.MaxDatabaseBytes {
			return errors.New("administrator command journal byte limit too small")
		}
		return createErr
	}); err != nil {
		_ = database.Close()
		return nil, errors.New("initialize administrator command journal")
	}
	return &Controller{
		database: database, terminalID: config.TerminalID, keys: keys,
		lifecycle: config.Lifecycle, maximum: config.MaxRecords,
		maxBytes: config.MaxDatabaseBytes, retention: config.Retention,
	}, nil
}

func (c *Controller) Execute(
	ctx context.Context,
	envelope identity.Envelope,
	now time.Time,
) (Result, error) {
	if c == nil || ctx == nil || now.IsZero() {
		return Result{}, errors.New("invalid administrator command request")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Result{}, errors.New("administrator controller is closed")
	}
	key := c.keys[envelope.KeyID]
	if len(key) != ed25519.PublicKeySize {
		return Result{}, errors.New("administrator command is unauthorized")
	}
	var command Command
	if err := envelope.VerifyCanonical(key, CommandDomain, now.UTC(), &command); err != nil ||
		command.validate(c.terminalID) != nil {
		return Result{}, errors.New("administrator command is unauthorized")
	}
	fingerprint, err := envelope.Fingerprint()
	if err != nil {
		return Result{}, errors.New("administrator command is unauthorized")
	}
	claim, err := c.claim(command, fingerprint, envelope.ExpiresAt, now.UTC())
	if err != nil || claim != nil {
		if claim != nil {
			if !claim.Succeeded {
				return *claim, errors.New("administrator lifecycle command failed")
			}
			return *claim, err
		}
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, ErrUncertain
	}
	result := Result{CommandID: command.CommandID, Action: command.Action}
	status, executeErr := c.callLifecycle(command)
	result.ActiveSlot = status.ActiveSlot
	result.Succeeded = executeErr == nil
	if persistErr := c.complete(command, fingerprint, result, envelope.ExpiresAt, now.UTC()); persistErr != nil {
		return Result{}, ErrUncertain
	}
	if executeErr != nil {
		return result, errors.New("administrator lifecycle command failed")
	}
	return result, nil
}

func (c *Controller) claim(
	command Command,
	fingerprint string,
	expiresAt int64,
	now time.Time,
) (*Result, error) {
	var replay *Result
	err := c.database.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(commandBucket)
		order := tx.Bucket(orderBucket)
		expiry := tx.Bucket(expiryBucket)
		if bucket == nil || order == nil || expiry == nil {
			return errors.New("administrator command journal missing")
		}
		if encoded := bucket.Get([]byte(command.CommandID)); encoded != nil {
			var record commandRecord
			if json.Unmarshal(encoded, &record) != nil || record.Fingerprint != fingerprint ||
				record.Action != command.Action {
				return errors.New("administrator command identifier conflict")
			}
			if record.State != "completed" {
				return ErrUncertain
			}
			replay = &Result{
				CommandID: command.CommandID, Action: record.Action,
				ActiveSlot: record.ActiveSlot, Succeeded: record.Succeeded, Replay: true,
			}
			return nil
		}
		removed, pruneErr := pruneExpired(bucket, order, expiry, now.UnixMilli(), pruneBatch)
		if pruneErr != nil {
			return pruneErr
		}
		// bbolt's Bucket.Stats snapshot does not reflect cursor deletions made
		// earlier in this transaction, so subtract the exact bounded count.
		if bucket.Stats().KeyN-removed >= c.maximum {
			return errors.New("administrator command journal capacity exhausted")
		}
		if info, statErr := os.Stat(c.database.Path()); statErr != nil || info.Size() >= c.maxBytes {
			return errors.New("administrator command journal byte limit reached")
		}
		sequence, sequenceErr := order.NextSequence()
		if sequenceErr != nil || sequence == 0 {
			return errors.New("administrator command sequence exhausted")
		}
		record := commandRecord{
			Fingerprint: fingerprint, Action: command.Action, State: "claimed",
			RetainUntil: retainedUntil(expiresAt, now, c.retention),
			Sequence:    sequence,
		}
		encoded, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return marshalErr
		}
		if err := bucket.Put([]byte(command.CommandID), encoded); err != nil {
			return err
		}
		if err := order.Put(sequenceKey(sequence), []byte(command.CommandID)); err != nil {
			return err
		}
		if err := expiry.Put(
			expiryKey(record.RetainUntil, sequence), []byte(command.CommandID),
		); err != nil {
			return err
		}
		// Tx.Size includes pages allocated by this transaction. Returning an
		// error rolls the write back before bbolt grows the durable file.
		if tx.Size() > c.maxBytes {
			return errors.New("administrator command journal byte limit reached")
		}
		return nil
	})
	if err != nil {
		return replay, err
	}
	return replay, nil
}

func (c *Controller) complete(
	command Command,
	fingerprint string,
	result Result,
	expiresAt int64,
	now time.Time,
) error {
	return c.database.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(commandBucket)
		expiry := tx.Bucket(expiryBucket)
		if bucket == nil || expiry == nil {
			return errors.New("administrator command journal missing")
		}
		encoded := bucket.Get([]byte(command.CommandID))
		var existing commandRecord
		if encoded == nil || json.Unmarshal(encoded, &existing) != nil ||
			existing.Fingerprint != fingerprint || existing.State != "claimed" {
			return errors.New("administrator command claim changed")
		}
		retainUntil := retainedUntil(expiresAt, now, c.retention)
		record := commandRecord{
			Fingerprint: fingerprint, Action: command.Action, State: "completed",
			ActiveSlot: result.ActiveSlot, Succeeded: result.Succeeded,
			RetainUntil: retainUntil,
			Sequence:    existing.Sequence,
		}
		value, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(command.CommandID), value); err != nil {
			return err
		}
		if existing.RetainUntil != retainUntil {
			if err := expiry.Delete(expiryKey(existing.RetainUntil, existing.Sequence)); err != nil {
				return err
			}
			if err := expiry.Put(
				expiryKey(retainUntil, existing.Sequence), []byte(command.CommandID),
			); err != nil {
				return err
			}
		}
		if tx.Size() > c.maxBytes {
			return errors.New("administrator command journal byte limit reached")
		}
		return nil
	})
}

// History returns newest-first, bounded lifecycle events from the local
// journal. It is intentionally not a network endpoint; a deployment must add
// its own authenticated export and rate policy.
func (c *Controller) History(limit int) ([]HistoryEntry, error) {
	if c == nil || limit <= 0 || limit > MaxHistoryEntries {
		return nil, errors.New("invalid administrator history limit")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("administrator controller is closed")
	}
	entries := make([]HistoryEntry, 0, limit)
	err := c.database.View(func(tx *bolt.Tx) error {
		commands := tx.Bucket(commandBucket)
		order := tx.Bucket(orderBucket)
		if commands == nil || order == nil {
			return errors.New("administrator command journal missing")
		}
		cursor := order.Cursor()
		for sequenceBytes, commandID := cursor.Last(); sequenceBytes != nil && len(entries) < limit; sequenceBytes, commandID = cursor.Prev() {
			if len(sequenceBytes) != 8 {
				return errors.New("administrator command order is corrupt")
			}
			var record commandRecord
			if encoded := commands.Get(commandID); encoded == nil || json.Unmarshal(encoded, &record) != nil ||
				record.Sequence != binary.BigEndian.Uint64(sequenceBytes) {
				return errors.New("administrator command history is corrupt")
			}
			entries = append(entries, HistoryEntry{
				CommandID: string(commandID), Action: record.Action, State: record.State,
				ActiveSlot: record.ActiveSlot, Succeeded: record.Succeeded,
				Sequence: record.Sequence,
			})
		}
		return nil
	})
	if err != nil {
		return nil, errors.New("read administrator command history")
	}
	return entries, nil
}

func (c *Controller) callLifecycle(command Command) (status softwareupdate.Status, err error) {
	defer func() {
		if recover() != nil {
			status = softwareupdate.Status{}
			err = errors.New("administrator lifecycle dependency panicked")
		}
	}()
	before, err := c.lifecycle.Status()
	if err != nil || before.ActiveSlot != command.ExpectedActiveSlot {
		return before, errors.New("administrator command precondition failed")
	}
	switch command.Action {
	case ActionActivate:
		err = c.lifecycle.ActivatePending()
	case ActionConfirm:
		err = c.lifecycle.ConfirmHealthy()
	case ActionRollback:
		err = c.lifecycle.Rollback()
	default:
		err = errors.New("unsupported administrator command")
	}
	after, statusErr := c.lifecycle.Status()
	if statusErr != nil {
		return softwareupdate.Status{}, errors.Join(err, statusErr)
	}
	return after, err
}

func (c *Controller) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		if c.database != nil {
			c.closeErr = c.database.Close()
		}
		c.mu.Unlock()
	})
	return c.closeErr
}

func (command Command) validate(terminalID string) error {
	if command.Version != CommandVersion || !validIdentifier(command.CommandID, 128) ||
		command.TerminalID != terminalID ||
		(command.ExpectedActiveSlot != "" && command.ExpectedActiveSlot != "a" &&
			command.ExpectedActiveSlot != "b") {
		return errors.New("invalid administrator command")
	}
	switch command.Action {
	case ActionActivate, ActionConfirm, ActionRollback:
		return nil
	default:
		return fmt.Errorf("unsupported administrator action %q", command.Action)
	}
}

func validIdentifier(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && !strings.ContainsRune(value, 0)
}

func retainedUntil(expiresAt int64, now time.Time, retention time.Duration) int64 {
	base := expiresAt
	if nowMillis := now.UnixMilli(); base < nowMillis {
		base = nowMillis
	}
	retentionMillis := retention.Milliseconds()
	if base > int64(^uint64(0)>>1)-retentionMillis {
		return int64(^uint64(0) >> 1)
	}
	return base + retentionMillis
}

func pruneExpired(
	commands, order, expiry *bolt.Bucket,
	now int64,
	maximum int,
) (int, error) {
	cursor := expiry.Cursor()
	removed := 0
	for key, commandID := cursor.First(); key != nil && removed < maximum; key, commandID = cursor.Next() {
		if len(key) != 16 {
			return removed, errors.New("administrator command expiry index is corrupt")
		}
		retainUntil := int64(binary.BigEndian.Uint64(key[:8]) ^ (uint64(1) << 63))
		if retainUntil > now {
			break
		}
		var record commandRecord
		encoded := commands.Get(commandID)
		if encoded == nil || json.Unmarshal(encoded, &record) != nil ||
			record.RetainUntil != retainUntil || record.Sequence != binary.BigEndian.Uint64(key[8:]) {
			return removed, errors.New("administrator command expiry index is corrupt")
		}
		if err := order.Delete(sequenceKey(record.Sequence)); err != nil {
			return removed, err
		}
		if err := commands.Delete(commandID); err != nil {
			return removed, err
		}
		if err := cursor.Delete(); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func sequenceKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}

func expiryKey(retainUntil int64, sequence uint64) []byte {
	key := make([]byte, 16)
	// Flip the sign bit so lexicographic byte order matches signed int64 time
	// order even for synthetic pre-epoch tests.
	binary.BigEndian.PutUint64(key[:8], uint64(retainUntil)^(uint64(1)<<63))
	binary.BigEndian.PutUint64(key[8:], sequence)
	return key
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid()
}
