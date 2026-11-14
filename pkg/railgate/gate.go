package railgate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
)

const slotSchema = "tos.ai.railgate-slot.v1"

// SlotState is a slot's position in its at-most-once lifecycle.
type SlotState string

const (
	// SlotAdmitted: verified and claimed; execution has not started.
	SlotAdmitted SlotState = "admitted"
	// SlotStarted: the fresh pre-execution check passed and the execution
	// was handed to the runner. Without a terminal record after a restart
	// the slot is ambiguous.
	SlotStarted SlotState = "started"
	// SlotCompleted: the execution succeeded and its response hash is final.
	SlotCompleted SlotState = "completed"
	// SlotFailed: the execution ran and did not succeed; it is never attested.
	SlotFailed SlotState = "failed"
	// SlotAbandoned: the pre-execution check refused; nothing ran.
	SlotAbandoned SlotState = "abandoned"
	// SlotAmbiguous: a started execution has no recorded outcome; it is never
	// re-run or attested, and the buyer's refund path applies.
	SlotAmbiguous SlotState = "ambiguous"
)

var (
	// ErrSlotTaken reports a claim whose slot already exists: it is never
	// admitted for a second execution.
	ErrSlotTaken = errors.New("railgate: this request already has an execution slot")
	// ErrNotPendingNow reports that the fresh pre-execution check refused and
	// the slot was abandoned without running anything.
	ErrNotPendingNow = errors.New("railgate: the request no longer admits execution; nothing ran")

	responseHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// SlotKey is the at-most-once identity of an execution.
type SlotKey struct {
	QuoteCommitment string
	Actor           string
	RequestID       uint64
}

func (k SlotKey) valid() bool {
	return cellDigestPattern.MatchString(k.QuoteCommitment) && rawAddressPattern.MatchString(k.Actor)
}

func (k SlotKey) fileName() string {
	digest := sha256.Sum256([]byte(k.QuoteCommitment + "\x00" + k.Actor + "\x00" + strconv.FormatUint(k.RequestID, 10)))
	return hex.EncodeToString(digest[:]) + ".json"
}

// Slot is the durable record of one admitted request.
type Slot struct {
	Schema          string    `json:"schema"`
	QuoteCommitment string    `json:"quote_commitment"`
	Actor           string    `json:"actor"`
	RequestID       uint64    `json:"request_id"`
	RequestHash     string    `json:"request_hash"`
	State           SlotState `json:"state"`
	ResponseHash    string    `json:"response_hash,omitempty"`
	UpdatedUnix     int64     `json:"updated_unix"`
}

// Key returns the slot's identity.
func (s Slot) Key() SlotKey {
	return SlotKey{QuoteCommitment: s.QuoteCommitment, Actor: s.Actor, RequestID: s.RequestID}
}

func (s Slot) valid() bool {
	if s.Schema != slotSchema || !s.Key().valid() || !responseHashPattern.MatchString(s.RequestHash) || s.UpdatedUnix <= 0 {
		return false
	}
	switch s.State {
	case SlotCompleted:
		return responseHashPattern.MatchString(s.ResponseHash)
	case SlotAdmitted, SlotStarted, SlotFailed, SlotAbandoned, SlotAmbiguous:
		return s.ResponseHash == ""
	default:
		return false
	}
}

// Gate owns a directory of slots. One process owns the directory.
type Gate struct {
	verifier *Verifier
	root     string
	lock     *dirlock.Lock
	mutex    sync.Mutex
	now      func() time.Time
}

// OpenGate opens or creates an owner-private slot directory. Every slot found
// started without an outcome becomes ambiguous before the gate is returned.
func OpenGate(verifier *Verifier, directory string) (*Gate, error) {
	if verifier == nil || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, errors.New("railgate: invalid gate configuration")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, errors.New("railgate: create slot directory")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, errors.New("railgate: slot directory must be owner-private")
	}
	lock, err := dirlock.Acquire(directory, ".railgate.lock")
	if err != nil {
		return nil, errors.New("railgate: slot directory is already owned")
	}
	gate := &Gate{verifier: verifier, root: directory, lock: lock, now: time.Now}
	if err := gate.recover(); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return gate, nil
}

// Close releases the directory.
func (g *Gate) Close() error {
	if g == nil || g.lock == nil {
		return nil
	}
	return g.lock.Close()
}

func (g *Gate) recover() error {
	entries, err := os.ReadDir(g.root)
	if err != nil {
		return errors.New("railgate: read slot directory")
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == ".railgate.lock" || filepath.Ext(name) != ".json" || len(name) != 69 {
			continue
		}
		slot, err := g.read(name)
		if err != nil {
			return err
		}
		if slot.State == SlotStarted {
			slot.State, slot.UpdatedUnix = SlotAmbiguous, g.now().Unix()
			if err := g.write(slot, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Admit verifies the claim at a fresh checkpoint and creates its slot. A
// claim whose slot exists returns that slot and ErrSlotTaken, so a request is
// admitted for execution at most once whatever the claim's transport. When
// the local slot store fails after verification the verified claim is
// returned with the error, so the caller can look the slot up.
func (g *Gate) Admit(ctx context.Context, claim Claim) (Slot, *Verified, error) {
	if g == nil {
		return Slot{}, nil, errors.New("railgate: invalid gate")
	}
	verified, err := g.verifier.Verify(ctx, claim)
	if err != nil {
		return Slot{}, nil, err
	}
	key := verified.SlotKey()
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if existing, found, err := g.lookup(key); err != nil {
		return Slot{}, verified, err
	} else if found {
		return existing, verified, ErrSlotTaken
	}
	slot := Slot{Schema: slotSchema, QuoteCommitment: key.QuoteCommitment, Actor: key.Actor, RequestID: key.RequestID,
		RequestHash: hex.EncodeToString(verified.RequestHash[:]), State: SlotAdmitted, UpdatedUnix: g.now().Unix()}
	if err := g.write(slot, true); err != nil {
		return Slot{}, verified, err
	}
	return slot, verified, nil
}

// Run re-verifies the claim at a fresh checkpoint immediately before
// execution. If the request no longer admits execution the slot is
// abandoned and ErrNotPendingNow is returned without calling execute;
// otherwise the slot is durably marked started and then execute runs. Its
// outcome is recorded by Complete or Fail.
func (g *Gate) Run(ctx context.Context, slot Slot, claim Claim, execute func(context.Context, *Verified) error) error {
	if g == nil || execute == nil {
		return errors.New("railgate: invalid run")
	}
	if err := g.transition(slot.Key(), SlotAdmitted, SlotAdmitted, ""); err != nil {
		return err
	}
	started := g.now()
	verified, err := g.verifier.Verify(ctx, claim)
	if err == nil && (verified.SlotKey() != slot.Key() || hex.EncodeToString(verified.RequestHash[:]) != slot.RequestHash) {
		err = errors.New("railgate: the claim names another request than the slot")
	}
	if err == nil {
		// Re-measure right before execution: the budget must still fit after
		// the verification and the slot write that precede it.
		budget := g.verifier.config.ExecutionBudget + g.verifier.config.RespondBudget
		err = checkSlack(currentTime(verified.ChainTime, started, g.now()), verified.ResponseDeadline, budget)
	}
	if err != nil {
		if transitionErr := g.transition(slot.Key(), SlotAdmitted, SlotAbandoned, ""); transitionErr != nil {
			return errors.Join(err, transitionErr)
		}
		return fmt.Errorf("%w: %v", ErrNotPendingNow, err)
	}
	if err := g.transition(slot.Key(), SlotAdmitted, SlotStarted, ""); err != nil {
		return err
	}
	return execute(ctx, verified)
}

// Complete records a successful execution's response hash (64 lowercase hex).
func (g *Gate) Complete(key SlotKey, responseHash string) error {
	if !responseHashPattern.MatchString(responseHash) {
		return errors.New("railgate: invalid response hash")
	}
	return g.transition(key, SlotStarted, SlotCompleted, responseHash)
}

// Fail records an execution that ran and did not succeed.
func (g *Gate) Fail(key SlotKey) error {
	return g.transition(key, SlotStarted, SlotFailed, "")
}

// Recheck proves the slot's request is still pending, unchanged and with the
// respond budget ahead, at a fresh checkpoint. Only a completed slot may be
// rechecked for respond.
func (g *Gate) Recheck(ctx context.Context, slot Slot, claim Claim, verified *Verified) error {
	if g == nil {
		return errors.New("railgate: invalid gate")
	}
	g.mutex.Lock()
	current, found, err := g.lookup(slot.Key())
	g.mutex.Unlock()
	if err != nil {
		return err
	}
	if !found || current.State != SlotCompleted {
		return errors.New("railgate: only a completed execution may be attested")
	}
	if verified == nil || verified.SlotKey() != slot.Key() {
		return errors.New("railgate: the verified request is not this slot's")
	}
	return g.verifier.Recheck(ctx, claim, verified)
}

// Lookup returns a slot by key.
func (g *Gate) Lookup(key SlotKey) (Slot, bool, error) {
	if g == nil {
		return Slot{}, false, errors.New("railgate: invalid gate")
	}
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return g.lookup(key)
}

func (g *Gate) lookup(key SlotKey) (Slot, bool, error) {
	if !key.valid() {
		return Slot{}, false, errors.New("railgate: invalid slot key")
	}
	slot, err := g.read(key.fileName())
	if errors.Is(err, os.ErrNotExist) {
		return Slot{}, false, nil
	}
	if err != nil {
		return Slot{}, false, err
	}
	if slot.Key() != key {
		return Slot{}, false, errors.New("railgate: a slot file names another request")
	}
	return slot, true, nil
}

func (g *Gate) transition(key SlotKey, from, to SlotState, responseHash string) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	slot, found, err := g.lookup(key)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("railgate: no slot for this request")
	}
	if slot.State != from {
		return fmt.Errorf("railgate: slot is %s, not %s", slot.State, from)
	}
	if from == to {
		return nil
	}
	slot.State, slot.ResponseHash, slot.UpdatedUnix = to, responseHash, g.now().Unix()
	return g.write(slot, false)
}

func (g *Gate) read(name string) (Slot, error) {
	path := filepath.Join(g.root, name)
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Slot{}, err
		}
		return Slot{}, errors.New("railgate: stat slot")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > 4<<10 {
		return Slot{}, errors.New("railgate: slot file is not an owner-only regular record")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Slot{}, errors.New("railgate: read slot")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var slot Slot
	if err := decoder.Decode(&slot); err != nil {
		return Slot{}, errors.New("railgate: decode slot")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Slot{}, errors.New("railgate: trailing slot data")
	}
	if !slot.valid() || slot.Key().fileName() != name {
		return Slot{}, errors.New("railgate: invalid slot record")
	}
	return slot, nil
}

// write stores a slot. create refuses an existing file (O_EXCL), so two
// admissions of one key cannot both succeed; an update replaces the file
// atomically. Both sync the file and the directory before returning.
func (g *Gate) write(slot Slot, create bool) error {
	if !slot.valid() {
		return errors.New("railgate: refusing to store an invalid slot")
	}
	encoded, err := json.Marshal(slot)
	if err != nil {
		return errors.New("railgate: encode slot")
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(g.root, slot.Key().fileName())
	if create {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return ErrSlotTaken
			}
			return errors.New("railgate: create slot")
		}
		if _, err := file.Write(encoded); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return errors.New("railgate: write slot")
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return errors.New("railgate: sync slot")
		}
		if err := file.Close(); err != nil {
			_ = os.Remove(path)
			return errors.New("railgate: close slot")
		}
		return syncDirectory(g.root)
	}
	temporary, err := os.CreateTemp(g.root, ".slot-")
	if err != nil {
		return errors.New("railgate: create slot update")
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return errors.New("railgate: protect slot update")
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return errors.New("railgate: write slot update")
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errors.New("railgate: sync slot update")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("railgate: close slot update")
	}
	if err := os.Rename(name, path); err != nil {
		return errors.New("railgate: commit slot update")
	}
	return syncDirectory(g.root)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("railgate: open slot directory")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("railgate: sync slot directory")
	}
	return nil
}
