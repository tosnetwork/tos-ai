// Package admission implements the terminal-owned, bounded resource
// admission authority. A quote may call Check, but only Reserve creates a
// short-lived local reservation.
package admission

import (
	"crypto/sha256"
	"errors"
	"sync"
	"time"
)

var (
	ErrStopped        = errors.New("admission is draining")
	ErrQueueFull      = errors.New("admission queue is full")
	ErrCapacity       = errors.New("local resource capacity is unavailable")
	ErrLimit          = errors.New("request exceeds local admission limits")
	ErrConflict       = errors.New("request ID was reused with different content")
	ErrPriority       = errors.New("priority is not accepted by local admission")
	ErrAlreadyRunning = errors.New("reservation is already running")
	ErrConcurrency    = errors.New("concurrent execution capacity is unavailable")
)

const (
	MaxConcurrentHard = 128
	MaxQueueHard      = 4096
)

var hardResourceLimits = Resources{
	RAMBytes: 1 << 40, VRAMBytes: 1 << 40, KVCacheBytes: 1 << 38,
	ContextTokens: 1 << 30, BatchSize: 1 << 20, OutputBytes: 1 << 36,
	ExecutionTime: time.Hour,
}

type Class uint8

const (
	ClassLocalAsync Class = iota + 1
	ClassExternalService
	ClassBackground
)

type Resources struct {
	RAMBytes      uint64
	VRAMBytes     uint64
	KVCacheBytes  uint64
	ContextTokens uint64
	BatchSize     uint32
	OutputBytes   uint64
	ExecutionTime time.Duration
}

type Config struct {
	MaxConcurrent int
	MaxQueue      int
	Capacity      Resources
	OwnerReserved Resources
	PerRequestMax Resources
}

type Request struct {
	ID          string
	Fingerprint [sha256.Size]byte
	Class       Class
	Resources   Resources
}

type Snapshot struct {
	Accepting     bool
	Running       int
	Reserved      int
	MaxRunning    int
	MaxQueue      int
	Used          Resources
	Capacity      Resources
	OwnerReserved Resources
}

type reservationState uint8

const (
	stateReserved reservationState = iota + 1
	stateRunning
)

type record struct {
	fingerprint [sha256.Size]byte
	class       Class
	resources   Resources
	state       reservationState
}

type Controller struct {
	mu      sync.Mutex
	config  Config
	records map[string]*record
	used    Resources
	running int
	stopped bool
}

type Reservation struct {
	controller *Controller
	id         string
	owner      bool
	once       sync.Once
}

func New(config Config) (*Controller, error) {
	if config.MaxConcurrent <= 0 || config.MaxConcurrent > MaxConcurrentHard ||
		config.MaxQueue < 0 || config.MaxQueue > MaxQueueHard ||
		config.Capacity.RAMBytes == 0 || config.Capacity.OutputBytes == 0 ||
		config.PerRequestMax.RAMBytes == 0 || config.PerRequestMax.OutputBytes == 0 ||
		config.PerRequestMax.ContextTokens == 0 || config.PerRequestMax.BatchSize == 0 ||
		config.PerRequestMax.ExecutionTime <= 0 ||
		!fits(config.Capacity, hardResourceLimits) ||
		!fits(config.OwnerReserved, config.Capacity) ||
		!fits(config.PerRequestMax, config.Capacity) {
		return nil, errors.New("invalid admission configuration")
	}
	return &Controller{
		config:  config,
		records: make(map[string]*record, config.MaxConcurrent+config.MaxQueue),
	}, nil
}

func (c *Controller) Check(request Request) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkLocked(request, true)
}

// Reserve is idempotent for the same ID and fingerprint. The owner result is
// true only for the caller that created the reservation; retry handles cannot
// start or release another caller's resources.
func (c *Controller) Reserve(request Request) (*Reservation, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.records[request.ID]; existing != nil {
		if existing.fingerprint != request.Fingerprint {
			return nil, false, ErrConflict
		}
		return &Reservation{controller: c, id: request.ID}, false, nil
	}
	if err := c.checkLocked(request, true); err != nil {
		return nil, false, err
	}
	c.records[request.ID] = &record{
		fingerprint: request.Fingerprint,
		class:       request.Class,
		resources:   request.Resources,
		state:       stateReserved,
	}
	addResources(&c.used, request.Resources)
	return &Reservation{controller: c, id: request.ID, owner: true}, true, nil
}

func (c *Controller) checkLocked(request Request, includeSlots bool) error {
	if c.stopped {
		return ErrStopped
	}
	if request.ID == "" || len(request.ID) > 128 {
		return ErrLimit
	}
	if request.Class < ClassLocalAsync || request.Class > ClassBackground {
		return ErrPriority
	}
	if !validRequestResources(request.Resources) ||
		!fits(request.Resources, c.config.PerRequestMax) {
		return ErrLimit
	}
	if includeSlots && len(c.records) >= c.config.MaxConcurrent+c.config.MaxQueue {
		return ErrQueueFull
	}
	capacity := c.config.Capacity
	if request.Class != ClassLocalAsync {
		capacity = subtractResources(capacity, c.config.OwnerReserved)
	}
	projected := c.used
	addResources(&projected, request.Resources)
	if !fits(projected, capacity) {
		return ErrCapacity
	}
	return nil
}

func validRequestResources(value Resources) bool {
	return value.RAMBytes > 0 && value.ContextTokens > 0 && value.BatchSize > 0 &&
		value.OutputBytes > 0 && value.ExecutionTime > 0
}

func (r *Reservation) Start() error {
	if r == nil || !r.owner || r.controller == nil {
		return ErrConflict
	}
	c := r.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.records[r.id]
	if current == nil {
		return ErrStopped
	}
	if current.state == stateRunning {
		return ErrAlreadyRunning
	}
	if c.running >= c.config.MaxConcurrent {
		return ErrConcurrency
	}
	current.state = stateRunning
	c.running++
	return nil
}

func (r *Reservation) Release() {
	if r == nil || !r.owner || r.controller == nil {
		return
	}
	r.once.Do(func() {
		c := r.controller
		c.mu.Lock()
		defer c.mu.Unlock()
		current := c.records[r.id]
		if current == nil {
			return
		}
		if current.state == stateRunning {
			c.running--
		}
		subtractUsed(&c.used, current.resources)
		delete(c.records, r.id)
	})
}

func (c *Controller) BeginDrain() {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
}

func (c *Controller) Shutdown() {
	c.mu.Lock()
	c.stopped = true
	clear(c.records)
	c.used = Resources{}
	c.running = 0
	c.mu.Unlock()
}

func (c *Controller) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Snapshot{
		Accepting:     !c.stopped,
		Running:       c.running,
		Reserved:      len(c.records),
		MaxRunning:    c.config.MaxConcurrent,
		MaxQueue:      c.config.MaxQueue,
		Used:          c.used,
		Capacity:      c.config.Capacity,
		OwnerReserved: c.config.OwnerReserved,
	}
}

func fits(value, maximum Resources) bool {
	return value.RAMBytes <= maximum.RAMBytes &&
		value.VRAMBytes <= maximum.VRAMBytes &&
		value.KVCacheBytes <= maximum.KVCacheBytes &&
		value.ContextTokens <= maximum.ContextTokens &&
		value.BatchSize <= maximum.BatchSize &&
		value.OutputBytes <= maximum.OutputBytes &&
		value.ExecutionTime <= maximum.ExecutionTime
}

func addResources(target *Resources, value Resources) {
	target.RAMBytes += value.RAMBytes
	target.VRAMBytes += value.VRAMBytes
	target.KVCacheBytes += value.KVCacheBytes
	target.ContextTokens += value.ContextTokens
	target.BatchSize += value.BatchSize
	target.OutputBytes += value.OutputBytes
}

func subtractResources(value, reserved Resources) Resources {
	return Resources{
		RAMBytes:      value.RAMBytes - reserved.RAMBytes,
		VRAMBytes:     value.VRAMBytes - reserved.VRAMBytes,
		KVCacheBytes:  value.KVCacheBytes - reserved.KVCacheBytes,
		ContextTokens: value.ContextTokens - reserved.ContextTokens,
		BatchSize:     value.BatchSize - reserved.BatchSize,
		OutputBytes:   value.OutputBytes - reserved.OutputBytes,
		ExecutionTime: value.ExecutionTime,
	}
}

func subtractUsed(target *Resources, value Resources) {
	target.RAMBytes -= value.RAMBytes
	target.VRAMBytes -= value.VRAMBytes
	target.KVCacheBytes -= value.KVCacheBytes
	target.ContextTokens -= value.ContextTokens
	target.BatchSize -= value.BatchSize
	target.OutputBytes -= value.OutputBytes
}
