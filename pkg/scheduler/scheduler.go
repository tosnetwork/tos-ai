// Package scheduler provides bounded, explicit priority scheduling. It is a
// soft real-time admission layer; hard real-time and physical safety loops
// remain outside Go and outside TOS networking.
package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

var (
	ErrQueueFull = errors.New("scheduler queue is full")
	ErrDuplicate = errors.New("request ID already exists")
	ErrStopped   = errors.New("scheduler stopped")
	ErrCanceled  = errors.New("request canceled")
)

const (
	MaxWorkersHard = 128
	MaxQueueHard   = 4096
)

type Work func(context.Context) (airuntime.Response, error)

type Item struct {
	ID       string
	Priority airuntime.Priority
	Deadline time.Time
	Context  context.Context
	Work     Work
}

type Result struct {
	Response airuntime.Response
	Err      error
}

type Config struct {
	Workers  int
	MaxQueue int
	// OwnerReservedWorkers never execute external or background work.
	// They may be zero but must remain below Workers.
	OwnerReservedWorkers int
}

type queuedItem struct {
	item     Item
	sequence uint64
	index    int
	result   chan Result
}

type Scheduler struct {
	mu       sync.Mutex
	cond     *sync.Cond
	config   Config
	queue    priorityQueue
	pending  map[string]*queuedItem
	running  map[string]context.CancelFunc
	sequence uint64
	started  bool
	stopped  bool
	workers  sync.WaitGroup
	done     chan struct{}
	waitOnce sync.Once
}

func New(config Config) (*Scheduler, error) {
	if config.Workers <= 0 || config.Workers > MaxWorkersHard ||
		config.MaxQueue <= 0 || config.MaxQueue > MaxQueueHard ||
		config.OwnerReservedWorkers < 0 ||
		config.OwnerReservedWorkers >= config.Workers {
		return nil, errors.New("invalid scheduler configuration")
	}
	scheduler := &Scheduler{
		config:  config,
		pending: make(map[string]*queuedItem, config.MaxQueue),
		running: make(map[string]context.CancelFunc, config.Workers),
		done:    make(chan struct{}),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	heap.Init(&scheduler.queue)
	return scheduler, nil
}

func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrStopped
	}
	if s.started {
		return nil
	}
	s.started = true
	generalWorkers := s.config.Workers - s.config.OwnerReservedWorkers
	for index := 0; index < s.config.Workers; index++ {
		s.workers.Add(1)
		go s.worker(index >= generalWorkers)
	}
	return nil
}

func (s *Scheduler) Submit(item Item) (<-chan Result, error) {
	if item.ID == "" || len(item.ID) > 128 || item.Work == nil {
		return nil, errors.New("invalid work item")
	}
	if item.Context == nil {
		item.Context = context.Background()
	}
	if item.Deadline.IsZero() || !item.Deadline.After(time.Now()) {
		return nil, context.DeadlineExceeded
	}
	if item.Priority < airuntime.PriorityEmergency || item.Priority > airuntime.PriorityBackground {
		return nil, errors.New("invalid priority")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, ErrStopped
	}
	if _, exists := s.pending[item.ID]; exists {
		return nil, ErrDuplicate
	}
	if _, exists := s.running[item.ID]; exists {
		return nil, ErrDuplicate
	}
	if len(s.queue) >= s.config.MaxQueue {
		return nil, ErrQueueFull
	}
	s.sequence++
	queued := &queuedItem{
		item:     item,
		sequence: s.sequence,
		result:   make(chan Result, 1),
	}
	heap.Push(&s.queue, queued)
	s.pending[item.ID] = queued
	if s.config.OwnerReservedWorkers > 0 {
		s.cond.Broadcast()
	} else {
		s.cond.Signal()
	}
	return queued.result, nil
}

func (s *Scheduler) Cancel(id string) bool {
	s.mu.Lock()
	if pending, exists := s.pending[id]; exists {
		heap.Remove(&s.queue, pending.index)
		delete(s.pending, id)
		s.mu.Unlock()
		pending.result <- Result{Err: ErrCanceled}
		close(pending.result)
		return true
	}
	if cancel, exists := s.running[id]; exists {
		cancel()
		s.mu.Unlock()
		return true
	}
	s.mu.Unlock()
	return false
}

func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		for len(s.queue) > 0 {
			pending := heap.Pop(&s.queue).(*queuedItem)
			delete(s.pending, pending.item.ID)
			pending.result <- Result{Err: ErrStopped}
			close(pending.result)
		}
		for _, cancel := range s.running {
			cancel()
		}
		s.cond.Broadcast()
	}
	s.mu.Unlock()
	s.waitOnce.Do(func() {
		go func() {
			s.workers.Wait()
			close(s.done)
		}()
	})
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) worker(ownerOnly bool) {
	defer s.workers.Done()
	for {
		s.mu.Lock()
		for !s.stopped && !s.hasEligibleWorkLocked(ownerOnly) {
			s.cond.Wait()
		}
		if s.stopped {
			s.mu.Unlock()
			return
		}
		queued := heap.Pop(&s.queue).(*queuedItem)
		delete(s.pending, queued.item.ID)
		runContext, cancel := context.WithDeadline(queued.item.Context, queued.item.Deadline)
		s.running[queued.item.ID] = cancel
		s.mu.Unlock()

		response, err := safeWork(runContext, queued.item.Work)
		cancel()

		s.mu.Lock()
		delete(s.running, queued.item.ID)
		s.mu.Unlock()
		queued.result <- Result{Response: response, Err: err}
		close(queued.result)
	}
}

func (s *Scheduler) hasEligibleWorkLocked(ownerOnly bool) bool {
	if len(s.queue) == 0 {
		return false
	}
	return !ownerOnly || isOwnerPriority(s.queue[0].item.Priority)
}

func isOwnerPriority(priority airuntime.Priority) bool {
	return priority <= airuntime.PriorityLocalAsync
}

func safeWork(ctx context.Context, work Work) (response airuntime.Response, err error) {
	defer func() {
		if recover() != nil {
			response = airuntime.Response{}
			err = airuntime.NewError(airuntime.ErrorInternal, nil)
		}
	}()
	return work(ctx)
}

type priorityQueue []*queuedItem

func (q priorityQueue) Len() int { return len(q) }
func (q priorityQueue) Less(a, b int) bool {
	if q[a].item.Priority == q[b].item.Priority {
		return q[a].sequence < q[b].sequence
	}
	return q[a].item.Priority < q[b].item.Priority
}
func (q priorityQueue) Swap(a, b int) {
	q[a], q[b] = q[b], q[a]
	q[a].index = a
	q[b].index = b
}
func (q *priorityQueue) Push(value interface{}) {
	item := value.(*queuedItem)
	item.index = len(*q)
	*q = append(*q, item)
}
func (q *priorityQueue) Pop() interface{} {
	old := *q
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*q = old[:last]
	return item
}
