package worker

import (
	"container/list"
	"crypto/sha256"
	"errors"
	"sync"

	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"google.golang.org/protobuf/proto"
)

var (
	errInvocationConflict = errors.New("request ID was reused with different invocation content")
	errInvocationCapacity = errors.New("invocation replay capacity exhausted")
)

type invocation struct {
	id            string
	taskID        string
	requestDigest string
	done          chan struct{}
	response      *edgev1.InvokeResponse
	err           error
	completed     bool
	fingerprint   [sha256.Size]byte
}

type invocationStore struct {
	mu      sync.Mutex
	max     int
	records map[string]*list.Element
	order   *list.List
}

func newInvocationStore(maximum int) *invocationStore {
	return &invocationStore{
		max:     maximum,
		records: make(map[string]*list.Element, maximum),
		order:   list.New(),
	}
}

func (s *invocationStore) begin(
	id string,
	taskID string,
	requestDigest string,
	fingerprint [sha256.Size]byte,
) (*invocation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.records[id]; element != nil {
		if element.Value.(*invocation).fingerprint != fingerprint {
			return nil, false, errInvocationConflict
		}
		s.order.MoveToBack(element)
		return element.Value.(*invocation), false, nil
	}
	for len(s.records) >= s.max {
		var victim *list.Element
		for element := s.order.Front(); element != nil; element = element.Next() {
			if element.Value.(*invocation).completed {
				victim = element
				break
			}
		}
		if victim == nil {
			return nil, false, errInvocationCapacity
		}
		delete(s.records, victim.Value.(*invocation).id)
		s.order.Remove(victim)
	}
	call := &invocation{
		id: id, taskID: taskID, requestDigest: requestDigest,
		done: make(chan struct{}), fingerprint: fingerprint,
	}
	s.records[id] = s.order.PushBack(call)
	return call, true, nil
}

func (s *invocationStore) activeIdentity(
	id string,
	taskID string,
	requestDigest string,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.records[id]
	if element == nil {
		return false
	}
	call := element.Value.(*invocation)
	return !call.completed && call.taskID == taskID &&
		call.requestDigest == requestDigest
}

func (s *invocationStore) find(id string, fingerprint [sha256.Size]byte) (*invocation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.records[id]
	if element == nil {
		return nil, false, nil
	}
	call := element.Value.(*invocation)
	if call.fingerprint != fingerprint {
		return nil, false, errInvocationConflict
	}
	s.order.MoveToBack(element)
	return call, true, nil
}

func (s *invocationStore) finish(call *invocation, response *edgev1.InvokeResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if call.completed {
		return
	}
	if response != nil {
		call.response = proto.Clone(response).(*edgev1.InvokeResponse)
	}
	call.err = err
	call.completed = true
	close(call.done)
}

func (s *invocationStore) result(call *invocation) (*edgev1.InvokeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if call.response == nil {
		return nil, call.err
	}
	return proto.Clone(call.response).(*edgev1.InvokeResponse), call.err
}
