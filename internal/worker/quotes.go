package worker

import (
	"container/list"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"google.golang.org/protobuf/proto"
)

var (
	errQuoteConflict = errors.New("request ID was reused with different quote content")
)

type quoteBinding struct {
	response       *edgev1.QuoteResponse
	serviceID      string
	operation      string
	model          string
	inputBytes     uint64
	maxOutputBytes uint64
	deadlineMillis int64
	priority       edgev1.Priority
	resources      admission.Resources
	fingerprint    [sha256.Size]byte
}

type quoteRecord struct {
	id        string
	requestID string
	binding   quoteBinding
}

type quoteStore struct {
	mu       sync.Mutex
	max      int
	records  map[string]*list.Element
	requests map[string]*list.Element
	order    *list.List
}

func newQuoteStore(maximum int) *quoteStore {
	return &quoteStore{
		max:      maximum,
		records:  make(map[string]*list.Element, maximum),
		requests: make(map[string]*list.Element, maximum),
		order:    list.New(),
	}
}

func (s *quoteStore) add(id, requestID string, binding quoteBinding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.records[id]; existing != nil {
		s.removeLocked(existing)
	}
	for len(s.records) >= s.max {
		oldest := s.order.Front()
		s.removeLocked(oldest)
	}
	binding.response = proto.Clone(binding.response).(*edgev1.QuoteResponse)
	element := s.order.PushBack(quoteRecord{id: id, requestID: requestID, binding: binding})
	s.records[id] = element
	s.requests[requestID] = element
}

func (s *quoteStore) get(id string, now time.Time) (quoteBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.records[id]
	if element == nil {
		return quoteBinding{}, errors.New("quote not found")
	}
	record := element.Value.(quoteRecord)
	if record.binding.response.ExpiresUnixMillis <= now.UnixMilli() {
		s.removeLocked(element)
		return quoteBinding{}, errors.New("quote expired")
	}
	s.order.MoveToBack(element)
	return record.binding, nil
}

func (s *quoteStore) findRequest(requestID string, fingerprint [sha256.Size]byte, now time.Time) (*edgev1.QuoteResponse, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.requests[requestID]
	if element == nil {
		return nil, false, nil
	}
	record := element.Value.(quoteRecord)
	if record.binding.fingerprint != fingerprint {
		return nil, false, errQuoteConflict
	}
	if record.binding.response.ExpiresUnixMillis <= now.UnixMilli() {
		s.removeLocked(element)
		return nil, false, nil
	}
	s.order.MoveToBack(element)
	return proto.Clone(record.binding.response).(*edgev1.QuoteResponse), true, nil
}

func (s *quoteStore) removeLocked(element *list.Element) {
	record := element.Value.(quoteRecord)
	delete(s.records, record.id)
	if s.requests[record.requestID] == element {
		delete(s.requests, record.requestID)
	}
	s.order.Remove(element)
}
