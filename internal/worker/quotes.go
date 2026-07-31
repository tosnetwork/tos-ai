package worker

import (
	"container/list"
	"errors"
	"sync"
	"time"

	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"google.golang.org/protobuf/proto"
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
}

type quoteRecord struct {
	id      string
	binding quoteBinding
}

type quoteStore struct {
	mu      sync.Mutex
	max     int
	records map[string]*list.Element
	order   *list.List
}

func newQuoteStore(maximum int) *quoteStore {
	return &quoteStore{
		max:     maximum,
		records: make(map[string]*list.Element, maximum),
		order:   list.New(),
	}
}

func (s *quoteStore) add(id string, binding quoteBinding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.records[id]; existing != nil {
		s.order.Remove(existing)
		delete(s.records, id)
	}
	for len(s.records) >= s.max {
		oldest := s.order.Front()
		delete(s.records, oldest.Value.(quoteRecord).id)
		s.order.Remove(oldest)
	}
	binding.response = proto.Clone(binding.response).(*edgev1.QuoteResponse)
	element := s.order.PushBack(quoteRecord{id: id, binding: binding})
	s.records[id] = element
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
		s.order.Remove(element)
		delete(s.records, id)
		return quoteBinding{}, errors.New("quote expired")
	}
	s.order.MoveToBack(element)
	return record.binding, nil
}
