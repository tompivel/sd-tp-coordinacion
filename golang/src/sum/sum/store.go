package sum

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/coordination"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type clientSession struct {
	accumulator *coordination.FruitAccumulator
	finished    bool
}

// SumSessionStore manages stateful fruit aggregation and completion lifecycle per client.
type SumSessionStore struct {
	sessions map[string]*clientSession
	mu       sync.Mutex
}

// NewSumSessionStore initializes an empty SumSessionStore.
func NewSumSessionStore() *SumSessionStore {
	return &SumSessionStore{
		sessions: make(map[string]*clientSession),
	}
}

// AddRecords incorporates fruit records for a given client.
// Returns false if the client has already completed processing.
func (s *SumSessionStore) AddRecords(clientID string, records []fruititem.FruitItem) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.sessions[clientID]
	if !exists {
		session = &clientSession{
			accumulator: coordination.NewFruitAccumulator(),
			finished:    false,
		}
		s.sessions[clientID] = session
	}

	if session.finished {
		return false
	}

	session.accumulator.AddAll(records)
	return true
}

// IsFinished returns whether the specified client has completed processing.
func (s *SumSessionStore) IsFinished(clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.sessions[clientID]
	return exists && session.finished
}

// FinishAndEvict marks the client as completed and retrieves all accumulated records,
// clearing the accumulated records from memory while preserving the completion marker.
// Returns (records, true) on first completion, or (nil, false) if already finished.
func (s *SumSessionStore) FinishAndEvict(clientID string) ([]fruititem.FruitItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.sessions[clientID]
	if !exists {
		session = &clientSession{
			accumulator: coordination.NewFruitAccumulator(),
			finished:    false,
		}
		s.sessions[clientID] = session
	}

	if session.finished {
		return nil, false
	}

	session.finished = true
	records := session.accumulator.All()
	session.accumulator.Reset() // Free accumulated items

	return records, true
}

// GetClientRecords returns a copy of accumulated records for testing/inspection.
func (s *SumSessionStore) GetClientRecords(clientID string) ([]fruititem.FruitItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.sessions[clientID]
	if !exists {
		return nil, false
	}
	return session.accumulator.All(), true
}
