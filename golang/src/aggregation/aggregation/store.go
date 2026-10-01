package aggregation

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/coordination"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type clientSession struct {
	accumulator *coordination.FruitAccumulator
	barrier     *coordination.Barrier
}

// AggregatorSessionStore manages per-client aggregation state and barrier synchronization.
type AggregatorSessionStore struct {
	sessions  map[string]*clientSession
	sumAmount int
	topSize   int
	mu        sync.Mutex
}

// NewAggregatorSessionStore creates an AggregatorSessionStore configured with the expected number
// of Sum workers and the desired Top-K ranking size.
func NewAggregatorSessionStore(sumAmount, topSize int) *AggregatorSessionStore {
	return &AggregatorSessionStore{
		sessions:  make(map[string]*clientSession),
		sumAmount: sumAmount,
		topSize:   topSize,
	}
}

func (s *AggregatorSessionStore) getOrCreateSession(clientID string) *clientSession {
	session, exists := s.sessions[clientID]
	if !exists {
		session = &clientSession{
			accumulator: coordination.NewFruitAccumulator(),
			barrier:     coordination.NewBarrier(s.sumAmount),
		}
		s.sessions[clientID] = session
	}
	return session
}

// AddRecords incorporates fruit records into the client's accumulator.
func (s *AggregatorSessionStore) AddRecords(clientID string, records []fruititem.FruitItem) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.getOrCreateSession(clientID)
	session.accumulator.AddAll(records)
}

// RecordEOF records an EOF signal from a Sum worker. If the barrier is satisfied (all Sum workers
// reported EOF for this client), it computes the partial top-K ranking, evicts the client's session
// state to reclaim memory, and returns the top records with isComplete=true.
func (s *AggregatorSessionStore) RecordEOF(clientID string, sumID int) ([]fruititem.FruitItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.getOrCreateSession(clientID)
	session.barrier.Record(sumID)

	if !session.barrier.IsComplete() {
		return nil, false
	}

	partialTop := session.accumulator.Top(s.topSize)
	delete(s.sessions, clientID) // Evict completed session

	return partialTop, true
}

// HasSession checks if a session is currently active for the client (primarily for testing).
func (s *AggregatorSessionStore) HasSession(clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.sessions[clientID]
	return exists
}

// ActiveSessions returns the number of currently active client sessions.
func (s *AggregatorSessionStore) ActiveSessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.sessions)
}
