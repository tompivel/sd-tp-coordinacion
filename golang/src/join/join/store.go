package join

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/coordination"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type clientSession struct {
	records []fruititem.FruitItem
	barrier *coordination.Barrier
}

// JoinSessionStore encapsulates the collection of partial tops and barrier synchronization
// across Aggregator workers to compute the final global top ranking per client.
type JoinSessionStore struct {
	sessions          map[string]*clientSession
	aggregationAmount int
	topSize           int
	mu                sync.Mutex
}

// NewJoinSessionStore creates a JoinSessionStore configured with the expected number of
// Aggregator workers and the final global top size.
func NewJoinSessionStore(aggregationAmount, topSize int) *JoinSessionStore {
	return &JoinSessionStore{
		sessions:          make(map[string]*clientSession),
		aggregationAmount: aggregationAmount,
		topSize:           topSize,
	}
}

func (s *JoinSessionStore) getOrCreateSession(clientID string) *clientSession {
	session, exists := s.sessions[clientID]
	if !exists {
		session = &clientSession{
			records: make([]fruititem.FruitItem, 0),
			barrier: coordination.NewBarrier(s.aggregationAmount),
		}
		s.sessions[clientID] = session
	}
	return session
}

// AddPartialTop records a partial top from an Aggregator worker. If all Aggregator workers
// have reported their partial tops for this client, it computes the global top ranking,
// evicts the client's session state to reclaim memory, and returns the global top with ready=true.
func (s *JoinSessionStore) AddPartialTop(clientID string, aggID int, records []fruititem.FruitItem) ([]fruititem.FruitItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.getOrCreateSession(clientID)
	session.barrier.Record(aggID)
	session.records = append(session.records, records...)

	if !session.barrier.IsComplete() {
		return nil, false
	}

	globalTop := coordination.ComputeTop(session.records, s.topSize)
	delete(s.sessions, clientID) // Evict completed session

	return globalTop, true
}

// HasSession returns whether a session for the client is active.
func (s *JoinSessionStore) HasSession(clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.sessions[clientID]
	return exists
}

// ActiveSessions returns the number of active client sessions.
func (s *JoinSessionStore) ActiveSessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.sessions)
}
