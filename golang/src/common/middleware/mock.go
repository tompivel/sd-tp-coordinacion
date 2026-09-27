package middleware

import "sync"

// MockMiddleware provides a thread-safe fake implementation of the Middleware interface for testing.
type MockMiddleware struct {
	mu                 sync.Mutex
	SentMessages       []Message
	StopConsumingCount int
	CloseCount         int
}

// NewMockMiddleware initializes a new MockMiddleware.
func NewMockMiddleware() *MockMiddleware {
	return &MockMiddleware{
		SentMessages: make([]Message, 0),
	}
}

func (m *MockMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (m *MockMiddleware) StopConsuming() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.StopConsumingCount++
	return nil
}

func (m *MockMiddleware) Send(msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SentMessages = append(m.SentMessages, msg)
	return nil
}

func (m *MockMiddleware) SendTo(routingKey string, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SentMessages = append(m.SentMessages, msg)
	return nil
}

func (m *MockMiddleware) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CloseCount++
	return nil
}
