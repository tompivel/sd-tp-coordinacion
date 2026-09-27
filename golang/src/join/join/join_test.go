package join

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type mockMiddleware struct {
	sentMessages []middleware.Message
}

func (m *mockMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	return nil
}
func (m *mockMiddleware) StopConsuming() error { return nil }
func (m *mockMiddleware) Send(msg middleware.Message) error {
	m.sentMessages = append(m.sentMessages, msg)
	return nil
}
func (m *mockMiddleware) SendTo(routingKey string, msg middleware.Message) error {
	m.sentMessages = append(m.sentMessages, msg)
	return nil
}
func (m *mockMiddleware) Close() error { return nil }

