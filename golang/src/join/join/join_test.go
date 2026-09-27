package join

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type mockMiddleware struct {
	sentMessages       []middleware.Message
	stopConsumingCount int
	closeCount         int
}

func (m *mockMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	return nil
}
func (m *mockMiddleware) StopConsuming() error {
	m.stopConsumingCount++
	return nil
}
func (m *mockMiddleware) Send(msg middleware.Message) error {
	m.sentMessages = append(m.sentMessages, msg)
	return nil
}
func (m *mockMiddleware) SendTo(routingKey string, msg middleware.Message) error {
	m.sentMessages = append(m.sentMessages, msg)
	return nil
}
func (m *mockMiddleware) Close() error {
	m.closeCount++
	return nil
}

func TestJoinSessionStore(t *testing.T) {
	store := NewJoinSessionStore(3, 3)

	clientID := "client-test"

	// 1st partial top
	top, ready := store.AddPartialTop(clientID, 0, []fruititem.FruitItem{
		{Fruit: "banana", Amount: 50},
		{Fruit: "kiwi", Amount: 10},
	})
	if ready || top != nil {
		t.Fatal("expected barrier not to be ready after 1 of 3 aggregators")
	}

	// 2nd partial top
	top, ready = store.AddPartialTop(clientID, 1, []fruititem.FruitItem{
		{Fruit: "apple", Amount: 100},
		{Fruit: "mango", Amount: 20},
	})
	if ready || top != nil {
		t.Fatal("expected barrier not to be ready after 2 of 3 aggregators")
	}

	// 3rd partial top (empty)
	top, ready = store.AddPartialTop(clientID, 2, []fruititem.FruitItem{})
	if !ready {
		t.Fatal("expected barrier to be complete after 3 of 3 aggregators")
	}

	if len(top) != 3 {
		t.Fatalf("expected top size 3, got %d", len(top))
	}
	if top[0].Fruit != "apple" || top[0].Amount != 100 {
		t.Errorf("rank 1 mismatch: %+v", top[0])
	}
	if top[1].Fruit != "banana" || top[1].Amount != 50 {
		t.Errorf("rank 2 mismatch: %+v", top[1])
	}
	if top[2].Fruit != "mango" || top[2].Amount != 20 {
		t.Errorf("rank 3 mismatch: %+v", top[2])
	}

	// Session must be evicted
	if store.HasSession(clientID) {
		t.Error("expected session to be evicted after global top computation")
	}
}

func TestJoinConsolidationAndBarrier(t *testing.T) {
	mockOut := &mockMiddleware{}
	config := JoinConfig{
		AggregationAmount: 3,
		TopSize:           3,
	}

	joinNode := &Join{
		config:      config,
		outputQueue: mockOut,
		store:       NewJoinSessionStore(config.AggregationAmount, config.TopSize),
	}

	clientID := "client-test"

	// Partial top from Aggregator 0
	msg0, _ := inner.SerializeTopMessage(clientID, 0, []fruititem.FruitItem{
		{Fruit: "banana", Amount: 50},
		{Fruit: "kiwi", Amount: 10},
	})
	joinNode.handleMessage(*msg0, func() {}, func() {})

	if len(mockOut.sentMessages) != 0 {
		t.Fatalf("should not emit global top before all 3 aggregators report")
	}

	// Partial top from Aggregator 1
	msg1, _ := inner.SerializeTopMessage(clientID, 1, []fruititem.FruitItem{
		{Fruit: "apple", Amount: 100},
		{Fruit: "mango", Amount: 20},
	})
	joinNode.handleMessage(*msg1, func() {}, func() {})

	if len(mockOut.sentMessages) != 0 {
		t.Fatalf("should not emit global top before all 3 aggregators report")
	}

	// Partial top from Aggregator 2 (empty list)
	msg2, _ := inner.SerializeTopMessage(clientID, 2, []fruititem.FruitItem{})
	joinNode.handleMessage(*msg2, func() {}, func() {})

	// Now all 3 reported
	if len(mockOut.sentMessages) != 1 {
		t.Fatalf("expected 1 global top message, got %d", len(mockOut.sentMessages))
	}

	innerMsg, err := inner.DeserializeInnerMessage(&mockOut.sentMessages[0])
	if err != nil {
		t.Fatalf("failed to deserialize sent message: %v", err)
	}

	if innerMsg.ClientID != clientID {
		t.Errorf("expected clientID %s, got %s", clientID, innerMsg.ClientID)
	}
	if len(innerMsg.Records) != 3 {
		t.Fatalf("expected global top size 3, got %d", len(innerMsg.Records))
	}

	// Expected descending order: apple (100), banana (50), mango (20)
	if innerMsg.Records[0].Fruit != "apple" || innerMsg.Records[0].Amount != 100 {
		t.Errorf("rank 1 mismatch: %+v", innerMsg.Records[0])
	}
	if innerMsg.Records[1].Fruit != "banana" || innerMsg.Records[1].Amount != 50 {
		t.Errorf("rank 2 mismatch: %+v", innerMsg.Records[1])
	}
	if innerMsg.Records[2].Fruit != "mango" || innerMsg.Records[2].Amount != 20 {
		t.Errorf("rank 3 mismatch: %+v", innerMsg.Records[2])
	}

	// Verify state cleanup
	if joinNode.store.HasSession(clientID) {
		t.Errorf("expected session cleaned up after global top emission")
	}
}

func TestJoinStopIdempotent(t *testing.T) {
	mockIn := &mockMiddleware{}
	mockOut := &mockMiddleware{}

	joinNode := &Join{
		inputQueue:  mockIn,
		outputQueue: mockOut,
		store:       NewJoinSessionStore(3, 3),
	}

	// First Stop() call
	joinNode.Stop()

	if mockIn.stopConsumingCount != 1 {
		t.Errorf("expected mockIn.StopConsuming() called once, got %d", mockIn.stopConsumingCount)
	}
	if mockIn.closeCount != 1 {
		t.Errorf("expected mockIn.Close() called once, got %d", mockIn.closeCount)
	}
	if mockOut.closeCount != 1 {
		t.Errorf("expected mockOut.Close() called once, got %d", mockOut.closeCount)
	}

	// Second Stop() call (must be no-op via sync.Once)
	joinNode.Stop()

	if mockIn.stopConsumingCount != 1 {
		t.Errorf("expected mockIn.StopConsuming() still called once, got %d", mockIn.stopConsumingCount)
	}
	if mockIn.closeCount != 1 {
		t.Errorf("expected mockIn.Close() still called once, got %d", mockIn.closeCount)
	}
	if mockOut.closeCount != 1 {
		t.Errorf("expected mockOut.Close() still called once, got %d", mockOut.closeCount)
	}
}
