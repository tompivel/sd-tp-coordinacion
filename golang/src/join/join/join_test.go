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

func TestJoinConsolidationAndBarrier(t *testing.T) {
	mockOut := &mockMiddleware{}
	config := JoinConfig{
		AggregationAmount: 3,
		TopSize:           3,
	}

	joinNode := &Join{
		config:       config,
		outputQueue:  mockOut,
		partialTops:  make(map[string][]fruititem.FruitItem),
		receivedTops: make(map[string]map[int]bool),
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
	if joinNode.partialTops[clientID] != nil {
		t.Errorf("expected partialTops cleaned up")
	}
	if joinNode.receivedTops[clientID] != nil {
		t.Errorf("expected receivedTops cleaned up")
	}
}
