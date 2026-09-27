package aggregation

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

func TestAggregationStateAndTop(t *testing.T) {
	config := AggregationConfig{
		Id:        0,
		SumAmount: 2,
		TopSize:   3,
	}

	agg := &Aggregation{
		config:       config,
		fruitSums:    make(map[string]map[string]fruititem.FruitItem),
		eofsReceived: make(map[string]map[int]bool),
	}

	// Client 1 records
	agg.handleDataMessage("client-1", []fruititem.FruitItem{
		{Fruit: "banana", Amount: 10},
		{Fruit: "apple", Amount: 20},
		{Fruit: "orange", Amount: 5},
		{Fruit: "pear", Amount: 15},
	})

	top := agg.buildFruitTop("client-1")
	if len(top) != 3 {
		t.Fatalf("expected top size 3, got %d", len(top))
	}
	// Expected descending order: apple (20), pear (15), banana (10)
	if top[0].Fruit != "apple" || top[0].Amount != 20 {
		t.Errorf("expected rank 1 apple 20, got %s %d", top[0].Fruit, top[0].Amount)
	}
	if top[1].Fruit != "pear" || top[1].Amount != 15 {
		t.Errorf("expected rank 2 pear 15, got %s %d", top[1].Fruit, top[1].Amount)
	}
	if top[2].Fruit != "banana" || top[2].Amount != 10 {
		t.Errorf("expected rank 3 banana 10, got %s %d", top[2].Fruit, top[2].Amount)
	}
}

func TestAggregationBarrier(t *testing.T) {
	mockOut := &mockMiddleware{}
	config := AggregationConfig{
		Id:        1,
		SumAmount: 3,
		TopSize:   3,
	}

	agg := &Aggregation{
		config:       config,
		outputQueue:  mockOut,
		fruitSums:    make(map[string]map[string]fruititem.FruitItem),
		eofsReceived: make(map[string]map[int]bool),
	}

	clientID := "client-xyz"
	agg.handleDataMessage(clientID, []fruititem.FruitItem{
		{Fruit: "apple", Amount: 50},
	})

	// 1st EOF from Sum 0
	agg.handleEOFMessage(clientID, 0)
	if len(agg.eofsReceived[clientID]) != 1 {
		t.Errorf("expected 1 EOF recorded")
	}
	if len(mockOut.sentMessages) != 0 {
		t.Errorf("should not emit top yet")
	}

	// 2nd EOF from Sum 1
	agg.handleEOFMessage(clientID, 1)
	if len(agg.eofsReceived[clientID]) != 2 {
		t.Errorf("expected 2 EOFs recorded")
	}
	if len(mockOut.sentMessages) != 0 {
		t.Errorf("should not emit top yet")
	}

	// 3rd EOF from Sum 2 - barrier reached
	agg.handleEOFMessage(clientID, 2)
	if len(mockOut.sentMessages) != 1 {
		t.Fatalf("expected 1 emitted top message, got %d", len(mockOut.sentMessages))
	}

	innerMsg, err := inner.DeserializeInnerMessage(&mockOut.sentMessages[0])
	if err != nil {
		t.Fatalf("failed to deserialize sent message: %v", err)
	}
	if innerMsg.Type != inner.MsgTop {
		t.Errorf("expected MsgTop, got %s", innerMsg.Type)
	}
	if innerMsg.ClientID != clientID {
		t.Errorf("expected clientID %s, got %s", clientID, innerMsg.ClientID)
	}
	if innerMsg.SenderID != 1 {
		t.Errorf("expected senderID 1, got %d", innerMsg.SenderID)
	}
	if len(innerMsg.Records) != 1 || innerMsg.Records[0].Fruit != "apple" {
		t.Errorf("unexpected records: %+v", innerMsg.Records)
	}

	// State must be cleaned up
	if agg.fruitSums[clientID] != nil {
		t.Errorf("expected fruitSums cleaned up")
	}
	if agg.eofsReceived[clientID] != nil {
		t.Errorf("expected eofsReceived cleaned up")
	}
}

func TestAggregationEmptyClientBarrier(t *testing.T) {
	mockOut := &mockMiddleware{}
	config := AggregationConfig{
		Id:        0,
		SumAmount: 2,
		TopSize:   3,
	}

	agg := &Aggregation{
		config:       config,
		outputQueue:  mockOut,
		fruitSums:    make(map[string]map[string]fruititem.FruitItem),
		eofsReceived: make(map[string]map[int]bool),
	}

	// Client has 0 records received by this aggregator
	clientID := "client-empty"
	agg.handleEOFMessage(clientID, 0)
	agg.handleEOFMessage(clientID, 1)

	if len(mockOut.sentMessages) != 1 {
		t.Fatalf("expected 1 emitted message for empty client, got %d", len(mockOut.sentMessages))
	}

	innerMsg, err := inner.DeserializeInnerMessage(&mockOut.sentMessages[0])
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}
	if len(innerMsg.Records) != 0 {
		t.Errorf("expected 0 records in partial top, got %d", len(innerMsg.Records))
	}
}
