package aggregation

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func TestAggregatorSessionStore(t *testing.T) {
	store := NewAggregatorSessionStore(2, 3)

	// Add records
	store.AddRecords("client-1", []fruititem.FruitItem{
		{Fruit: "banana", Amount: 10},
		{Fruit: "apple", Amount: 20},
		{Fruit: "orange", Amount: 5},
		{Fruit: "pear", Amount: 15},
	})

	// 1st EOF: should not be complete
	top, ready := store.RecordEOF("client-1", 0)
	if ready || top != nil {
		t.Fatal("expected barrier not to be ready after 1 of 2 EOFs")
	}

	// 2nd EOF: should be complete
	top, ready = store.RecordEOF("client-1", 1)
	if !ready {
		t.Fatal("expected barrier to be complete after 2 of 2 EOFs")
	}

	if len(top) != 3 {
		t.Fatalf("expected top size 3, got %d", len(top))
	}
	if top[0].Fruit != "apple" || top[0].Amount != 20 {
		t.Errorf("expected rank 1 apple 20, got %+v", top[0])
	}
	if top[1].Fruit != "pear" || top[1].Amount != 15 {
		t.Errorf("expected rank 2 pear 15, got %+v", top[1])
	}
	if top[2].Fruit != "banana" || top[2].Amount != 10 {
		t.Errorf("expected rank 3 banana 10, got %+v", top[2])
	}

	// Session should be evicted
	if store.HasSession("client-1") {
		t.Error("expected session for client-1 to be evicted after completion")
	}
}

func TestAggregationBarrier(t *testing.T) {
	mockOut := middleware.NewMockMiddleware()
	config := AggregationConfig{
		Id:        1,
		SumAmount: 3,
		TopSize:   3,
	}

	agg := &Aggregation{
		config:      config,
		outputQueue: mockOut,
		store:       NewAggregatorSessionStore(config.SumAmount, config.TopSize),
	}

	clientID := "client-xyz"
	agg.handleDataMessage(clientID, []fruititem.FruitItem{
		{Fruit: "apple", Amount: 50},
	})

	// 1st EOF from Sum 0
	agg.handleEOFMessage(clientID, 0)
	if len(mockOut.SentMessages) != 0 {
		t.Errorf("should not emit top yet")
	}

	// 2nd EOF from Sum 1
	agg.handleEOFMessage(clientID, 1)
	if len(mockOut.SentMessages) != 0 {
		t.Errorf("should not emit top yet")
	}

	// 3rd EOF from Sum 2 - barrier reached
	agg.handleEOFMessage(clientID, 2)
	if len(mockOut.SentMessages) != 1 {
		t.Fatalf("expected 1 emitted top message, got %d", len(mockOut.SentMessages))
	}

	innerMsg, err := inner.DeserializeInnerMessage(&mockOut.SentMessages[0])
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
	if agg.store.HasSession(clientID) {
		t.Errorf("expected session cleaned up after barrier")
	}
}

func TestAggregationEmptyClientBarrier(t *testing.T) {
	mockOut := middleware.NewMockMiddleware()
	config := AggregationConfig{
		Id:        0,
		SumAmount: 2,
		TopSize:   3,
	}

	agg := &Aggregation{
		config:      config,
		outputQueue: mockOut,
		store:       NewAggregatorSessionStore(config.SumAmount, config.TopSize),
	}

	// Client has 0 records received by this aggregator
	clientID := "client-empty"
	agg.handleEOFMessage(clientID, 0)
	agg.handleEOFMessage(clientID, 1)

	if len(mockOut.SentMessages) != 1 {
		t.Fatalf("expected 1 emitted message for empty client, got %d", len(mockOut.SentMessages))
	}

	innerMsg, err := inner.DeserializeInnerMessage(&mockOut.SentMessages[0])
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}
	if len(innerMsg.Records) != 0 {
		t.Errorf("expected 0 records in partial top, got %d", len(innerMsg.Records))
	}
}

func TestAggregationStopIdempotent(t *testing.T) {
	mockIn := middleware.NewMockMiddleware()
	mockOut := middleware.NewMockMiddleware()

	agg := &Aggregation{
		inputExchange: mockIn,
		outputQueue:   mockOut,
		store:         NewAggregatorSessionStore(2, 3),
	}

	// First Stop() call
	agg.Stop()

	if mockIn.StopConsumingCount != 1 {
		t.Errorf("expected mockIn.StopConsuming() called once, got %d", mockIn.StopConsumingCount)
	}
	if mockIn.CloseCount != 1 {
		t.Errorf("expected mockIn.Close() called once, got %d", mockIn.CloseCount)
	}
	if mockOut.CloseCount != 1 {
		t.Errorf("expected mockOut.Close() called once, got %d", mockOut.CloseCount)
	}

	// Second Stop() call (must be no-op via sync.Once)
	agg.Stop()

	if mockIn.StopConsumingCount != 1 {
		t.Errorf("expected mockIn.StopConsuming() still called once, got %d", mockIn.StopConsumingCount)
	}
	if mockIn.CloseCount != 1 {
		t.Errorf("expected mockIn.Close() still called once, got %d", mockIn.CloseCount)
	}
	if mockOut.CloseCount != 1 {
		t.Errorf("expected mockOut.Close() still called once, got %d", mockOut.CloseCount)
	}
}
