package messagehandler

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
)

func TestMessageHandlerClientIsolation(t *testing.T) {
	h1 := NewMessageHandler()
	h2 := NewMessageHandler()

	if h1.ClientID() == h2.ClientID() {
		t.Fatalf("expected distinct client IDs, got %s and %s", h1.ClientID(), h2.ClientID())
	}

	// Serialize data message with h1
	record := fruititem.FruitItem{Fruit: "apple", Amount: 15}
	msg, err := h1.SerializeDataMessage(record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	innerMsg, err := inner.DeserializeInnerMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if innerMsg.ClientID != h1.ClientID() {
		t.Errorf("expected client ID %s, got %s", h1.ClientID(), innerMsg.ClientID)
	}

	// Simulate result message for h1
	resultForH1, err := inner.SerializeTopMessage(h1.ClientID(), 0, []fruititem.FruitItem{record})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// h1 should process it
	records, err := h1.DeserializeResultMessage(resultForH1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 1 || records[0].Fruit != "apple" {
		t.Errorf("unexpected records: %+v", records)
	}

	// h2 should reject it (return nil, nil)
	records2, err := h2.DeserializeResultMessage(resultForH1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if records2 != nil {
		t.Errorf("expected nil records for mismatched client, got %+v", records2)
	}
}
