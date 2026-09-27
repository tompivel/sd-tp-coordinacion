package inner

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func TestDataMessageSerialization(t *testing.T) {
	records := []fruititem.FruitItem{
		{Fruit: "apple", Amount: 10},
		{Fruit: "banana", Amount: 20},
	}
	clientID := "client-42"

	msg, err := SerializeDataMessage(clientID, records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deserialized, err := DeserializeInnerMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if deserialized.Type != MsgData {
		t.Errorf("expected type %s, got %s", MsgData, deserialized.Type)
	}
	if deserialized.ClientID != clientID {
		t.Errorf("expected clientID %s, got %s", clientID, deserialized.ClientID)
	}
	if len(deserialized.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(deserialized.Records))
	}
	if deserialized.Records[0].Fruit != "apple" || deserialized.Records[0].Amount != 10 {
		t.Errorf("unexpected record 0: %+v", deserialized.Records[0])
	}
}

func TestEOFMessageSerialization(t *testing.T) {
	clientID := "client-99"
	senderID := 3

	msg, err := SerializeEOFMessage(clientID, senderID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deserialized, err := DeserializeInnerMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if deserialized.Type != MsgEOF {
		t.Errorf("expected type %s, got %s", MsgEOF, deserialized.Type)
	}
	if deserialized.ClientID != clientID {
		t.Errorf("expected clientID %s, got %s", clientID, deserialized.ClientID)
	}
	if deserialized.SenderID != senderID {
		t.Errorf("expected senderID %d, got %d", senderID, deserialized.SenderID)
	}
}

func TestTopMessageSerialization(t *testing.T) {
	clientID := "client-1"
	senderID := 2
	records := []fruititem.FruitItem{
		{Fruit: "mango", Amount: 100},
	}

	msg, err := SerializeTopMessage(clientID, senderID, records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deserialized, err := DeserializeInnerMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if deserialized.Type != MsgTop {
		t.Errorf("expected type %s, got %s", MsgTop, deserialized.Type)
	}
	if deserialized.ClientID != clientID {
		t.Errorf("expected clientID %s, got %s", clientID, deserialized.ClientID)
	}
	if deserialized.SenderID != senderID {
		t.Errorf("expected senderID %d, got %d", senderID, deserialized.SenderID)
	}
	if len(deserialized.Records) != 1 || deserialized.Records[0].Fruit != "mango" {
		t.Errorf("unexpected records: %+v", deserialized.Records)
	}
}

func TestLegacyMessageCompatibility(t *testing.T) {
	// Legacy data: [["apple", 5], ["banana", 10]]
	legacyDataMsg := &middleware.Message{
		Body: `[["apple", 5], ["banana", 10]]`,
	}

	records, isEOF, err := DeserializeMessage(legacyDataMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isEOF {
		t.Errorf("expected isEOF false")
	}
	if len(records) != 2 || records[0].Fruit != "apple" || records[0].Amount != 5 {
		t.Errorf("unexpected records: %+v", records)
	}

	// Legacy EOF: []
	legacyEOFMsg := &middleware.Message{
		Body: `[]`,
	}
	records, isEOF, err = DeserializeMessage(legacyEOFMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isEOF {
		t.Errorf("expected isEOF true")
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
}
