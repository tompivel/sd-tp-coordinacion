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

