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

