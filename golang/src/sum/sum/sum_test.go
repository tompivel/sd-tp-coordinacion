package sum

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func TestSumStateAccumulation(t *testing.T) {
	config := SumConfig{
		Id:                0,
		SumAmount:         3,
		SumPrefix:         "sum",
		AggregationAmount: 3,
		AggregationPrefix: "aggregation",
	}

	sumNode := &Sum{
		config:         config,
		fruitItemMap:   make(map[string]map[string]fruititem.FruitItem),
		clientFinished: make(map[string]bool),
	}

	// Process data messages for client-1
	sumNode.handleDataMessage("client-1", []fruititem.FruitItem{
		{Fruit: "apple", Amount: 10},
		{Fruit: "banana", Amount: 5},
	})
	sumNode.handleDataMessage("client-1", []fruititem.FruitItem{
		{Fruit: "apple", Amount: 15},
		{Fruit: "orange", Amount: 7},
	})

	// Process data messages for client-2
	sumNode.handleDataMessage("client-2", []fruititem.FruitItem{
		{Fruit: "apple", Amount: 100},
	})

	// Check client-1
	c1Map := sumNode.fruitItemMap["client-1"]
	if c1Map == nil {
		t.Fatalf("expected map for client-1")
	}
	if c1Map["apple"].Amount != 25 {
		t.Errorf("expected apple amount 25, got %d", c1Map["apple"].Amount)
	}
	if c1Map["banana"].Amount != 5 {
		t.Errorf("expected banana amount 5, got %d", c1Map["banana"].Amount)
	}
	if c1Map["orange"].Amount != 7 {
		t.Errorf("expected orange amount 7, got %d", c1Map["orange"].Amount)
	}

	// Check client-2 isolation
	c2Map := sumNode.fruitItemMap["client-2"]
	if c2Map == nil {
		t.Fatalf("expected map for client-2")
	}
	if c2Map["apple"].Amount != 100 {
		t.Errorf("expected apple amount 100, got %d", c2Map["apple"].Amount)
	}
}

func TestFruitHashing(t *testing.T) {
	config := SumConfig{
		AggregationAmount: 3,
	}
	sumNode := &Sum{config: config}

	p1 := sumNode.hashFruit("apple")
	p2 := sumNode.hashFruit("apple")
	if p1 != p2 {
		t.Errorf("hashFruit must be deterministic: got %d and %d", p1, p2)
	}
	if p1 < 0 || p1 >= 3 {
		t.Errorf("hashFruit partition must be in [0, 3), got %d", p1)
	}
}
