package coordination

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func TestBarrier(t *testing.T) {
	b := NewBarrier(3)
	if b.IsComplete() {
		t.Fatal("expected barrier not to be complete initially")
	}
	if b.Count() != 0 {
		t.Fatalf("expected count 0, got %d", b.Count())
	}

	// Record sender 0
	if !b.Record(0) {
		t.Errorf("expected first record of 0 to return true")
	}
	// Duplicate record
	if b.Record(0) {
		t.Errorf("expected duplicate record of 0 to return false")
	}
	if b.IsComplete() {
		t.Error("expected barrier not to be complete with 1/3")
	}

	// Record sender 1
	b.Record(1)
	if b.IsComplete() {
		t.Error("expected barrier not to be complete with 2/3")
	}

	// Record sender 2
	b.Record(2)
	if !b.IsComplete() {
		t.Error("expected barrier to be complete with 3/3")
	}
	if b.Count() != 3 {
		t.Errorf("expected count 3, got %d", b.Count())
	}

	b.Reset()
	if b.IsComplete() || b.Count() != 0 {
		t.Error("expected barrier to be reset")
	}
}

func TestFruitAccumulator(t *testing.T) {
	acc := NewFruitAccumulator()

	acc.Add(fruititem.FruitItem{Fruit: "apple", Amount: 10})
	acc.Add(fruititem.FruitItem{Fruit: "banana", Amount: 20})
	acc.Add(fruititem.FruitItem{Fruit: "apple", Amount: 15})

	if acc.Len() != 2 {
		t.Fatalf("expected 2 fruits, got %d", acc.Len())
	}

	apple, ok := acc.Get("apple")
	if !ok || apple.Amount != 25 {
		t.Errorf("expected apple amount 25, got %+v", apple)
	}

	banana, ok := acc.Get("banana")
	if !ok || banana.Amount != 20 {
		t.Errorf("expected banana amount 20, got %+v", banana)
	}

	top1 := acc.Top(1)
	if len(top1) != 1 || top1[0].Fruit != "apple" || top1[0].Amount != 25 {
		t.Errorf("expected top 1 apple 25, got %+v", top1)
	}

	top5 := acc.Top(5)
	if len(top5) != 2 {
		t.Fatalf("expected top size 2, got %d", len(top5))
	}
	if top5[0].Fruit != "apple" || top5[1].Fruit != "banana" {
		t.Errorf("expected [apple, banana], got %+v", top5)
	}
}

func TestComputeTop(t *testing.T) {
	records := []fruititem.FruitItem{
		{Fruit: "banana", Amount: 10},
		{Fruit: "cherry", Amount: 50},
		{Fruit: "apple", Amount: 30},
		{Fruit: "date", Amount: 5},
	}

	top3 := ComputeTop(records, 3)
	if len(top3) != 3 {
		t.Fatalf("expected 3 items, got %d", len(top3))
	}
	if top3[0].Fruit != "cherry" || top3[0].Amount != 50 {
		t.Errorf("rank 1 should be cherry (50), got %+v", top3[0])
	}
	if top3[1].Fruit != "apple" || top3[1].Amount != 30 {
		t.Errorf("rank 2 should be apple (30), got %+v", top3[1])
	}
	if top3[2].Fruit != "banana" || top3[2].Amount != 10 {
		t.Errorf("rank 3 should be banana (10), got %+v", top3[2])
	}

	// Empty list
	empty := ComputeTop(nil, 3)
	if len(empty) != 0 {
		t.Errorf("expected empty result for nil records")
	}

	// k larger than records
	top10 := ComputeTop(records, 10)
	if len(top10) != 4 {
		t.Errorf("expected 4 items, got %d", len(top10))
	}
}
