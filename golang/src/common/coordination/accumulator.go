package coordination

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

// FruitAccumulator encapsulates accumulation of FruitItem quantities by fruit name.
// It maintains internal totals using the opaque FruitItem.Sum method.
type FruitAccumulator struct {
	items map[string]fruititem.FruitItem
}

// NewFruitAccumulator initializes an empty FruitAccumulator.
func NewFruitAccumulator() *FruitAccumulator {
	return &FruitAccumulator{
		items: make(map[string]fruititem.FruitItem),
	}
}

// Add incorporates a single FruitItem into the accumulator.
func (a *FruitAccumulator) Add(record fruititem.FruitItem) {
	if existing, ok := a.items[record.Fruit]; ok {
		a.items[record.Fruit] = existing.Sum(record)
	} else {
		a.items[record.Fruit] = record
	}
}

// AddAll incorporates multiple FruitItem records into the accumulator.
func (a *FruitAccumulator) AddAll(records []fruititem.FruitItem) {
	for _, record := range records {
		a.Add(record)
	}
}

// Get returns the accumulated FruitItem for a specific fruit and whether it exists.
func (a *FruitAccumulator) Get(fruit string) (fruititem.FruitItem, bool) {
	item, ok := a.items[fruit]
	return item, ok
}

// All returns a slice containing all accumulated FruitItem records in unspecified order.
func (a *FruitAccumulator) All() []fruititem.FruitItem {
	result := make([]fruititem.FruitItem, 0, len(a.items))
	for _, item := range a.items {
		result = append(result, item)
	}
	return result
}

// Top computes and returns the top k accumulated FruitItem records in descending order.
func (a *FruitAccumulator) Top(k int) []fruititem.FruitItem {
	return ComputeTop(a.All(), k)
}

// Len returns the number of distinct fruits tracked.
func (a *FruitAccumulator) Len() int {
	return len(a.items)
}

// Reset clears all accumulated records.
func (a *FruitAccumulator) Reset() {
	clear(a.items)
}
