package coordination

import (
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

// ComputeTop sorts records in descending order according to FruitItem.Less
// and returns the top k records (or fewer if len(records) < k).
// It does not mutate the input slice.
func ComputeTop(records []fruititem.FruitItem, k int) []fruititem.FruitItem {
	if len(records) == 0 || k <= 0 {
		return []fruititem.FruitItem{}
	}

	sorted := make([]fruititem.FruitItem, len(records))
	copy(sorted, records)

	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[j].Less(sorted[i])
	})

	finalSize := min(k, len(sorted))
	return sorted[:finalSize]
}
