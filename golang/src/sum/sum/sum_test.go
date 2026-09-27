package sum

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func TestSumSessionStore(t *testing.T) {
	store := NewSumSessionStore()

	// Accumulate records for client-1
	added := store.AddRecords("client-1", []fruititem.FruitItem{
		{Fruit: "apple", Amount: 10},
		{Fruit: "banana", Amount: 5},
	})
	if !added {
		t.Fatal("expected records to be added")
	}

	store.AddRecords("client-1", []fruititem.FruitItem{
		{Fruit: "apple", Amount: 15},
		{Fruit: "orange", Amount: 7},
	})

	// Accumulate for client-2
	store.AddRecords("client-2", []fruititem.FruitItem{
		{Fruit: "apple", Amount: 100},
	})

	// Verify client-1 records
	c1Records, ok := store.GetClientRecords("client-1")
	if !ok || len(c1Records) != 3 {
		t.Fatalf("expected 3 records for client-1, got %d", len(c1Records))
	}

	recordMap := make(map[string]uint32)
	for _, r := range c1Records {
		recordMap[r.Fruit] = r.Amount
	}
	if recordMap["apple"] != 25 || recordMap["banana"] != 5 || recordMap["orange"] != 7 {
		t.Errorf("unexpected sums for client-1: %+v", recordMap)
	}

	// Verify client-2
	c2Records, ok := store.GetClientRecords("client-2")
	if !ok || len(c2Records) != 1 || c2Records[0].Amount != 100 {
		t.Errorf("unexpected sums for client-2: %+v", c2Records)
	}

	// Finish client-1
	evicted, finishedOk := store.FinishAndEvict("client-1")
	if !finishedOk || len(evicted) != 3 {
		t.Fatalf("expected 3 evicted records on finish, got %d", len(evicted))
	}
	if !store.IsFinished("client-1") {
		t.Error("expected client-1 to be marked finished")
	}

	// Second finish should return false
	_, finishedAgain := store.FinishAndEvict("client-1")
	if finishedAgain {
		t.Error("expected second finish to return false")
	}

	// Data added after finish should be rejected
	addedAfter := store.AddRecords("client-1", []fruititem.FruitItem{{Fruit: "apple", Amount: 5}})
	if addedAfter {
		t.Error("expected data added after finish to be rejected")
	}
}

func TestSumStateAccumulation(t *testing.T) {
	config := SumConfig{
		Id:                0,
		SumAmount:         3,
		SumPrefix:         "sum",
		AggregationAmount: 3,
		AggregationPrefix: "aggregation",
	}

	sumNode := &Sum{
		config: config,
		store:  NewSumSessionStore(),
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
	c1Records, ok := sumNode.store.GetClientRecords("client-1")
	if !ok {
		t.Fatalf("expected records for client-1")
	}
	recordMap := make(map[string]uint32)
	for _, r := range c1Records {
		recordMap[r.Fruit] = r.Amount
	}
	if recordMap["apple"] != 25 {
		t.Errorf("expected apple amount 25, got %d", recordMap["apple"])
	}
	if recordMap["banana"] != 5 {
		t.Errorf("expected banana amount 5, got %d", recordMap["banana"])
	}
	if recordMap["orange"] != 7 {
		t.Errorf("expected orange amount 7, got %d", recordMap["orange"])
	}

	// Check client-2 isolation
	c2Records, ok := sumNode.store.GetClientRecords("client-2")
	if !ok {
		t.Fatalf("expected records for client-2")
	}
	if len(c2Records) != 1 || c2Records[0].Amount != 100 {
		t.Errorf("expected apple amount 100, got %+v", c2Records)
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

func TestSumStopIdempotent(t *testing.T) {
	mockIn := middleware.NewMockMiddleware()
	mockOut := middleware.NewMockMiddleware()
	mockFanoutIn := middleware.NewMockMiddleware()
	mockFanoutOut := middleware.NewMockMiddleware()

	sumNode := &Sum{
		inputQueue:        mockIn,
		outputExchange:    mockOut,
		eofFanoutConsumer: mockFanoutIn,
		eofFanoutProducer: mockFanoutOut,
		store:             NewSumSessionStore(),
	}

	// First Stop() call
	sumNode.Stop()

	if mockIn.StopConsumingCount != 1 {
		t.Errorf("expected mockIn.StopConsuming() called once, got %d", mockIn.StopConsumingCount)
	}
	if mockFanoutIn.StopConsumingCount != 1 {
		t.Errorf("expected mockFanoutIn.StopConsuming() called once, got %d", mockFanoutIn.StopConsumingCount)
	}
	if mockIn.CloseCount != 1 {
		t.Errorf("expected mockIn.Close() called once, got %d", mockIn.CloseCount)
	}
	if mockOut.CloseCount != 1 {
		t.Errorf("expected mockOut.Close() called once, got %d", mockOut.CloseCount)
	}
	if mockFanoutIn.CloseCount != 1 {
		t.Errorf("expected mockFanoutIn.Close() called once, got %d", mockFanoutIn.CloseCount)
	}
	if mockFanoutOut.CloseCount != 1 {
		t.Errorf("expected mockFanoutOut.Close() called once, got %d", mockFanoutOut.CloseCount)
	}

	// Second Stop() call (must be no-op via sync.Once)
	sumNode.Stop()

	if mockIn.StopConsumingCount != 1 {
		t.Errorf("expected mockIn.StopConsuming() still called once, got %d", mockIn.StopConsumingCount)
	}
	if mockFanoutIn.StopConsumingCount != 1 {
		t.Errorf("expected mockFanoutIn.StopConsuming() still called once, got %d", mockFanoutIn.StopConsumingCount)
	}
	if mockIn.CloseCount != 1 {
		t.Errorf("expected mockIn.Close() still called once, got %d", mockIn.CloseCount)
	}
	if mockOut.CloseCount != 1 {
		t.Errorf("expected mockOut.Close() still called once, got %d", mockOut.CloseCount)
	}
	if mockFanoutIn.CloseCount != 1 {
		t.Errorf("expected mockFanoutIn.Close() still called once, got %d", mockFanoutIn.CloseCount)
	}
	if mockFanoutOut.CloseCount != 1 {
		t.Errorf("expected mockFanoutOut.Close() still called once, got %d", mockFanoutOut.CloseCount)
	}
}

