package aggregation

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	config        AggregationConfig
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitSums     map[string]map[string]fruititem.FruitItem // clientID -> fruit -> FruitItem
	eofsReceived  map[string]map[int]bool                  // clientID -> sumID -> bool
	mu            sync.Mutex
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	queueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)
	routingKey := fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)

	inputExchange, err := middleware.CreateNamedTopicConsumerMiddleware(config.AggregationPrefix, queueName, routingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		config:        config,
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitSums:     make(map[string]map[string]fruititem.FruitItem),
		eofsReceived:  make(map[string]map[int]bool),
	}, nil
}

func (aggregation *Aggregation) Run() {
	err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("In inputExchange StartConsuming", "err", err)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMsg, err := inner.DeserializeInnerMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch innerMsg.Type {
	case inner.MsgData:
		aggregation.handleDataMessage(innerMsg.ClientID, innerMsg.Records)
	case inner.MsgEOF:
		aggregation.handleEOFMessage(innerMsg.ClientID, innerMsg.SenderID)
	default:
		slog.Warn("Unknown inner message type", "type", innerMsg.Type)
	}
}

func (aggregation *Aggregation) handleDataMessage(clientID string, records []fruititem.FruitItem) {
	aggregation.mu.Lock()
	defer aggregation.mu.Unlock()

	if aggregation.fruitSums[clientID] == nil {
		aggregation.fruitSums[clientID] = make(map[string]fruititem.FruitItem)
	}

	for _, fruitRecord := range records {
		if existing, ok := aggregation.fruitSums[clientID][fruitRecord.Fruit]; ok {
			aggregation.fruitSums[clientID][fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			aggregation.fruitSums[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem) {
	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[fruitRecord.Fruit] = aggregation.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop() []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap))
	for _, item := range aggregation.fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
