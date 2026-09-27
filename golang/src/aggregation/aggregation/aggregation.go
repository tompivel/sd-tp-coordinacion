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

func (aggregation *Aggregation) handleEOFMessage(clientID string, sumID int) {
	aggregation.mu.Lock()
	defer aggregation.mu.Unlock()

	slog.Info("Received EOF from Sum worker", "clientID", clientID, "sumID", sumID)

	if aggregation.eofsReceived[clientID] == nil {
		aggregation.eofsReceived[clientID] = make(map[int]bool)
	}
	aggregation.eofsReceived[clientID][sumID] = true

	// Check if all N Sum workers have reported EOF for this client
	if len(aggregation.eofsReceived[clientID]) == aggregation.config.SumAmount {
		slog.Info("Barrier reached for client, emitting partial top", "clientID", clientID)

		partialTop := aggregation.buildFruitTop(clientID)

		topMsg, err := inner.SerializeTopMessage(clientID, aggregation.config.Id, partialTop)
		if err != nil {
			slog.Error("While serializing partial top message", "err", err)
		} else {
			if err := aggregation.outputQueue.Send(*topMsg); err != nil {
				slog.Error("While sending partial top to join queue", "err", err)
			}
		}

		// Clean up state for this client
		delete(aggregation.fruitSums, clientID)
		delete(aggregation.eofsReceived, clientID)
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID string) []fruititem.FruitItem {
	clientMap := aggregation.fruitSums[clientID]
	fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		fruitItems = append(fruitItems, item)
	}

	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})

	finalTopSize := min(aggregation.config.TopSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
