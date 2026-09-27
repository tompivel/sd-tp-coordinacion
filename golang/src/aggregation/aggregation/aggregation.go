package aggregation

import (
	"fmt"
	"log/slog"

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
	store         *AggregatorSessionStore
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
		store:         NewAggregatorSessionStore(config.SumAmount, config.TopSize),
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
	aggregation.store.AddRecords(clientID, records)
}

func (aggregation *Aggregation) handleEOFMessage(clientID string, sumID int) {
	slog.Info("Received EOF from Sum worker", "clientID", clientID, "sumID", sumID)

	partialTop, ready := aggregation.store.RecordEOF(clientID, sumID)
	if !ready {
		return
	}

	slog.Info("Barrier reached for client, emitting partial top", "clientID", clientID)

	topMsg, err := inner.SerializeTopMessage(clientID, aggregation.config.Id, partialTop)
	if err != nil {
		slog.Error("While serializing partial top message", "err", err)
		return
	}

	if err := aggregation.outputQueue.Send(*topMsg); err != nil {
		slog.Error("While sending partial top to join queue", "err", err)
	}
}
