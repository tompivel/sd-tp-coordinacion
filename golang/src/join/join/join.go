package join

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	config      JoinConfig
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	store       *JoinSessionStore
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		config:      config,
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		store:       NewJoinSessionStore(config.AggregationAmount, config.TopSize),
	}, nil
}

func (join *Join) Run() {
	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("In inputQueue StartConsuming", "err", err)
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMsg, err := inner.DeserializeInnerMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message in Join", "err", err)
		return
	}

	clientID := innerMsg.ClientID
	aggID := innerMsg.SenderID

	slog.Info("Received top message from Aggregator", "clientID", clientID, "aggID", aggID, "records", len(innerMsg.Records))

	globalTop, ready := join.store.AddPartialTop(clientID, aggID, innerMsg.Records)
	if !ready {
		return
	}

	slog.Info("All partial tops collected for client, producing global top", "clientID", clientID)

	topMsg, err := inner.SerializeTopMessage(clientID, 0, globalTop)
	if err != nil {
		slog.Error("While serializing global top message", "err", err)
		return
	}

	if err := join.outputQueue.Send(*topMsg); err != nil {
		slog.Error("While sending global top to Gateway results queue", "err", err)
	}
}
