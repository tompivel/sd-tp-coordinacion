package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

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
	stopOnce      sync.Once
	wg            sync.WaitGroup
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
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	consumeErr := make(chan error, 1)

	aggregation.wg.Add(1)
	go func() {
		defer aggregation.wg.Done()
		err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("In inputExchange StartConsuming", "err", err)
			consumeErr <- err
		}
	}()

	select {
	case sig := <-sigChan:
		slog.Info("Termination signal received in Aggregation", "signal", sig)
	case err := <-consumeErr:
		slog.Error("Aggregation consumer stopped unexpectedly", "err", err)
	}

	aggregation.Stop()
}

func (aggregation *Aggregation) Stop() {
	aggregation.stopOnce.Do(func() {
		slog.Info("Stopping Aggregation node...")
		if aggregation.inputExchange != nil {
			if err := aggregation.inputExchange.StopConsuming(); err != nil {
				slog.Debug("While stopping inputExchange consuming in Aggregation", "err", err)
			}
		}

		// Wait for active message processing callback to finish and ACK
		aggregation.wg.Wait()

		if aggregation.inputExchange != nil {
			if err := aggregation.inputExchange.Close(); err != nil {
				slog.Debug("While closing inputExchange in Aggregation", "err", err)
			}
		}
		if aggregation.outputQueue != nil {
			if err := aggregation.outputQueue.Close(); err != nil {
				slog.Debug("While closing outputQueue in Aggregation", "err", err)
			}
		}
		slog.Info("Aggregation node stopped cleanly")
	})
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
