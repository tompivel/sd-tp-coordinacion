package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

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
	stopOnce    sync.Once
	wg          sync.WaitGroup
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
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	consumeErr := make(chan error, 1)

	join.wg.Add(1)
	go func() {
		defer join.wg.Done()
		err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.handleMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("In inputQueue StartConsuming", "err", err)
			consumeErr <- err
		}
	}()

	select {
	case sig := <-sigChan:
		slog.Info("Termination signal received in Join", "signal", sig)
	case err := <-consumeErr:
		slog.Error("Join consumer stopped unexpectedly", "err", err)
	}

	join.Stop()
}

func (join *Join) Stop() {
	join.stopOnce.Do(func() {
		slog.Info("Stopping Join node...")
		if join.inputQueue != nil {
			if err := join.inputQueue.StopConsuming(); err != nil {
				slog.Debug("While stopping inputQueue consuming in Join", "err", err)
			}
		}

		// Wait for active message processing callback to finish and ACK
		join.wg.Wait()

		if join.inputQueue != nil {
			if err := join.inputQueue.Close(); err != nil {
				slog.Debug("While closing inputQueue in Join", "err", err)
			}
		}
		if join.outputQueue != nil {
			if err := join.outputQueue.Close(); err != nil {
				slog.Debug("While closing outputQueue in Join", "err", err)
			}
		}
		slog.Info("Join node stopped cleanly")
	})
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
