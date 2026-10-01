package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	config            SumConfig
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	eofFanoutConsumer middleware.Middleware
	eofFanoutProducer middleware.Middleware
	store             *SumSessionStore
	mu                sync.Mutex
	stopOnce          sync.Once
	wg                sync.WaitGroup
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateTopicProducerMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	var eofFanoutConsumer middleware.Middleware
	var eofFanoutProducer middleware.Middleware

	if config.SumAmount > 1 {
		fanoutExchange := fmt.Sprintf("%s_eof", config.SumPrefix)
		fanoutQueue := fmt.Sprintf("%s_eof_%d", config.SumPrefix, config.Id)

		eofFanoutConsumer, err = middleware.CreateNamedFanoutConsumerMiddleware(fanoutExchange, fanoutQueue, connSettings)
		if err != nil {
			inputQueue.Close()
			outputExchange.Close()
			return nil, err
		}

		eofFanoutProducer, err = middleware.CreateFanoutProducerMiddleware(fanoutExchange, connSettings)
		if err != nil {
			inputQueue.Close()
			outputExchange.Close()
			eofFanoutConsumer.Close()
			return nil, err
		}
	}

	return &Sum{
		config:            config,
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		eofFanoutConsumer: eofFanoutConsumer,
		eofFanoutProducer: eofFanoutProducer,
		store:             NewSumSessionStore(),
	}, nil
}

func (sum *Sum) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	consumeErr := make(chan error, 2)

	if sum.eofFanoutConsumer != nil {
		sum.wg.Add(1)
		go func() {
			defer sum.wg.Done()
			err := sum.eofFanoutConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
				defer ack()
				sum.handleEofFanoutMessage(msg)
			})
			if err != nil {
				slog.Error("In eofFanoutConsumer StartConsuming", "err", err)
				consumeErr <- err
			}
		}()
	}

	sum.wg.Add(1)
	go func() {
		defer sum.wg.Done()
		err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleGatewayMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("In inputQueue StartConsuming", "err", err)
			consumeErr <- err
		}
	}()

	select {
	case sig := <-sigChan:
		slog.Info("Termination signal received in Sum", "signal", sig)
	case err := <-consumeErr:
		slog.Error("Sum consumer stopped unexpectedly", "err", err)
	}

	sum.Stop()
}

func (sum *Sum) Stop() {
	sum.stopOnce.Do(func() {
		slog.Info("Stopping Sum node...")
		if sum.inputQueue != nil {
			if err := sum.inputQueue.StopConsuming(); err != nil {
				slog.Debug("While stopping inputQueue consuming in Sum", "err", err)
			}
		}
		if sum.eofFanoutConsumer != nil {
			if err := sum.eofFanoutConsumer.StopConsuming(); err != nil {
				slog.Debug("While stopping eofFanoutConsumer consuming in Sum", "err", err)
			}
		}

		// Wait for active message processing callbacks to finish and ACK
		sum.wg.Wait()

		if sum.inputQueue != nil {
			if err := sum.inputQueue.Close(); err != nil {
				slog.Debug("While closing inputQueue in Sum", "err", err)
			}
		}
		if sum.outputExchange != nil {
			if err := sum.outputExchange.Close(); err != nil {
				slog.Debug("While closing outputExchange in Sum", "err", err)
			}
		}
		if sum.eofFanoutConsumer != nil {
			if err := sum.eofFanoutConsumer.Close(); err != nil {
				slog.Debug("While closing eofFanoutConsumer in Sum", "err", err)
			}
		}
		if sum.eofFanoutProducer != nil {
			if err := sum.eofFanoutProducer.Close(); err != nil {
				slog.Debug("While closing eofFanoutProducer in Sum", "err", err)
			}
		}
		slog.Info("Sum node stopped cleanly")
	})
}

func (sum *Sum) handleGatewayMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMsg, err := inner.DeserializeInnerMessage(&msg)
	if err != nil {
		slog.Error("While deserializing gateway message", "err", err)
		return
	}

	if innerMsg.Type == inner.MsgEOF {
		sum.handleGatewayEOF(innerMsg.ClientID)
		return
	}

	sum.handleDataMessage(innerMsg.ClientID, innerMsg.Records)
}

func (sum *Sum) handleDataMessage(clientID string, records []fruititem.FruitItem) {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	if !sum.store.AddRecords(clientID, records) {
		slog.Warn("Received data message for already finished client", "clientID", clientID)
	}
}

func (sum *Sum) handleGatewayEOF(clientID string) {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	slog.Info("Received EOF from Gateway", "clientID", clientID)

	records, ok := sum.store.FinishAndEvict(clientID)
	if !ok {
		return
	}

	// Broadcast EOF to peer sum nodes via fanout exchange
	if sum.config.SumAmount > 1 && sum.eofFanoutProducer != nil {
		eofMsg, err := inner.SerializeEOFMessage(clientID, sum.config.Id)
		if err != nil {
			slog.Error("While serializing EOF broadcast", "err", err)
		} else {
			if err := sum.eofFanoutProducer.Send(*eofMsg); err != nil {
				slog.Error("While broadcasting EOF to peer Sum nodes", "err", err)
			}
		}
	}

	sum.flushAndFinishClient(clientID, records)
}

func (sum *Sum) handleEofFanoutMessage(msg middleware.Message) {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	innerMsg, err := inner.DeserializeInnerMessage(&msg)
	if err != nil {
		slog.Error("While deserializing fanout EOF message", "err", err)
		return
	}

	if innerMsg.Type != inner.MsgEOF {
		return
	}

	clientID := innerMsg.ClientID
	slog.Info("Received EOF from peer Sum via fanout", "clientID", clientID, "senderID", innerMsg.SenderID)

	records, ok := sum.store.FinishAndEvict(clientID)
	if !ok {
		// Already processed for this client
		return
	}

	sum.flushAndFinishClient(clientID, records)
}

func (sum *Sum) flushAndFinishClient(clientID string, records []fruititem.FruitItem) {
	// 1. Send all accumulated FruitTotalAmount messages to partitioned Aggregators
	for _, record := range records {
		partition := sum.hashFruit(record.Fruit)
		routingKey := fmt.Sprintf("%s_%d", sum.config.AggregationPrefix, partition)

		msg, err := inner.SerializeDataMessage(clientID, []fruititem.FruitItem{record})
		if err != nil {
			slog.Error("While serializing fruit total amount message", "err", err)
			continue
		}

		if err := sum.outputExchange.SendTo(routingKey, *msg); err != nil {
			slog.Error("While sending fruit total amount to aggregator", "routingKey", routingKey, "err", err)
		}
	}

	// 2. Send EOF(clientID, sumID) to ALL Aggregators
	eofMsg, err := inner.SerializeEOFMessage(clientID, sum.config.Id)
	if err != nil {
		slog.Error("While serializing EOF to aggregators", "err", err)
	} else {
		for j := range sum.config.AggregationAmount {
			routingKey := fmt.Sprintf("%s_%d", sum.config.AggregationPrefix, j)
			if err := sum.outputExchange.SendTo(routingKey, *eofMsg); err != nil {
				slog.Error("While sending EOF to aggregator", "routingKey", routingKey, "err", err)
			}
		}
	}
}

func (sum *Sum) hashFruit(fruit string) int {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	return int(h.Sum32() % uint32(sum.config.AggregationAmount))
}
