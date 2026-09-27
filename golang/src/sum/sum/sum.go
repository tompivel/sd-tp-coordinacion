package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

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
	config             SumConfig
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	eofFanoutConsumer  middleware.Middleware
	eofFanoutProducer  middleware.Middleware
	fruitItemMap       map[string]map[string]fruititem.FruitItem // clientID -> fruit -> FruitItem
	clientFinished     map[string]bool                          // clientID -> bool
	mu                 sync.Mutex
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
		fruitItemMap:      make(map[string]map[string]fruititem.FruitItem),
		clientFinished:    make(map[string]bool),
	}, nil
}

func (sum *Sum) Run() {
	if sum.eofFanoutConsumer != nil {
		go func() {
			err := sum.eofFanoutConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
				defer ack()
				sum.handleEofFanoutMessage(msg)
			})
			if err != nil {
				slog.Error("In eofFanoutConsumer StartConsuming", "err", err)
			}
		}()
	}

	err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleGatewayMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("In inputQueue StartConsuming", "err", err)
	}
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

	if sum.clientFinished[clientID] {
		slog.Warn("Received data message for already finished client", "clientID", clientID)
		return
	}

	if sum.fruitItemMap[clientID] == nil {
		sum.fruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	for _, fruitRecord := range records {
		if existing, ok := sum.fruitItemMap[clientID][fruitRecord.Fruit]; ok {
			sum.fruitItemMap[clientID][fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			sum.fruitItemMap[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (sum *Sum) handleGatewayEOF(clientID string) {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	slog.Info("Received EOF from Gateway", "clientID", clientID)

	if sum.clientFinished[clientID] {
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

	sum.flushAndFinishClient(clientID)
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

	if sum.clientFinished[clientID] {
		// Already processed for this client
		return
	}

	sum.flushAndFinishClient(clientID)
}

func (sum *Sum) flushAndFinishClient(clientID string) {
	// 1. Send all accumulated FruitTotalAmount messages to partitioned Aggregators
	if clientSums, ok := sum.fruitItemMap[clientID]; ok {
		for _, record := range clientSums {
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

	// 3. Mark client finished and clean up memory
	sum.clientFinished[clientID] = true
	delete(sum.fruitItemMap, clientID)
}

func (sum *Sum) hashFruit(fruit string) int {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	return int(h.Sum32() % uint32(sum.config.AggregationAmount))
}
