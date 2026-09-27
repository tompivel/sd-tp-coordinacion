package join

import (
	"log/slog"
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
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
	config       JoinConfig
	inputQueue   middleware.Middleware
	outputQueue  middleware.Middleware
	partialTops  map[string][]fruititem.FruitItem // clientID -> combined records
	receivedTops map[string]map[int]bool         // clientID -> aggID -> bool
	mu           sync.Mutex
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
		config:       config,
		inputQueue:   inputQueue,
		outputQueue:  outputQueue,
		partialTops:  make(map[string][]fruititem.FruitItem),
		receivedTops: make(map[string]map[int]bool),
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

	join.mu.Lock()
	defer join.mu.Unlock()

	clientID := innerMsg.ClientID
	aggID := innerMsg.SenderID

	slog.Info("Received top message from Aggregator", "clientID", clientID, "aggID", aggID, "records", len(innerMsg.Records))

	if join.receivedTops[clientID] == nil {
		join.receivedTops[clientID] = make(map[int]bool)
	}
	join.receivedTops[clientID][aggID] = true
	join.partialTops[clientID] = append(join.partialTops[clientID], innerMsg.Records...)

	if len(join.receivedTops[clientID]) == join.config.AggregationAmount {
		slog.Info("All partial tops collected for client, producing global top", "clientID", clientID)

		allRecords := join.partialTops[clientID]
		sort.SliceStable(allRecords, func(i, j int) bool {
			return allRecords[j].Less(allRecords[i])
		})

		finalTopSize := min(join.config.TopSize, len(allRecords))
		globalTop := allRecords[:finalTopSize]

		topMsg, err := inner.SerializeTopMessage(clientID, 0, globalTop)
		if err != nil {
			slog.Error("While serializing global top message", "err", err)
		} else {
			if err := join.outputQueue.Send(*topMsg); err != nil {
				slog.Error("While sending global top to Gateway results queue", "err", err)
			}
		}

		delete(join.partialTops, clientID)
		delete(join.receivedTops, clientID)
	}
}
