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
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	if err := join.outputQueue.Send(msg); err != nil {
		slog.Error("While sending top", "err", err)
	}
}
