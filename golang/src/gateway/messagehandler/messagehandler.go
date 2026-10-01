package messagehandler

import (
	"fmt"
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var clientCounter uint64

type MessageHandler struct {
	clientID string
}

func NewMessageHandler() MessageHandler {
	id := atomic.AddUint64(&clientCounter, 1)
	return MessageHandler{
		clientID: fmt.Sprintf("client-%d", id),
	}
}

func (messageHandler *MessageHandler) ClientID() string {
	return messageHandler.clientID
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	return inner.SerializeDataMessage(messageHandler.clientID, []fruititem.FruitItem{fruitRecord})
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOFMessage(messageHandler.clientID, 0)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	innerMsg, err := inner.DeserializeInnerMessage(message)
	if err != nil {
		return nil, err
	}

	if innerMsg.ClientID != "" && innerMsg.ClientID != messageHandler.clientID {
		// This message belongs to another client
		return nil, nil
	}

	return innerMsg.Records, nil
}

