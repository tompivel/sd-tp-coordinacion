package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageType string

const (
	MsgData MessageType = "DATA"
	MsgEOF  MessageType = "EOF"
	MsgTop  MessageType = "TOP"
)

type InnerMessage struct {
	Type     MessageType           `json:"type"`
	ClientID string                `json:"client_id"`
	SenderID int                   `json:"sender_id,omitempty"`
	Records  []fruititem.FruitItem `json:"records,omitempty"`
}

func (m *InnerMessage) Serialize() (*middleware.Message, error) {
	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(bytes)}, nil
}

func SerializeDataMessage(clientID string, records []fruititem.FruitItem) (*middleware.Message, error) {
	msg := InnerMessage{
		Type:     MsgData,
		ClientID: clientID,
		Records:  records,
	}
	return msg.Serialize()
}

func SerializeEOFMessage(clientID string, senderID int) (*middleware.Message, error) {
	msg := InnerMessage{
		Type:     MsgEOF,
		ClientID: clientID,
		SenderID: senderID,
	}
	return msg.Serialize()
}

func SerializeTopMessage(clientID string, senderID int, records []fruititem.FruitItem) (*middleware.Message, error) {
	msg := InnerMessage{
		Type:     MsgTop,
		ClientID: clientID,
		SenderID: senderID,
		Records:  records,
	}
	return msg.Serialize()
}

func DeserializeInnerMessage(message *middleware.Message) (*InnerMessage, error) {
	if message == nil {
		return nil, errors.New("nil message")
	}

	var innerMsg InnerMessage
	if err := json.Unmarshal([]byte(message.Body), &innerMsg); err != nil {
		return nil, err
	}
	if innerMsg.Type == "" {
		return nil, errors.New("invalid or empty message type in inner message")
	}

	return &innerMsg, nil
}

