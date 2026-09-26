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
	if err := json.Unmarshal([]byte(message.Body), &innerMsg); err == nil && innerMsg.Type != "" {
		return &innerMsg, nil
	}

	// Fallback to legacy format: [[fruit, amount], ...]
	var legacyData []interface{}
	if err := json.Unmarshal([]byte(message.Body), &legacyData); err == nil {
		if len(legacyData) == 0 {
			return &InnerMessage{Type: MsgEOF}, nil
		}
		var fruitRecords []fruititem.FruitItem
		for _, datum := range legacyData {
			pair, ok := datum.([]interface{})
			if !ok || len(pair) < 2 {
				continue
			}
			fruit, ok1 := pair[0].(string)
			amount, ok2 := pair[1].(float64)
			if ok1 && ok2 {
				fruitRecords = append(fruitRecords, fruititem.FruitItem{
					Fruit:  fruit,
					Amount: uint32(amount),
				})
			}
		}
		return &InnerMessage{
			Type:    MsgData,
			Records: fruitRecords,
		}, nil
	}

	return nil, errors.New("failed to deserialize inner message")
}

// Legacy wrappers to prevent breaking unupdated components:
func SerializeMessage(fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	if len(fruitRecords) == 0 {
		return SerializeEOFMessage("", 0)
	}
	return SerializeDataMessage("", fruitRecords)
}

func DeserializeMessage(message *middleware.Message) ([]fruititem.FruitItem, bool, error) {
	innerMsg, err := DeserializeInnerMessage(message)
	if err != nil {
		return nil, false, err
	}
	if innerMsg.Type == MsgEOF {
		return nil, true, nil
	}
	return innerMsg.Records, false, nil
}
