package middleware

import (
	"time"
)

type QueueMiddleware struct {
	*BaseMiddleware
	queueName string
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return q.BaseMiddleware.StartConsumingQueue(q.queueName, callbackFunc)
}

func (q *QueueMiddleware) Send(msg Message) error {
	if q.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return q.BaseMiddleware.PublishWithTimeout(DefaultExchange, q.queueName, msg, 5*time.Second)
}

func (q *QueueMiddleware) SendTo(routingKey string, msg Message) error {
	if q.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return q.BaseMiddleware.PublishWithTimeout(DefaultExchange, routingKey, msg, 5*time.Second)
}
