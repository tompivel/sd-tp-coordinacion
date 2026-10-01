package middleware

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BaseMiddleware struct {
	conn          *amqp.Connection
	ch            *amqp.Channel
	mu            sync.Mutex
	pubMu         sync.Mutex
	isConsuming   bool
	consumerTag   string
	prefetchCount int
}

func NewBaseMiddleware(settings ConnSettings) (*BaseMiddleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, ErrMessageMiddlewareDisconnected
	}

	return &BaseMiddleware{
		conn:          conn,
		ch:            ch,
		prefetchCount: 1,
	}, nil
}

func (b *BaseMiddleware) SetPrefetch(count int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prefetchCount = count
	if b.ch != nil && !b.ch.IsClosed() && count > 0 {
		return b.ch.Qos(count, 0, false)
	}
	return nil
}

func (b *BaseMiddleware) StartConsumingQueue(queueName string, callbackFunc func(msg Message, ack func(), nack func())) error {
	b.mu.Lock()
	if b.isConsuming {
		b.mu.Unlock()
		return fmt.Errorf("already consuming")
	}
	
	if b.conn.IsClosed() {
		b.mu.Unlock()
		return ErrMessageMiddlewareDisconnected
	}

	if b.prefetchCount > 0 {
		if err := b.ch.Qos(b.prefetchCount, 0, false); err != nil {
			b.mu.Unlock()
			return ErrMessageMiddlewareMessage
		}
	}

	// unique consumer tag
	b.consumerTag = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	b.isConsuming = true
	b.mu.Unlock()

	msgs, err := b.ch.Consume(
		queueName,
		b.consumerTag, // consumer
		ManualAck,     // auto-ack
		Shared,        // exclusive
		Local,         // no-local
		Wait,          // no-wait
		nil,           // args
	)
	
	if err != nil {
		b.mu.Lock()
		b.isConsuming = false
		b.mu.Unlock()
		
		if b.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := Message{Body: string(d.Body)}
		
		delivery := d
		ack := func() { delivery.Ack(SingleAck) }
		nack := func() { delivery.Nack(SingleAck, Requeue) }
		
		callbackFunc(msg, ack, nack)
	}

	b.mu.Lock()
	b.isConsuming = false
	b.mu.Unlock()

	if b.conn != nil && b.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (b *BaseMiddleware) StopConsuming() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	if !b.isConsuming {
		return nil
	}

	err := b.ch.Cancel(b.consumerTag, Wait)
	if err != nil {
		if b.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareClose
	}

	b.isConsuming = false
	return nil
}

func (b *BaseMiddleware) Close() error {
	b.StopConsuming()

	var err error
	if b.ch != nil && !b.ch.IsClosed() {
		err = b.ch.Close()
	}
	
	if b.conn != nil && !b.conn.IsClosed() {
		closeErr := b.conn.Close()
		if closeErr != nil {
			err = closeErr
		}
	}
	
	if err != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}

func (b *BaseMiddleware) PublishWithTimeout(exchange, routingKey string, msg Message, timeout time.Duration) error {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := b.ch.PublishWithContext(
		ctx,
		exchange,     // exchange
		routingKey,   // routing key
		NonMandatory, // mandatory
		NonImmediate, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		if b.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}
