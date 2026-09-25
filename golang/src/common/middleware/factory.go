package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	base, err := NewBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			base.Close()
		}
	}()

	_, err = base.ch.QueueDeclare(
		queueName,
		Transient, // param: durable
		Keep,      // param: autoDelete
		Shared,    // param: exclusive
		Wait,      // param: noWait
		nil,       // param: args
	)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	success = true
	return &QueueMiddleware{
		BaseMiddleware: base,
		queueName:      queueName,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	base, err := NewBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			base.Close()
		}
	}()

	err = base.ch.ExchangeDeclare(
		exchange,
		"topic",     // param: kind/type
		Transient,   // param: durable
		Keep,        // param: autoDelete
		NonInternal, // param: internal
		Wait,        // param: noWait
		nil,         // param: args
	)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	q, err := base.ch.QueueDeclare(
		"",          // param: name (auto-generated)
		Transient,   // param: durable
		AutoDelete,  // param: autoDelete
		Exclusive,   // param: exclusive
		Wait,        // param: noWait
		nil,         // param: args
	)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	for _, key := range keys {
		err = base.ch.QueueBind(
			q.Name,
			key,
			exchange,
			Wait, // param: noWait
			nil,
		)
		if err != nil {
			return nil, ErrMessageMiddlewareDisconnected
		}
	}

	success = true
	return &ExchangeMiddleware{
		BaseMiddleware: base,
		exchangeName:   exchange,
		routingKeys:    keys,
		queueName:      q.Name,
	}, nil
}

func CreateNamedTopicConsumerMiddleware(exchange string, queueName string, routingKey string, connectionSettings ConnSettings) (Middleware, error) {
	base, err := NewBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			base.Close()
		}
	}()

	err = base.ch.ExchangeDeclare(
		exchange,
		TopicExchange,
		Transient,
		Keep,
		NonInternal,
		Wait,
		nil,
	)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	_, err = base.ch.QueueDeclare(
		queueName,
		Transient,
		Keep,
		Shared,
		Wait,
		nil,
	)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	err = base.ch.QueueBind(
		queueName,
		routingKey,
		exchange,
		Wait,
		nil,
	)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	success = true
	return &ExchangeMiddleware{
		BaseMiddleware: base,
		exchangeName:   exchange,
		routingKeys:    []string{routingKey},
		queueName:      queueName,
	}, nil
}

func CreateTopicProducerMiddleware(exchange string, routingKeys []string, connectionSettings ConnSettings) (Middleware, error) {
	base, err := NewBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			base.Close()
		}
	}()

	err = base.ch.ExchangeDeclare(
		exchange,
		TopicExchange,
		Transient,
		Keep,
		NonInternal,
		Wait,
		nil,
	)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	// Declare and bind all known downstream queues so messages are never dropped if sent early
	for _, key := range routingKeys {
		_, err = base.ch.QueueDeclare(
			key,
			Transient,
			Keep,
			Shared,
			Wait,
			nil,
		)
		if err == nil {
			_ = base.ch.QueueBind(
				key,
				key,
				exchange,
				Wait,
				nil,
			)
		}
	}

	success = true
	return &ExchangeMiddleware{
		BaseMiddleware: base,
		exchangeName:   exchange,
		routingKeys:    routingKeys,
	}, nil
}

