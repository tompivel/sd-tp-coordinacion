package middleware

import (
	"testing"
)

func TestMiddlewareInterfaceSatisfaction(t *testing.T) {
	var _ Middleware = (*QueueMiddleware)(nil)
	var _ Middleware = (*ExchangeMiddleware)(nil)
}
