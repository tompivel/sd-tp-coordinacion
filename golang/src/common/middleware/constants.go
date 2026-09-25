package middleware

const (
	DefaultExchange = ""
	TopicExchange   = "topic"
	FanoutExchange  = "fanout"
	
	Durable      = true
	Transient    = false
	AutoDelete   = true
	Keep         = false
	Exclusive    = true
	Shared       = false
	NoWait       = true
	Wait         = false
	Internal     = true
	NonInternal  = false
	AutoAck      = true
	ManualAck    = false
	NoLocal      = true
	Local        = false
	Mandatory    = true
	NonMandatory = false
	Immediate    = true
	NonImmediate = false
	MultipleAck  = true
	SingleAck    = false
	Requeue      = true
	Discard      = false
)
