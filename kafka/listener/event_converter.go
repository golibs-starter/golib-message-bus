package listener

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib/pubsub"
)

type EventConverter interface {

	// Convert internal Event to kafka message
	Convert(event pubsub.Event) (*core.Message, error)
}
