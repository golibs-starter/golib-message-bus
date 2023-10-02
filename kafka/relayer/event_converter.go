package relayer

import (
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib/pubsub"
)

type EventConverter interface {

	// Convert internal Event to kafka message
	Convert(event pubsub.Event) (*core.Message, error)

	// Restore a consumed message back to destination event
	Restore(msg *core.ConsumerMessage, dest pubsub.Event) error
}
