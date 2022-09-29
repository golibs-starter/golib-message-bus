package golibmsgTestUtil

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib/log"
)

type MessageCollectorHandler struct {
	collector *MessageCollector
}

func NewMessageCollectorHandler(collector *MessageCollector) core.ConsumerHandler {
	return &MessageCollectorHandler{collector: collector}
}

func (c *MessageCollectorHandler) HandlerFunc(msg *core.ConsumerMessage) {
	log.Infof("[MessageCollectorHandler] Receive message [%s] from topic [%s]", string(msg.Value), msg.Topic)
	c.collector.PushMessage(msg)
}

func (c MessageCollectorHandler) Close() {
	//
}
