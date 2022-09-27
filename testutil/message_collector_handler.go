package golibmsgTestUtil

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib/web/log"
)

type MessageCollectorHandler struct {
	collector *MessageCollector
}

func NewMessageCollectorHandler(collector *MessageCollector) core.ConsumerHandler {
	return &MessageCollectorHandler{collector: collector}
}

func (c *MessageCollectorHandler) HandlerFunc(msg *core.ConsumerMessage) {
	log.Infof("[MessageCollectorHandler] Receive message [%s]", string(msg.Value))
	c.collector.PushMessage(msg)
}

func (c MessageCollectorHandler) Close() {
	//
}
