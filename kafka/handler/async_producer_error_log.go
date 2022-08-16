package handler

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/log"
	"gitlab.com/golibs-starter/golib/event"
)

func AsyncProducerErrorLogHandler(producer core.AsyncProducer, eventProps *event.Properties) {
	go func() {
		for msg := range producer.Errors() {
			log.Error(msg.Msg, "Exception while producing kafka message %s. Error [%s]",
				log.DescMessage(msg.Msg, eventProps.Log.NotLogPayloadForEvents), msg.Error())
		}
	}()
}
