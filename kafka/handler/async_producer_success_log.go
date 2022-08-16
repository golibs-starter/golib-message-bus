package handler

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/log"
	"gitlab.com/golibs-starter/golib/event"
)

func AsyncProducerSuccessLogHandler(producer core.AsyncProducer, eventProps *event.Properties) {
	go func() {
		for msg := range producer.Successes() {
			log.Info(msg, "Success to produce kafka message %s",
				log.DescMessage(msg, eventProps.Log.NotLogPayloadForEvents))
		}
	}()
}
