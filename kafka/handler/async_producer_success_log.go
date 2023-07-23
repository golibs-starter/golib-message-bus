package handler

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/log"
	"gitlab.com/golibs-starter/golib/event"
	coreLog "gitlab.com/golibs-starter/golib/log"
)

func AsyncProducerSuccessLogHandler(producer core.AsyncProducer, eventProps *event.Properties) {
	go func() {
		messageFormat := "Success to produce to kafka partition [%d], offset [%d], message %s"
		for msg := range producer.Successes() {
			descMessage := log.DescMessage(msg, eventProps.Log.NotLogPayloadForEvents)
			if metadata, ok := msg.Metadata.(map[string]interface{}); ok {
				coreLog.WithField(log.GetLoggingContext(metadata)...).
					Infof(messageFormat, msg.Partition, msg.Offset, descMessage)
			} else {
				coreLog.Infof(messageFormat, msg.Partition, msg.Offset, descMessage)
			}
		}
	}()
}
