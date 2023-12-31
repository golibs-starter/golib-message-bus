package handler

import (
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib-message-bus/kafka/log"
	"github.com/golibs-starter/golib/event"
	coreLog "github.com/golibs-starter/golib/log"
)

func AsyncProducerErrorLogHandler(producer core.AsyncProducer, eventProps *event.Properties) {
	go func() {
		msgFormat := "Exception while producing kafka message %s. Error [%s]"
		for err := range producer.Errors() {
			descMessage := log.DescMessage(err.Msg, eventProps.Log.NotLogPayloadForEvents)
			if metadata, ok := err.Msg.Metadata.(map[string]interface{}); ok {
				coreLog.WithField(log.GetLoggingContext(metadata)...).Errorf(msgFormat, descMessage, err.Error())
			} else {
				coreLog.Errorf(msgFormat, descMessage, err.Error())
			}
		}
	}()
}
