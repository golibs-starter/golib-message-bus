package logging

import (
	kafkaConstant "gitlab.id.vin/vincart/golib-message-bus/kafka/constant"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib/event"
	"gitlab.id.vin/vincart/golib/log"
	"gitlab.id.vin/vincart/golib/web/constant"
	webLog "gitlab.id.vin/vincart/golib/web/log"
	"strings"
)

func ProducerErrorsHandler(producer core.AsyncProducer, eventProps *event.Properties) {
	notLogPayloadForEvents := make(map[string]bool)
	for _, e := range eventProps.Log.NotLogPayloadForEvents {
		notLogPayloadForEvents[e] = true
	}
	go func() {
		for msg := range producer.Errors() {
			if metadata, ok := msg.Msg.Metadata.(map[string]interface{}); ok {
				eventId, _ := metadata[kafkaConstant.EventId].(string)
				eventName, _ := metadata[kafkaConstant.EventName].(string)
				logContext := []interface{}{constant.ContextReqMeta, getLoggingContext(metadata)}
				if notLogPayloadForEvents[strings.ToLower(eventName)] {
					log.Errorw(logContext, "Exception while sending message [%s], id [%s], headers [%v] to kafka topic [%s]",
						eventName, eventId, msg.Msg.Headers, msg.Msg.Topic)
				} else {
					log.Errorw(logContext, "Exception while sending message [%s], id [%s], headers [%v], payload [%s] to kafka topic [%s]",
						eventName, eventId, msg.Msg.Headers, string(msg.Msg.Value), msg.Msg.Topic)
				}
			} else {
				log.Errorf("Exception while sending message headers [%v], payload [%s] to kafka topic [%s]",
					msg.Msg.Headers, string(msg.Msg.Value), msg.Msg.Topic)
			}
		}
	}()
}

func getLoggingContext(metadata map[string]interface{}) *webLog.LoggingContext {
	loggingCtx := metadata[kafkaConstant.LoggingContext]
	if loggingCtx == nil {
		return nil
	}
	ctx, ok := loggingCtx.(*webLog.LoggingContext)
	if !ok {
		return nil
	}
	return ctx
}
