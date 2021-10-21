package logging

import (
	kafkaConstant "gitlab.id.vin/vincart/golib-message-bus/kafka/constant"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib/event"
	"gitlab.id.vin/vincart/golib/log"
	"gitlab.id.vin/vincart/golib/web/constant"
	"strings"
)

func ProducerSuccessHandler(producer core.AsyncProducer, eventProps *event.Properties) {
	notLogPayloadForEvents := make(map[string]bool)
	for _, e := range eventProps.Log.NotLogPayloadForEvents {
		notLogPayloadForEvents[e] = true
	}
	go func() {
		for msg := range producer.Successes() {
			if metadata, ok := msg.Metadata.(map[string]interface{}); ok {
				eventId, _ := metadata[kafkaConstant.EventId].(string)
				eventName, _ := metadata[kafkaConstant.EventName].(string)
				logContext := []interface{}{constant.ContextReqMeta, getLoggingContext(metadata)}
				if notLogPayloadForEvents[strings.ToLower(eventName)] {
					log.Infow(logContext, "Success to produce Kafka message [%s] with id [%s] to topic [%s]",
						eventName, eventId, msg.Topic)
				} else {
					log.Infow(logContext, "Success to produce Kafka message [%s] with id [%s] to topic [%s], value [%s]",
						eventName, eventId, msg.Topic, string(msg.Value))
				}
			} else {
				log.Infof("Success to produce Kafka message to topic [%s], value [%s]",
					msg.Topic, string(msg.Value))
			}
		}
	}()
}
