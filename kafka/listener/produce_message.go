package listener

import (
	"encoding/json"
	kafkaConstant "gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/log"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib/config"
	"gitlab.com/golibs-starter/golib/event"
	"gitlab.com/golibs-starter/golib/pubsub"
	"gitlab.com/golibs-starter/golib/web/constant"
	webEvent "gitlab.com/golibs-starter/golib/web/event"
	webLog "gitlab.com/golibs-starter/golib/web/log"
	"strings"
)

type ProduceMessage struct {
	producer               core.SyncProducer
	appProps               *config.AppProperties
	eventProducerProps     *properties.EventProducer
	eventProps             *event.Properties
	notLogPayloadForEvents map[string]bool
}

func NewProduceMessage(
	producer core.SyncProducer,
	appProps *config.AppProperties,
	eventProducerProps *properties.EventProducer,
	eventProps *event.Properties,
) pubsub.Subscriber {
	notLogPayloadForEvents := make(map[string]bool)
	for _, e := range eventProps.Log.NotLogPayloadForEvents {
		notLogPayloadForEvents[e] = true
	}
	return &ProduceMessage{
		producer:               producer,
		appProps:               appProps,
		eventProducerProps:     eventProducerProps,
		eventProps:             eventProps,
		notLogPayloadForEvents: notLogPayloadForEvents,
	}
}

func (p ProduceMessage) Supports(event pubsub.Event) bool {
	return true
}

func (p ProduceMessage) Handle(event pubsub.Event) {
	lcEvent := strings.ToLower(event.Name())
	var webAbsEvent *webEvent.AbstractEvent
	if we, ok := event.(webEvent.AbstractEventWrapper); ok {
		webAbsEvent = we.GetAbstractEvent()
		webAbsEvent.ServiceCode = p.appProps.Name
	}
	eventTopic, exists := p.eventProducerProps.EventMappings[lcEvent]
	if !exists {
		webLog.Debuge(event, "Produce Kafka message is skip, no mapping found for event [%s]", event.Name())
		return
	}
	if eventTopic.Disable {
		webLog.Debuge(event, "Produce Kafka message is disabled for event [%s]", event.Name())
		return
	}
	if eventTopic.TopicName == "" {
		webLog.Errore(event, "Cannot find topic for event [%s]", event.Name())
		return
	}
	msgBytes, err := json.Marshal(event)
	if err != nil {
		webLog.Errore(event, "Error when marshalling event [%+v], error [%v]", event, err)
		return
	}
	message := core.Message{
		Topic: eventTopic.TopicName,
		Value: msgBytes,
		Headers: []core.MessageHeader{
			{
				Key:   []byte(constant.HeaderEventId),
				Value: []byte(event.Identifier()),
			},
			{
				Key:   []byte(constant.HeaderServiceClientName),
				Value: []byte(p.appProps.Name),
			},
		},
		Metadata: map[string]interface{}{
			kafkaConstant.EventId:   event.Identifier(),
			kafkaConstant.EventName: event.Name(),
		},
	}
	if webAbsEvent != nil {
		message.Headers = p.appendMsgHeaders(message.Headers, webAbsEvent)
		message.Metadata = p.appendMsgMetadata(message.Metadata.(map[string]interface{}), webAbsEvent)
	}
	partition, offset, err := p.producer.Send(&message)
	if err != nil {
		webLog.Errore(event, "Error while producing kafka message %s. Error [%s]",
			log.DescMessage(&message, p.eventProps.Log.NotLogPayloadForEvents), err)
		return
	}
	webLog.Infoe(event, "Success to produce to kafka partition [%d], offset [%d], message %s",
		partition, offset, log.DescMessage(&message, p.eventProps.Log.NotLogPayloadForEvents))
}

func (p ProduceMessage) appendMsgHeaders(headers []core.MessageHeader, event *webEvent.AbstractEvent) []core.MessageHeader {
	deviceId, _ := event.AdditionalData[constant.HeaderDeviceId].(string)
	deviceSessionId, _ := event.AdditionalData[constant.HeaderDeviceSessionId].(string)
	clientIpAddress, _ := event.AdditionalData[constant.HeaderClientIpAddress].(string)
	return append(headers,
		core.MessageHeader{
			Key:   []byte(constant.HeaderCorrelationId),
			Value: []byte(event.RequestId),
		},
		core.MessageHeader{
			Key:   []byte(constant.HeaderDeviceId),
			Value: []byte(deviceId),
		},
		core.MessageHeader{
			Key:   []byte(constant.HeaderDeviceSessionId),
			Value: []byte(deviceSessionId),
		},
		core.MessageHeader{
			Key:   []byte(constant.HeaderClientIpAddress),
			Value: []byte(clientIpAddress),
		})
}

func (p ProduceMessage) appendMsgMetadata(metadata map[string]interface{}, event *webEvent.AbstractEvent) map[string]interface{} {
	metadata[kafkaConstant.LoggingContext] = webLog.BuildLoggingContextFromEvent(event)
	return metadata
}
