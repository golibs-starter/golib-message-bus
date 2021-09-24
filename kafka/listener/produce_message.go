package listener

import (
	"encoding/json"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"gitlab.id.vin/vincart/golib/config"
	"gitlab.id.vin/vincart/golib/event"
	"gitlab.id.vin/vincart/golib/log"
	"gitlab.id.vin/vincart/golib/pubsub"
	"gitlab.id.vin/vincart/golib/web/constant"
	webEvent "gitlab.id.vin/vincart/golib/web/event"
	webLog "gitlab.id.vin/vincart/golib/web/log"
)

type ProduceMessage struct {
	producer               core.Producer
	appProps               *config.AppProperties
	eventProducerProps     *properties.EventProducer
	eventProps             *event.Properties
	notLogPayloadForEvents map[string]bool
}

func NewProduceMessage(
	producer core.Producer,
	appProps *config.AppProperties,
	eventProducerProps *properties.EventProducer,
	eventProps *event.Properties,
) *ProduceMessage {
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
	logContext := make([]interface{}, 0)
	webAbstractEvent, isWebEvent := event.(*webEvent.AbstractEvent)
	if isWebEvent {
		logContext = []interface{}{constant.ContextReqMeta, p.makeLoggingContext(webAbstractEvent)}
		webAbstractEvent.ServiceCode = p.appProps.Name
	}
	eventTopic, exists := p.eventProducerProps.TopicMappings[event.Name()]
	if !exists {
		log.Debugw(logContext, "Produce Kafka message is skip, no mapping found for event [%s]", event.Name())
		return
	}
	if eventTopic.Disable {
		log.Debugw(logContext, "Produce Kafka message is disabled for event [%s]", event.Name())
		return
	}
	if eventTopic.TopicName == "" {
		log.Errorw(logContext, "Cannot find topic for event [%s]", event.Name())
		return
	}
	msgBytes, err := json.Marshal(event)
	if err != nil {
		log.Errorw(logContext, "Error when marshalling event [%+v], error [%v]", event, err)
		return
	}
	headers := make([]core.MessageHeader, 0)
	if isWebEvent {
		deviceId, _ := webAbstractEvent.AdditionalData[constant.HeaderDeviceId].(string)
		deviceSessionId, _ := webAbstractEvent.AdditionalData[constant.HeaderDeviceSessionId].(string)
		clientIpAddress, _ := webAbstractEvent.AdditionalData[constant.HeaderClientIpAddress].(string)
		headers = append(headers,
			core.MessageHeader{
				Key:   []byte(constant.HeaderEventId),
				Value: []byte(webAbstractEvent.Id),
			},
			core.MessageHeader{
				Key:   []byte(constant.HeaderCorrelationId),
				Value: []byte(webAbstractEvent.RequestId),
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
			},
			core.MessageHeader{
				Key:   []byte(constant.HeaderServiceClientName),
				Value: []byte(p.appProps.Name),
			},
		)
	}
	message := core.Message{
		Topic:   eventTopic.TopicName,
		Value:   msgBytes,
		Headers: headers,
	}
	p.producer.Send(&message)
	if p.notLogPayloadForEvents[event.Name()] {
		log.Infof("Success to produce Kafka message [%s] with id [%s] to topic [%s]",
			event.Name(), event.Identifier(), message.Topic)
	} else {
		log.Infof("Success to produce Kafka message [%s] with id [%s] to topic [%s], value [%s]",
			event.Name(), event.Identifier(), message.Topic, string(msgBytes))
	}
}

func (p ProduceMessage) makeLoggingContext(event *webEvent.AbstractEvent) webLog.LoggingContext {
	deviceId, _ := event.AdditionalData[constant.HeaderDeviceId].(string)
	deviceSessionId, _ := event.AdditionalData[constant.HeaderDeviceSessionId].(string)
	return webLog.LoggingContext{
		UserId:            event.UserId,
		DeviceId:          deviceId,
		DeviceSessionId:   deviceSessionId,
		CorrelationId:     event.RequestId,
		TechnicalUsername: event.TechnicalUsername,
	}
}
