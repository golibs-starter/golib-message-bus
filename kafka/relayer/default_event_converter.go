package relayer

import (
	"context"
	"encoding/json"
	kafkaConstant "github.com/golibs-starter/golib-message-bus/kafka/constant"
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib-message-bus/kafka/properties"
	"github.com/golibs-starter/golib/config"
	"github.com/golibs-starter/golib/pubsub"
	"github.com/golibs-starter/golib/web/constant"
	webEvent "github.com/golibs-starter/golib/web/event"
	webLog "github.com/golibs-starter/golib/web/log"
	"github.com/pkg/errors"
	"strings"
)

type DefaultEventConverter struct {
	appProps           *config.AppProperties
	eventProducerProps *properties.EventProducer
}

func NewDefaultEventConverter(
	appProps *config.AppProperties,
	eventProducerProps *properties.EventProducer,
) *DefaultEventConverter {
	return &DefaultEventConverter{
		appProps:           appProps,
		eventProducerProps: eventProducerProps,
	}
}

func (d DefaultEventConverter) Convert(event pubsub.Event) (*core.Message, error) {
	lcEvent := strings.ToLower(event.Name())
	eventTopic := d.eventProducerProps.EventMappings[lcEvent]
	msgBytes, err := json.Marshal(event)
	if err != nil {
		return nil, errors.WithMessage(err, "marshalling event failed")
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
				Value: []byte(d.appProps.Name),
			},
		},
		Metadata: map[string]interface{}{
			kafkaConstant.EventId:   event.Identifier(),
			kafkaConstant.EventName: event.Name(),
		},
	}

	if evtOrderable, ok := event.(EventOrderable); ok {
		message.Key = []byte(evtOrderable.OrderingKey())
	}

	if we, ok := event.(webEvent.AbstractEventWrapper); ok {
		webAbsEvent := we.GetAbstractEvent()
		message.Headers = d.appendMsgHeaders(message.Headers, webAbsEvent)
		message.Metadata = d.appendMsgMetadata(message.Metadata.(map[string]interface{}), webAbsEvent)
	}
	return &message, nil
}

func (d DefaultEventConverter) Restore(msg *core.ConsumerMessage, dest pubsub.Event) error {
	if err := json.Unmarshal(msg.Value, dest); err != nil {
		return errors.WithMessage(err, "unmarshal consumer message failed")
	}
	if we, ok := dest.(webEvent.AbstractEventWrapper); ok {
		abstractEvent := we.GetAbstractEvent()
		var attributes webEvent.Attributes
		d.restoreAttributesFromHeaders(msg.Headers, &attributes)
		d.restoreAttributesFromDeserializedEvent(abstractEvent, &attributes)
		abstractEvent.Ctx = context.WithValue(context.Background(), constant.ContextEventAttributes, &attributes)
	}
	return nil
}

func (d DefaultEventConverter) restoreAttributesFromHeaders(headers []core.MessageHeader, attributes *webEvent.Attributes) {
	for _, header := range headers {
		switch string(header.Key) {
		case constant.HeaderCorrelationId:
			attributes.CorrelationId = string(header.Value)
		case constant.HeaderDeviceId:
			attributes.DeviceId = string(header.Value)
		case constant.HeaderDeviceSessionId:
			attributes.DeviceSessionId = string(header.Value)
		}
	}
}

func (d DefaultEventConverter) restoreAttributesFromDeserializedEvent(evt *webEvent.AbstractEvent, attributes *webEvent.Attributes) {
	attributes.UserId = evt.UserId
	attributes.TechnicalUsername = evt.TechnicalUsername
}

func (d DefaultEventConverter) appendMsgHeaders(headers []core.MessageHeader, event *webEvent.AbstractEvent) []core.MessageHeader {
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

func (d DefaultEventConverter) appendMsgMetadata(metadata map[string]interface{}, event *webEvent.AbstractEvent) map[string]interface{} {
	if eventAttributes := webEvent.GetAttributes(event.Context()); eventAttributes != nil {
		metadata[kafkaConstant.LoggingContext] = webLog.NewContextAttributesFromEventAttrs(eventAttributes)
	}
	return metadata
}
