package listener

import (
	"context"
	"encoding/json"
	assert "github.com/stretchr/testify/require"
	kafkaConstant "gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib/config"
	"gitlab.com/golibs-starter/golib/event"
	"gitlab.com/golibs-starter/golib/web/constant"
	webContext "gitlab.com/golibs-starter/golib/web/context"
	webEvent "gitlab.com/golibs-starter/golib/web/event"
	webLog "gitlab.com/golibs-starter/golib/web/log"
	"testing"
)

type TestProducer struct {
	message *core.Message
}

func (t *TestProducer) Send(m *core.Message) (partition int32, offset int64, err error) {
	t.message = m
	return 1, 0, nil
}

func (t *TestProducer) Close() error {
	return nil
}

type TestEvent struct {
	*webEvent.AbstractEvent
}

func newTestEvent(ctx context.Context, payload interface{}) *TestEvent {
	return &TestEvent{AbstractEvent: webEvent.NewAbstractEvent(ctx, "TestEvent", event.WithPayload(payload))}
}

type TestOrderableEvent struct {
	*webEvent.AbstractEvent
	PayloadData interface{} `json:"payload"`
	OrderId     string
}

func (t TestOrderableEvent) OrderingKey() string {
	return t.OrderId
}

func newTestOrderableEvent(ctx context.Context, payload interface{}) *TestOrderableEvent {
	return &TestOrderableEvent{
		AbstractEvent: webEvent.NewAbstractEvent(ctx, "TestOrderableEvent", event.WithPayload(payload)),
	}
}

func TestProduceMessage_WhenTopicMappingNotExists_ShouldNotSupport(t *testing.T) {
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{EventMappings: map[string]properties.EventTopic{}}
	eventProps := &event.Properties{}
	converter := NewDefaultEventConverter(appProps, eventProducerProps)
	listener := NewProduceMessage(producer, eventProducerProps, eventProps, converter)
	assert.False(t, listener.Supports(webEvent.NewAbstractEvent(context.Background(), "TestEvent")))
}

func TestProduceMessage_WhenEventTopicIsDisabled_ShouldNotSupport(t *testing.T) {
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{EventMappings: map[string]properties.EventTopic{
		"TestEvent": {
			TopicName:     "test.topic",
			Transactional: false,
			Disable:       true,
		},
	}}
	eventProps := &event.Properties{}
	converter := NewDefaultEventConverter(appProps, eventProducerProps)
	listener := NewProduceMessage(producer, eventProducerProps, eventProps, converter)
	assert.False(t, listener.Supports(webEvent.NewAbstractEvent(context.Background(), "TestEvent")))
}

func TestProduceMessage_WhenEventTopicNameIsEmpty_ShouldNotSupport(t *testing.T) {
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{EventMappings: map[string]properties.EventTopic{
		"TestEvent": {TopicName: ""},
	}}
	eventProps := &event.Properties{}
	converter := NewDefaultEventConverter(appProps, eventProducerProps)
	listener := NewProduceMessage(producer, eventProducerProps, eventProps, converter)
	assert.False(t, listener.Supports(webEvent.NewAbstractEvent(context.Background(), "TestEvent")))
}

func TestProduceMessage_WhenIsApplicationEvent_ShouldSendMessageWithCorrectMessageAndHeaders(t *testing.T) {
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{EventMappings: map[string]properties.EventTopic{
		"testapplicationevent": {TopicName: "test.application.topic"},
	}}
	eventProps := &event.Properties{}
	converter := NewDefaultEventConverter(appProps, eventProducerProps)
	listener := NewProduceMessage(producer, eventProducerProps, eventProps, converter)
	testEvent := event.NewApplicationEvent("TestApplicationEvent")
	listener.Handle(testEvent)

	assert.NotNil(t, producer.message)
	assert.Equal(t, "test.application.topic", producer.message.Topic)

	expectedTestEventBytes, err := json.Marshal(testEvent)
	assert.NoError(t, err)
	assert.Equal(t, string(expectedTestEventBytes), string(producer.message.Value))
	assert.Nil(t, producer.message.Key)

	assert.Len(t, producer.message.Headers, 2)
	assert.Equal(t, constant.HeaderEventId, string(producer.message.Headers[0].Key))
	assert.Equal(t, testEvent.Identifier(), string(producer.message.Headers[0].Value))
	assert.Equal(t, constant.HeaderServiceClientName, string(producer.message.Headers[1].Key))
	assert.Equal(t, appProps.Name, string(producer.message.Headers[1].Value))
	assert.IsType(t, map[string]interface{}{}, producer.message.Metadata)
	assert.Len(t, producer.message.Metadata, 2)
	resultMetadata := producer.message.Metadata.(map[string]interface{})
	assert.Equal(t, testEvent.Identifier(), resultMetadata[kafkaConstant.EventId])
	assert.Equal(t, testEvent.Name(), resultMetadata[kafkaConstant.EventName])
	assert.Nil(t, resultMetadata[kafkaConstant.LoggingContext])
}

func TestProduceMessage_WhenIsWebEvent_ShouldSendMessageWithCorrectMessageAndHeaders(t *testing.T) {
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{EventMappings: map[string]properties.EventTopic{
		"testevent": {TopicName: "test.topic"},
	}}
	eventProps := &event.Properties{}
	converter := NewDefaultEventConverter(appProps, eventProducerProps)
	listener := NewProduceMessage(producer, eventProducerProps, eventProps, converter)
	fakeRequestCtx := context.WithValue(context.Background(), constant.ContextReqAttribute, &webContext.RequestAttributes{
		CorrelationId:   "test-request-id",
		DeviceId:        "test-device-id",
		DeviceSessionId: "test-device-session-id",
		ClientIpAddress: "test-client-ip",
		SecurityAttributes: webContext.SecurityAttributes{
			UserId:            "test-user-id",
			TechnicalUsername: "test-technical-username",
		},
		ServiceCode: appProps.Name,
	})
	testEvent := newTestEvent(fakeRequestCtx, "TestEvent")
	listener.Handle(testEvent)

	assert.NotNil(t, producer.message)
	assert.Equal(t, "test.topic", producer.message.Topic)

	expectedTestEventBytes, err := json.Marshal(testEvent)
	assert.NoError(t, err)
	assert.Equal(t, string(expectedTestEventBytes), string(producer.message.Value))
	assert.Nil(t, producer.message.Key)

	assert.Len(t, producer.message.Headers, 6)
	assert.Equal(t, constant.HeaderEventId, string(producer.message.Headers[0].Key))
	assert.Equal(t, testEvent.Identifier(), string(producer.message.Headers[0].Value))
	assert.Equal(t, constant.HeaderServiceClientName, string(producer.message.Headers[1].Key))
	assert.Equal(t, appProps.Name, string(producer.message.Headers[1].Value))
	assert.Equal(t, constant.HeaderCorrelationId, string(producer.message.Headers[2].Key))
	assert.Equal(t, "test-request-id", string(producer.message.Headers[2].Value))
	assert.Equal(t, constant.HeaderDeviceId, string(producer.message.Headers[3].Key))
	assert.Equal(t, "test-device-id", string(producer.message.Headers[3].Value))
	assert.Equal(t, constant.HeaderDeviceSessionId, string(producer.message.Headers[4].Key))
	assert.Equal(t, "test-device-session-id", string(producer.message.Headers[4].Value))
	assert.Equal(t, constant.HeaderClientIpAddress, string(producer.message.Headers[5].Key))
	assert.Equal(t, "test-client-ip", string(producer.message.Headers[5].Value))
	assert.IsType(t, map[string]interface{}{}, producer.message.Metadata)
	assert.Len(t, producer.message.Metadata, 3)
	resultMetadata := producer.message.Metadata.(map[string]interface{})
	assert.Equal(t, testEvent.Identifier(), resultMetadata[kafkaConstant.EventId])
	assert.Equal(t, testEvent.Name(), resultMetadata[kafkaConstant.EventName])
	assert.IsType(t, &webLog.LoggingContext{}, resultMetadata[kafkaConstant.LoggingContext])
	resultLoggingContext := resultMetadata[kafkaConstant.LoggingContext].(*webLog.LoggingContext)
	assert.Equal(t, "test-user-id", resultLoggingContext.UserId)
	assert.Equal(t, "test-technical-username", resultLoggingContext.TechnicalUsername)
	assert.Equal(t, "test-device-id", resultLoggingContext.DeviceId)
	assert.Equal(t, "test-device-session-id", resultLoggingContext.DeviceSessionId)
	assert.Equal(t, "test-request-id", resultLoggingContext.CorrelationId)
}

func TestProduceMessage_WhenEventIsOrderable_ShouldSendMessageWithCorrectKey(t *testing.T) {
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{EventMappings: map[string]properties.EventTopic{
		"testorderableevent": {TopicName: "test.topic"},
	}}
	eventProps := &event.Properties{}
	converter := NewDefaultEventConverter(appProps, eventProducerProps)
	listener := NewProduceMessage(producer, eventProducerProps, eventProps, converter)
	testEvent := newTestOrderableEvent(context.Background(), "TestEvent")
	testEvent.OrderId = "3"
	listener.Handle(testEvent)

	assert.NotNil(t, producer.message)
	assert.Equal(t, "test.topic", producer.message.Topic)

	expectedTestEventBytes, err := json.Marshal(testEvent)
	assert.NoError(t, err)
	assert.Equal(t, string(expectedTestEventBytes), string(producer.message.Value))
	assert.Equal(t, "3", string(producer.message.Key))
}

func TestProduceMessage_WhenIsWebEventAndNotLogPayload_ShouldSuccess(t *testing.T) {
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{EventMappings: map[string]properties.EventTopic{
		"testevent": {TopicName: "test.topic"},
	}}
	eventProps := &event.Properties{
		Log: event.LogProperties{
			NotLogPayloadForEvents: []string{"TestEvent"},
		},
	}
	converter := NewDefaultEventConverter(appProps, eventProducerProps)
	listener := NewProduceMessage(producer, eventProducerProps, eventProps, converter)
	testEvent := webEvent.NewAbstractEvent(context.Background(), "TestEvent")
	listener.Handle(testEvent)
	assert.NotNil(t, producer.message)
}
