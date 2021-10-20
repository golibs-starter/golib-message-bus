package listener

import (
	"context"
	"encoding/json"
	assert "github.com/stretchr/testify/require"
	kafkaConstant "gitlab.id.vin/vincart/golib-message-bus/kafka/constant"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"gitlab.id.vin/vincart/golib/config"
	"gitlab.id.vin/vincart/golib/event"
	"gitlab.id.vin/vincart/golib/log"
	"gitlab.id.vin/vincart/golib/web/constant"
	webContext "gitlab.id.vin/vincart/golib/web/context"
	webEvent "gitlab.id.vin/vincart/golib/web/event"
	log2 "gitlab.id.vin/vincart/golib/web/log"
	"testing"
)

type TestProducer struct {
	message *core.Message
}

func (t *TestProducer) Errors() <-chan *core.ProducerError {
	panic("implement me")
}

func (t *TestProducer) Send(m *core.Message) {
	t.message = m
}

func (t TestProducer) Close() {
}

type TestEvent struct {
	*webEvent.AbstractEvent
}

func newTestEvent(ctx context.Context, payload interface{}) *TestEvent {
	return &TestEvent{webEvent.NewAbstractEvent(ctx, "TestEvent", payload)}
}

func replaceTestGlobalLogger(t *testing.T) {
	logger, err := log.NewLogger(&log.Options{Development: true})
	assert.NoError(t, err)
	log.ReplaceGlobal(logger)
}

func TestProduceMessage_WhenTopicMappingNotExists_ShouldDoNothing(t *testing.T) {
	replaceTestGlobalLogger(t)
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{TopicMappings: map[string]properties.EventTopic{}}
	eventProps := &event.Properties{}
	listener := NewProduceMessage(producer, appProps, eventProducerProps, eventProps)
	listener.Handle(webEvent.NewAbstractEvent(context.Background(), "TestEvent", nil))
	assert.Nil(t, producer.message)
}

func TestProduceMessage_WhenEventTopicIsDisabled_ShouldDoNothing(t *testing.T) {
	replaceTestGlobalLogger(t)
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{TopicMappings: map[string]properties.EventTopic{
		"TestEvent": {
			TopicName:     "test.topic",
			Transactional: false,
			Disable:       true,
		},
	}}
	eventProps := &event.Properties{}
	listener := NewProduceMessage(producer, appProps, eventProducerProps, eventProps)
	listener.Handle(webEvent.NewAbstractEvent(context.Background(), "TestEvent", nil))
	assert.Nil(t, producer.message)
}

func TestProduceMessage_WhenEventTopicNameIsEmpty_ShouldDoNothing(t *testing.T) {
	replaceTestGlobalLogger(t)
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{TopicMappings: map[string]properties.EventTopic{
		"TestEvent": {TopicName: ""},
	}}
	eventProps := &event.Properties{}
	listener := NewProduceMessage(producer, appProps, eventProducerProps, eventProps)
	listener.Handle(webEvent.NewAbstractEvent(context.Background(), "TestEvent", nil))
	assert.Nil(t, producer.message)
}

func TestProduceMessage_WhenIsApplicationEvent_ShouldSendMessageWithCorrectMessageAndHeaders(t *testing.T) {
	replaceTestGlobalLogger(t)
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{TopicMappings: map[string]properties.EventTopic{
		"testapplicationevent": {TopicName: "test.application.topic"},
	}}
	eventProps := &event.Properties{}
	listener := NewProduceMessage(producer, appProps, eventProducerProps, eventProps)
	testEvent := event.NewApplicationEvent("TestApplicationEvent", nil)
	listener.Handle(testEvent)

	assert.NotNil(t, producer.message)
	assert.Equal(t, "test.application.topic", producer.message.Topic)

	expectedTestEventBytes, err := json.Marshal(testEvent)
	assert.NoError(t, err)
	assert.Equal(t, string(expectedTestEventBytes), string(producer.message.Value))

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
	replaceTestGlobalLogger(t)
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{TopicMappings: map[string]properties.EventTopic{
		"testevent": {TopicName: "test.topic"},
	}}
	eventProps := &event.Properties{}
	listener := NewProduceMessage(producer, appProps, eventProducerProps, eventProps)
	fakeRequestCtx := context.WithValue(context.Background(), constant.ContextReqAttribute, &webContext.RequestAttributes{
		CorrelationId:   "test-request-id",
		DeviceId:        "test-device-id",
		DeviceSessionId: "test-device-session-id",
		ClientIpAddress: "test-client-ip",
		SecurityAttributes: webContext.SecurityAttributes{
			UserId:            "test-user-id",
			TechnicalUsername: "test-technical-username",
		},
	})
	testEvent := newTestEvent(fakeRequestCtx, "TestEvent")
	listener.Handle(testEvent)

	assert.NotNil(t, producer.message)
	assert.Equal(t, "test.topic", producer.message.Topic)

	expectedTestEventBytes, err := json.Marshal(testEvent)
	assert.NoError(t, err)
	assert.Equal(t, string(expectedTestEventBytes), string(producer.message.Value))

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
	assert.IsType(t, &log2.LoggingContext{}, resultMetadata[kafkaConstant.LoggingContext])
	resultLoggingContext := resultMetadata[kafkaConstant.LoggingContext].(*log2.LoggingContext)
	assert.Equal(t, "test-user-id", resultLoggingContext.UserId)
	assert.Equal(t, "test-technical-username", resultLoggingContext.TechnicalUsername)
	assert.Equal(t, "test-device-id", resultLoggingContext.DeviceId)
	assert.Equal(t, "test-device-session-id", resultLoggingContext.DeviceSessionId)
	assert.Equal(t, "test-request-id", resultLoggingContext.CorrelationId)
}

func TestProduceMessage_WhenIsWebEventAndNotLogPayload_ShouldSuccess(t *testing.T) {
	replaceTestGlobalLogger(t)
	producer := &TestProducer{}
	appProps := &config.AppProperties{Name: "TestApp"}
	eventProducerProps := &properties.EventProducer{TopicMappings: map[string]properties.EventTopic{
		"testevent": {TopicName: "test.topic"},
	}}
	eventProps := &event.Properties{
		Log: event.LogProperties{
			NotLogPayloadForEvents: []string{"TestEvent"},
		},
	}
	listener := NewProduceMessage(producer, appProps, eventProducerProps, eventProps)
	testEvent := webEvent.NewAbstractEvent(context.Background(), "TestEvent", nil)
	listener.Handle(testEvent)
	assert.NotNil(t, producer.message)
}
