package log

import (
	"fmt"
	kafkaConstant "gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib/web/constant"
	"gitlab.com/golibs-starter/golib/web/log"
	"strings"
)

func Info(msg *core.Message, messageFormat string, args ...interface{}) {
	if metadata, ok := msg.Metadata.(map[string]interface{}); ok {
		logContext := []interface{}{constant.ContextReqMeta, getLoggingContext(metadata)}
		log.Infow(logContext, messageFormat, args...)
	} else {
		log.Infof(messageFormat, args...)
	}
}

func Error(msg *core.Message, messageFormat string, args ...interface{}) {
	if metadata, ok := msg.Metadata.(map[string]interface{}); ok {
		logContext := []interface{}{constant.ContextReqMeta, getLoggingContext(metadata)}
		log.Errorw(logContext, messageFormat, args...)
	} else {
		log.Errorf(messageFormat, args...)
	}
}

func DescMessage(msg *core.Message, notLogPayloadForEvents []string) string {
	notLogPayloadForEventsMap := make(map[string]bool)
	for _, e := range notLogPayloadForEvents {
		notLogPayloadForEventsMap[e] = true
	}
	if metadata, ok := msg.Metadata.(map[string]interface{}); ok {
		eventId, _ := metadata[kafkaConstant.EventId].(string)
		eventName, _ := metadata[kafkaConstant.EventName].(string)
		var msgStr string
		if notLogPayloadForEventsMap[strings.ToLower(eventName)] {
			msgStr = fmt.Sprintf("[Topic: %s, Key: %s, Headers: %s]", msg.Topic, string(msg.Key), msg.Headers)
		} else {
			msgStr = msg.String()
		}
		return fmt.Sprintf("[Event: %s, EventId: %s, Msg: %s]", eventName, eventId, msgStr)
	}
	return fmt.Sprintf("[Msg: %s]", msg.String())
}
