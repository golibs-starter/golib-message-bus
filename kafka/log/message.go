package log

import (
	"fmt"
	kafkaConstant "gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"strings"
)

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
