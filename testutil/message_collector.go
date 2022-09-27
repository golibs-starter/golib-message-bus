package golibmsgTestUtil

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"go.uber.org/fx"
	"sync"
)

func MessageCollectorOpt() fx.Option {
	return fx.Provide(NewMessageCollector)
}

type MessageCollector struct {
	messages map[string][]string
	mu       sync.RWMutex
}

func NewMessageCollector() *MessageCollector {
	return &MessageCollector{
		messages: map[string][]string{},
	}
}

func (k *MessageCollector) PushMessage(message *core.ConsumerMessage) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.messages[message.Topic]; ok {
		k.messages[message.Topic] = append(k.messages[message.Topic], string(message.Value))
	} else {
		k.messages[message.Topic] = []string{string(message.Value)}
	}
}

func (k *MessageCollector) ClearMessages(topic string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.messages[topic]; ok {
		k.messages[topic] = []string{}
	}
}

func (k *MessageCollector) Count(topic string) int64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if val, ok := k.messages[topic]; ok {
		return int64(len(val))
	}
	return 0
}

func (k *MessageCollector) GetMessages(topic string) []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if messages, ok := k.messages[topic]; ok {
		return messages
	}
	return []string{}
}
