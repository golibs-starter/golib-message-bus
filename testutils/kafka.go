package golibmsgTestUtils

import (
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"go.uber.org/fx"
)

func ResetKafkaConsumerGroupOpt() fx.Option {
	return fx.Invoke(func(kafkaAdmin core.Admin, props *properties.KafkaConsumer) {
		groupIds := make([]string, 0)
		for _, handler := range props.HandlerMappings {
			groupIds = append(groupIds, handler.GroupId)
		}
		_ = kafkaAdmin.DeleteGroups(groupIds)
	})
}

func KafkaTestUtilsOpt() fx.Option {
	return fx.Provide(NewKafkaTestUtils)
}

type KafkaTestUtils struct {
	kafkaProperties *properties.Client
	messages        map[string][]string
}

func NewKafkaTestUtils(kafkaProperties *properties.Client) *KafkaTestUtils {
	return &KafkaTestUtils{
		kafkaProperties: kafkaProperties,
		messages:        map[string][]string{},
	}
}

func (k *KafkaTestUtils) PushMessage(message *core.ConsumerMessage) {
	if _, ok := k.messages[message.Topic]; ok {
		k.messages[message.Topic] = append(k.messages[message.Topic], string(message.Value))
	} else {
		k.messages[message.Topic] = []string{string(message.Value)}
	}
}

func (k *KafkaTestUtils) ClearMessages(topic string) {
	if _, ok := k.messages[topic]; ok {
		k.messages[topic] = []string{}
	}
}

func (k *KafkaTestUtils) Count(topic string) int64 {
	if val, ok := k.messages[topic]; ok {
		return int64(len(val))
	}
	return 0
}

func (k *KafkaTestUtils) GetMessages(topic string) []string {
	if messages, ok := k.messages[topic]; ok {
		return messages
	}
	return []string{}
}
