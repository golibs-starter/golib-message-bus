package golibmsgTestUtil

import (
	"gitlab.com/golibs-starter/golib-message-bus"
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

func MessageCollectorOpt() fx.Option {
	return fx.Options(
		fx.Provide(NewMessageCollector),
		golibmsg.ProvideConsumer(NewMessageCollectorHandler),
	)
}
