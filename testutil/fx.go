package golibmsgTestUtil

import (
	"github.com/golibs-starter/golib-message-bus"
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib-message-bus/kafka/properties"
	"go.uber.org/fx"
	"reflect"
	"strings"
)

var consumerMap map[string]core.ConsumerHandler

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

func EnableKafkaConsumerTestUtil() fx.Option {
	return fx.Invoke(fx.Annotate(func(consumers []core.ConsumerHandler) {
		consumerMap = make(map[string]core.ConsumerHandler, 0)
		for _, consumer := range consumers {
			consumerName := strings.ToLower(getStructName(consumer))
			consumerMap[consumerName] = consumer
		}
	}, fx.ParamTags(`group:"kafka_consumer_handler"`)))
}

func getStructName(val interface{}) string {
	if t := reflect.TypeOf(val); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}
