package golibmsg

import (
	"gitlab.id.vin/vincart/golib"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/impl"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/listener"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"go.uber.org/fx"
)

func KafkaPropsOpt() fx.Option {
	return fx.Options(
		golib.EnablePropsAutoload(new(properties.Client)),
		fx.Provide(properties.NewClient),
	)
}

func KafkaAdminOpt() fx.Option {
	return fx.Options(
		golib.EnablePropsAutoload(new(properties.TopicAdmin)),
		fx.Provide(properties.NewTopicAdmin),
		fx.Provide(impl.NewSaramaAdmin),
	)
}

func KafkaProducerOpt() fx.Option {
	return fx.Options(
		fx.Provide(impl.NewSaramaProducer),
		fx.Provide(listener.NewProduceMessage),
	)
}
