package golibmsg

import (
	"gitlab.id.vin/vincart/golib"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/handler"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/impl"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/listener"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"go.uber.org/fx"
)

func KafkaPropsOpt() fx.Option {
	return fx.Options(
		golib.ProvideProps(properties.NewClient),
	)
}

func KafkaAdminOpt() fx.Option {
	return fx.Options(
		golib.ProvideProps(properties.NewTopicAdmin),
		fx.Provide(impl.NewSaramaAdmin),
		fx.Invoke(handler.CreateKafkaTopicHandler),
	)
}

func KafkaProducerOpt() fx.Option {
	return fx.Options(
		fx.Provide(impl.NewSaramaProducer),
		golib.ProvideProps(properties.NewEventProducer),
		golib.ProvideEventListener(listener.NewProduceMessage),
		fx.Invoke(handler.ProducerErrorLogHandler),
		fx.Invoke(handler.ProducerSuccessLogHandler),
	)
}
