package golibmsg

import (
	"gitlab.id.vin/vincart/golib"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/handler"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/impl"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/listener"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"go.uber.org/fx"
)

func KafkaCommonOpt() fx.Option {
	return fx.Options(
		golib.ProvideProps(properties.NewClient),
		fx.Provide(impl.NewSaramaMapper),
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

func KafkaConsumerOpt() fx.Option {
	return fx.Options(
		golib.ProvideProps(properties.NewKafkaConsumer),
		fx.Provide(NewSaramaConsumers),
		fx.Invoke(handler.StartConsumers),
	)
}

type NewKafkaConsumersIn struct {
	fx.In
	Props              *properties.Client
	KafkaConsumerProps *properties.KafkaConsumer
	Mapper             *impl.SaramaMapper
	Handlers           []core.ConsumerHandler `group:"kafka_consumer_handler"`
}

func NewSaramaConsumers(in NewKafkaConsumersIn) (core.Consumer, error) {
	return impl.NewSaramaConsumers(in.Props, in.KafkaConsumerProps, in.Mapper, in.Handlers)
}

func ProvideConsumer(handler interface{}) fx.Option {
	return fx.Provide(fx.Annotated{Group: "kafka_consumer_handler", Target: handler})
}
