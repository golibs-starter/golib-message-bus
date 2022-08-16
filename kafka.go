package golibmsg

import (
	"gitlab.com/golibs-starter/golib"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/handler"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/impl"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/listener"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
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
		fx.Provide(impl.NewSaramaProducerClient),
		fx.Provide(impl.NewSaramaSyncProducer),
		fx.Provide(impl.NewSaramaAsyncProducer),
		golib.ProvideProps(properties.NewEventProducer),
		golib.ProvideEventListener(listener.NewProduceMessage),
		fx.Invoke(handler.AsyncProducerErrorLogHandler),
		fx.Invoke(handler.AsyncProducerSuccessLogHandler),
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
