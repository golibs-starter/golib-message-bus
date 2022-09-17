package golibmsg

import (
	"context"
	"github.com/Shopify/sarama"
	"gitlab.com/golibs-starter/golib"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/handler"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/impl"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/listener"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib/log"
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
		fx.Provide(fx.Annotated{
			Name:   "sarama_producer_client",
			Target: impl.NewSaramaProducerClient,
		}),
		fx.Provide(fx.Annotate(
			impl.NewSaramaSyncProducer,
			fx.ParamTags(`name:"sarama_producer_client"`),
		)),
		fx.Provide(fx.Annotate(
			impl.NewSaramaAsyncProducer,
			fx.ParamTags(`name:"sarama_producer_client"`),
		)),
		golib.ProvideProps(properties.NewEventProducer),
		golib.ProvideEventListener(listener.NewProduceMessage),
		fx.Invoke(handler.AsyncProducerErrorLogHandler),
		fx.Invoke(handler.AsyncProducerSuccessLogHandler),
	)
}

func KafkaConsumerOpt() fx.Option {
	return fx.Options(
		golib.ProvideProps(properties.NewKafkaConsumer),
		fx.Provide(fx.Annotated{
			Name:   "sarama_consumer_client",
			Target: impl.NewSaramaConsumerClient,
		}),
		fx.Provide(NewSaramaConsumers),
		fx.Invoke(handler.StartConsumers),
		fx.Invoke(func(lc fx.Lifecycle, consumer core.Consumer) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					log.Infof("Stop kafka consumer")
					consumer.Close()
					return nil
				},
			})
		}),
	)
}

type NewKafkaConsumersIn struct {
	fx.In
	Client             sarama.Client `name:"sarama_consumer_client"`
	Props              *properties.Client
	KafkaConsumerProps *properties.KafkaConsumer
	Mapper             *impl.SaramaMapper
	Handlers           []core.ConsumerHandler `group:"kafka_consumer_handler"`
}

func NewSaramaConsumers(in NewKafkaConsumersIn) (core.Consumer, error) {
	return impl.NewSaramaConsumers(in.Client, in.Props, in.KafkaConsumerProps, in.Mapper, in.Handlers)
}

func ProvideConsumer(handler interface{}) fx.Option {
	return fx.Provide(fx.Annotated{Group: "kafka_consumer_handler", Target: handler})
}
