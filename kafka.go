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
			fx.As(new(core.SyncProducer)),
			fx.ParamTags(`name:"sarama_producer_client"`),
		)),
		fx.Provide(fx.Annotate(
			impl.NewSaramaAsyncProducer,
			fx.As(new(core.AsyncProducer)),
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
	)
}

func KafkaConsumerReadyWaitOpt() fx.Option {
	return fx.Invoke(func(consumers core.Consumer) {
		log.Info("Wait all consumers are ready")
		// This will block until all consumers are ready
		<-consumers.WaitForReady()
		log.Info("All consumers are ready")
	})
}

func KafkaGracefulShutdownOpt() fx.Option {
	return fx.Invoke(AppendGracefulShutdownBehavior)
}

type KafkaConsumersIn struct {
	fx.In
	Client        sarama.Client `name:"sarama_consumer_client"`
	GlobalProps   *properties.Client
	ConsumerProps *properties.KafkaConsumer
	SaramaMapper  *impl.SaramaMapper
	Handlers      []core.ConsumerHandler `group:"kafka_consumer_handler"`
}

func NewSaramaConsumers(in KafkaConsumersIn) (core.Consumer, error) {
	return impl.NewSaramaConsumers(in.Client, in.GlobalProps, in.ConsumerProps, in.SaramaMapper, in.Handlers)
}

func ProvideConsumer(handler interface{}) fx.Option {
	return fx.Provide(fx.Annotated{Group: "kafka_consumer_handler", Target: handler})
}

type KafkaGracefulShutDownIn struct {
	fx.In
	Lc             fx.Lifecycle
	ProducerClient sarama.Client      `name:"sarama_producer_client" optional:"true"`
	SyncProducer   core.SyncProducer  `optional:"true"`
	AsyncProducer  core.AsyncProducer `optional:"true"`
	ConsumerClient sarama.Client      `name:"sarama_consumer_client" optional:"true"`
	ConsumerGroup  core.Consumer      `optional:"true"`
}

func AppendGracefulShutdownBehavior(in KafkaGracefulShutDownIn) {
	in.Lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Infof("Receive stop signal for kafka")
			if in.SyncProducer != nil {
				if err := in.SyncProducer.Close(); err != nil {
					log.Errorf("Cannot close kafka sync producer. Error [%v]", err)
				}
			}
			if in.AsyncProducer != nil {
				err := in.AsyncProducer.Close()
				if err != nil {
					log.Errorf("Cannot close kafka async producer. Error [%v]", err)
				}
			}
			if in.ProducerClient != nil {
				if err := in.ProducerClient.Close(); err != nil {
					log.Errorf("Cannot stop kafka producer client. Error [%v]", err)
				}
			}
			if in.ConsumerGroup != nil {
				in.ConsumerGroup.Stop()
			}
			if in.ConsumerClient != nil {
				if err := in.ConsumerClient.Close(); err != nil {
					log.Errorf("Cannot stop kafka consumer client. Error [%v]", err)
				}
			}
			return nil
		},
	})
}
