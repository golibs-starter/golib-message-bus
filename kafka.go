package golibmsg

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/golibs-starter/golib"
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib-message-bus/kafka/handler"
	"github.com/golibs-starter/golib-message-bus/kafka/impl"
	"github.com/golibs-starter/golib-message-bus/kafka/properties"
	"github.com/golibs-starter/golib-message-bus/kafka/relayer"
	"github.com/golibs-starter/golib/log"
	"go.uber.org/fx"
)

func KafkaCommonOpt() fx.Option {
	return fx.Options(
		golib.ProvideProps(properties.NewClient),
		fx.Provide(impl.NewSaramaMapper),
		fx.Provide(impl.NewDebugLogger),
		fx.Invoke(func(props *properties.Client, debugLogger *impl.DebugLogger) {
			if props.Debug {
				log.Debug("Kafka debug mode is enabled")
				sarama.DebugLogger = debugLogger
			}
		}),
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
		fx.Provide(fx.Annotate(
			relayer.NewDefaultEventConverter,
			fx.As(new(relayer.EventConverter)),
		)),
		golib.ProvideProps(properties.NewEventProducer),
		golib.ProvideEventListener(relayer.NewEventMessageRelayer),
		fx.Invoke(handler.AsyncProducerErrorLogHandler),
		fx.Invoke(handler.AsyncProducerSuccessLogHandler),
	)
}

func KafkaConsumerOpt() fx.Option {
	return fx.Options(
		golib.ProvideProps(properties.NewKafkaConsumer),
		fx.Provide(NewSaramaConsumers),
		fx.Invoke(OnStartConsumerHook),
	)
}

func KafkaConsumerReadyWaitOpt() fx.Option {
	return fx.Invoke(func(lc fx.Lifecycle, consumer core.Consumer) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				log.Info("Wait all consumers are ready")
				// This will block until all consumers are ready
				<-consumer.WaitForReady()
				log.Info("All consumers are ready")
				return nil
			},
		})
	})
}

func OnStopProducerOpt() fx.Option {
	return fx.Invoke(OnStopProducerHook)
}

func OnStopConsumerOpt() fx.Option {
	return fx.Invoke(OnStopConsumerHook)
}

type KafkaConsumersIn struct {
	fx.In
	GlobalProps   *properties.Client
	ConsumerProps *properties.KafkaConsumer
	SaramaMapper  *impl.SaramaMapper
	Handlers      []core.ConsumerHandler `group:"kafka_consumer_handler"`
}

func NewSaramaConsumers(in KafkaConsumersIn) (core.Consumer, error) {
	return impl.NewSaramaConsumers(in.GlobalProps, in.ConsumerProps, in.SaramaMapper, in.Handlers)
}

func ProvideConsumer(handler interface{}) fx.Option {
	return fx.Provide(fx.Annotated{Group: "kafka_consumer_handler", Target: handler})
}

type OnStopProducerIn struct {
	fx.In
	Lc             fx.Lifecycle
	ProducerClient sarama.Client      `name:"sarama_producer_client" optional:"true"`
	SyncProducer   core.SyncProducer  `optional:"true"`
	AsyncProducer  core.AsyncProducer `optional:"true"`
}

func OnStopProducerHook(in OnStopProducerIn) {
	in.Lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Infof("Receive stop signal for kafka producer")
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
			return nil
		},
	})
}

type OnStopConsumerIn struct {
	fx.In
	Lc            fx.Lifecycle
	ConsumerGroup core.Consumer `optional:"true"`
}

func OnStartConsumerHook(lc fx.Lifecycle, consumer core.Consumer, golibCtx context.Context) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go consumer.Start(golibCtx)
			return nil
		},
	})
}

func OnStopConsumerHook(in OnStopConsumerIn) {
	in.Lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Infof("Receive stop signal for kafka consumer")
			if in.ConsumerGroup != nil {
				in.ConsumerGroup.Stop()
			}
			return nil
		},
	})
}
