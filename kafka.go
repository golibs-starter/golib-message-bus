package golibmsg

import (
	"github.com/pkg/errors"
	"gitlab.id.vin/vincart/golib"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
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
		fx.Invoke(initiateKafkaAdmin),
	)
}

func KafkaProducerOpt() fx.Option {
	return fx.Options(
		fx.Provide(impl.NewSaramaProducer),
		fx.Provide(listener.NewProduceMessage),
	)
}

func initiateKafkaAdmin(admin core.Admin, props *properties.TopicAdmin) error {
	err := admin.CreateTopics(props.Topics)
	if err != nil {
		return errors.WithMessage(err, "failed create topics")
	}
	return nil
}
