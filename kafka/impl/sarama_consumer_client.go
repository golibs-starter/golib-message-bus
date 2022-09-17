package impl

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"time"
)

func NewSaramaConsumerClient(globalProps *properties.Client) (sarama.Client, error) {
	config, err := CreateCommonSaramaConfig(globalProps.Version, globalProps.Consumer)
	if err != nil {
		return nil, errors.WithMessage(err, "Create sarama config error")
	}
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Heartbeat.Interval = 10 * time.Millisecond
	props := globalProps.Consumer
	switch props.InitialOffset {
	case constant.InitialOffsetNewest:
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	case constant.InitialOffsetOldest:
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		config.Consumer.Offsets.Initial = props.InitialOffset
	}

	client, err := sarama.NewClient(props.BootstrapServers, config)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when create sarama consumer client")
	}
	return client, nil
}
