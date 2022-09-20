package impl

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib/log"
)

func NewSaramaConsumerClient(globalProps *properties.Client) (sarama.Client, error) {
	config, err := CreateCommonSaramaConfig(globalProps.Version, globalProps.Consumer)
	if err != nil {
		return nil, errors.WithMessage(err, "Create sarama config error")
	}
	props := globalProps.Consumer
	config.Consumer.Return.Errors = true
	if props.CommitMode == constant.CommitModeAutoInterval {
		config.Consumer.Offsets.AutoCommit.Enable = true
	} else if props.CommitMode == constant.CommitModeAutoImmediately {
		config.Consumer.Offsets.AutoCommit.Enable = false
	} else {
		return nil, fmt.Errorf("commit mode [%s] is not supported", props.CommitMode)
	}
	log.Debugf("Initial sarama consumer client with commit mode: %s", props.CommitMode)
	switch props.InitialOffset {
	case constant.InitialOffsetNewest:
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	case constant.InitialOffsetOldest:
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		config.Consumer.Offsets.Initial = props.InitialOffset
	}
	if err := config.Validate(); err != nil {
		return nil, errors.WithMessage(err, "Error when validate consumer client config")
	}
	client, err := sarama.NewClient(props.BootstrapServers, config)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when create sarama consumer client")
	}
	return client, nil
}
