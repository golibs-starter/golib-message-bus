package impl

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
)

func NewSaramaProducerClient(globalProps *properties.Client) (sarama.Client, error) {
	config, err := CreateCommonSaramaConfig(globalProps.Version, globalProps.Producer)
	if err != nil {
		return nil, errors.WithMessage(err, "Create sarama config error")
	}
	props := globalProps.Producer
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Flush.Messages = props.FlushMessages
	config.Producer.Flush.Frequency = props.FlushFrequency
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return sarama.NewClient(props.BootstrapServers, config)
}
