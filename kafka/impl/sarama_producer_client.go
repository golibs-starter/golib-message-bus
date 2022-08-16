package impl

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/utils"
)

func NewSaramaProducerClient(props *properties.Client) (sarama.Client, error) {
	config := sarama.NewConfig()
	if props.Producer.ClientId != "" {
		config.ClientID = props.Producer.ClientId
	}
	config.Version = sarama.V1_1_0_0
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Flush.Messages = props.Producer.FlushMessages
	config.Producer.Flush.Frequency = props.Producer.FlushFrequency
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	if props.Producer.SecurityProtocol == core.SecurityProtocolTls {
		if props.Producer.Tls == nil {
			return nil, errors.New("Tls config not found when using SecurityProtocol=TLS")
		}
		tlsConfig, err := utils.NewTLSConfig(
			props.Producer.Tls.CertFileLocation,
			props.Producer.Tls.KeyFileLocation,
			props.Producer.Tls.CaFileLocation,
		)
		if err != nil {
			return nil, errors.WithMessage(err, "Error when load TLS config")
		}
		tlsConfig.InsecureSkipVerify = props.Producer.Tls.InsecureSkipVerify
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	return sarama.NewClient(props.Producer.BootstrapServers, config)
}
