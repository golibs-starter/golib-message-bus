package impl

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/utils"
)

type CommonProperties interface {
	GetClientId() string
	GetSecurityProtocol() string
	GetTls() *properties.Tls
}

func CreateCommonSaramaConfig(version string, props CommonProperties) (*sarama.Config, error) {
	config := sarama.NewConfig()
	configVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, errors.WithMessage(err, "Error parsing Kafka version")
	}
	config.Version = configVersion

	if props.GetClientId() != "" {
		config.ClientID = props.GetClientId()
	}

	if props.GetSecurityProtocol() == core.SecurityProtocolTls {
		tlsConfig, err := createTlsConfiguration(props.GetTls())
		if err != nil {
			return nil, errors.WithMessage(err, "Error when create tls config")
		}
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	return config, nil
}

func createTlsConfiguration(tlsProps *properties.Tls) (*tls.Config, error) {
	if tlsProps == nil {
		return nil, errors.New("Tls config not found when using SecurityProtocol=TLS")
	}
	tlsConfig, err := utils.NewTLSConfig(
		tlsProps.CertFileLocation,
		tlsProps.KeyFileLocation,
		tlsProps.CaFileLocation,
	)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when load TLS config")
	}
	tlsConfig.InsecureSkipVerify = tlsProps.InsecureSkipVerify
	return tlsConfig, nil
}
