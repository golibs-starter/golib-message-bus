package impl

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/utils"
	"strings"
	"time"
)

type SaramaAdmin struct {
	props properties.Admin
}

func NewSaramaAdmin(props properties.Client) core.Admin {
	return &SaramaAdmin{props: props.Admin}
}

func (s SaramaAdmin) CreateTopics(topics map[string]properties.TopicConfiguration) error {
	for _, server := range s.props.BootstrapServers {
		if server == "" {
			return errors.New("kafka host port is required")
		}
		hash := strings.Split(server, ":")
		host := hash[0]
		port := hash[1]
		if host == "" || port == "" {
			return errors.New("a kafka bootstrap server is invalid")
		}

		broker := sarama.NewBroker(fmt.Sprintf("%v:%v", host, port))
		config := sarama.NewConfig()
		config.Version = sarama.V1_1_0_0

		if s.props.SecurityProtocol == core.SecurityProtocolTls {
			tlsConfig, err := utils.NewTLSConfig(
				s.props.Tls.CertFileLocation,
				s.props.Tls.KeyFileLocation,
				s.props.Tls.CaFileLocation,
			)
			if err != nil {
				return err
			}
			tlsConfig.InsecureSkipVerify = s.props.Tls.InsecureSkipVerify
			config.Net.TLS.Enable = true
			config.Net.TLS.Config = tlsConfig
		}

		// Open connection to broker
		if err := broker.Open(config); err != nil {
			return err
		}

		topicDetails := make(map[string]*sarama.TopicDetail)
		for topic, configuration := range topics {
			topicDetails[topic] = &sarama.TopicDetail{
				NumPartitions:     configuration.Partitions,
				ReplicationFactor: configuration.ReplicaFactor,
				ConfigEntries:     make(map[string]*string),
			}
		}

		if _, err := broker.CreateTopics(&sarama.CreateTopicsRequest{
			Timeout:      time.Second * 15,
			TopicDetails: topicDetails,
		}); err != nil {
			return err
		}

		// Close connection to broker
		if err := broker.Close(); err != nil {
			return err
		}
	}

	return nil
}