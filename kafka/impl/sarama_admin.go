package impl

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib/web/log"
)

type SaramaAdmin struct {
	props *properties.Client
}

func NewSaramaAdmin(props *properties.Client) core.Admin {
	return &SaramaAdmin{props: props}
}

func (s SaramaAdmin) CreateTopics(topics []core.TopicConfiguration) error {
	if len(topics) == 0 {
		log.Infof("Skip create Kafka topics. No topics are defined")
		return nil
	}
	adminProps := s.props.Admin
	config, err := CreateCommonSaramaConfig(s.props.Version, adminProps)
	if err != nil {
		return errors.WithMessage(err, "create sarama config error")
	}

	for _, server := range adminProps.BootstrapServers {
		// Open connection to broker
		broker := sarama.NewBroker(server)
		if err := broker.Open(config); err != nil {
			return errors.WithMessage(err, "fail to open connection to kafka admin broker")
		}

		if _, err := broker.CreateTopics(&sarama.CreateTopicsRequest{
			Timeout:      adminProps.CreateTopicTimeout,
			TopicDetails: s.buildTopicDetails(topics),
		}); err != nil {
			return errors.WithMessage(err, "error when create topics")
		}
		log.Infof("All Kafka topics have been created")

		// Close connection to broker
		if err := broker.Close(); err != nil {
			return errors.WithMessage(err, "error while close kafka admin broker connection")
		}
	}
	return nil
}

func (s SaramaAdmin) buildTopicDetails(topics []core.TopicConfiguration) map[string]*sarama.TopicDetail {
	topicDetails := make(map[string]*sarama.TopicDetail)
	for _, configuration := range topics {
		configEntries := map[string]*string{}
		if configuration.Retention > 0 {
			retentionMs := fmt.Sprintf("%d", configuration.Retention.Milliseconds())
			configEntries["retention.ms"] = &retentionMs
		}
		topicDetails[configuration.Name] = &sarama.TopicDetail{
			NumPartitions:     configuration.Partitions,
			ReplicationFactor: configuration.ReplicaFactor,
			ConfigEntries:     configEntries,
		}
		log.Infof("Init Kafka topic [%s] with config [%+v]", configuration.Name, configuration)
	}
	return topicDetails
}
