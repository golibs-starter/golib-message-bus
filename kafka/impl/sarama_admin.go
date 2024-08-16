package impl

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib-message-bus/kafka/properties"
	"github.com/golibs-starter/golib/log"
	"github.com/pkg/errors"
)

type SaramaAdmin struct {
	props  *properties.Client
	config *sarama.Config
}

func NewSaramaAdmin(props *properties.Client) (core.Admin, error) {
	config, err := CreateCommonSaramaConfig(props.Version, props.Admin)
	if err != nil {
		return nil, errors.WithMessage(err, "create sarama config error")
	}
	return &SaramaAdmin{props: props, config: config}, nil
}

func (s SaramaAdmin) CreateTopics(configurations []core.TopicConfiguration) error {
	if len(configurations) == 0 {
		log.Infof("Skip create Kafka topics. No topics are defined")
		return nil
	}
	topicDetails := s.buildTopicDetails(configurations)
	for _, server := range s.props.Admin.BootstrapServers {
		if err := s.createTopics(server, topicDetails); err != nil {
			return errors.WithMessage(err, "create topics failed")
		}
	}
	return nil
}

func (s SaramaAdmin) createTopics(server string, topicDetails map[string]*sarama.TopicDetail) error {
	broker, err := s.connectBroker(server, s.config)
	if err != nil {
		return err
	}
	defer func() {
		if err := broker.Close(); err != nil {
			log.Errorf("Cannot close kafka admin connection on server [%s], err [%s]", server, err)
		}
	}()
	if _, err = broker.CreateTopics(&sarama.CreateTopicsRequest{
		Timeout:      s.props.Admin.CreateTopicTimeout,
		TopicDetails: topicDetails,
	}); err != nil {
		return errors.WithMessagef(err, "create topics failed on server [%s]", server)
	}
	log.Infof("All Kafka topics have been created on server [%s]", server)
	return nil
}

func (s SaramaAdmin) DeleteTopics(topics []string) error {
	if len(topics) == 0 {
		log.Infof("No topics are defined for deletion")
		return nil
	}
	for _, server := range s.props.Admin.BootstrapServers {
		if err := s.deleteTopics(server, topics); err != nil {
			return errors.WithMessage(err, "delete topics failed")
		}
	}
	return nil
}

func (s SaramaAdmin) deleteTopics(server string, topics []string) error {
	broker, err := s.connectBroker(server, s.config)
	if err != nil {
		return err
	}
	defer func() {
		if err := broker.Close(); err != nil {
			log.Errorf("Cannot close kafka admin connection on server [%s], err [%s]", server, err)
		}
	}()
	if _, err = broker.DeleteTopics(&sarama.DeleteTopicsRequest{Topics: topics}); err != nil {
		return errors.WithMessagef(err, "delete topics failed on server [%s]", server)
	}
	log.Infof("Kafka topics [%v] have been deleted on server [%s]", topics, server)
	return nil
}

func (s SaramaAdmin) DeleteGroups(groupIds []string) error {
	if len(groupIds) == 0 {
		log.Infof("No group ids are defined for deletion")
		return nil
	}
	for _, server := range s.props.Admin.BootstrapServers {
		if err := s.deleteGroups(server, groupIds); err != nil {
			return errors.WithMessage(err, "delete groups failed")
		}
	}
	return nil
}

func (s SaramaAdmin) deleteGroups(server string, groupIds []string) error {
	broker, err := s.connectBroker(server, s.config)
	if err != nil {
		return err
	}
	defer func() {
		if err := broker.Close(); err != nil {
			log.Errorf("Cannot close kafka admin connection on server [%s], err [%s]", server, err)
		}
	}()
	if _, err = broker.DeleteGroups(&sarama.DeleteGroupsRequest{Groups: groupIds}); err != nil {
		return errors.WithMessagef(err, "delete groups failed on server [%s]", server)
	}
	log.Infof("Kafka groups [%v] have been deleted on server [%s]", groupIds, server)
	return nil
}

func (s SaramaAdmin) CountPartitions(topic string) (map[string]int32, error) {
	partitions := make(map[string]int32)
	for _, server := range s.props.Admin.BootstrapServers {
		count, err := s.countPartition(server, topic)
		if err != nil {
			return nil, errors.WithMessagef(err, "count partition failed on server [%s]", server)
		}
		partitions[server] = count
	}
	return partitions, nil
}

func (s SaramaAdmin) countPartition(server string, topic string) (int32, error) {
	broker, err := s.connectBroker(server, s.config)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := broker.Close(); err != nil {
			log.Errorf("Cannot close kafka admin connection on server [%s], err [%s]", server, err)
		}
	}()

	request := &sarama.MetadataRequest{Topics: []string{topic}}
	response, err := broker.GetMetadata(request)
	if err != nil {
		return 0, errors.WithMessage(err, "failed to get metadata")
	}

	for _, topicMetadata := range response.Topics {
		if topicMetadata.Name == topic {
			return int32(len(topicMetadata.Partitions)), nil
		}
	}

	return 0, fmt.Errorf("topic %s not found", topic)
}

func (s SaramaAdmin) connectBroker(server string, config *sarama.Config) (*sarama.Broker, error) {
	broker := sarama.NewBroker(server)
	if err := broker.Open(config); err != nil {
		return nil, errors.WithMessagef(err, "connect to kafka admin broker [%s] failed", server)
	}
	return broker, nil
}

func (s SaramaAdmin) buildTopicDetails(configurations []core.TopicConfiguration) map[string]*sarama.TopicDetail {
	topicDetails := make(map[string]*sarama.TopicDetail)
	for _, configuration := range configurations {
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
