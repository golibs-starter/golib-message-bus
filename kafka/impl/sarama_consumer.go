package impl

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/utils"
	"gitlab.com/golibs-starter/golib/web/log"
)

type SaramaConsumer struct {
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	mapper        *SaramaMapper
	topic         string
	handler       core.ConsumerHandler
	running       bool
}

func NewSaramaConsumer(
	props *properties.Consumer,
	mapper *SaramaMapper,
	topicConsumer *properties.TopicConsumer,
	handler core.ConsumerHandler,
) (*SaramaConsumer, error) {
	config := sarama.NewConfig()
	if props.ClientId != "" {
		config.ClientID = props.ClientId
	}
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true

	if props.SecurityProtocol == core.SecurityProtocolTls {
		if props.Tls == nil {
			return nil, errors.New("Tls config not found when using SecurityProtocol=TLS")
		}
		tlsConfig, err := utils.NewTLSConfig(
			props.Tls.CertFileLocation,
			props.Tls.KeyFileLocation,
			props.Tls.CaFileLocation,
		)
		if err != nil {
			return nil, errors.WithMessage(err, "Error when load TLS config")
		}
		tlsConfig.InsecureSkipVerify = props.Tls.InsecureSkipVerify
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

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

	consumerGroup, err := sarama.NewConsumerGroupFromClient(topicConsumer.GroupId, client)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when create sarama consumer group")
	}

	return &SaramaConsumer{
		client:        client,
		mapper:        mapper,
		handler:       handler,
		topic:         topicConsumer.Topic,
		consumerGroup: consumerGroup,
	}, nil
}

func (c *SaramaConsumer) Start(ctx context.Context) {
	topics := []string{c.topic}
	consumerName := utils.GetStructName(c.handler)
	log.Infof("Consumer [%s] with topic [%v] is starting", consumerName, topics)

	// Track errors
	go func() {
		for err := range c.consumerGroup.Errors() {
			log.Errorf("ConsumerGroup error for consumer [%s], detail: [%v]", consumerName, err)
		}
	}()

	// Iterate over consumers sessions.
	c.running = true
	for c.running {
		log.Infof("Consumer [%s] with topic [%v] is running", consumerName, topics)
		handler := NewConsumerGroupHandler(c.handler.HandlerFunc, c.mapper)
		if err := c.consumerGroup.Consume(ctx, topics, handler); err != nil {
			if !c.running {
				log.Infof("Consumer [%s] with topic [%v] is closed, err [%s]", consumerName, topics, err)
			} else {
				log.Errorf("Error when consume message in topics [%v] for consumer [%s], detail [%v]",
					topics, consumerName, err)
			}
		}
	}
	log.Infof("Consumer [%s] with topic [%v] is closed", consumerName, topics)
}

func (c *SaramaConsumer) Close() {
	c.running = false
	c.consumerGroup.Close()
	c.handler.Close()
}
