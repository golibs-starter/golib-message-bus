package impl

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/constant"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/utils"
	"gitlab.id.vin/vincart/golib/log"
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
	// Track errors
	go func() {
		for err := range c.consumerGroup.Errors() {
			log.Error("ConsumerGroup error, detail: ", err)
		}
	}()

	// Iterate over consumers sessions.
	c.running = true
	for c.running {
		topics := []string{c.topic}
		handler := NewConsumerGroupHandler(c.handler.HandlerFunc, c.mapper)
		log.Infof("Consumer with topic %v is running", topics)
		err := c.consumerGroup.Consume(ctx, topics, handler)
		if err != nil {
			if !c.running {
				log.Info("Consumer with topic %v is closed", topics)
			} else {
				log.Info("Error: ", err)
			}
		}
	}
}

func (c *SaramaConsumer) Close() {
	c.running = false
	c.consumerGroup.Close()
	c.handler.Close()
}
