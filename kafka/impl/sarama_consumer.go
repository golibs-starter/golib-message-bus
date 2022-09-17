package impl

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	coreUtils "gitlab.com/golibs-starter/golib/utils"
	"gitlab.com/golibs-starter/golib/web/log"
)

type SaramaConsumer struct {
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	mapper        *SaramaMapper
	topic         string
	name          string
	handler       core.ConsumerHandler
}

func NewSaramaConsumer(
	client sarama.Client,
	mapper *SaramaMapper,
	topicConsumer *properties.TopicConsumer,
	handler core.ConsumerHandler,
) (*SaramaConsumer, error) {
	consumerGroup, err := sarama.NewConsumerGroupFromClient(topicConsumer.GroupId, client)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when create sarama consumer group")
	}

	return &SaramaConsumer{
		client:        client,
		mapper:        mapper,
		name:          coreUtils.GetStructShortName(handler),
		handler:       handler,
		topic:         topicConsumer.Topic,
		consumerGroup: consumerGroup,
	}, nil
}

func (c *SaramaConsumer) Start(ctx context.Context) {
	topics := []string{c.topic}
	log.Infof("Consumer [%s] with topic [%v] is starting", c.name, topics)

	// Track errors
	go func() {
		for err := range c.consumerGroup.Errors() {
			log.Errorf("ConsumerGroup error for consumer [%s], detail: [%v]", c.name, err)
		}
	}()

	// Iterate over consumers sessions.
	log.Infof("Consumer [%s] with topic [%v] is running", c.name, topics)
	handler := NewConsumerGroupHandler(c.handler.HandlerFunc, c.mapper)
	if err := c.consumerGroup.Consume(ctx, topics, handler); err != nil {
		if err == sarama.ErrClosedConsumerGroup {
			log.Infof("Consume group [%s] is closed when consume topics [%v], detail [%s]",
				c.name, topics, err.Error())
		} else {
			log.Errorf("Error when consume message in topics [%v] for consumer [%s], detail [%v]",
				topics, c.name, err)
		}
	}
	log.Infof("Consumer [%s] with topic [%v] is closed", c.name, topics)
}

func (c *SaramaConsumer) Close() {
	log.Infof("Consumer [%s] is stopping", c.name)
	defer log.Infof("Consumer [%s] stopped", c.name)
	if err := c.consumerGroup.Close(); err != nil {
		log.Errorf("Consumer [%s] could not stop. Error [%v]", c.name, err)
	}
	c.handler.Close()
}
