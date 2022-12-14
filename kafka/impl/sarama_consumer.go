package impl

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	coreUtils "gitlab.com/golibs-starter/golib/utils"
	"gitlab.com/golibs-starter/golib/web/log"
	"strings"
)

type SaramaConsumer struct {
	client               sarama.Client
	consumerGroup        sarama.ConsumerGroup
	consumerHandler      core.ConsumerHandler
	consumerGroupHandler *ConsumerGroupHandler
	name                 string
	topics               []string
	running              bool
}

func NewSaramaConsumer(
	mapper *SaramaMapper,
	clientProps *properties.Client,
	topicConsumer *properties.TopicConsumer,
	handler core.ConsumerHandler,
) (*SaramaConsumer, error) {
	handlerName := coreUtils.GetStructShortName(handler)
	client, err := NewSaramaConsumerClient(clientProps)
	if err != nil {
		return nil, errors.WithMessage(err,
			fmt.Sprintf("Error when create sarama consumer client for handler [%s]", handlerName))
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(strings.TrimSpace(topicConsumer.GroupId), client)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when create sarama consumer group")
	}
	topics := make([]string, 0)
	if topicConsumer.Topic != "" {
		topics = append(topics, strings.TrimSpace(topicConsumer.Topic))
	} else {
		for _, topic := range topicConsumer.Topics {
			topics = append(topics, strings.TrimSpace(topic))
		}
	}
	consumerGroupHandler := NewConsumerGroupHandler(client, handler, mapper)
	return &SaramaConsumer{
		client:               client,
		name:                 handlerName,
		topics:               topics,
		consumerGroup:        consumerGroup,
		consumerHandler:      handler,
		consumerGroupHandler: consumerGroupHandler,
	}, nil
}

func (c *SaramaConsumer) Start(ctx context.Context) {
	log.Infof("Consumer [%s] with topics [%v] is starting", c.name, c.topics)

	// Track errors
	go func() {
		for err := range c.consumerGroup.Errors() {
			log.Errorf("Consumer group error for consumer [%s], detail: [%v]", c.name, err)
		}
	}()

	// Iterate over consumers sessions.
	c.running = true
	for c.running {
		log.Infof("Consumer [%s] with topics [%v] is running", c.name, c.topics)
		if err := c.consumerGroup.Consume(ctx, c.topics, c.consumerGroupHandler); err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				log.Infof("Consumer [%s] is closed when consume topics [%v], detail [%s]",
					c.name, c.topics, err.Error())
			} else if !c.running {
				log.Infof("Consumer [%s] is closed when consume topics [%v]",
					c.name, c.topics)
			} else {
				log.Errorf("Consume [%s] error when consume topics [%v], error [%v]",
					c.name, c.topics, err)
			}
		}
		c.consumerGroupHandler.MarkUnready()
	}
	log.Infof("Consumer [%s] with topics [%v] is closed", c.name, c.topics)
}

func (c SaramaConsumer) WaitForReady() chan bool {
	return c.consumerGroupHandler.WaitForReady()
}

func (c *SaramaConsumer) Stop() {
	log.Infof("Consumer [%s] is stopping", c.name)
	defer log.Infof("Consumer [%s] stopped", c.name)
	c.running = false
	c.consumerHandler.Close()
	if err := c.consumerGroup.Close(); err != nil {
		log.Errorf("Consumer [%s] could not stop. Error [%v]", c.name, err)
	}
	if err := c.client.Close(); err != nil {
		log.Errorf("Consumer client [%s] could not stop. Error [%v]", c.name, err)
	}
}
