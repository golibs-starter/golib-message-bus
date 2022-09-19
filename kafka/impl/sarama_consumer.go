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
	client               sarama.Client
	consumerGroup        sarama.ConsumerGroup
	consumerHandler      core.ConsumerHandler
	consumerGroupHandler *ConsumerGroupHandler
	name                 string
	topic                string
	running              bool
	waitHandlerReady     chan bool
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
	consumerGroupHandler := NewConsumerGroupHandler(client, handler.HandlerFunc, mapper)
	return &SaramaConsumer{
		client:               client,
		consumerGroup:        consumerGroup,
		consumerHandler:      handler,
		consumerGroupHandler: consumerGroupHandler,
		name:                 coreUtils.GetStructShortName(handler),
		topic:                topicConsumer.Topic,
		waitHandlerReady:     consumerGroupHandler.WaitForReady(),
	}, nil
}

func (c *SaramaConsumer) Start(ctx context.Context) {
	topics := []string{c.topic}
	log.Infof("Consumer [%s] with topic [%v] is starting", c.name, topics)

	// Track errors
	go func() {
		for err := range c.consumerGroup.Errors() {
			log.Errorf("Consumer group error for consumer [%s], detail: [%v]", c.name, err)
		}
	}()

	// Iterate over consumers sessions.
	c.running = true
	for c.running {
		log.Infof("Consumer [%s] with topic [%v] is running", c.name, topics)
		if err := c.consumerGroup.Consume(ctx, topics, c.consumerGroupHandler); err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				log.Infof("Consumer [%s] is closed when consume topics [%v], detail [%s]",
					c.name, topics, err.Error())
			} else if !c.running {
				log.Infof("Consumer [%s] is closed when consume topics [%v]",
					c.name, topics)
			} else {
				log.Errorf("Consume [%s] error when consume topics [%v], error [%v]",
					c.name, topics, err)
			}
		}
		c.consumerGroupHandler.MarkUnready()
	}
	log.Infof("Consumer [%s] with topic [%v] is closed", c.name, topics)
}

func (c SaramaConsumer) WaitForReady() chan bool {
	return c.waitHandlerReady
}

func (c *SaramaConsumer) Stop() {
	log.Infof("Consumer [%s] is stopping", c.name)
	defer log.Infof("Consumer [%s] stopped", c.name)
	c.running = false
	if err := c.consumerGroup.Close(); err != nil {
		log.Errorf("Consumer [%s] could not stop. Error [%v]", c.name, err)
	}
	c.consumerHandler.Close()
}
