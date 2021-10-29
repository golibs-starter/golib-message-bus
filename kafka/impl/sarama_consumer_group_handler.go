package impl

import (
	"github.com/Shopify/sarama"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
)

type ConsumerGroupHandler struct {
	handleFunc func(message *core.ConsumerMessage)
	mapper     *SaramaMapper
}

func NewConsumerGroupHandler(handleFunc func(message *core.ConsumerMessage), mapper *SaramaMapper) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{handleFunc: handleFunc, mapper: mapper}
}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (cg ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		cg.handleFunc(&core.ConsumerMessage{
			Key:       msg.Key,
			Value:     msg.Value,
			Topic:     msg.Topic,
			Headers:   cg.mapper.PtrToCoreHeaders(msg.Headers),
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
		})
		sess.MarkMessage(msg, "")
	}
	return nil
}
