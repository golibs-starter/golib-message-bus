package impl

import (
	"github.com/Shopify/sarama"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib/web/log"
)

type ConsumerGroupHandler struct {
	handleFunc func(message *core.ConsumerMessage)
	client     sarama.Client
	mapper     *SaramaMapper
	unready    chan bool
}

func NewConsumerGroupHandler(client sarama.Client, handleFunc func(message *core.ConsumerMessage), mapper *SaramaMapper) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		handleFunc: handleFunc,
		client:     client,
		mapper:     mapper,
		unready:    make(chan bool),
	}
}

func (cg ConsumerGroupHandler) WaitForReady() chan bool {
	return cg.unready
}

func (cg *ConsumerGroupHandler) MarkUnready() {
	cg.unready = make(chan bool)
}

func (cg *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(cg.unready)
	return nil
}

func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (cg ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
		case <-sess.Context().Done():
			log.Infof("Consumer session closed, stop taking new messages")
			return nil
		default:
			cg.handleFunc(cg.mapper.ToCoreConsumerMessage(msg))

			// Mark this message as consumed
			sess.MarkMessage(msg, "")

			if !cg.client.Config().Consumer.Offsets.AutoCommit.Enable {
				// Manual commit if auto commit is disabled
				sess.Commit()
			}
		}
	}
	return nil
}
