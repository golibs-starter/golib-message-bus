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

func (ConsumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	if sess.Context().Err() != nil {
		log.Infof("Consumer group cleanup with err [%v]", sess.Context().Err())
	}
	return nil
}

func (cg ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			cg.handleFunc(cg.mapper.ToCoreConsumerMessage(msg))

			// Mark this message as consumed
			sess.MarkMessage(msg, "")

			if !cg.client.Config().Consumer.Offsets.AutoCommit.Enable {
				// Manual commit if auto commit is disabled
				sess.Commit()
			}
			break
		case <-sess.Context().Done():
			log.Infof("Consumer session closed, stop taking new messages")
			return nil
		}
	}
	return nil
}
