package impl

import (
	"github.com/Shopify/sarama"
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib/log"
	coreUtils "github.com/golibs-starter/golib/utils"
)

type ConsumerGroupHandler struct {
	handler     core.ConsumerHandler
	handlerName string
	client      sarama.Client
	mapper      *SaramaMapper
	unready     chan bool
}

func NewConsumerGroupHandler(client sarama.Client, handler core.ConsumerHandler, mapper *SaramaMapper) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		handler:     handler,
		handlerName: coreUtils.GetStructShortName(handler),
		client:      client,
		mapper:      mapper,
		unready:     make(chan bool),
	}
}

func (cg *ConsumerGroupHandler) WaitForReady() chan bool {
	return cg.unready
}

func (cg *ConsumerGroupHandler) MarkUnready() {
	cg.unready = make(chan bool)
}

func (cg *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	log.Debugf("Setup consumer group handler [%s]", cg.handlerName)
	// Mark the consumer as ready
	close(cg.unready)
	return nil
}

func (cg *ConsumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	if sess.Context().Err() != nil {
		log.WithErrors(sess.Context().Err()).Debugf("Cleanup consumer group handler [%s]", cg.handlerName)
	}
	return nil
}

func (cg *ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			cg.handler.HandlerFunc(cg.mapper.ToCoreConsumerMessage(msg))

			// Mark this message as consumed
			sess.MarkMessage(msg, "")

			if !cg.client.Config().Consumer.Offsets.AutoCommit.Enable {
				// Manual commit if auto commit is disabled
				sess.Commit()
			}
			break
		case <-sess.Context().Done():
			log.Infof("Consumer session closed, [%s] stops taking new messages", cg.handlerName)
			return nil
		}
	}
}
