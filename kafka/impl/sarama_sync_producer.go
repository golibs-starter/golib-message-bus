package impl

import (
	"github.com/Shopify/sarama"
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib/log"
	"github.com/pkg/errors"
)

type SaramaSyncProducer struct {
	producer    sarama.SyncProducer
	errorsCh    chan *core.ProducerError
	successesCh chan *core.Message
	mapper      *SaramaMapper
}

func NewSaramaSyncProducer(client sarama.Client, mapper *SaramaMapper) (*SaramaSyncProducer, error) {
	syncProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when create new sync producer")
	}
	p := &SaramaSyncProducer{
		producer: syncProducer,
		mapper:   mapper,
	}
	return p, nil
}

func (s SaramaSyncProducer) Send(m *core.Message) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{
		Topic:    m.Topic,
		Value:    sarama.ByteEncoder(m.Value),
		Headers:  s.mapper.ToSaramaHeaders(m.Headers),
		Metadata: m.Metadata,
	}
	if m.Key != nil {
		msg.Key = sarama.ByteEncoder(m.Key)
	}
	return s.producer.SendMessage(msg)
}

func (s *SaramaSyncProducer) Close() error {
	log.Info("Kafka sync producer is stopping")
	defer log.Info("Kafka sync producer is stopped")
	return s.producer.Close()
}
