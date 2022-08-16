package impl

import (
	"github.com/Shopify/sarama"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
)

type SaramaMapper struct {
}

func NewSaramaMapper() *SaramaMapper {
	return &SaramaMapper{}
}

func (p SaramaMapper) ToSaramaHeaders(headers []core.MessageHeader) []sarama.RecordHeader {
	saramaHeaders := make([]sarama.RecordHeader, 0)
	for _, header := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	return saramaHeaders
}

func (p SaramaMapper) ToCoreHeaders(headers []sarama.RecordHeader) []core.MessageHeader {
	coreHeaders := make([]core.MessageHeader, 0)
	for _, header := range headers {
		coreHeaders = append(coreHeaders, p.toCoreHeader(&header))
	}
	return coreHeaders
}

func (p SaramaMapper) PtrToCoreHeaders(headers []*sarama.RecordHeader) []core.MessageHeader {
	coreHeaders := make([]core.MessageHeader, 0)
	for _, header := range headers {
		coreHeaders = append(coreHeaders, p.toCoreHeader(header))
	}
	return coreHeaders
}

func (p SaramaMapper) toCoreHeader(header *sarama.RecordHeader) core.MessageHeader {
	return core.MessageHeader{
		Key:   header.Key,
		Value: header.Value,
	}
}

func (p SaramaMapper) ToCoreMessage(msg *sarama.ProducerMessage) *core.Message {
	var key, value []byte
	if msg.Key != nil {
		key, _ = msg.Key.Encode()
	}
	if msg.Value != nil {
		value, _ = msg.Value.Encode()
	}
	return &core.Message{
		Topic:    msg.Topic,
		Key:      key,
		Value:    value,
		Headers:  p.ToCoreHeaders(msg.Headers),
		Metadata: msg.Metadata,
	}
}
