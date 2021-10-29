package impl

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/utils"
)

type SaramaProducer struct {
	producer    sarama.AsyncProducer
	errorsCh    chan *core.ProducerError
	successesCh chan *core.Message
	mapper      *SaramaMapper
}

func NewSaramaProducer(props *properties.Client, mapper *SaramaMapper) (core.AsyncProducer, error) {
	config := sarama.NewConfig()
	if props.Producer.ClientId != "" {
		config.ClientID = props.Producer.ClientId
	}
	config.Version = sarama.V1_1_0_0
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Flush.Messages = props.Producer.FlushMessages
	config.Producer.Flush.Frequency = props.Producer.FlushFrequency
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	if props.Producer.SecurityProtocol == core.SecurityProtocolTls {
		if props.Producer.Tls == nil {
			return nil, errors.New("Tls config not found when using SecurityProtocol=TLS")
		}
		tlsConfig, err := utils.NewTLSConfig(
			props.Producer.Tls.CertFileLocation,
			props.Producer.Tls.KeyFileLocation,
			props.Producer.Tls.CaFileLocation,
		)
		if err != nil {
			return nil, errors.WithMessage(err, "Error when load TLS config")
		}
		tlsConfig.InsecureSkipVerify = props.Producer.Tls.InsecureSkipVerify
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	asyncProducer, err := sarama.NewAsyncProducer(props.Producer.BootstrapServers, config)
	if err != nil {
		return nil, errors.WithMessage(err, "Error when create new AsyncProducer")
	}
	p := &SaramaProducer{
		producer:    asyncProducer,
		errorsCh:    make(chan *core.ProducerError),
		successesCh: make(chan *core.Message),
		mapper:      mapper,
	}
	go func() {
		for e := range asyncProducer.Successes() {
			p.successesCh <- p.toCoreMessage(e)
		}
	}()
	go func() {
		for e := range asyncProducer.Errors() {
			p.errorsCh <- &core.ProducerError{
				Msg: p.toCoreMessage(e.Msg),
				Err: e.Err,
			}
		}
	}()
	return p, nil
}

func (p *SaramaProducer) Send(m *core.Message) {
	msg := &sarama.ProducerMessage{
		Topic:    m.Topic,
		Value:    sarama.ByteEncoder(m.Value),
		Headers:  p.mapper.ToSaramaHeaders(m.Headers),
		Metadata: m.Metadata,
	}
	if m.Key != nil {
		msg.Key = sarama.ByteEncoder(m.Key)
	}
	p.producer.Input() <- msg
}

func (p *SaramaProducer) Successes() <-chan *core.Message {
	return p.successesCh
}

func (p *SaramaProducer) Errors() <-chan *core.ProducerError {
	return p.errorsCh
}

func (p *SaramaProducer) Close() {
	p.producer.AsyncClose()
}

func (p SaramaProducer) toCoreMessage(msg *sarama.ProducerMessage) *core.Message {
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
		Headers:  p.mapper.ToCoreHeaders(msg.Headers),
		Metadata: msg.Metadata,
	}
}
