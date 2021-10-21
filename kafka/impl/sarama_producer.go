package impl

import (
	"github.com/Shopify/sarama"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/utils"
)

type SaramaProducer struct {
	producer    sarama.AsyncProducer
	errorsCh    chan *core.ProducerError
	successesCh chan *core.Message
}

func NewSaramaProducer(props *properties.Client) (core.AsyncProducer, error) {
	config := sarama.NewConfig()
	if props.Producer.ClientId != "" {
		config.ClientID = props.Producer.ClientId
	}
	config.Version = sarama.V1_1_0_0
	config.Producer.Flush.Messages = props.Producer.FlushMessages
	config.Producer.Flush.Frequency = props.Producer.FlushFrequency
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	//config.Producer.MaxMessageBytes = 1

	if props.Producer.SecurityProtocol == core.SecurityProtocolTls {
		tlsConfig, err := utils.NewTLSConfig(
			props.Producer.Tls.CertFileLocation,
			props.Producer.Tls.KeyFileLocation,
			props.Producer.Tls.CaFileLocation,
		)
		if err != nil {
			return nil, err
		}
		tlsConfig.InsecureSkipVerify = props.Producer.Tls.InsecureSkipVerify
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	asyncProducer, err := sarama.NewAsyncProducer(props.Producer.BootstrapServers, config)
	if err != nil {
		return nil, err
	}
	p := &SaramaProducer{
		producer:    asyncProducer,
		errorsCh:    make(chan *core.ProducerError),
		successesCh: make(chan *core.Message),
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
		Headers:  p.toSaramaHeaders(m.Headers),
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
		Headers:  p.toCoreHeaders(msg.Headers),
		Metadata: msg.Metadata,
	}
}

func (p SaramaProducer) toSaramaHeaders(headers []core.MessageHeader) []sarama.RecordHeader {
	saramaHeaders := make([]sarama.RecordHeader, 0)
	for _, header := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	return saramaHeaders
}

func (p SaramaProducer) toCoreHeaders(headers []sarama.RecordHeader) []core.MessageHeader {
	coreHeaders := make([]core.MessageHeader, 0)
	for _, header := range headers {
		coreHeaders = append(coreHeaders, core.MessageHeader{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	return coreHeaders
}
