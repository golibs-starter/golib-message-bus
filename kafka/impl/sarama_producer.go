package impl

import (
	"fmt"
	"github.com/Shopify/sarama"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/properties"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/utils"
	"sync"
)

type SaramaProducer struct {
	producer sarama.AsyncProducer
	errorsCh chan *core.ProducerError
}

func NewSaramaProducer(props *properties.Client) (core.AsyncProducer, error) {
	config := sarama.NewConfig()
	if props.Producer.ClientId != "" {
		config.ClientID = props.Producer.ClientId
	}
	config.Version = sarama.V1_1_0_0
	config.Producer.Flush.Messages = props.Producer.FlushMessages
	config.Producer.Flush.Frequency = props.Producer.FlushFrequency
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

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
	return &SaramaProducer{
		producer: asyncProducer,
		errorsCh: make(chan *core.ProducerError),
	}, nil
}

func (p *SaramaProducer) Send(m *core.Message) {
	msg := &sarama.ProducerMessage{
		Topic:   m.Topic,
		Value:   sarama.ByteEncoder(m.Value),
		Headers: p.toSaramaHeaders(m.Headers),
	}
	if m.Key != nil {
		msg.Key = sarama.ByteEncoder(m.Key)
	}
	p.producer.Input() <- msg
}

func (p *SaramaProducer) Errors() <-chan *core.ProducerError {
	err := <-p.producer.Errors()
	p.errorsCh <- &core.ProducerError{
		Msg: p.toCoreMessage(err.Msg),
		Err: err.Err,
	}
	return p.errorsCh
}

func (p *SaramaProducer) Close() {
	var wg sync.WaitGroup
	p.producer.AsyncClose()

	wg.Add(2)
	go func() {
		for range p.producer.Successes() {
			fmt.Println("Unexpected message on Successes()")
		}
		wg.Done()
	}()
	go func() {
		for msg := range p.producer.Errors() {
			fmt.Println(msg.Err)
		}
		wg.Done()
	}()
	wg.Wait()
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
