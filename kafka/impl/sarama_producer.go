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
}

func NewSaramaProducer(props properties.Client) (core.Producer, error) {
	config := sarama.NewConfig()
	if props.Producer.ClientId != "" {
		config.ClientID = props.Producer.ClientId
	}
	config.Version = sarama.V1_1_0_0
	config.Producer.Flush.Messages = props.Producer.FlushMessages
	config.Producer.Flush.Frequency = props.Producer.FlushFrequency
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
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
	return &SaramaProducer{producer: asyncProducer}, nil
}

func (p *SaramaProducer) Send(m *core.Message) {
	msg := &sarama.ProducerMessage{
		Topic:   m.Topic,
		Value:   sarama.ByteEncoder(m.Value),
		Headers: p.castHeaders(m.Headers),
	}
	if m.Key != nil {
		msg.Key = sarama.ByteEncoder(m.Key)
	}
	p.producer.Input() <- msg
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

func (p SaramaProducer) castHeaders(headers []core.MessageHeader) []sarama.RecordHeader {
	saramaHeaders := make([]sarama.RecordHeader, 0)
	for _, header := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	return saramaHeaders
}
