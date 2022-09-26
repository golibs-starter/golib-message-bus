package impl

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	coreUtils "gitlab.com/golibs-starter/golib/utils"
	"strings"
	"sync"
)

type SaramaConsumers struct {
	client             sarama.Client
	props              *properties.Consumer
	kafkaConsumerProps *properties.KafkaConsumer
	mapper             *SaramaMapper
	consumers          map[string]*SaramaConsumer
	unready            chan bool
}

func NewSaramaConsumers(
	client sarama.Client,
	globalProps *properties.Client,
	consumerProps *properties.KafkaConsumer,
	mapper *SaramaMapper,
	handlers []core.ConsumerHandler,
) (*SaramaConsumers, error) {
	if len(consumerProps.HandlerMappings) < 1 {
		return nil, errors.New("[SaramaConsumers] Missing handler mapping")
	}

	handlerMap := make(map[string]core.ConsumerHandler)
	for _, handler := range handlers {
		handlerMap[strings.ToLower(coreUtils.GetStructShortName(handler))] = handler
	}

	kafkaConsumers := SaramaConsumers{
		client:             client,
		props:              &globalProps.Consumer,
		kafkaConsumerProps: consumerProps,
		mapper:             mapper,
		consumers:          make(map[string]*SaramaConsumer),
		unready:            make(chan bool),
	}

	if err := kafkaConsumers.init(handlerMap); err != nil {
		return nil, errors.WithMessage(err, "[SaramaConsumers] Error when init kafka consumers")
	}

	return &kafkaConsumers, nil
}

func (s *SaramaConsumers) init(handlerMap map[string]core.ConsumerHandler) error {
	for key, config := range s.kafkaConsumerProps.HandlerMappings {
		if !config.Enable {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		handler, exists := handlerMap[key]
		if !exists {
			continue
		}
		config.Topic = strings.TrimSpace(config.Topic)
		config.GroupId = strings.TrimSpace(config.GroupId)
		saramaConsumer, err := NewSaramaConsumer(s.client, s.mapper, &config, handler)
		if err != nil {
			return err
		}
		s.consumers[key] = saramaConsumer
	}
	return nil
}

func (s *SaramaConsumers) Start(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(len(s.consumers))
	for _, consumer := range s.consumers {
		go func(consumer *SaramaConsumer) {
			defer wg.Done()
			<-consumer.WaitForReady()
		}(consumer)
		go func(consumer *SaramaConsumer) {
			consumer.Start(ctx)
		}(consumer)
	}
	wg.Wait()
	close(s.unready)
}

func (s SaramaConsumers) WaitForReady() chan bool {
	return s.unready
}

func (s *SaramaConsumers) Stop() {
	var wg sync.WaitGroup
	wg.Add(len(s.consumers))
	for _, consumer := range s.consumers {
		go func(consumer *SaramaConsumer) {
			defer wg.Done()
			consumer.Stop()
		}(consumer)
	}
	wg.Wait()
}
