package impl

import (
	"context"
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/utils"
	"strings"
)

type SaramaConsumers struct {
	props              *properties.Consumer
	kafkaConsumerProps *properties.KafkaConsumer
	mapper             *SaramaMapper
	consumers          map[string]*SaramaConsumer
}

func NewSaramaConsumers(
	props *properties.Client,
	kafkaConsumerProps *properties.KafkaConsumer,
	mapper *SaramaMapper,
	handlers []core.ConsumerHandler,
) (core.Consumer, error) {
	if len(kafkaConsumerProps.HandlerMappings) < 1 {
		return nil, errors.New("[SaramaConsumers] Missing handler config")
	}

	handlerMap := make(map[string]core.ConsumerHandler)
	for _, handler := range handlers {
		handlerMap[strings.ToLower(utils.GetStructName(handler))] = handler
	}

	kafkaConsumers := SaramaConsumers{
		props:              &props.Consumer,
		kafkaConsumerProps: kafkaConsumerProps,
		mapper:             mapper,
		consumers:          make(map[string]*SaramaConsumer),
	}

	if err := kafkaConsumers.init(handlerMap); err != nil {
		return nil, errors.WithMessage(err, "[SaramaConsumers] Error when create kafka consumers")
	}

	return &kafkaConsumers, nil
}

func (s *SaramaConsumers) init(handlerMap map[string]core.ConsumerHandler) error {
	for key, config := range s.kafkaConsumerProps.HandlerMappings {
		if !config.Enable {
			continue
		}
		handler, exists := handlerMap[strings.ToLower(key)]
		if !exists {
			continue
		}
		key = strings.TrimSpace(key)
		config.Topic = strings.TrimSpace(config.Topic)
		config.GroupId = strings.TrimSpace(config.GroupId)
		saramaConsumer, err := NewSaramaConsumer(s.props, s.mapper, &config, handler)
		if err != nil {
			return err
		}
		s.consumers[key] = saramaConsumer
	}
	return nil
}

func (s *SaramaConsumers) Start(ctx context.Context) {
	for _, consumer := range s.consumers {
		go consumer.Start(ctx)
	}
}

func (s *SaramaConsumers) Close() {
	for _, consumer := range s.consumers {
		go consumer.Close()
	}
}
