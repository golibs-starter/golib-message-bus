package handler

import (
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib-message-bus/kafka/properties"
	"github.com/pkg/errors"
)

func CreateKafkaTopicHandler(admin core.Admin, props *properties.TopicAdmin) error {
	err := admin.CreateTopics(props.Topics)
	if err != nil {
		return errors.WithMessage(err, "create topics failed")
	}
	return nil
}
