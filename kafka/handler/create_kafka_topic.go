package handler

import (
	"github.com/pkg/errors"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/properties"
)

func CreateKafkaTopicHandler(admin core.Admin, props *properties.TopicAdmin) error {
	err := admin.CreateTopics(props.Topics)
	if err != nil {
		return errors.WithMessage(err, "create topics failed")
	}
	return nil
}
