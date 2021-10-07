package properties

import (
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
	"gitlab.id.vin/vincart/golib/config"
)

func NewTopicAdmin(loader config.Loader) (*TopicAdmin, error) {
	props := TopicAdmin{}
	err := loader.Bind(&props)
	return &props, err
}

type TopicAdmin struct {
	Topics []core.TopicConfiguration
}

func (h TopicAdmin) Prefix() string {
	return "vinid.kafka"
}
