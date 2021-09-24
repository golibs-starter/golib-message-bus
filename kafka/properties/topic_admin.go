package properties

import (
	"gitlab.id.vin/vincart/golib/config"
	"time"
)

func NewTopicAdmin(loader config.Loader) (*TopicAdmin, error) {
	props := TopicAdmin{}
	err := loader.Bind(&props)
	return &props, err
}

type TopicAdmin struct {
	Topics map[string]TopicConfiguration
}

func (h TopicAdmin) Prefix() string {
	return "vinid.kafka"
}

type TopicConfiguration struct {
	Partitions    int32 `default:"1"`
	ReplicaFactor int16 `default:"1"`
	Retention     time.Duration
}
