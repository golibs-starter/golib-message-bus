package properties

import "gitlab.com/golibs-starter/golib/config"

func NewKafkaConsumer(loader config.Loader) (*KafkaConsumer, error) {
	props := KafkaConsumer{}
	err := loader.Bind(&props)
	return &props, err
}

type KafkaConsumer struct {
	Topics map[string]TopicConsumer
}

func (c KafkaConsumer) Prefix() string {
	return "vinid.kafka.consumer"
}

type TopicConsumer struct {
	Topic       string
	GroupId     string
	Enable      bool
	Concurrency int
}
