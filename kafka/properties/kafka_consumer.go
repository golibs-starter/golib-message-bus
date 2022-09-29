package properties

import "gitlab.com/golibs-starter/golib/config"

func NewKafkaConsumer(loader config.Loader) (*KafkaConsumer, error) {
	props := KafkaConsumer{}
	err := loader.Bind(&props)
	return &props, err
}

type KafkaConsumer struct {
	HandlerMappings map[string]TopicConsumer
}

func (c KafkaConsumer) Prefix() string {
	return "app.kafka.consumer"
}

type TopicConsumer struct {
	// Enable or disable this consumer
	Enable bool

	// Topic field is high priority than Topics.
	// When Topic is provided, Topics will be ignored.
	Topic string

	// Topics field is lower priority than Topic.
	// When Topic is provided, Topics will be ignored.
	Topics []string

	// GroupId of consumer
	GroupId string

	// TODO implement it
	Concurrency int
}
