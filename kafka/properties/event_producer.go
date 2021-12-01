package properties

import "gitlab.com/golibs-starter/golib/config"

func NewEventProducer(loader config.Loader) (*EventProducer, error) {
	props := EventProducer{}
	err := loader.Bind(&props)
	return &props, err
}

type EventProducer struct {
    EventMappings map[string]EventTopic
}

func (p EventProducer) Prefix() string {
	return "app.kafka.producer"
}

type EventTopic struct {
	TopicName     string
	Transactional bool `default:"true"`
	Disable       bool
}
