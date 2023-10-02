package properties

import (
	"github.com/golibs-starter/golib-message-bus/kafka/core"
	"github.com/golibs-starter/golib/config"
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
	return "app.kafka.admin"
}
