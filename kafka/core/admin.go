package core

import "gitlab.id.vin/vincart/golib-message-bus/kafka/properties"

type Admin interface {

	// CreateTopics create multiple topics at once with custom configurations.
	// Returns error if any error occurred
	CreateTopics(topics map[string]properties.TopicConfiguration) error
}
