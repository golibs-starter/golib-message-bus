package core

import (
	"time"
)

type Admin interface {

	// CreateTopics create multiple topics at once with custom configurations.
	// Returns error if any error occurred
	CreateTopics(topics map[string]TopicConfiguration) error
}

type TopicConfiguration struct {
	Partitions    int32 `default:"1"`
	ReplicaFactor int16 `default:"1"`
	Retention     time.Duration
}
