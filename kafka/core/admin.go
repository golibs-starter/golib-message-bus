package core

import (
	"time"
)

type Admin interface {

	// CreateTopics create multiple topics at once with custom configurations.
	// Returns error if any error occurred
	CreateTopics(configurations []TopicConfiguration) error

	// DeleteTopics delete multiple topics at once.
	// Returns error if any error occurred
	DeleteTopics(topics []string) error

	// DeleteGroups delete multiple groups at once.
	// Returns error if any error occurred
	DeleteGroups(groupIds []string) error
}

type TopicConfiguration struct {
	Name          string
	Partitions    int32 `default:"1"`
	ReplicaFactor int16 `default:"1"`
	Retention     time.Duration
}
