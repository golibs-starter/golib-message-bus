package relayer

type EventOrderable interface {

	// OrderingKey specify key for relayed message.
	// This will ensure messages with the same key always go to the same partition in a topic.
	OrderingKey() string
}
